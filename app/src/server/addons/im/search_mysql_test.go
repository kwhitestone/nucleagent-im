package im

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// The FULLTEXT path on a real MySQL 8 (ngram). Opt-in: IM_TEST_MYSQL_DSN=user:pass@tcp(host:port)/
// (no database; each test gets its own schema, dropped after). Integration tier — never in the
// fast suite. IM_TEST_MYSQL_ROWS=1000000 adds the S6 latency run.

func mysqlSearchDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("IM_TEST_MYSQL_DSN not set (MySQL integration)")
	}
	schema := "im_t_" + strings.ToLower(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name()))
	raw, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("DROP DATABASE IF EXISTS " + schema); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("CREATE DATABASE " + schema + " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(mysql.Open(dsn+schema+"?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&IMGroupMember{}, &IMMessage{}, &IMConversation{}); err != nil {
		t.Fatal(err)
	}
	previous := global.PRISM_DB
	global.PRISM_DB = db
	t.Cleanup(func() {
		global.PRISM_DB = previous
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		_, _ = raw.Exec("DROP DATABASE IF EXISTS " + schema)
		_ = raw.Close()
	})
	return db
}

// seedSearch saves messages through persistMessage (search_text and the fan-out included).
func seedSearch(t *testing.T, db *gorm.DB, msgs ...webhookMessage) {
	t.Helper()
	for i := range msgs {
		if _, err := persistMessage(db, msgs[i]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSearchMySQLFullText(t *testing.T) {
	db := mysqlSearchDB(t)
	p := &Plugin{}
	ok, err := ensureSearchIndex(t.Context(), db)
	if err != nil || !ok {
		t.Fatalf("index ok=%v err=%v", ok, err)
	}
	if ok, err := ensureSearchIndex(t.Context(), db); err != nil || !ok {
		t.Fatalf("second ensure ok=%v err=%v", ok, err) // idempotent (every replica runs it)
	}
	p.searchFullText.Store(true)
	router := searchRouter(t, p)
	if err := insertGroupMembers(db, "g9", []uint{3, 4}); err != nil {
		t.Fatal(err)
	}
	file := webhookText("f1", 2, "2@1", personChannel, "")
	file.RawPayload = filePayload("Q3季度报告-final.PDF")
	seedSearch(t, db,
		webhookText("s1", 2, "2@1", personChannel, "验收Q3蓝鲸ABC"),
		webhookText("s2", 2, "2@1", personChannel, "a big cat"),
		webhookText("s3", 2, "2@1", personChannel, `<b>蓝鲸</b>&50%`),
		webhookText("p1", 3, "3@4", personChannel, "暗号私聊XQ7"),
		webhookText("p2", 4, "g9", groupChannel, "暗号群聊XQ7"),
		file,
	)
	hits := func(user uint, q, typ string) string {
		code, page := search(t, router, user, q, typ, "")
		if code != 200 {
			t.Fatalf("%q: status %d", q, code)
		}
		ids := make([]string, len(page.Messages))
		for i, m := range page.Messages {
			ids[i] = m.MessageIDStr
		}
		sort.Strings(ids)
		return strings.Join(ids, ",")
	}
	for _, c := range []struct {
		user      uint
		q, typ, w string
	}{
		{1, "蓝鲸", "", "s1,s3"}, {1, "abc", "", "s1"}, {1, "ABC", "", "s1"}, {1, "鲸A", "", "s1"}, {1, "q3蓝", "", "s1"},
		{1, "big", "", "s2"}, // stopword-containing tokens: the index was built with stopwords off
		{1, "zzqq", "", ""}, {1, "蓝鲸X", "", ""}, {1, "50%", "", "s3"}, {1, "&50", "", "s3"}, {1, "<b>", "", "s3"},
		{1, "暗号", "", ""}, {3, "暗号", "", "p1,p2"}, {4, "xq7", "", "p1,p2"}, // S4
		{1, "季度报告", "file", "f1"}, {1, "final.pdf", "file", "f1"}, {1, "季度报告", "", ""}, // S7
	} {
		if got := hits(c.user, c.q, c.typ); got != c.w {
			t.Errorf("uid %d %q %s: got [%s] want [%s]", c.user, c.q, c.typ, got, c.w)
		}
	}
	// A row inserted after the index exists is found too (the stopword setting is per index).
	seedSearch(t, db, webhookText("s9", 2, "2@1", personChannel, "新的蓝鲸Abc行 it is"))
	if got := hits(1, "鲸a", ""); got != "s1,s9" {
		t.Errorf("after insert: %s", got)
	}
	// The plan uses the FULLTEXT index.
	var plan []map[string]any
	if err := db.Raw("EXPLAIN " + db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		var rows []searchRow
		return searchMessages(tx, 1, "蓝鲸", false, 0, true).Limit(21).Scan(&rows)
	})).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(plan), searchIndexName) || !strings.Contains(fmt.Sprint(plan), "fulltext") {
		t.Fatalf("plan does not use the FULLTEXT index: %v", plan)
	}
}

// Not MySQL or not eligible: no index, LIKE answers, and boot says so.
func TestSearchIndexSkippedWhenTooLarge(t *testing.T) {
	db := mysqlSearchDB(t)
	previous := searchIndexMaxRows
	searchIndexMaxRows = -1
	t.Cleanup(func() { searchIndexMaxRows = previous })
	logs := captureLogs(t)
	if ok, err := ensureSearchIndex(t.Context(), db); ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if !strings.Contains(logs.String(), "FULLTEXT index not built") {
		t.Fatal("no log line")
	}
	var n int64
	db.Raw("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND index_name = ?", searchIndexName).Scan(&n)
	if n != 0 {
		t.Fatal("index built anyway")
	}
}

// S6: synthetic rows (IM_TEST_MYSQL_ROWS, e.g. 1000000) spread over 2000 channels; the viewer
// is in 50 of them. p95 of 40 searches must stay under 500 ms. Local/DEV only, never PROD.
func TestSearchMySQLLatency(t *testing.T) {
	total, _ := strconv.Atoi(os.Getenv("IM_TEST_MYSQL_ROWS"))
	if total == 0 {
		t.Skip("IM_TEST_MYSQL_ROWS not set")
	}
	db := mysqlSearchDB(t)
	start := time.Now()
	words := []string{"项目", "进度", "会议", "报告", "客户", "需求", "测试", "发布", "上线", "合同", "预算", "设计", "评审", "周报", "接口", "数据"}
	const channels, batch = 2000, 2000
	for i := 0; i < total; i += batch {
		var b strings.Builder
		b.WriteString("INSERT INTO im_messages (message_idstr, channel_key, channel_type, from_uid, payload_type, payload, wk_timestamp, search_text, created_at) VALUES ")
		for j := i; j < min(i+batch, total); j++ {
			if j > i {
				b.WriteByte(',')
			}
			text := fmt.Sprintf("%s%s%s 第%d条 %s", words[j%16], words[(j/16)%16], words[(j/256)%16], j, words[(j*7)%16])
			fmt.Fprintf(&b, "('m%d','g%d',2,%d,1,'{}',%d,'%s',NOW())", j, j%channels, 100+j%500, 1700000000+j, text)
		}
		if err := db.Exec(b.String()).Error; err != nil {
			t.Fatal(err)
		}
	}
	var members []IMGroupMember
	var convs []IMConversation
	for g := 0; g < 50; g++ {
		members = append(members, IMGroupMember{GroupID: fmt.Sprintf("g%d", g*40), UID: 1})
		convs = append(convs, IMConversation{UID: 1, ChannelType: groupChannel, ChannelKey: fmt.Sprintf("g%d", g*40), LastMessageID: 1})
	}
	if err := db.Create(&members).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&convs).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d rows in %s", total, time.Since(start).Round(time.Second))
	start = time.Now()
	previous := searchIndexMaxRows
	searchIndexMaxRows = int64(total) * 2
	t.Cleanup(func() { searchIndexMaxRows = previous })
	if ok, err := ensureSearchIndex(t.Context(), db); !ok || err != nil {
		t.Fatalf("index ok=%v err=%v", ok, err)
	}
	t.Logf("FULLTEXT build on %d rows: %s", total, time.Since(start).Round(time.Second))
	p := &Plugin{}
	p.searchFullText.Store(true)
	router := searchRouter(t, p)
	queries := []string{"项目进度", "会议", "报告客户", "第12345条", "需求测试", "发布", "合同预算", "zzqq", "设计评审", "周报"}
	var took []time.Duration
	for round := 0; round < 4; round++ {
		for _, q := range queries {
			s := time.Now()
			if code, _ := search(t, router, 1, q, "", ""); code != 200 {
				t.Fatalf("%s: %d", q, code)
			}
			took = append(took, time.Since(s))
		}
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	p95 := took[len(took)*95/100]
	var plan []map[string]any
	db.Raw("EXPLAIN " + db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		var rows []searchRow
		return searchMessages(tx, 1, "项目进度", false, 0, true).Limit(21).Scan(&rows)
	})).Scan(&plan)
	t.Logf("S6 rows=%d searches=%d p50=%s p95=%s max=%s plan=%v", total, len(took), took[len(took)/2], p95, took[len(took)-1], plan)
	if p95 > 500*time.Millisecond {
		t.Fatalf("p95 %s ≥ 500ms", p95)
	}
}
