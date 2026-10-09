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
	// Both plans: budget 0 = FULLTEXT statement only; default = primary-key scan first.
	previousBudget := searchScanBudgetMS
	for _, budget := range []int{0, previousBudget} {
		searchScanBudgetMS = budget
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
				t.Errorf("budget %d uid %d %q %s: got [%s] want [%s]", budget, c.user, c.q, c.typ, got, c.w)
			}
		}
		searchScanBudgetMS = previousBudget
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

// The scan runs out of its MAX_EXECUTION_TIME budget (MySQL 3024) and the FULLTEXT statement
// answers instead: same rows, no error.
func TestSearchMySQLScanTimeoutFallsBack(t *testing.T) {
	db := mysqlSearchDB(t)
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1) // the session setting below must apply to the INSERT
	if err := db.Exec("SET SESSION cte_max_recursion_depth = 200001").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO im_messages (message_idstr, channel_key, channel_type, from_uid, payload_type, payload, wk_timestamp, search_text, created_at)
		WITH RECURSIVE s AS (SELECT 0 n UNION ALL SELECT n + 1 FROM s WHERE n < 199999)
		SELECT CONCAT('bulk', n), 'g1', 2, 2, 1, '{}', n, CONCAT('普通消息', n), NOW() FROM s`).Error; err != nil {
		t.Fatal(err)
	}
	if err := insertGroupMembers(db, "g1", []uint{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&IMConversation{UID: 1, ChannelType: groupChannel, ChannelKey: "g1", LastMessageID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	// The one rare hit is the oldest row: the newest-first scan must read all 200k to find it.
	if err := db.Exec("UPDATE im_messages SET search_text = '罕见暗号蓝鲸' WHERE message_idstr = 'bulk0'").Error; err != nil {
		t.Fatal(err)
	}
	if ok, err := ensureSearchIndex(t.Context(), db); !ok || err != nil {
		t.Fatalf("index ok=%v err=%v", ok, err)
	}
	previous := searchScanBudgetMS
	searchScanBudgetMS = 1
	t.Cleanup(func() { searchScanBudgetMS = previous })
	scanErr := searchMessages(db, 1, "罕见暗号", false, 0, false).
		Select(fmt.Sprintf("/*+ MAX_EXECUTION_TIME(1) */ %s", searchColumns)).Limit(21).Scan(&[]searchRow{}).Error
	if scanErr == nil || !strings.Contains(scanErr.Error(), "3024") {
		t.Fatalf("the scan alone did not time out (%v): the fallback is not exercised", scanErr)
	}
	rows, err := runSearch(db, 1, "罕见暗号", false, 0, 21, true)
	if err != nil || len(rows) != 1 || rows[0].MessageIDStr != "bulk0" {
		t.Fatalf("rows=%v err=%v", rows, err)
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

// S6: the handler against an already-seeded 1M-row schema with the FULLTEXT index
// (IM_TEST_MYSQL_S6=<dsn with database>, e.g. the docs-sentence corpus from s6-real.py: 2000
// groups, viewer 1 in 50 of them, viewer 2 in 1000), queries from IM_TEST_MYSQL_S6_QUERIES (one
// per line). Both viewers × every query; p95 < 500 ms. Local/DEV only — never PROD.
func TestSearchMySQLLatency(t *testing.T) {
	dsn, file := os.Getenv("IM_TEST_MYSQL_S6"), os.Getenv("IM_TEST_MYSQL_S6_QUERIES")
	if dsn == "" || file == "" {
		t.Skip("IM_TEST_MYSQL_S6 / IM_TEST_MYSQL_S6_QUERIES not set")
	}
	db, err := gorm.Open(mysql.Open(dsn+"?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	previous := global.PRISM_DB
	global.PRISM_DB = db
	t.Cleanup(func() { global.PRISM_DB = previous })
	var rows int64
	db.Raw("SELECT COUNT(*) FROM im_messages").Scan(&rows)
	if ok, err := ensureSearchIndex(t.Context(), db); !ok || err != nil {
		t.Fatalf("index ok=%v err=%v", ok, err)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	queries := strings.Fields(string(raw))
	p := &Plugin{}
	p.searchFullText.Store(true)
	router := searchRouter(t, p)
	var took []time.Duration
	hits := 0
	for _, viewer := range []uint{1, 2} {
		for _, q := range queries {
			s := time.Now()
			code, page := search(t, router, viewer, q, "", "")
			took = append(took, time.Since(s))
			if code != 200 && code != 400 {
				t.Fatalf("%s: %d", q, code)
			}
			hits += len(page.Messages)
		}
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	p95 := took[len(took)*95/100]
	t.Logf("S6 rows=%d searches=%d hits=%d p50=%s p95=%s max=%s", rows, len(took), hits, took[len(took)/2], p95, took[len(took)-1])
	if p95 > 500*time.Millisecond {
		t.Fatalf("p95 %s ≥ 500ms", p95)
	}
	_ = strconv.Itoa
}
