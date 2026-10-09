package im

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Q3 §5 message search (IM2-D2). S1/S3/S4/S5/S7 are the design's acceptance ids; these are
// their backend equivalents on the SQLite (LIKE) path. search_mysql_test.go runs the same
// cases on MySQL's ngram FULLTEXT path.

func searchRouter(t *testing.T, p *Plugin) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		uid, _ := strconv.ParseUint(c.GetHeader("X-Test-User"), 10, 64)
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), userIDKey, uint(uid)))
		c.Next()
	})
	api := humagin.New(router, huma.DefaultConfig("search test", "1"))
	p.registerReads(api)
	p.registerSearch(api)
	return router
}

func getRequest(t *testing.T, router http.Handler, user uint, v url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/im/search?"+v.Encode(), nil)
	req.Header.Set("X-Test-User", strconv.FormatUint(uint64(user), 10))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func search(t *testing.T, router http.Handler, user uint, q, typ, cursor string) (int, searchPage) {
	t.Helper()
	v := url.Values{"q": {q}}
	if typ != "" {
		v.Set("type", typ)
	}
	if cursor != "" {
		v.Set("cursor", cursor)
	}
	rec := getRequest(t, router, user, v)
	var page searchPage
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body.String())
		}
	}
	return rec.Code, page
}

func hitTexts(page searchPage) []string {
	out := make([]string, len(page.Messages))
	for i, m := range page.Messages {
		out[i] = m.Snippet
	}
	return out
}

func filePayload(name string) []byte {
	return []byte(fmt.Sprintf(`{"type":8,"name":%q,"url":"https://files.example/x","size":10}`, name))
}

// S1: substring, case-insensitive, middle fragment; the hit carries what messagesync needs to
// locate it (message_seq = the history cursor) and an escaped <mark> highlight.
func TestSearchS1SubstringAndHighlight(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	mustOK(t, post(webhookText("s1", 2, "2@1", personChannel, "验收Q3蓝鲸ABC"), webhookText("s2", 2, "2@1", personChannel, "无关")))
	for _, q := range []string{"蓝鲸", "abc", "ABC", "鲸A", "q3蓝"} {
		code, page := search(t, router, 1, q, "", "")
		if code != 200 || len(page.Messages) != 1 {
			t.Fatalf("%s: status=%d hits=%v", q, code, hitTexts(page))
		}
		h := page.Messages[0]
		if h.ChannelID != "2" || h.ChannelType != personChannel || h.FromUID != "2" || h.MessageIDStr != "s1" || h.MessageSeq == 0 || h.Snippet != "验收Q3蓝鲸ABC" {
			t.Fatalf("%s: hit=%+v", q, h)
		}
		mark := strings.Index(h.SnippetHTML, "<mark>")
		end := strings.Index(h.SnippetHTML, "</mark>")
		if mark < 0 || end < mark || !strings.EqualFold(h.SnippetHTML[mark+6:end], q) || len(h.Ranges) != 1 {
			t.Fatalf("%s: html=%q ranges=%v", q, h.SnippetHTML, h.Ranges)
		}
		if got := string([]rune(h.Snippet)[h.Ranges[0][0]:h.Ranges[0][1]]); !strings.EqualFold(got, q) {
			t.Fatalf("%s: range covers %q", q, got)
		}
	}
	// The hit opens through the existing history page (start = message_seq).
	_, page := search(t, router, 1, "蓝鲸", "", "")
	code, history := postAs[messageSyncPage](t, router, "/api/v1/im/channel/messagesync", 1, map[string]any{
		"channel_id": "2", "channel_type": personChannel, "start_message_seq": page.Messages[0].MessageSeq, "pull_mode": 1, "limit": 1,
	})
	if code != 200 || len(history.Messages) != 1 || history.Messages[0].MessageIDStr != "s1" {
		t.Fatalf("locate: %d %+v", code, history.Messages)
	}
}

// S5: no match is an empty 200; a single character or an over-long query is a 400 with a
// code the UI maps to its rule text (ngram tokens are 2 characters: 1 character never matches).
func TestSearchS5EmptyAndLengthRules(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	mustOK(t, post(webhookText("e1", 2, "2@1", personChannel, "碧波荡漾")))
	if code, page := search(t, router, 1, "zzqq", "", ""); code != 200 || len(page.Messages) != 0 || page.Total != 0 || page.NextCursor != "" {
		t.Fatalf("zzqq: %d %+v", code, page)
	}
	for q, want := range map[string]string{"碧": "query_too_short", " 碧 ": "query_too_short", `"`: "query_too_short", strings.Repeat("长", 65): "query_too_long"} {
		rec := getRequest(t, router, 1, url.Values{"q": {q}})
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("%q: %d %s", q, rec.Code, rec.Body.String())
		}
	}
	if code, page := search(t, router, 1, strings.Repeat("长", 64), "", ""); code != 200 || len(page.Messages) != 0 {
		t.Fatalf("64 chars: %d", code)
	}
}

// S4: only the caller's own conversations; a group needs current membership (a removed
// member keeps a stale conversation row but must not see the group's messages).
func TestSearchS4OwnConversationsOnly(t *testing.T) {
	db, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	if err := insertGroupMembers(db, "g9", []uint{3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, post(webhookText("p1", 3, "3@4", personChannel, "暗号私聊XQ7"), webhookText("p2", 4, "g9", groupChannel, "暗号群聊XQ7")))
	if code, page := search(t, router, 1, "暗号", "", ""); code != 200 || len(page.Messages) != 0 {
		t.Fatalf("outsider sees %v", hitTexts(page))
	}
	if _, page := search(t, router, 3, "暗号", "", ""); len(page.Messages) != 2 {
		t.Fatalf("owner hits=%v", hitTexts(page))
	}
	if _, page := search(t, router, 5, "xq7", "", ""); fmt.Sprint(hitTexts(page)) != "[暗号群聊XQ7]" {
		t.Fatalf("member 5 hits=%v", hitTexts(page))
	}
	if err := db.Where("group_id = ? AND uid = ?", "g9", 5).Delete(&IMGroupMember{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, page := search(t, router, 5, "暗号", "", ""); len(page.Messages) != 0 {
		t.Fatalf("removed member still sees %v", hitTexts(page))
	}
}

// S7: file messages are found by file name under type=file and only there.
func TestSearchS7FileNames(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	file := webhookText("f1", 2, "2@1", personChannel, "")
	file.RawPayload = filePayload("Q3季度报告-final.PDF")
	mustOK(t, post(file, webhookText("f2", 2, "2@1", personChannel, "季度报告我晚点发")))
	if _, page := search(t, router, 1, "季度报告", "file", ""); len(page.Messages) != 1 || page.Messages[0].MessageIDStr != "f1" ||
		page.Messages[0].PayloadType != filePayloadType || page.Messages[0].Snippet != "Q3季度报告-final.PDF" {
		t.Fatalf("file hits=%+v", page.Messages)
	}
	if _, page := search(t, router, 1, "final.pdf", "file", ""); len(page.Messages) != 1 {
		t.Fatalf("extension case=%v", hitTexts(page))
	}
	if _, page := search(t, router, 1, "季度报告", "message", ""); fmt.Sprint(hitTexts(page)) != "[季度报告我晚点发]" {
		t.Fatalf("message hits=%v", hitTexts(page))
	}
}

// Markup in a message or in the query is text: escaped everywhere, only <mark> is markup.
func TestSearchHighlightEscapesHTML(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	mustOK(t, post(webhookText("x1", 2, "2@1", personChannel, `<img src=x onerror=alert(1)>蓝鲸&<b>"x"</b>`)))
	for q, want := range map[string]string{
		"蓝鲸":             `&lt;img src=x onerror=alert(1)&gt;<mark>蓝鲸</mark>&amp;&lt;b&gt;&#34;x&#34;&lt;/b&gt;`,
		"<img src":       `<mark>&lt;img src</mark>=x onerror=alert(1)&gt;蓝鲸&amp;&lt;b&gt;&#34;x&#34;&lt;…`,
		"&<b>":           `&lt;img src=x onerror=alert(1)&gt;蓝鲸<mark>&amp;&lt;b&gt;</mark>&#34;x&#34;&lt;/b&gt;`,
		"onerror=alert(": `&lt;img src=x <mark>onerror=alert(</mark>1)&gt;蓝鲸&amp;&lt;b&gt;&#34;x&#34;&lt;/b&gt;`,
	} {
		_, page := search(t, router, 1, q, "", "")
		if len(page.Messages) != 1 || page.Messages[0].SnippetHTML != want {
			t.Fatalf("%s: %+v", q, page.Messages)
		}
	}
}

// Snippets are 30 characters either side of the first hit, with ellipses; every hit inside is
// marked; ranges are rune offsets into the snippet.
func TestSearchSnippetWindow(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	text := strings.Repeat("前", 50) + "蓝鲸" + strings.Repeat("中", 10) + "蓝鲸" + strings.Repeat("后", 50)
	mustOK(t, post(webhookText("w1", 2, "2@1", personChannel, text)))
	_, page := search(t, router, 1, "蓝鲸", "", "")
	h := page.Messages[0]
	want := "…" + strings.Repeat("前", 30) + "蓝鲸" + strings.Repeat("中", 10) + "蓝鲸" + strings.Repeat("后", 18) + "…"
	if h.Snippet != want || fmt.Sprint(h.Ranges) != "[[31 33] [43 45]]" || strings.Count(h.SnippetHTML, "<mark>") != 2 {
		t.Fatalf("snippet=%q ranges=%v", h.Snippet, h.Ranges)
	}
}

// Newest first, 20 per page, keyset cursor with no gap or duplicate; the first page carries a
// total capped at 100; hidden conversations are included and flagged.
func TestSearchPagingTotalAndHidden(t *testing.T) {
	db, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	var batch []webhookMessage
	for i := 0; i < 45; i++ {
		peer := uint(10 + i%3)
		batch = append(batch, webhookText(fmt.Sprintf("pg%02d", i), peer, fmt.Sprintf("%d@1", peer), personChannel, fmt.Sprintf("周报%02d", i)))
	}
	mustOK(t, post(batch...))
	if err := db.Model(&IMConversation{}).Where("uid = 1 AND channel_key = ?", "1@11").Update("hidden_at", gorm.Expr("CURRENT_TIMESTAMP")).Error; err != nil {
		t.Fatal(err)
	}
	var seen []string
	var sizes []int
	cursor := ""
	for {
		code, page := search(t, router, 1, "周报", "", cursor)
		if code != 200 {
			t.Fatalf("status=%d", code)
		}
		if cursor == "" && page.Total != 45 {
			t.Fatalf("total=%d", page.Total)
		}
		sizes = append(sizes, len(page.Messages))
		for _, m := range page.Messages {
			seen = append(seen, m.MessageIDStr)
			if m.Hidden != (m.ChannelID == "11") {
				t.Fatalf("hidden flag %+v", m)
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if fmt.Sprint(sizes) != "[20 20 5]" || seen[0] != "pg44" || seen[44] != "pg00" {
		t.Fatalf("sizes=%v first=%s last=%s", sizes, seen[0], seen[len(seen)-1])
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] >= seen[i-1] {
			t.Fatalf("order/dup at %d: %v", i, seen)
		}
	}
	if code, _ := search(t, router, 1, "周报", "", "x"); code != 400 {
		t.Fatalf("bad cursor=%d", code)
	}
	// The total is capped.
	batch = batch[:0]
	for i := 0; i < 110; i++ {
		batch = append(batch, webhookText(fmt.Sprintf("cap%03d", i), 2, "2@1", personChannel, "月报"))
	}
	mustOK(t, post(batch...))
	if _, page := search(t, router, 1, "月报", "", ""); page.Total != searchTotalCap {
		t.Fatalf("capped total=%d", page.Total)
	}
}

// S3 (backend part): the per-tab counts. total is per type and capped; group titles, contacts
// and agents are already on the client (im/groups, auth directory) and are filtered there.
func TestSearchS3CountsPerType(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	var batch []webhookMessage
	for i := 0; i < 3; i++ {
		f := webhookText(fmt.Sprintf("cf%d", i), 2, "2@1", personChannel, "")
		f.RawPayload = filePayload(fmt.Sprintf("蓝鲸方案%d.docx", i))
		batch = append(batch, f, webhookText(fmt.Sprintf("cm%d", i), 2, "2@1", personChannel, "蓝鲸方案讨论"))
	}
	batch = append(batch, webhookText("cm9", 2, "2@1", personChannel, "蓝鲸"))
	mustOK(t, post(batch...))
	_, messages := search(t, router, 1, "蓝鲸", "message", "")
	_, files := search(t, router, 1, "蓝鲸方案", "file", "")
	if messages.Total != 4 || files.Total != 3 || len(files.Messages) != 3 {
		t.Fatalf("message total=%d file total=%d", messages.Total, files.Total)
	}
	if code, _ := search(t, router, 1, "蓝鲸", "group", ""); code != 422 && code != 400 {
		t.Fatalf("unknown type=%d", code)
	}
}

// Rows saved before search_text existed (NULL) are filled in id order, a batch at a time; an
// interrupted run resumes; a second run changes nothing; filled rows become searchable.
func TestSearchTextBackfillResumable(t *testing.T) {
	db, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	for i := 0; i < 5; i++ {
		mustOK(t, post(webhookText(fmt.Sprintf("b%d", i), 2, "2@1", personChannel, fmt.Sprintf("旧消息%d", i))))
	}
	file := webhookText("b5", 2, "2@1", personChannel, "")
	file.RawPayload = filePayload("旧文件.txt")
	raw := webhookText("b6", 2, "2@1", personChannel, "")
	raw.RawPayload = []byte("not json")
	mustOK(t, post(file, raw))
	if err := db.Model(&IMMessage{}).Where("1 = 1").Update("search_text", nil).Error; err != nil {
		t.Fatal(err)
	}
	if _, page := search(t, router, 1, "旧消息", "", ""); len(page.Messages) != 0 {
		t.Fatal("NULL rows searchable before backfill")
	}
	previous := searchBackfillBatch
	searchBackfillBatch = 2
	t.Cleanup(func() { searchBackfillBatch = previous })
	n, err := backfillSearchText(t.Context(), db, 2) // stopped after 2 batches
	if err != nil || n != 4 {
		t.Fatalf("first run n=%d err=%v", n, err)
	}
	var pending int64
	db.Model(&IMMessage{}).Where("search_text IS NULL").Count(&pending)
	if pending != 3 {
		t.Fatalf("pending=%d", pending)
	}
	if n, err := backfillSearchText(t.Context(), db, 0); err != nil || n != 3 {
		t.Fatalf("resume n=%d err=%v", n, err)
	}
	if n, _ := backfillSearchText(t.Context(), db, 0); n != 0 {
		t.Fatalf("second pass n=%d", n)
	}
	if _, page := search(t, router, 1, "旧消息", "", ""); len(page.Messages) != 5 {
		t.Fatalf("after backfill=%v", hitTexts(page))
	}
	if _, page := search(t, router, 1, "旧文件", "file", ""); len(page.Messages) != 1 {
		t.Fatal("file name not backfilled")
	}
	var empty IMMessage
	if err := db.Where("message_idstr = ?", "b6").First(&empty).Error; err != nil || empty.SearchText == nil || *empty.SearchText != "" {
		t.Fatalf("non-JSON row=%+v err=%v", empty.SearchText, err)
	}
}

// Both statement paths: FULLTEXT (+ an exact LIKE recheck) when the index is in place (MySQL),
// LIKE alone otherwise (SQLite, MySQL before the index exists, or a query with no 2-character
// word). SQL rendered by the SQLite dialect (its quoting); search_mysql_test.go executes the
// FULLTEXT one on MySQL.
func TestSearchStatementPaths(t *testing.T) {
	db := m2DB(t)
	build := func(fullText bool) string {
		return db.ToSQL(func(tx *gorm.DB) *gorm.DB {
			var rows []searchRow
			return searchMessages(tx, 7, "蓝鲸", false, 99, fullText).Limit(21).Scan(&rows)
		})
	}
	ft, like := build(true), build(false)
	for _, c := range []struct {
		sql  string
		want []string
		not  string
	}{
		{ft, []string{`MATCH(m.search_text) AGAINST("+""蓝鲸""" IN BOOLEAN MODE)`, `m.search_text LIKE "%蓝鲸%" ESCAPE '!'`, "c.uid = 7", "g.uid = 7", "m.id < 99", "ORDER BY m.id DESC"}, "NOPE"},
		{like, []string{`m.search_text LIKE "%蓝鲸%" ESCAPE '!'`, "c.uid = 7", "g.uid = 7", "m.id < 99", "ORDER BY m.id DESC"}, "MATCH"},
	} {
		for _, w := range c.want {
			if !strings.Contains(c.sql, w) {
				t.Errorf("missing %q in %s", w, c.sql)
			}
		}
		if strings.Contains(c.sql, c.not) {
			t.Errorf("unexpected %s in %s", c.not, c.sql)
		}
	}
	if got := likeEscape(`50%_a!b`); got != `50!%!_a!!b` {
		t.Fatalf("likeEscape=%s", got)
	}
	for q, want := range map[string]string{"蓝鲸": `+"蓝鲸"`, "<img src": `+"img" +"src"`, "鲸A x": `+"鲸A"`, "&<b>": "", "a b": "", `say "hi"`: `+"say" +"hi"`} {
		if got := booleanQuery(q); got != want {
			t.Errorf("booleanQuery(%q)=%q want %q", q, got, want)
		}
	}
}

// LIKE wildcards in a query are literal.
func TestSearchLikeWildcardsLiteral(t *testing.T) {
	_, _, post := persistHarness(t)
	router := searchRouter(t, &Plugin{})
	mustOK(t, post(webhookText("l1", 2, "2@1", personChannel, "完成50%了"), webhookText("l2", 2, "2@1", personChannel, "完成500个")))
	if _, page := search(t, router, 1, "50%", "", ""); fmt.Sprint(hitTexts(page)) != "[完成50%了]" {
		t.Fatalf("hits=%v", hitTexts(page))
	}
	if _, page := search(t, router, 1, "_0", "", ""); len(page.Messages) != 0 {
		t.Fatalf("underscore matched as wildcard: %v", hitTexts(page))
	}
}
