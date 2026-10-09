package im

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/danielgtaylor/huma/v2"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
)

// Q3 §5 message search (IM2-D2). im_messages.search_text holds what is searchable (a text
// message's content, a file's name), filled on persist and by the backfill below. On MySQL
// a FULLTEXT ngram index (2-character tokens) narrows the candidates and an exact LIKE
// rechecks them; without the index (SQLite tests, or a MySQL where boot did not build it)
// LIKE alone answers. Either way only the caller's own conversations are searched.
//
// Query rules: 2–64 characters after trimming. One character is refused: an ngram token is
// two characters, so a single character never matches (the UI filters conversation titles
// locally instead). Matching is a case-insensitive substring of the whole query.

const (
	filePayloadType  = 8 // WuKong's file content type: {"type":8,"name":…}
	searchPageSize   = 20
	searchTotalCap   = 100
	searchQueryMin   = 2
	searchQueryMax   = 64
	searchSnippetPad = 30 // characters kept on each side of the first hit
	searchTextMax    = 10000
	searchIndexName  = "ft_im_msg_search"
)

var (
	// ponytail: boot builds the index only while im_messages is below this (information_schema
	// estimate); adding the first FULLTEXT index rebuilds the table, so a bigger one needs the
	// DDL in a maintenance window (release notes) and stays on LIKE until then.
	searchIndexMaxRows           int64 = 1_000_000
	searchBackfillBatch                = 500
	searchBackfillBatchesPerTick       = 20
	searchMaintenanceInterval          = time.Minute
	searchScanBudgetMS                 = 300 // 0 = FULLTEXT only (tests); S6: 300 → p95 ~325 ms, max < 550 ms
)

type SearchInput struct {
	Q      string `query:"q" doc:"2–64 characters"`
	Type   string `query:"type" enum:"message,file" default:"message"`
	Cursor string `query:"cursor" doc:"next_cursor of the previous page"`
}

type searchHit struct {
	MessageIDStr string   `json:"message_idstr"`
	MessageSeq   uint64   `json:"message_seq" doc:"history cursor: messagesync start_message_seq"`
	ChannelID    string   `json:"channel_id"`
	ChannelType  uint8    `json:"channel_type"`
	FromUID      string   `json:"from_uid"`
	PayloadType  int      `json:"payload_type"`
	Timestamp    int64    `json:"timestamp"`
	Hidden       bool     `json:"hidden"`
	Snippet      string   `json:"snippet" doc:"plain text, ±30 characters around the first hit"`
	SnippetHTML  string   `json:"snippet_html" doc:"snippet HTML-escaped, hits wrapped in <mark>"`
	Ranges       [][2]int `json:"ranges" doc:"hit offsets in snippet, in characters"`
}

type searchPage struct {
	Messages   []searchHit `json:"messages"`
	Total      int         `json:"total" doc:"first page only; capped at 100"`
	NextCursor string      `json:"next_cursor"`
}

const searchColumns = "m.id, m.message_idstr, m.channel_key, m.channel_type, m.from_uid, m.payload_type, m.search_text, m.wk_timestamp, c.hidden_at IS NOT NULL AS hidden"

type searchRow struct {
	ID           uint64
	MessageIDStr string `gorm:"column:message_idstr"`
	ChannelKey   string
	ChannelType  uint8
	FromUID      uint
	PayloadType  int
	SearchText   string
	WKTimestamp  int64 `gorm:"column:wk_timestamp"`
	Hidden       bool
}

// searchText is what search sees of a payload: the text content and/or the file name.
func searchText(payload []byte) string {
	var p struct {
		Content any `json:"content"`
		Name    any `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return ""
	}
	var parts []string
	for _, v := range []any{p.Content, p.Name} {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			parts = append(parts, s)
		}
	}
	text := []rune(strings.Join(parts, " "))
	// ponytail: only the first 10k characters of a long agent answer are searchable.
	return string(text[:min(len(text), searchTextMax)])
}

func likeEscape(s string) string {
	return strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(s)
}

// booleanQuery is the FULLTEXT prefilter: every run of 2+ letters/digits in q as a required
// phrase. Each is a substring of any text containing q, so the prefilter never drops a real
// hit; "" means q has no such run (e.g. "&<b>") and LIKE alone answers.
func booleanQuery(q string) string {
	var words []string
	for _, w := range strings.FieldsFunc(q, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		if len([]rune(w)) >= searchQueryMin {
			words = append(words, `+"`+w+`"`)
		}
	}
	return strings.Join(words, " ")
}

// searchMessages selects the caller's matching messages, newest first: a DM needs the
// caller's conversation row, a group also current membership (a removed member keeps a stale
// row). Per page, the FULLTEXT path reads the matches and joins on uidx_im_conv.
func searchMessages(db *gorm.DB, viewer uint, q string, files bool, cursor uint64, fullText bool) *gorm.DB {
	query := db.Table("im_messages AS m").
		Select(searchColumns).
		Joins("JOIN im_conversations AS c ON c.uid = ? AND c.channel_type = m.channel_type AND c.channel_key = m.channel_key", viewer).
		Joins("LEFT JOIN im_group_members AS g ON m.channel_type = ? AND g.group_id = m.channel_key AND g.uid = ?", groupChannel, viewer).
		Where("m.channel_type = ? OR g.uid IS NOT NULL", personChannel)
	if bq := booleanQuery(q); fullText && bq != "" {
		query = query.Where("MATCH(m.search_text) AGAINST(? IN BOOLEAN MODE)", bq)
	}
	query = query.Where("m.search_text LIKE ? ESCAPE '!'", "%"+likeEscape(q)+"%")
	if files {
		query = query.Where("m.payload_type = ?", filePayloadType)
	} else {
		query = query.Where("m.payload_type <> ?", filePayloadType)
	}
	if cursor != 0 {
		query = query.Where("m.id < ?", cursor)
	}
	return query.Order("m.id DESC")
}

// runSearch picks the plan (S6, 1M local rows, docs-sentence corpus, 120 searches): FULLTEXT
// alone is p95 ~1 s, because a common word (one in four messages) matches 200k+ rows that are
// all joined and sorted; a newest-first primary-key scan with LIKE stops at the first `limit`
// hits and is p95 ~150 ms, but crawls for a rare word. So: the scan first under a
// MAX_EXECUTION_TIME budget, and on timeout (MySQL error 3024) the FULLTEXT statement, which
// is fast exactly when the word is rare. Through the handler (first page reads 101 rows for
// the total): FULLTEXT-only p95 1.2 s; scan budget 150 ms p95 200–590 ms; 300 ms p95 320–331
// ms over 3 runs, max < 550 ms. Without the index: LIKE only.
func runSearch(db *gorm.DB, viewer uint, q string, files bool, cursor uint64, limit int, fullText bool) ([]searchRow, error) {
	var rows []searchRow
	if fullText && searchScanBudgetMS > 0 {
		scan := searchMessages(db, viewer, q, files, cursor, false)
		err := scan.Select(fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%d) */ %s", searchScanBudgetMS, searchColumns)).Limit(limit).Scan(&rows).Error
		var timeout *mysqldriver.MySQLError
		if !errors.As(err, &timeout) || timeout.Number != 3024 {
			return rows, err
		}
		rows = nil
	}
	return rows, searchMessages(db, viewer, q, files, cursor, fullText).Limit(limit).Scan(&rows).Error
}

// highlight cuts the snippet around the first case-insensitive hit and marks every hit in it.
// Everything is escaped; only the <mark> tags are markup.
func highlight(text, q string) (string, string, [][2]int) {
	runes := []rune(text)
	fold := func(rs []rune) []rune {
		out := make([]rune, len(rs))
		for i, r := range rs {
			out[i] = unicode.ToLower(r)
		}
		return out
	}
	hay, needle := fold(runes), fold([]rune(q))
	var hits [][2]int
	for i := 0; i+len(needle) <= len(hay); {
		if string(hay[i:i+len(needle)]) == string(needle) {
			hits = append(hits, [2]int{i, i + len(needle)})
			i += len(needle)
			continue
		}
		i++
	}
	start, end := 0, min(len(runes), 2*searchSnippetPad)
	if len(hits) > 0 {
		start, end = max(0, hits[0][0]-searchSnippetPad), min(len(runes), hits[0][1]+searchSnippetPad)
	}
	var plain, marked strings.Builder
	offset := -start
	if start > 0 {
		plain.WriteString("…")
		marked.WriteString("…")
		offset++
	}
	var ranges [][2]int
	at := start
	for _, h := range hits {
		if h[0] < start || h[1] > end {
			continue
		}
		marked.WriteString(html.EscapeString(string(runes[at:h[0]])))
		marked.WriteString("<mark>" + html.EscapeString(string(runes[h[0]:h[1]])) + "</mark>")
		ranges = append(ranges, [2]int{h[0] + offset, h[1] + offset})
		at = h[1]
	}
	marked.WriteString(html.EscapeString(string(runes[at:end])))
	plain.WriteString(string(runes[start:end]))
	if end < len(runes) {
		plain.WriteString("…")
		marked.WriteString("…")
	}
	return plain.String(), marked.String(), ranges
}

func (p *Plugin) registerSearch(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "imSearch", Method: http.MethodGet, Path: "/api/v1/im/search",
		Summary: "Search messages or file names in the caller's conversations", Tags: []string{"IM"},
		Security: []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, input *SearchInput) (*ProxyOutput, error) {
		q := strings.TrimSpace(input.Q)
		switch n := len([]rune(q)); {
		case n < searchQueryMin:
			return nil, newIMProblem(http.StatusBadRequest, "query_too_short", "the query needs at least 2 characters")
		case n > searchQueryMax:
			return nil, newIMProblem(http.StatusBadRequest, "query_too_long", "the query allows at most 64 characters")
		}
		cursor, err := strconv.ParseUint(input.Cursor, 10, 64)
		if err != nil && input.Cursor != "" {
			return nil, newIMProblem(http.StatusBadRequest, "invalid_cursor", "cursor is invalid")
		}
		viewer := ctx.Value(userIDKey).(uint)
		// The first page reads up to the total cap in the same statement (total = rows found).
		limit := searchPageSize + 1
		if cursor == 0 {
			limit = searchTotalCap + 1
		}
		rows, err := runSearch(global.PRISM_DB.WithContext(ctx), viewer, q, input.Type == "file", cursor, limit, p.searchFullText.Load())
		if err != nil {
			slog.Warn("im search failed", "error", err)
			return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "search is unavailable")
		}
		page := searchPage{Messages: make([]searchHit, 0, searchPageSize)}
		if cursor == 0 {
			page.Total = min(len(rows), searchTotalCap)
		}
		if len(rows) > searchPageSize {
			rows = rows[:searchPageSize]
			page.NextCursor = strconv.FormatUint(rows[len(rows)-1].ID, 10)
		}
		for _, r := range rows {
			snippet, marked, ranges := highlight(r.SearchText, q)
			page.Messages = append(page.Messages, searchHit{
				MessageIDStr: r.MessageIDStr, MessageSeq: r.ID, ChannelID: channelIDFor(r.ChannelKey, r.ChannelType, viewer),
				ChannelType: r.ChannelType, FromUID: strconv.FormatUint(uint64(r.FromUID), 10), PayloadType: r.PayloadType,
				Timestamp: r.WKTimestamp, Hidden: r.Hidden, Snippet: snippet, SnippetHTML: marked, Ranges: ranges,
			})
		}
		return &ProxyOutput{Body: page}, nil
	})
}

// ensureSearchIndex builds the ngram FULLTEXT index on MySQL once (idempotent across replicas)
// and reports whether search may use it. Stopwords are off for this index: with InnoDB's
// default list, ngram drops every token containing a stopword ("鲸A" and "big" would never
// match); the setting is fixed per index at creation. lock_wait_timeout keeps the DDL from
// queueing behind a long transaction and stalling every query on the table (servers run 1 year).
func ensureSearchIndex(ctx context.Context, db *gorm.DB) (bool, error) {
	if db.Dialector.Name() != "mysql" {
		return false, nil
	}
	db = db.WithContext(ctx)
	exists := func() bool {
		var n int64
		db.Raw("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'im_messages' AND index_name = ?", searchIndexName).Scan(&n)
		return n > 0
	}
	if exists() {
		return true, nil
	}
	var plugin string
	var rows int64
	db.Raw("SELECT plugin_status FROM information_schema.plugins WHERE plugin_name = 'ngram'").Scan(&plugin)
	db.Raw("SELECT COALESCE(table_rows, 0) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'im_messages'").Scan(&rows)
	if plugin != "ACTIVE" || rows > searchIndexMaxRows {
		slog.Warn("im search: FULLTEXT index not built, searching with LIKE", "ngram", plugin, "rows", rows, "max_rows", searchIndexMaxRows)
		return false, nil
	}
	err := db.Connection(func(tx *gorm.DB) error {
		defer tx.Exec("SET SESSION innodb_ft_enable_stopword = DEFAULT, lock_wait_timeout = DEFAULT")
		if err := tx.Exec("SET SESSION innodb_ft_enable_stopword = OFF, lock_wait_timeout = 5").Error; err != nil {
			return err
		}
		return tx.Exec("ALTER TABLE im_messages ADD FULLTEXT INDEX " + searchIndexName + " (search_text) WITH PARSER ngram").Error
	})
	if err != nil && !exists() { // another replica may have built it meanwhile
		return false, err
	}
	slog.Info("im search: FULLTEXT ngram index ready", "rows", rows)
	return true, nil
}

// backfillSearchText fills search_text for rows saved before it existed (NULL): batches of
// searchBackfillBatch walking the primary key upward, at most maxBatches (0 = until done). Each
// row is its own UPDATE guarded by IS NULL, so it is idempotent and safe to stop anywhere; the
// next call resumes at the first NULL row. Never one table-wide UPDATE.
func backfillSearchText(ctx context.Context, db *gorm.DB, maxBatches int) (int, error) {
	filled := 0
	var from uint64
	for batch := 0; maxBatches == 0 || batch < maxBatches; batch++ {
		var rows []struct {
			ID      uint64
			Payload string
		}
		if err := db.WithContext(ctx).Model(&IMMessage{}).Select("id, payload").
			Where("id > ? AND search_text IS NULL", from).Order("id ASC").Limit(searchBackfillBatch).Scan(&rows).Error; err != nil {
			return filled, err
		}
		for _, r := range rows {
			res := db.WithContext(ctx).Model(&IMMessage{}).Where("id = ? AND search_text IS NULL", r.ID).
				Update("search_text", searchText([]byte(r.Payload)))
			if res.Error != nil {
				return filled, res.Error
			}
			filled += int(res.RowsAffected)
			from = r.ID
		}
		if len(rows) < searchBackfillBatch {
			break
		}
	}
	return filled, nil
}

// runSearchMaintenance builds the index (MySQL) and drains the backfill in the background, so
// boot never waits on either and search answers (LIKE) meanwhile. Retries each tick until done.
func (p *Plugin) runSearchMaintenance(ctx context.Context, db *gorm.DB) {
	indexed, drained := false, false
	for !(indexed && drained) {
		if !indexed {
			ok, err := ensureSearchIndex(ctx, db)
			if err != nil {
				slog.Warn("im search: FULLTEXT index build failed, retrying next tick", "error", err)
			}
			p.searchFullText.Store(ok)
			indexed = ok || err == nil // not MySQL / not eligible: settled until the next boot
		}
		if !drained {
			n, err := backfillSearchText(ctx, db, searchBackfillBatchesPerTick)
			if err != nil {
				slog.Warn("im search backfill failed, retrying next tick", "filled", n, "error", err)
			} else if n > 0 {
				slog.Info("im search backfill", "filled", n)
			}
			drained = err == nil && n < searchBackfillBatchesPerTick*searchBackfillBatch
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(searchMaintenanceInterval):
		}
	}
}
