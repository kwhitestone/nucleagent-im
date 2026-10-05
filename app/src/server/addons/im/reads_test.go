package im

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func readsRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		uid, _ := strconv.ParseUint(c.GetHeader("X-Test-User"), 10, 64)
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), userIDKey, uint(uid)))
		c.Next()
	})
	(&Plugin{}).registerReads(humagin.New(router, huma.DefaultConfig("reads test", "1")))
	return router
}

func postAs[T any](t *testing.T, router http.Handler, path string, user uint, body any) (int, T) {
	t.Helper()
	rec := groupRequest(t, router, http.MethodPost, path, user, body)
	var out T
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode %s: %v (%s)", path, err, rec.Body.String())
		}
	}
	return rec.Code, out
}

func seedMessages(t *testing.T, db *gorm.DB, channelType uint8, key string, from []uint, n int) []uint64 {
	t.Helper()
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		m := IMMessage{
			MessageIDStr: fmt.Sprintf("%s-%d", key, i), ChannelKey: key, ChannelType: channelType,
			FromUID: from[i%len(from)], PayloadType: 1, Payload: fmt.Sprintf(`{"type":1,"content":"m%d"}`, i),
			WKTimestamp: int64(1000 + i),
		}
		if err := db.Create(&m).Error; err != nil {
			t.Fatal(err)
		}
		ids[i] = m.ID
	}
	return ids
}

func seqs(page messageSyncPage) []uint64 {
	out := make([]uint64, len(page.Messages))
	for i, m := range page.Messages {
		out[i] = m.MessageSeq
	}
	return out
}

// Up/down pagination over 70 rows, WuKong's semantics: start 0 = the latest page; pull_mode 0
// = at or before start (older); 1 = at or after start; ascending; more = a further page.
func TestMessageSyncPagesFromMySQL(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	ids := seedMessages(t, db, groupChannel, "g1", []uint{1, 2}, 70)
	seedMessages(t, db, groupChannel, "g-noise", []uint{1}, 5)
	if err := insertGroupMembers(db, "g1", []uint{1, 2}); err != nil {
		t.Fatal(err)
	}
	sync := func(start uint64, mode int) messageSyncPage {
		code, page := postAs[messageSyncPage](t, router, "/api/v1/im/channel/messagesync", 1, map[string]any{
			"channel_id": "g1", "channel_type": groupChannel, "start_message_seq": start, "pull_mode": mode, "limit": 30,
		})
		if code != http.StatusOK {
			t.Fatalf("status=%d", code)
		}
		return page
	}
	latest := sync(0, 1)
	if got := seqs(latest); len(got) != 30 || got[0] != ids[40] || got[29] != ids[69] || latest.More != 1 {
		t.Fatalf("latest=%v more=%d", got, latest.More)
	}
	// "Load earlier" walks down with start = first - 1 until the channel is exhausted.
	seen := append([]uint64{}, seqs(latest)...)
	cursor := latest.Messages[0].MessageSeq - 1
	for {
		page := sync(cursor, 0)
		seen = append(seqs(page), seen...)
		if page.More == 0 {
			break
		}
		cursor = page.Messages[0].MessageSeq - 1
	}
	if fmt.Sprint(seen) != fmt.Sprint(ids) {
		t.Fatalf("down walk=%v\nwant=%v", seen, ids)
	}
	up := sync(ids[10], 1)
	if got := seqs(up); len(got) != 30 || got[0] != ids[10] || got[29] != ids[39] || up.More != 1 {
		t.Fatalf("up=%v more=%d", got, up.More)
	}
	tail := sync(ids[60], 1)
	if got := seqs(tail); len(got) != 10 || tail.More != 0 {
		t.Fatalf("up tail=%v more=%d", got, tail.More)
	}
	m := latest.Messages[29]
	if m.ChannelID != "g1" || m.FromUID != "2" || m.MessageIDStr != "g1-69" || m.Timestamp != 1069 ||
		string(m.Payload) != `{"type":1,"content":"m69"}` || m.Header.RedDot != 1 {
		t.Fatalf("shape=%+v", m)
	}
}

// R8: authorization moved from WuKong to im. A DM needs the viewer in the pair, a group needs
// an im_group_members row; a DM's channel_id renders as the peer for each viewer, as WuKong does.
func TestMessageSyncAuthorizationAndDMRendering(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	seedMessages(t, db, personChannel, "1@2", []uint{1, 2}, 3)
	seedMessages(t, db, groupChannel, "g1", []uint{1}, 2)
	if err := insertGroupMembers(db, "g1", []uint{1}); err != nil {
		t.Fatal(err)
	}
	body := func(id string, typ uint8) map[string]any {
		return map[string]any{"channel_id": id, "channel_type": typ}
	}
	for _, c := range []struct {
		user uint
		body map[string]any
		code int
		peer string
	}{
		{1, body("2", personChannel), 200, "2"},
		{2, body("1", personChannel), 200, "1"},
		{2, body("2@1", personChannel), 200, "1"},
		{3, body("1", personChannel), 200, ""}, // 3's own (empty) DM with 1, never 1@2
		{3, body("1@2", personChannel), 403, ""},
		{2, body("g1", groupChannel), 403, ""},
		{1, body("g1", groupChannel), 200, "g1"},
		{1, body("g1", 9), 403, ""},
	} {
		code, page := postAs[messageSyncPage](t, router, "/api/v1/im/channel/messagesync", c.user, c.body)
		if code != c.code {
			t.Fatalf("user %d %v: status=%d want %d", c.user, c.body, code, c.code)
		}
		if c.peer == "" {
			if len(page.Messages) != 0 {
				t.Fatalf("user %d %v leaked %d messages", c.user, c.body, len(page.Messages))
			}
			continue
		}
		for _, m := range page.Messages {
			if m.ChannelID != c.peer {
				t.Fatalf("user %d: channel_id=%q want %q", c.user, m.ChannelID, c.peer)
			}
		}
	}
	// The read endpoint shares the check.
	if rec := groupRequest(t, router, http.MethodPost, "/api/v1/im/conversation/read", 3, body("1@2", personChannel)); rec.Code != 403 {
		t.Fatalf("non-member read=%d", rec.Code)
	}
	if rec := groupRequest(t, router, http.MethodPost, "/api/v1/im/conversation/read", 2, body("g1", groupChannel)); rec.Code != 403 {
		t.Fatalf("non-member group read=%d", rec.Code)
	}
}

// Q3: the list pages by cursor (default 50) with no gap or duplicate, newest first.
func TestConversationListCursorPages(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	const total = 120
	for i := 0; i < total; i++ {
		peer := uint(100 + i)
		key := fmt.Sprintf("1@%d", peer)
		ids := seedMessages(t, db, personChannel, key, []uint{peer}, 1)
		for _, uid := range []uint{1, peer} {
			if err := db.Create(&IMConversation{UID: uid, ChannelType: personChannel, ChannelKey: key,
				LastMessageID: ids[0], Unread: int(uid % 3)}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	var pages []int
	var peers []string
	cursor := ""
	for {
		code, page := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", 1, map[string]any{"cursor": cursor})
		if code != http.StatusOK {
			t.Fatalf("status=%d", code)
		}
		pages = append(pages, len(page.Conversations))
		for _, c := range page.Conversations {
			peers = append(peers, c.ChannelID)
		}
		if page.Done {
			if page.NextCursor != "" {
				t.Fatalf("done page has cursor %q", page.NextCursor)
			}
			break
		}
		cursor = page.NextCursor
	}
	if fmt.Sprint(pages) != "[50 50 20]" {
		t.Fatalf("pages=%v", pages)
	}
	for i, peer := range peers {
		if want := strconv.Itoa(100 + total - 1 - i); peer != want {
			t.Fatalf("position %d: %s want %s (gap, duplicate or order)", i, peer, want)
		}
	}
	// Shape: what im.ts conversationFromRow reads.
	rec := groupRequest(t, router, http.MethodPost, "/api/v1/im/conversation/list", 1, map[string]any{"limit": 1})
	var raw struct {
		Conversations []map[string]any `json:"conversations"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &raw)
	c := raw.Conversations[0]
	last, _ := c["last_message"].(map[string]any)
	if c["channel_id"] != "219" || c["channel_type"] != float64(1) || c["unread"] != float64(1) ||
		last["from_uid"] != "219" || last["message_idstr"] != "1@219-0" || last["payload"] == nil || last["timestamp"] != float64(1000) {
		t.Fatalf("shape=%v", c)
	}
	if code, _ := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", 1, map[string]any{"cursor": "x"}); code != 400 {
		t.Fatalf("bad cursor=%d", code)
	}
}

// The unread/read cycle through the real webhook: a DM from the other side → 1 → read → 0;
// the sender's own row and the peer's are untouched.
func TestConversationReadClearsUnread(t *testing.T) {
	db, _, post := persistHarness(t)
	router := readsRouter(t)
	mustOK(t, post(webhookText("r1", 2, "2@1", personChannel, "hello")))
	unread := func(uid uint) int {
		_, page := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", uid, map[string]any{})
		if len(page.Conversations) != 1 {
			t.Fatalf("uid %d conversations=%d", uid, len(page.Conversations))
		}
		return page.Conversations[0].Unread
	}
	if unread(1) != 1 || unread(2) != 0 {
		t.Fatalf("before read: %d %d", unread(1), unread(2))
	}
	if rec := groupRequest(t, router, http.MethodPost, "/api/v1/im/conversation/read", 1,
		map[string]any{"channel_id": "2", "channel_type": personChannel}); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"code":0`) {
		t.Fatalf("read=%d %s", rec.Code, rec.Body.String())
	}
	if unread(1) != 0 {
		t.Fatalf("after read: %d", unread(1))
	}
	mustOK(t, post(webhookText("r2", 2, "2@1", personChannel, "again")))
	if unread(1) != 1 {
		t.Fatalf("next message: %d", unread(1))
	}
	_ = db
}

// fakeHistory is WuKong's /channel/messagesync for the backfill, holding full channel histories.
type fakeHistory struct {
	mu       sync.Mutex
	channels map[string][]webhookMessage // keyed by the stored key ("1@2", "g1")
	down     bool                        // abort the connection: a transport failure
	calls    map[string]int
	logins   map[string]string
}

func (f *fakeHistory) serve(t *testing.T) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.down {
			panic(http.ErrAbortHandler)
		}
		if r.URL.Path != "/channel/messagesync" {
			w.WriteHeader(http.StatusOK) // the group rebuild's /channel, irrelevant here
			return
		}
		var body struct {
			LoginUID    string `json:"login_uid"`
			ChannelID   string `json:"channel_id"`
			ChannelType uint8  `json:"channel_type"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.calls[body.ChannelID]++
		f.logins[body.ChannelID] = body.LoginUID
		out := []map[string]any{}
		for _, m := range f.channels[body.ChannelID] {
			channelID := m.ChannelID
			if body.ChannelType == personChannel { // WuKong renders a DM as the peer
				channelID = channelIDFor(body.ChannelID, personChannel, uint(must(strconv.ParseUint(body.LoginUID, 10, 64))))
			}
			out = append(out, map[string]any{
				"message_id": 1, "message_idstr": m.MessageIDStr, "message_seq": m.MessageSeq,
				"client_msg_no": m.ClientMsgNo, "from_uid": m.FromUID, "channel_id": channelID,
				"channel_type": body.ChannelType, "timestamp": m.Timestamp, "payload": m.RawPayload,
				"header": map[string]int{"red_dot": 1},
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": out, "more": 0})
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func history(key string, channelType uint8, n int, from ...uint) []webhookMessage {
	base := time.Now().Unix() - 100
	out := make([]webhookMessage, n)
	for i := range out {
		out[i] = webhookText(fmt.Sprintf("%s#%d", key, i+1), from[i%len(from)], key, channelType, fmt.Sprintf("m%d", i+1))
		out[i].MessageSeq = int64(i + 1)
		out[i].Timestamp = base + int64(i)
	}
	return out
}

func storedIDs(t *testing.T, db *gorm.DB, key string) string {
	t.Helper()
	var rows []IMMessage
	if err := db.Where("channel_key = ?", key).Order("wk_timestamp ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.MessageIDStr
	}
	return strings.Join(out, ",")
}

// Q1 option 2: a gap in the middle (im was down) and a lost tail are filled through the same
// idempotent save; a clean state adds nothing; an old message never becomes the preview.
func TestBackfillFillsGapsIdempotently(t *testing.T) {
	db, _, post := persistHarness(t)
	fake := &fakeHistory{channels: map[string][]webhookMessage{}, calls: map[string]int{}, logins: map[string]string{}}
	p := &Plugin{apiAddr: fake.serve(t)}
	if err := insertGroupMembers(db, "g1", []uint{3, 4}); err != nil {
		t.Fatal(err)
	}
	dm := history("1@2", personChannel, 5, 1, 2)
	group := history("g1", groupChannel, 3, 3, 4)
	stale := history("1@9", personChannel, 2, 1, 9)
	for i := range stale {
		stale[i].Timestamp -= 8 * 24 * 3600 // outside the 7-day window
	}
	fake.channels["1@2"], fake.channels["g1"], fake.channels["1@9"] = dm, group, stale
	// The webhook delivered DM 1, 2, 4 (3 and 5 were lost while im was down; DM 4 is from uid 2,
	// the reader the backfill uses), the whole group,
	// and the stale DM's first message.
	mustOK(t, post(dm[0], dm[1], dm[3], group[0], group[1], group[2], stale[0]))
	before := conversations(t, db, personChannel, "1@2")

	if !p.backfillMessages(t.Context(), db) {
		t.Fatal("backfill reported WuKong unreachable")
	}
	if got := storedIDs(t, db, "1@2"); got != "1@2#1,1@2#2,1@2#3,1@2#4,1@2#5" {
		t.Fatalf("DM after backfill=%s", got)
	}
	if got := storedIDs(t, db, "1@9"); got != "1@9#1" {
		t.Fatalf("stale channel was backfilled: %s", got)
	}
	if fake.calls["1@9"] != 0 || fake.logins["g1"] != "3" || fake.logins["1@2"] != "2" {
		t.Fatalf("calls=%v logins=%v", fake.calls, fake.logins)
	}
	after := conversations(t, db, personChannel, "1@2")
	// Preview = DM 5 (the newest). The backfilled #3 and #5 are both from uid 1: uid 2's unread
	// grows by 2, the sender's own stays.
	if after[1][1] != lastID(t, db, "1@2#5") || after[2][1] != lastID(t, db, "1@2#5") ||
		after[1][0] != before[1][0] || after[2][0] != before[2][0]+2 {
		t.Fatalf("conversations before=%v after=%v", before, after)
	}

	// Clean state: a second pass adds nothing and changes nothing.
	total := messageCount(t, db)
	if !p.backfillMessages(t.Context(), db) || messageCount(t, db) != total ||
		fmt.Sprint(conversations(t, db, personChannel, "1@2")) != fmt.Sprint(after) {
		t.Fatal("second backfill pass changed state")
	}

	// Only the older message lost: it is saved, but the preview stays on the newer one.
	fake.channels["1@2"] = append(fake.channels["1@2"], history("1@2", personChannel, 7, 1, 2)[5:]...)
	mustOK(t, post(fake.channels["1@2"][6])) // #7 arrived, #6 was lost
	if !p.backfillMessages(t.Context(), db) {
		t.Fatal("third pass")
	}
	if got := conversations(t, db, personChannel, "1@2"); got[1][1] != lastID(t, db, "1@2#7") || storedIDs(t, db, "1@2") != "1@2#1,1@2#2,1@2#3,1@2#4,1@2#5,1@2#6,1@2#7" {
		t.Fatalf("preview moved to the backfilled older message: %v %s", got, storedIDs(t, db, "1@2"))
	}
}

// WuKong unreachable: the pass is logged and the loop retries it on the next tick; once
// WuKong is back the gap fills. It runs in the W3 rebuild loop's slot.
func TestBackfillRetriesWhenWuKongUnreachable(t *testing.T) {
	db, _, post := persistHarness(t)
	logs := captureLogs(t)
	fake := &fakeHistory{channels: map[string][]webhookMessage{}, calls: map[string]int{}, logins: map[string]string{}, down: true}
	dm := history("1@2", personChannel, 3, 1, 2)
	fake.channels["1@2"] = dm
	mustOK(t, post(dm[0], dm[2]))
	previous := groupRebuildInterval
	groupRebuildInterval = 50 * time.Millisecond
	t.Cleanup(func() { groupRebuildInterval = previous })
	p := &Plugin{apiAddr: fake.serve(t)}
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Stop(ctx)
	})
	waitFor(t, "two failed passes", func() bool {
		return strings.Count(logs.String(), "WuKong unreachable, retrying next tick") >= 2
	})
	if messageCount(t, db) != 2 {
		t.Fatal("rows changed while WuKong was down")
	}
	fake.mu.Lock()
	fake.down = false
	fake.mu.Unlock()
	waitFor(t, "gap filled after recovery", func() bool { return messageCount(t, db) == 3 })
	if got := storedIDs(t, db, "1@2"); got != "1@2#1,1@2#2,1@2#3" {
		t.Fatalf("after recovery=%s", got)
	}
}

// A failed save (503 to WuKong, which may then drop the batch) makes the backfill due again.
func TestPersistFailureSchedulesBackfill(t *testing.T) {
	db, p, post := persistHarness(t)
	if err := db.Migrator().DropTable(&IMMessage{}); err != nil {
		t.Fatal(err)
	}
	if rec := post(webhookText("x", 1, "1@2", personChannel, "hi")); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rec.Code)
	}
	if !p.backfillDue.Load() {
		t.Fatal("backfill not scheduled after a failed save")
	}
}

// A WuKong page spanning a WK reset (seq restarts at 1) is saved in time order, not seq order.
func TestBackfillOrdersByTimestampAcrossWKReset(t *testing.T) {
	db, _, post := persistHarness(t)
	fake := &fakeHistory{channels: map[string][]webhookMessage{}, calls: map[string]int{}, logins: map[string]string{}}
	p := &Plugin{apiAddr: fake.serve(t)}
	dm := history("1@2", personChannel, 4, 1, 2)
	dm[0].MessageSeq, dm[1].MessageSeq = 900, 901 // before the reset
	dm[2].MessageSeq, dm[3].MessageSeq = 1, 2     // after it
	fake.channels["1@2"] = dm
	mustOK(t, post(dm[3]))
	if !p.backfillMessages(t.Context(), db) {
		t.Fatal("backfill")
	}
	var ids []string
	if err := db.Model(&IMMessage{}).Where("channel_key = ?", "1@2").Order("id ASC").Pluck("message_idstr", &ids).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Join(ids, ",") != "1@2#4,1@2#1,1@2#2,1@2#3" {
		t.Fatalf("id order=%v (the backfilled three must keep time order)", ids)
	}
}
