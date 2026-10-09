package im

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"gorm.io/gorm"
)

// Q3 §2 hide + batch (IM2-D1). H1–H8 are the design's acceptance ids; these are their
// backend equivalents.

type batchResults struct {
	Results []struct {
		ChannelID   string `json:"channel_id"`
		ChannelType uint8  `json:"channel_type"`
		OK          bool   `json:"ok"`
		Code        string `json:"code"`
	} `json:"results"`
}

func ch(id string, typ uint8) map[string]any {
	return map[string]any{"channel_id": id, "channel_type": typ}
}

func batch(t *testing.T, router http.Handler, user uint, action string, channels ...map[string]any) batchResults {
	t.Helper()
	code, out := postAs[batchResults](t, router, "/api/v1/im/conversation/batch", user,
		map[string]any{"action": action, "channels": channels})
	if code != http.StatusOK {
		t.Fatalf("batch %s: status=%d", action, code)
	}
	if len(out.Results) != len(channels) {
		t.Fatalf("batch %s: %d results for %d channels", action, len(out.Results), len(channels))
	}
	return out
}

// listIDs walks every page of the viewer's list (hidden view when hidden) and returns channel ids.
func listIDs(t *testing.T, router http.Handler, user uint, hidden bool) []string {
	t.Helper()
	var ids []string
	cursor := ""
	for {
		code, page := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", user,
			map[string]any{"cursor": cursor, "hidden": hidden})
		if code != http.StatusOK {
			t.Fatalf("list status=%d", code)
		}
		for _, c := range page.Conversations {
			ids = append(ids, c.ChannelID)
		}
		if page.Done {
			return ids
		}
		cursor = page.NextCursor
	}
}

// seedDM gives viewer and peer a one-message DM with the given unread for viewer.
func seedDM(t *testing.T, db *gorm.DB, viewer, peer uint, unread int) {
	t.Helper()
	key := fmt.Sprintf("%d@%d", min(viewer, peer), max(viewer, peer))
	ids := seedMessages(t, db, personChannel, key, []uint{peer}, 1)
	for _, uid := range []uint{viewer, peer} {
		n := 0
		if uid == viewer {
			n = unread
		}
		if err := db.Create(&IMConversation{UID: uid, ChannelType: personChannel, ChannelKey: key, LastMessageID: ids[0], Unread: n}).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func seedGroupConv(t *testing.T, db *gorm.DB, group string, members ...uint) {
	t.Helper()
	ids := seedMessages(t, db, groupChannel, group, members, 1)
	if err := insertGroupMembers(db, group, members); err != nil {
		t.Fatal(err)
	}
	for _, uid := range members {
		if err := db.Create(&IMConversation{UID: uid, ChannelType: groupChannel, ChannelKey: group, LastMessageID: ids[0], Unread: 2}).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func convRow(t *testing.T, db *gorm.DB, uid uint, typ uint8, key string) IMConversation {
	t.Helper()
	var row IMConversation
	if err := db.Where("uid = ? AND channel_type = ? AND channel_key = ?", uid, typ, key).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

// H1 + H2: hide moves the row to the hidden view and clears unread; history stays readable;
// unhide puts it back in its original position.
func TestBatchHideUnhideKeepsHistory(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	for _, peer := range []uint{2, 3, 4} {
		seedDM(t, db, 1, peer, 3)
	}
	before := listIDs(t, router, 1, false)
	if fmt.Sprint(before) != "[4 3 2]" {
		t.Fatalf("before=%v", before)
	}
	if r := batch(t, router, 1, "hide", ch("3", personChannel)); !r.Results[0].OK || r.Results[0].Code != "" ||
		r.Results[0].ChannelID != "3" || r.Results[0].ChannelType != personChannel {
		t.Fatalf("hide=%+v", r)
	}
	if got := listIDs(t, router, 1, false); fmt.Sprint(got) != "[4 2]" {
		t.Fatalf("after hide=%v", got)
	}
	if got := listIDs(t, router, 1, true); fmt.Sprint(got) != "[3]" {
		t.Fatalf("hidden view=%v", got)
	}
	row := convRow(t, db, 1, personChannel, "1@3")
	if row.HiddenAt == nil || row.Unread != 0 {
		t.Fatalf("hidden row=%+v", row)
	}
	// H2: the history is untouched while hidden.
	code, page := postAs[messageSyncPage](t, router, "/api/v1/im/channel/messagesync", 1, ch("3", personChannel))
	if code != http.StatusOK || len(page.Messages) != 1 {
		t.Fatalf("history while hidden: %d %d", code, len(page.Messages))
	}
	if r := batch(t, router, 1, "unhide", ch("3", personChannel)); !r.Results[0].OK {
		t.Fatalf("unhide=%+v", r)
	}
	if got := listIDs(t, router, 1, false); fmt.Sprint(got) != fmt.Sprint(before) {
		t.Fatalf("after unhide=%v want %v", got, before)
	}
	if got := listIDs(t, router, 1, true); len(got) != 0 {
		t.Fatalf("hidden view after unhide=%v", got)
	}
}

// H3: a message from someone else brings the hidden row back on top with unread 1; the
// viewer's own message (another device) does not.
func TestNewMessageFromOtherUnhides(t *testing.T) {
	_, _, post := persistHarness(t)
	router := readsRouter(t)
	mustOK(t, post(webhookText("a1", 2, "2@1", personChannel, "hi"), webhookText("a2", 3, "3@1", personChannel, "yo")))
	batch(t, router, 1, "hide", ch("2", personChannel))
	mustOK(t, post(webhookText("a3", 1, "1@2", personChannel, "from my phone")))
	if got := listIDs(t, router, 1, true); fmt.Sprint(got) != "[2]" {
		t.Fatalf("own message unhid the row: hidden=%v", got)
	}
	mustOK(t, post(webhookText("a4", 2, "2@1", personChannel, "are you there")))
	if got := listIDs(t, router, 1, true); len(got) != 0 {
		t.Fatalf("still hidden=%v", got)
	}
	_, page := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", 1, map[string]any{})
	if len(page.Conversations) != 2 || page.Conversations[0].ChannelID != "2" || page.Conversations[0].Unread != 1 {
		t.Fatalf("list=%+v", page.Conversations)
	}
}

// H4: 5 of 6 groups hidden in one call, all ok; undo (unhide) restores all.
func TestBatchHideFiveGroupsAndUndo(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	var chans []map[string]any
	for i := 1; i <= 6; i++ {
		g := fmt.Sprintf("g%d", i)
		seedGroupConv(t, db, g, 1, 2)
		if i <= 5 {
			chans = append(chans, ch(g, groupChannel))
		}
	}
	r := batch(t, router, 1, "hide", chans...)
	for _, x := range r.Results {
		if !x.OK {
			t.Fatalf("hide=%+v", r)
		}
	}
	if got := listIDs(t, router, 1, false); fmt.Sprint(got) != "[g6]" {
		t.Fatalf("after hide=%v", got)
	}
	batch(t, router, 1, "unhide", chans...)
	if got := listIDs(t, router, 1, false); len(got) != 6 {
		t.Fatalf("after undo=%v", got)
	}
	// The other member's view never changed.
	if got := listIDs(t, router, 2, false); len(got) != 6 {
		t.Fatalf("member 2=%v", got)
	}
}

// H5: a selection spanning two pages of 60 conversations hides exactly those 4.
func TestBatchHideAcrossPages(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	for peer := uint(100); peer < 160; peer++ {
		seedDM(t, db, 1, peer, 0)
	}
	_, first := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", 1, map[string]any{})
	_, second := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", 1, map[string]any{"cursor": first.NextCursor})
	if len(first.Conversations) != 50 || len(second.Conversations) != 10 {
		t.Fatalf("pages=%d,%d", len(first.Conversations), len(second.Conversations))
	}
	picked := []string{first.Conversations[0].ChannelID, first.Conversations[49].ChannelID,
		second.Conversations[0].ChannelID, second.Conversations[9].ChannelID}
	var chans []map[string]any
	for _, id := range picked {
		chans = append(chans, ch(id, personChannel))
	}
	batch(t, router, 1, "hide", chans...)
	visible := listIDs(t, router, 1, false)
	if len(visible) != 56 || fmt.Sprint(listIDs(t, router, 1, true)) != fmt.Sprint(picked) {
		t.Fatalf("visible=%d hidden=%v picked=%v", len(visible), listIDs(t, router, 1, true), picked)
	}
}

// H6 + H8: invalid and foreign channels come back not_found in place; the rest succeed; the
// owners of the foreign channels are untouched.
func TestBatchPartialNotFoundAndForeignChannels(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	seedDM(t, db, 1, 2, 4)
	seedDM(t, db, 3, 4, 0)
	seedGroupConv(t, db, "g1", 1, 2)
	seedGroupConv(t, db, "g9", 3, 4)
	r := batch(t, router, 1, "hide",
		ch("2", personChannel),
		ch("3@4", personChannel), // H8: someone else's DM
		ch("g9", groupChannel),   // H8: a group the caller is not in
		ch("999", personChannel), // never talked
		ch("", groupChannel),
		ch("g1", 9), // bad type
		ch("g1", groupChannel),
	)
	var got []string
	for _, x := range r.Results {
		got = append(got, fmt.Sprintf("%s/%d:%v:%s", x.ChannelID, x.ChannelType, x.OK, x.Code))
	}
	want := "[2/1:true: 3@4/1:false:not_found g9/2:false:not_found 999/1:false:not_found /2:false:not_found g1/9:false:not_found g1/2:true:]"
	if fmt.Sprint(got) != want {
		t.Fatalf("results=%v\nwant   %s", got, want)
	}
	for _, c := range []struct {
		uid uint
		typ uint8
		key string
	}{{3, personChannel, "3@4"}, {4, personChannel, "3@4"}, {3, groupChannel, "g9"}, {2, personChannel, "1@2"}, {2, groupChannel, "g1"}} {
		if row := convRow(t, db, c.uid, c.typ, c.key); row.HiddenAt != nil || (c.key == "g9" && row.Unread != 2) {
			t.Fatalf("foreign row changed: %+v", row)
		}
	}
	// read also refuses a foreign channel.
	if r := batch(t, router, 3, "read", ch("g1", groupChannel)); r.Results[0].OK || r.Results[0].Code != "not_found" {
		t.Fatalf("foreign read=%+v", r)
	}
	if convRow(t, db, 2, groupChannel, "g1").Unread != 2 {
		t.Fatal("foreign read cleared someone's unread")
	}
}

// H7: the state is per viewer and server-side: A hides, B (same DM) still sees it; A's other
// device (another list call) sees it hidden.
func TestHideIsPerViewer(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	seedDM(t, db, 1, 2, 1)
	batch(t, router, 1, "hide", ch("2", personChannel))
	if got := listIDs(t, router, 2, false); fmt.Sprint(got) != "[1]" {
		t.Fatalf("peer view=%v", got)
	}
	if got := listIDs(t, router, 1, false); len(got) != 0 {
		t.Fatalf("A second device=%v", got)
	}
	if convRow(t, db, 1, personChannel, "1@2").HiddenAt == nil || convRow(t, db, 2, personChannel, "1@2").HiddenAt != nil {
		t.Fatal("hidden_at not per viewer")
	}
}

// read clears unread without hiding; every action is idempotent; "a@b" and the peer uid name
// the same DM.
func TestBatchReadAndIdempotent(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	seedDM(t, db, 1, 2, 5)
	for i := 0; i < 2; i++ {
		if r := batch(t, router, 1, "read", ch("1@2", personChannel)); !r.Results[0].OK {
			t.Fatalf("read #%d=%+v", i, r)
		}
	}
	if row := convRow(t, db, 1, personChannel, "1@2"); row.Unread != 0 || row.HiddenAt != nil {
		t.Fatalf("after read=%+v", row)
	}
	for _, action := range []string{"hide", "hide", "unhide", "unhide"} {
		if r := batch(t, router, 1, action, ch("2", personChannel)); !r.Results[0].OK {
			t.Fatalf("%s=%+v", action, r)
		}
	}
	if got := listIDs(t, router, 1, false); fmt.Sprint(got) != "[2]" {
		t.Fatalf("final=%v", got)
	}
}

// 1–100 channels; anything else, or an unknown action, is a 400 and changes nothing.
func TestBatchLimitsAndValidation(t *testing.T) {
	db := m2DB(t)
	router := readsRouter(t)
	seedDM(t, db, 1, 2, 1)
	many := func(n int) []map[string]any {
		out := make([]map[string]any, n)
		for i := range out {
			out[i] = ch("2", personChannel)
		}
		return out
	}
	for _, c := range []struct {
		body map[string]any
		code int
	}{
		{map[string]any{"action": "hide", "channels": many(101)}, 400},
		{map[string]any{"action": "hide", "channels": many(0)}, 400},
		{map[string]any{"action": "hide"}, 400},
		{map[string]any{"action": "delete", "channels": many(1)}, 400},
		{map[string]any{"action": "pin", "channels": many(1)}, 400},
		{map[string]any{"action": "hide", "channels": many(100)}, 200},
	} {
		rec := groupRequest(t, router, http.MethodPost, "/api/v1/im/conversation/batch", 1, c.body)
		if rec.Code != c.code {
			t.Fatalf("%v channels=%d: status=%d want %d (%s)", c.body["action"], len(c.body["channels"].([]map[string]any)), rec.Code, c.code, rec.Body.String())
		}
		if c.code == 400 && convRow(t, db, 1, personChannel, "1@2").HiddenAt != nil {
			t.Fatal("a rejected batch changed state")
		}
	}
}

// Concurrent batches on the same rows all succeed and converge; the audit line is written.
func TestBatchConcurrentAndAudited(t *testing.T) {
	db := m2DB(t)
	// SQLite shared-cache locks whole tables ("database table is locked"); InnoDB locks rows.
	// One connection keeps the handlers concurrent while SQLite serializes the statements.
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	router := readsRouter(t)
	logs := captureLogs(t)
	var chans []map[string]any
	for peer := uint(10); peer < 20; peer++ {
		seedDM(t, db, 1, peer, 2)
		chans = append(chans, ch(fmt.Sprint(peer), personChannel))
	}
	var wg sync.WaitGroup
	errs := make(chan string, 16*11)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(action string) {
			defer wg.Done()
			code, out := postAs[batchResults](t, router, "/api/v1/im/conversation/batch", 1, map[string]any{"action": action, "channels": chans})
			if code != http.StatusOK {
				errs <- fmt.Sprintf("%s status=%d", action, code)
				return
			}
			for _, r := range out.Results {
				if !r.OK {
					errs <- fmt.Sprintf("%s %+v", action, r)
				}
			}
		}([]string{"hide", "read"}[i%2])
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if got := listIDs(t, router, 1, true); len(got) != 10 {
		t.Fatalf("hidden=%d", len(got))
	}
	if !strings.Contains(logs.String(), `msg="im conversation batch" uid=1 action=hide n=10 ok=10`) {
		t.Fatalf("audit line missing:\n%s", logs.String())
	}
}
