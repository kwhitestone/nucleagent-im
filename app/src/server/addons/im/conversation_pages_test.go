package im

import (
	"fmt"
	"net/http"
	"testing"
)

// IM-FIX1: AS3 saw "17 + 43 → 59" and "78 + 12 → 89" around the 50-row boundary. This replays
// that shape through the real webhook (every message in the same second, self-only groups) and
// walks every page: no row may be lost or repeated, at any page size, nor when rows are hidden
// or bumped by a new message between two page reads (the keyset is last_message_id, unique
// per viewer, so a bumped row moves to page 1 and an unbumped one never shifts pages).
func TestConversationListBoundaryKeepsEveryRow(t *testing.T) {
	db, _, post := persistHarness(t)
	router := readsRouter(t)
	const viewer = 880723
	var n int
	send := func(channelType uint8, channel string, from uint) {
		n++
		rec := post(webhookMessage{MessageIDStr: fmt.Sprintf("fix1-%d", n), ClientMsgNo: fmt.Sprintf("c%d", n),
			FromUID: fmt.Sprint(from), ChannelID: channel, ChannelType: channelType, Timestamp: 1760000000,
			RawPayload: []byte(`{"type":1,"content":"x"}`)})
		if rec.Code != http.StatusOK {
			t.Fatalf("webhook %d: %d %s", n, rec.Code, rec.Body.String())
		}
	}
	for i := 0; i < 17; i++ {
		send(personChannel, fmt.Sprintf("%d@%d", viewer, viewer+1+i), viewer) // WuKong's DM id on msg.notify
	}
	for i := 0; i < 43; i++ {
		g := fmt.Sprintf("seed-%02d", i)
		if err := insertGroupMembers(db, g, []uint{viewer}); err != nil {
			t.Fatal(err)
		}
		send(groupChannel, g, viewer)
	}
	requests := 0
	walk := func(limit int, between func(page int)) []string {
		var got []string
		cursor := ""
		requests = 0
		for page := 0; ; page++ {
			requests++
			code, p := postAs[conversationPage](t, router, "/api/v1/im/conversation/list", viewer, map[string]any{"cursor": cursor, "limit": limit})
			if code != http.StatusOK {
				t.Fatalf("limit %d page %d: status %d", limit, page, code)
			}
			for _, c := range p.Conversations {
				got = append(got, fmt.Sprintf("%d:%s", c.ChannelType, c.ChannelID))
			}
			if p.Done {
				return got
			}
			if between != nil {
				between(page)
			}
			cursor = p.NextCursor
		}
	}
	distinct := func(rows []string) int {
		seen := map[string]bool{}
		for _, r := range rows {
			seen[r] = true
		}
		return len(seen)
	}
	for _, limit := range []int{0, 1, 7, 49, 50, 51, 59, 60, 100} {
		if rows := walk(limit, nil); len(rows) != 60 || distinct(rows) != 60 {
			t.Fatalf("limit %d: %d rows (%d distinct), want 60", limit, len(rows), distinct(rows))
		}
		size := limit
		if size == 0 {
			size = conversationPageDefault
		}
		if want := (60 + size - 1) / size; requests != want {
			t.Fatalf("limit %d: %d requests, want %d (no empty trailing page)", limit, requests, want)
		}
	}
	// Page 1 (50) = the 43 groups + the 7 newest DMs; page 2 = the 10 oldest DMs. Between the
	// two reads: a new message bumps a page-1 group (it moves up, page 2 must not repeat it) and
	// a page-2 DM is hidden (page 2 must not show it). Exactly 59 distinct rows come back.
	hiddenDM := fmt.Sprintf("1:%d", viewer+5)
	rows := walk(50, func(page int) {
		if page != 0 {
			return
		}
		send(groupChannel, "seed-01", viewer)
		if r, err := conversationBatch(db, viewer, "hide", []batchChannel{{fmt.Sprint(viewer + 5), personChannel}}); err != nil || !r[0].OK {
			t.Fatalf("hide: %v %v", r, err)
		}
	})
	if len(rows) != 59 || distinct(rows) != 59 {
		t.Fatalf("bump+hide mid-walk: %d rows (%d distinct), want 59", len(rows), distinct(rows))
	}
	for _, r := range rows {
		if r == hiddenDM {
			t.Fatalf("a row hidden before its page was read is still listed")
		}
	}
	if rows := walk(50, nil); len(rows) != 59 || distinct(rows) != 59 || rows[0] != "2:seed-01" {
		t.Fatalf("after: %d rows (%d distinct), first %s", len(rows), distinct(rows), rows[0])
	}
}
