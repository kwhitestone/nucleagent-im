package im

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MUYU-160 independent-review paths: a run that finished without text while Core
// keeps pinging, a delegation whose continuation never arrives, and a failure
// notice whose first send fails.

// Core finished the run with an empty result and keeps the stream alive with
// pings: the turn must complete with a reply, not wait for pings to stop.
func TestLongRequestEmptyResultWithHeartbeatsCompletesTurn(t *testing.T) {
	p, db, core := longRequestFixture(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		emitCoreMessage(w, 7, "user", "text", "q", "empty-turn")
		emitCoreTool(w, 8, "read_file", "completed")
		emitCoreMessage(w, 9, "agent", "result", "", "")
		for { // Core's 15s heartbeat, compressed; never stops on its own
			select {
			case <-release:
				return
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
				fmt.Fprint(w, ": ping\n\n")
				flusher.Flush()
			}
		}
	}
	row := longRequestRow(t, db, "empty-turn", 9)
	done := make(chan error, 1)
	go func() { done <- p.consumeCoreStream(t.Context(), row) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("turn still processing behind Core heartbeats after an empty result")
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxCompleted || got.FinalSentAt == nil || got.LeaseOwner != "" || got.LeaseExpiresAt != nil {
		t.Fatalf("row=%+v, want completed and released", got)
	}
	sent := core.notices()
	if len(sent) != 1 || sentPayload(t, sent[0]).Content != emptyResultText {
		t.Fatalf("sent=%+v, want one reply with the empty-result text", sent)
	}
}

// A delegated run ended empty and the continuation never comes while Core keeps
// pinging: the wait is bounded, the turn fails with one notice and is released.
func TestLongRequestMissingContinuationFailsBounded(t *testing.T) {
	previous := coreContinuationWait
	coreContinuationWait = 150 * time.Millisecond
	t.Cleanup(func() { coreContinuationWait = previous })
	p, db, core := longRequestFixture(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		emitCoreMessage(w, 7, "user", "text", "q", "orphan-turn")
		emitCoreTool(w, 8, "delegate_task", "completed")
		emitCoreMessage(w, 9, "agent", "result", "", "")
		for {
			select {
			case <-release:
				return
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
				fmt.Fprint(w, ": ping\n\n")
				flusher.Flush()
			}
		}
	}
	row := longRequestRow(t, db, "orphan-turn", 9)
	done := make(chan error, 1)
	go func() { done <- p.consumeCoreStream(t.Context(), row) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("consume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delegated turn waited forever for a continuation")
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxDead || got.LastErrorCode != "im_continuation_timeout" || got.LeaseOwner != "" ||
		got.LeaseExpiresAt != nil || got.NoticeDueAt != nil || got.FinalWuKongClientMsgNo == "" {
		t.Fatalf("row=%+v, want dead, released, notice delivered", got)
	}
	sent := core.notices()
	if len(sent) != 1 || sent[0].ClientMsgNo != got.FinalWuKongClientMsgNo || sentPayload(t, sent[0]).Content != execFailedText {
		t.Fatalf("sent=%+v, want one failure notice", sent)
	}
}

// A failure notice whose first send fails stays owed and the worker's resend
// sweep delivers it once WuKong is back; it is never sent twice.
func TestLongRequestFailureNoticeResentAfterSendError(t *testing.T) {
	p, db, core := longRequestFixture(t)
	var failing atomic.Bool
	failing.Store(true)
	sends := map[string]int{}
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "notice-turn")
		emitCoreMessage(w, 8, "system", "error", "❌ executor lease timeout (no result)", "")
	}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := longRequestRow(t, db, "notice-turn", 9)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatalf("consume: %v", err)
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxDead || got.LeaseOwner != "" || got.NoticeDueAt == nil || got.NoticeAttempts != 1 {
		t.Fatalf("row=%+v, want dead with the notice still owed", got)
	}
	// Not yet due: the sweep leaves it alone.
	p.resendDueNotices(t.Context(), time.Now())
	if n := sends["ok"]; n != 0 {
		t.Fatalf("notice resent before its backoff: %d", n)
	}
	failing.Store(false)
	p.resendDueNotices(t.Context(), got.NoticeDueAt.Add(time.Second))
	got = reloadRow(t, db, row.ID)
	if got.NoticeDueAt != nil || sends["ok"] != 1 || sends["failed"] != 1 {
		t.Fatalf("row=%+v sends=%v, want delivered exactly once after one failure", got, sends)
	}
	p.resendDueNotices(t.Context(), time.Now().Add(time.Hour))
	if sends["ok"] != 1 {
		t.Fatalf("delivered notice resent: %v", sends)
	}
	if len(core.notices()) != 0 {
		t.Fatalf("unexpected direct sends: %+v", core.notices())
	}
}

// The fast resend schedule ends without dropping the notice: it stays owed and
// moves to the slow retry, and the turn stays dead.
func TestLongRequestFailureNoticeGivesUpAfterSchedule(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	failing.Store(true)
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := longRequestRow(t, db, "give-up-turn", 9)
	row.Attempts = len(retryDelays) + 1
	p.failInbox(row, errCoreStreamIdle, time.Now())
	at := time.Now()
	for range len(noticeRetryDelays) + 3 {
		at = at.Add(noticeSlowRetry + time.Minute)
		p.resendDueNotices(t.Context(), at)
	}
	got := reloadRow(t, db, row.ID)
	// failInbox makes the first attempt itself, then one per sweep.
	want := 1 + len(noticeRetryDelays) + 3
	if got.State != inboxDead || got.NoticeDueAt == nil || got.NoticeSentAt != nil ||
		got.NoticeAttempts != want || sends["failed"] != want {
		t.Fatalf("row=%+v sends=%v, want still owed after %d failed tries", got, sends, want)
	}
}

// After each terminal outcome the next question in the same chat completes:
// an empty result, a missing continuation and an exhausted failure.
func TestLongRequestFollowUpAfterEachOutcome(t *testing.T) {
	previous := coreContinuationWait
	coreContinuationWait = 100 * time.Millisecond
	t.Cleanup(func() { coreContinuationWait = previous })
	for _, outcome := range []string{"empty", "orphan", "exhausted"} {
		t.Run(outcome, func(t *testing.T) {
			p, db, core := longRequestFixture(t)
			var turn atomic.Int32
			core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				emitCoreMessage(w, 7, "user", "text", "q", "first")
				if turn.Add(1) == 1 {
					switch outcome {
					case "empty":
						emitCoreMessage(w, 8, "agent", "result", "", "")
					case "orphan":
						emitCoreTool(w, 8, "delegate_task", "completed")
						emitCoreMessage(w, 9, "agent", "result", "", "")
						time.Sleep(300 * time.Millisecond) // pings omitted: the wait bound ends it
					}
					return
				}
				emitCoreMessage(w, 20, "user", "text", "again", "second")
				emitCoreMessage(w, 21, "agent", "result", "second answer", "")
			}
			first := longRequestRow(t, db, "first", 9)
			if outcome == "exhausted" {
				turn.Store(1) // the first turn never reached Core's stream
				first.Attempts = len(retryDelays) + 1
				p.failInbox(first, errCoreStreamIdle, time.Now())
			} else if err := p.consumeCoreStream(t.Context(), first); err != nil {
				t.Fatalf("first: %v", err)
			}
			if got := reloadRow(t, db, first.ID); got.State == inboxProcessing || got.LeaseOwner != "" {
				t.Fatalf("first=%+v, want terminal and released", got)
			}
			second := longRequestRow(t, db, "second", 9)
			if err := p.consumeCoreStream(t.Context(), second); err != nil {
				t.Fatalf("second: %v", err)
			}
			if got := reloadRow(t, db, second.ID); got.State != inboxCompleted || got.CumulativeAnswer != "second answer" {
				t.Fatalf("second=%+v, want completed with its own answer", got)
			}
			var processing int64
			db.Model(&IMWebhookInbox{}).Where("state = ?", inboxProcessing).Count(&processing)
			if processing != 0 {
				t.Fatalf("processing rows=%d", processing)
			}
		})
	}
}

// wrapSend is a WuKong /message/send that fails while failing is set and counts
// outcomes. Notices go to apiAddr; the Core stream stays on the fixture server.
func wrapSend(t *testing.T, failing *atomic.Bool, sends map[string]int) string {
	t.Helper()
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/message/send" {
			http.NotFound(w, r)
			return
		}
		var body noticeSend
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		defer mu.Unlock()
		if failing.Load() {
			sends["failed"]++
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		sends["ok"]++
	}))
	t.Cleanup(server.Close)
	return server.URL
}
