package im

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// MUYU-160 second review: the deadlines and the owed notice must be durable —
// a reconnect, a restart, a cursor that moved on, or a process that dies
// between claim and send must not lose a reply or leave a turn processing.

// The fast schedule runs out while WuKong is down: the notice is never dropped.
// It stays owed on a slow retry and is delivered once WuKong is back, exactly once.
func TestNoticeOutlivesFastScheduleAndIsDelivered(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	failing.Store(true)
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := longRequestRow(t, db, "last-try", 9)
	exhaustRetries(t, db, row)
	p.failInbox(row, errCoreStreamIdle, time.Now())
	at := time.Now()
	for range len(noticeRetryDelays) + 2 { // past the fast schedule
		at = at.Add(noticeSlowRetry + time.Minute)
		p.resendDueNotices(t.Context(), at)
	}
	got := reloadRow(t, db, row.ID)
	if got.NoticeDueAt == nil || got.NoticeSentAt != nil || got.NoticeAttempts <= len(noticeRetryDelays) {
		t.Fatalf("after the fast schedule: %+v, want still owed on slow retry", got)
	}
	if want := at.Add(noticeSlowRetry); got.NoticeDueAt.Sub(want).Abs() > time.Second {
		t.Fatalf("slow retry due=%v, want %v", got.NoticeDueAt, want)
	}
	failing.Store(false)
	p.resendDueNotices(t.Context(), at.Add(noticeSlowRetry+time.Minute))
	got = reloadRow(t, db, row.ID)
	if sends["ok"] != 1 || got.NoticeSentAt == nil || got.NoticeDueAt != nil {
		t.Fatalf("row=%+v sends=%v, want delivered once after the outage", got, sends)
	}
	p.resendDueNotices(t.Context(), at.Add(48*time.Hour))
	if sends["ok"] != 1 {
		t.Fatalf("delivered notice resent: %v", sends)
	}
}

// The process dies after claiming an attempt and before WuKong answers
// (modelled by a cancelled context): the claim already moved the notice to its
// next due time instead of clearing it, so a later sweep still delivers it.
// Regression guard for the claim-then-crash window.
func TestNoticeInterruptedSendIsResent(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := longRequestRow(t, db, "crash-turn", 9)
	now := time.Now()
	if err := db.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).Updates(map[string]any{
		"state": inboxDead, "lease_owner": "", "final_wu_kong_client_msg_no": deterministicClientMessageNo(row.SourceKey, 0),
		"notice_attempts": 0, "notice_due_at": now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	dying, cancel := context.WithCancel(t.Context())
	cancel()
	p.deliverNotice(dying, row, now)
	if sends["ok"]+sends["failed"] != 0 {
		t.Fatalf("interrupted send reached WuKong: %v", sends)
	}
	if got := reloadRow(t, db, row.ID); got.NoticeDueAt == nil || got.NoticeAttempts != 1 {
		t.Fatalf("interrupted attempt cleared the owed notice: %+v", got)
	}
	p.resendDueNotices(t.Context(), now.Add(time.Hour))
	if got := reloadRow(t, db, row.ID); sends["ok"] != 1 || got.NoticeSentAt == nil || got.NoticeDueAt != nil {
		t.Fatalf("owed notice not delivered after an interrupted attempt: %v %+v", sends, got)
	}
}

// Owed notices are resent on their own goroutine while the inbox worker is
// blocked in a long turn.
func TestNoticeResenderRunsWhileWorkerBusy(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	owed := longRequestRow(t, db, "owed-turn", 9)
	if err := db.Model(&IMWebhookInbox{}).Where("id = ?", owed.ID).Updates(map[string]any{
		"state": inboxDead, "lease_owner": "", "final_wu_kong_client_msg_no": deterministicClientMessageNo(owed.SourceKey, 0),
		"notice_due_at": time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	// A long turn is in flight on the worker (never released during the test).
	_ = longRequestRow(t, db, "busy-long-turn", 10)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go p.runNoticeResender(ctx, 20*time.Millisecond)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := reloadRow(t, db, owed.ID); got.NoticeSentAt != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("owed notice not resent while a long turn held the worker: %+v", reloadRow(t, db, owed.ID))
}

// Continuation deadline survives a reconnect: the first stream sees the
// delegation and the empty result, then drops; the resumed stream (cursor
// already past both rows, so neither is replayed) still fails at the original
// deadline instead of waiting forever.
func TestContinuationDeadlineSurvivesReconnect(t *testing.T) {
	previous := coreContinuationWait
	coreContinuationWait = 300 * time.Millisecond
	t.Cleanup(func() { coreContinuationWait = previous })
	p, db, core := longRequestFixture(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var calls atomic.Int32
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			emitCoreMessage(w, 7, "user", "text", "q", "resume-turn")
			emitCoreTool(w, 8, "delegate_task", "completed")
			emitCoreMessage(w, 9, "agent", "result", "", "")
			emitCoreMessage(w, 10, "system", "status", "progress", "") // moves the cursor past 9
			return                                                     // connection drops
		}
		if r.Header.Get("Last-Event-ID") != "10" {
			t.Errorf("resume cursor=%q, want 10", r.Header.Get("Last-Event-ID"))
		}
		flusher := w.(http.Flusher)
		for { // only pings after the resume: nothing replays rows 8 and 9
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
	row := longRequestRow(t, db, "resume-turn", 9)
	if err := p.consumeCoreStream(t.Context(), row); err == nil {
		t.Fatal("first stream ended without a terminal event and must be retried")
	}
	stored := reloadRow(t, db, row.ID)
	if !stored.DelegationStarted || stored.ContinuationDueAt == nil {
		t.Fatalf("delegation state not persisted: %+v", stored)
	}
	due := *stored.ContinuationDueAt
	resumed := stored // as a fresh lease would read it
	done := make(chan error, 1)
	go func() { done <- p.consumeCoreStream(t.Context(), &resumed) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed stream reset the continuation wait and never ended")
	}
	if time.Now().Before(due) {
		t.Fatalf("ended before the persisted deadline")
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxDead || got.LastErrorCode != "im_continuation_timeout" || got.LeaseOwner != "" || got.NoticeSentAt == nil {
		t.Fatalf("row=%+v, want dead with the notice sent", got)
	}
}

// A lease picked up after the deadline already passed (restart during the
// wait) ends immediately, without opening the stream.
func TestContinuationDeadlinePassedWhileDownEndsTurn(t *testing.T) {
	p, db, core := longRequestFixture(t)
	opened := atomic.Bool{}
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		opened.Store(true)
	}
	row := longRequestRow(t, db, "down-turn", 9)
	past := time.Now().Add(-time.Minute)
	if err := db.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).Updates(map[string]any{
		"delegation_started": true, "continuation_due_at": past,
	}).Error; err != nil {
		t.Fatal(err)
	}
	fresh := reloadRow(t, db, row.ID)
	if err := p.consumeCoreStream(t.Context(), &fresh); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if opened.Load() {
		t.Fatal("stream opened although the deadline had passed")
	}
	if got := reloadRow(t, db, row.ID); got.State != inboxDead || got.LastErrorCode != "im_continuation_timeout" {
		t.Fatalf("row=%+v", got)
	}
}

// More than 64 tool rows after the delegate_task: Core's resume replays only
// the last 64, so the delegation must come from the row, not from replay.
func TestDelegationSurvivesMoreThan64ToolRows(t *testing.T) {
	p, db, core := longRequestFixture(t)
	var calls atomic.Int32
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if calls.Add(1) == 1 {
			emitCoreMessage(w, 7, "user", "text", "q", "many-tools")
			emitCoreTool(w, 8, "delegate_task", "completed")
			for i := range 70 {
				emitCoreTool(w, 9+i, "read_file", "completed")
			}
			return // drops before any result
		}
		// Resume: only the newest 64 tool rows are replayed (no delegate_task),
		// then the empty result and, later, the continuation answer.
		for i := 6; i < 70; i++ {
			emitCoreTool(w, 9+i, "read_file", "completed")
		}
		emitCoreMessage(w, 100, "agent", "result", "", "")
		emitCoreMessage(w, 101, "agent", "result", "delegated answer", "")
	}
	row := longRequestRow(t, db, "many-tools", 9)
	_ = p.consumeCoreStream(t.Context(), row)
	resumed := reloadRow(t, db, row.ID)
	if !resumed.DelegationStarted {
		t.Fatalf("delegation not persisted: %+v", resumed)
	}
	if err := p.consumeCoreStream(t.Context(), &resumed); err != nil {
		t.Fatalf("resume: %v", err)
	}
	sent := core.notices()
	if len(sent) != 1 || sentPayload(t, sent[0]).Content != "delegated answer" {
		t.Fatalf("sent=%+v, want only the continuation answer (empty result not taken as the reply)", sent)
	}
}

// A delegated turn timed out in IM and its continuation answer lands in Core
// late. That answer belongs to the timed-out turn (Core keeps it); the user's
// next question gets its own answer, never the late one.
func TestLateContinuationAnswerNotGivenToFollowUp(t *testing.T) {
	previous := coreContinuationWait
	coreContinuationWait = 100 * time.Millisecond
	t.Cleanup(func() { coreContinuationWait = previous })
	p, db, core := longRequestFixture(t)
	var calls atomic.Int32
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "slow-turn")
		emitCoreTool(w, 8, "delegate_task", "completed")
		emitCoreMessage(w, 9, "agent", "result", "", "")
		if calls.Add(1) == 1 {
			time.Sleep(300 * time.Millisecond) // continuation not here before the IM deadline
			return
		}
		emitCoreMessage(w, 11, "agent", "result", "late delegated answer", "") // after the IM timeout
		emitCoreMessage(w, 12, "user", "text", "next", "next-turn")
		emitCoreMessage(w, 13, "agent", "result", "next answer", "")
	}
	first := longRequestRow(t, db, "slow-turn", 9)
	if err := p.consumeCoreStream(t.Context(), first); err != nil {
		t.Fatalf("first: %v", err)
	}
	if got := reloadRow(t, db, first.ID); got.State != inboxDead || got.LastErrorCode != "im_continuation_timeout" {
		t.Fatalf("first=%+v", got)
	}
	next := longRequestRow(t, db, "next-turn", 9)
	if err := p.consumeCoreStream(t.Context(), next); err != nil {
		t.Fatalf("next: %v", err)
	}
	got := reloadRow(t, db, next.ID)
	if got.State != inboxCompleted || got.CumulativeAnswer != "next answer" {
		t.Fatalf("next=%+v, want its own answer", got)
	}
	for _, s := range core.notices() {
		if sentPayload(t, s).Content == "late delegated answer" {
			t.Fatalf("late answer was delivered to the follow-up: %+v", core.notices())
		}
	}
}
