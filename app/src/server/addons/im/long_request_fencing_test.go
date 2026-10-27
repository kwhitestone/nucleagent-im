package im

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kwhitestone/prism-fusion/global"
)

// MUYU-160 review 7 (P1-a): a holder that was paused past its lease and came back
// must not end, answer or move the cursor of a turn another instance took over.

// takeOver gives row to another holder exactly as a lease takeover does.
func takeOver(t *testing.T, row *IMWebhookInbox) IMWebhookInbox {
	t.Helper()
	db := global.PRISM_DB
	if err := db.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).
		Update("lease_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	taken, err := leaseInbox(t.Context(), db, "instance-b", time.Now())
	if err != nil || taken == nil || taken.ID != row.ID {
		t.Fatalf("takeover lease=%+v err=%v", taken, err)
	}
	return *taken
}

// The paused holder's continuation deadline fires after another instance took
// the turn over: it must not mark the turn dead or post a failure notice.
func TestStaleHolderCannotFailTakenOverTurn(t *testing.T) {
	p, db, core := longRequestFixture(t)
	row := longRequestRow(t, db, "stale-fail", 9)
	takeOver(t, row)
	if err := p.endTurnWithFailure(t.Context(), row, "im_continuation_timeout", deterministicClientMessageNo(row.SourceKey, 0)); !errors.Is(err, errLeaseLost) {
		t.Fatalf("stale end: %v, want errLeaseLost", err)
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxProcessing || got.LeaseOwner != "instance-b" || got.NoticeDueAt != nil {
		t.Fatalf("stale holder ended the new holder's turn: %+v", got)
	}
	if n := len(core.notices()); n != 0 {
		t.Fatalf("stale holder posted %d notices", n)
	}
}

// The paused holder resumes in the middle of its stream and reads the answer:
// it must not post it, mark the turn completed, or move the cursor.
func TestStaleHolderCannotAnswerTakenOverTurn(t *testing.T) {
	p, db, core := longRequestFixture(t)
	row := longRequestRow(t, db, "stale-answer", 9)
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "stale-answer")
		emitCoreMessage(w, 8, "agent", "result", "late answer", "")
	}
	taken := takeOver(t, row)
	if err := p.consumeCoreStream(t.Context(), row); !errors.Is(err, errLeaseLost) {
		t.Fatalf("stale stream: %v, want errLeaseLost", err)
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxProcessing || got.LeaseOwner != "instance-b" || got.Attempts != taken.Attempts ||
		got.LastCoreEventID != "" || got.FinalSentAt != nil {
		t.Fatalf("stale holder wrote to the new holder's turn: %+v", got)
	}
	if n := len(core.notices()); n != 0 {
		t.Fatalf("stale holder posted %d messages", n)
	}
	// The new holder still finishes the turn normally.
	if err := p.consumeCoreStream(t.Context(), &taken); err != nil {
		t.Fatalf("new holder: %v", err)
	}
	if got := reloadRow(t, db, row.ID); got.State != inboxCompleted || got.CumulativeAnswer != "late answer" {
		t.Fatalf("new holder: %+v", got)
	}
}

// failInbox from a stale holder (its turn errored after the takeover) leaves the
// new holder's row alone: no retry scheduling, no notice.
func TestStaleHolderCannotRetryTakenOverTurn(t *testing.T) {
	p, db, core := longRequestFixture(t)
	row := longRequestRow(t, db, "stale-retry", 9)
	exhaustRetries(t, db, row) // the stale holder's last attempt: failing it would owe a notice
	takeOver(t, row)
	p.failInbox(row, errCoreStreamIdle, time.Now())
	if got := reloadRow(t, db, row.ID); got.State != inboxProcessing || got.LeaseOwner != "instance-b" {
		t.Fatalf("stale failInbox moved the new holder's turn: %+v", got)
	}
	if n := len(core.notices()); n != 0 {
		t.Fatalf("stale holder posted %d notices", n)
	}
}

// A holder whose renewals fail for most of the lease stops its turn before the
// lease can expire under it, instead of running on as a second holder.
func TestUnconfirmedLeaseStopsTurn(t *testing.T) {
	previousDuration, previousRenew := inboxLeaseDuration, inboxLeaseRenewInterval
	inboxLeaseDuration, inboxLeaseRenewInterval = 300*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { inboxLeaseDuration, inboxLeaseRenewInterval = previousDuration, previousRenew })
	p, db, core := longRequestFixture(t)
	var opened atomic.Bool
	core.turns["/api/v1/addons/conversation/im-dispatch"] = func(w http.ResponseWriter, r *http.Request) {
		var input dispatchRequest
		_ = json.NewDecoder(r.Body).Decode(&input)
		_ = json.NewEncoder(w).Encode(dispatchResult{ConversationID: 9, SourceKey: input.SourceKey, Status: "accepted"})
	}
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "lost-turn")
		opened.Store(true)
		<-r.Context().Done() // a long run; the turn must end because of the lease
	}
	row := longRequestRow(t, db, "lost-turn", 9)
	go func() {
		for !opened.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		takeOver(t, row) // the renewal now fails: another instance owns the row
	}()
	done := make(chan struct{})
	go func() { p.processLeased(t.Context(), row); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("turn kept running after its lease was lost")
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxProcessing || got.LeaseOwner != "instance-b" {
		t.Fatalf("stale holder touched the row on its way out: %+v", got)
	}
}
