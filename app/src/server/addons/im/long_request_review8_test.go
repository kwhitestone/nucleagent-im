package im

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
)

// MUYU-160 review 8 (REJECT, 3×P1).

// P1-1: the delivery check refuses (recipient disabled), the holder pauses
// before it records the refusal, another instance takes the turn over, then the
// old holder resumes: it must not end or unlock the new holder's turn.
func TestRefusedHolderPausedThenTakenOverCannotEndTurn(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	row := longRequestRow(t, db, "refused-stale", 9)
	stale := *row
	taken := takeOver(t, row) // happens while the old holder is paused
	if err := db.Model(&authmodel.User{}).Where("id = ?", 1).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	err := p.checkDelivery(t.Context(), &stale)
	if !errors.Is(err, errLeaseLost) {
		t.Fatalf("stale refusal: %v, want errLeaseLost", err)
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxProcessing || got.LeaseOwner != taken.LeaseOwner || got.Attempts != taken.Attempts {
		t.Fatalf("stale refusal ended the new holder's turn: %+v", got)
	}
	// The new holder sees the same refusal and ends its own turn through the
	// worker's exit, which stops the Core run first (review 9).
	var denied *httpStatusError
	err = p.checkDelivery(t.Context(), &taken)
	if !errors.As(err, &denied) {
		t.Fatalf("new holder refusal: %v", err)
	}
	p.failInbox(&taken, err, time.Now())
	if got := reloadRow(t, db, row.ID); got.State != inboxDead || got.LastErrorCode != "im_recipient_disabled" {
		t.Fatalf("new holder could not end its refused turn: %+v", got)
	}
}

// P1-2: holder A reads the answer, passes its last lease check and pauses
// before posting. B takes over and its continuation deadline passes. B must not
// post a failure notice for a turn whose answer is already claimed: it
// delivers A's claimed answer, with the same client number A will post, so the
// chat ends up with one answer and no contradictory failure line.
//
// Independent review (2026-10-27, P2): the posts go to dedupReceiver, which
// holds A's post in flight (counted, not yet stored) while B takes over and
// delivers; A's post lands after B's. Both POST attempts are counted, and the
// receiver must hold exactly one message, the claimed answer.
func TestPausedSenderAndTimedOutTakeoverPostOneOutcome(t *testing.T) {
	p, db, core := longRequestFixture(t)
	receiver, url := newDedupReceiver(t)
	p.apiAddr = url
	release := receiver.holdFirstPost()
	t.Cleanup(release) // never leave A's request hanging on a failed assertion
	row := longRequestRow(t, db, "one-outcome", 9)
	a := *row
	doneA := make(chan error, 1)
	go func() { doneA <- p.sendFinal(t.Context(), &a, 8, "the answer") }()
	for !receiver.held.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	// A is paused with its post in flight. Its lease expires, B takes over and
	// gives up on the continuation: B delivers the claimed answer.
	b := takeOver(t, row)
	if err := p.endContinuationTimeout(t.Context(), &b); err != nil {
		t.Fatalf("B: %v", err)
	}
	if attempts, stored := receiver.counts(); attempts != 2 || stored != 1 {
		t.Fatalf("before A resumes: attempts=%d stored=%d, want A in flight and B's post stored", attempts, stored)
	}
	release() // A's post now reaches the receiver, after B's
	if err := <-doneA; !errors.Is(err, errLeaseLost) {
		t.Fatalf("A after resume: %v, want errLeaseLost (B finished the turn)", err)
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxCompleted || got.CumulativeAnswer != "the answer" || got.NoticeDueAt != nil ||
		got.LeaseOwner != "" || got.Attempts != b.Attempts {
		t.Fatalf("row=%+v, want completed by B with A's answer and no notice owed", got)
	}
	attempts, stored := receiver.counts()
	if attempts != 2 || stored != 1 {
		t.Fatalf("attempts=%d stored=%d, want A's and B's posts counted and exactly 1 message stored", attempts, stored)
	}
	want := deterministicClientMessageNo("one-outcome", 8)
	for _, s := range receiver.messages() {
		var payload textPayload
		_ = json.Unmarshal(s.Payload, &payload)
		if s.ClientMsgNo != want || payload.Content != "the answer" || payload.SystemNotice != "" {
			t.Fatalf("stored %+v (%q): the one message must be the claimed answer, no failure notice", s, payload.Content)
		}
	}
	if n := len(core.notices()); n != 0 {
		t.Fatalf("%d posts reached another receiver", n)
	}
}

// P1-3: Core's stop keeps failing (unreachable, then a plain 404 from a Core
// without the route, then 401). The turn is never ended or released: no
// failure notice, no next turn dispatched into Core. Only once Core confirms
// does the turn end, and then the follow-up runs through the real dispatch path.
func TestCoreStopFailingUntilConfirmedKeepsTurnOpen(t *testing.T) {
	previousWait, previousDelays := coreContinuationWait, coreStopRetryDelays
	coreContinuationWait, coreStopRetryDelays = 100*time.Millisecond, []time.Duration{20 * time.Millisecond}
	t.Cleanup(func() { coreContinuationWait, coreStopRetryDelays = previousWait, previousDelays })
	p, db, core := busyFixture(t, map[string]string{"first": "delegate-forever", "second": "answer"})
	var stopMode atomic.Int32 // 0 unreachable(502), 1 plain 404, 2 401, 3 Core answers
	var stopCalls atomic.Int32
	core.stopOverride = func(w http.ResponseWriter) bool {
		stopCalls.Add(1)
		switch stopMode.Load() {
		case 0:
			w.WriteHeader(http.StatusBadGateway)
		case 1:
			http.NotFound(w, nil) // route missing: not "im_turn_not_running"
		case 2:
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"code":"unauthorized"}`)
		default:
			return false
		}
		return true
	}
	first := pendingRow(t, db, "first", "1@42", 1)
	second := pendingRow(t, db, "second", "1@42", 1)
	ctx, cancel := context.WithCancel(t.Context())
	go p.runWorker(ctx)
	t.Cleanup(func() { cancel(); <-p.done })
	for mode := int32(0); mode < 3; mode++ {
		stopMode.Store(mode)
		before := stopCalls.Load()
		deadline := time.Now().Add(5 * time.Second)
		for stopCalls.Load() < before+3 {
			if time.Now().After(deadline) {
				t.Fatalf("mode %d: stop not retried (calls=%d)", mode, stopCalls.Load())
			}
			time.Sleep(10 * time.Millisecond)
		}
		f, s := reloadRow(t, db, first.ID), reloadRow(t, db, second.ID)
		if f.State == inboxDead || f.CoreStopCode != "im_continuation_timeout" || f.NoticeDueAt != nil {
			t.Fatalf("mode %d: first=%+v, want open with the stop owed and no notice", mode, f)
		}
		if s.State != inboxPending {
			t.Fatalf("mode %d: second=%s, must wait behind the unstopped turn", mode, s.State)
		}
	}
	stopMode.Store(3) // Core reachable again
	waitState(t, db, first.ID, inboxDead, 5*time.Second)
	got := waitState(t, db, second.ID, inboxCompleted, 5*time.Second)
	if got.CumulativeAnswer != "second answer" {
		t.Fatalf("follow-up=%+v", got)
	}
	f := reloadRow(t, db, first.ID)
	if f.LastErrorCode != "im_continuation_timeout" || f.CoreStopCode != "" || f.NoticeDueAt != nil && f.NoticeSentAt == nil {
		t.Fatalf("first after the stop=%+v, want ended with its notice", f)
	}
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.busy != 0 {
		t.Fatalf("follow-up hit a busy Core %d times", core.busy)
	}
}

// A turn given up after its stream retries ran out (stream kept dropping) keeps
// its Core stop owed as well, across later leases.
func TestExhaustedTurnKeepsStopOwedUntilConfirmed(t *testing.T) {
	p, db, core := longRequestFixture(t)
	var ok atomic.Bool
	core.turns["/api/v1/addons/conversation/im-turn-stop"] = func(w http.ResponseWriter, r *http.Request) {
		if !ok.Load() {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"stopped":true}`)
	}
	row := longRequestRow(t, db, "exhausted-stop", 9)
	exhaustRetries(t, db, row)
	p.failInbox(row, errCoreStreamIdle, time.Now())
	got := reloadRow(t, db, row.ID)
	if got.State != inboxRetryable || got.CoreStopCode != errCoreStreamIdleCode() || got.NoticeDueAt != nil || len(core.notices()) != 0 {
		t.Fatalf("row=%+v notices=%d, want parked with the stop owed and nothing posted", got, len(core.notices()))
	}
	ok.Store(true)
	leased, err := leaseInbox(t.Context(), global.PRISM_DB, "w2", time.Now().Add(time.Hour))
	if err != nil || leased == nil {
		t.Fatalf("re-lease: %+v %v", leased, err)
	}
	p.processLeased(t.Context(), leased)
	got = reloadRow(t, db, row.ID)
	if got.State != inboxDead || got.CoreStopCode != "" || got.NoticeSentAt == nil || len(core.notices()) != 1 {
		t.Fatalf("after Core confirmed: %+v notices=%d", got, len(core.notices()))
	}
}

func errCoreStreamIdleCode() string { return "im_internal_error" }

// wrapSendFunc is a WuKong /message/send that hands every post to record.
func wrapSendFunc(t *testing.T, record func(noticeSend)) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/message/send" {
			http.NotFound(w, r)
			return
		}
		var body noticeSend
		_ = json.NewDecoder(r.Body).Decode(&body)
		record(body)
	}))
	t.Cleanup(server.Close)
	return server.URL
}
