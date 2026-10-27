package im

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
)

// MUYU-160 review 9 (REJECT 2×P1 + P2).

// dedupReceiver models the receiving side's idempotency contract that IM relies
// on: a post carrying a client_msg_no it already stored is acknowledged and not
// stored again (WuKong dedups on client_msg_no; IM's own message store also has
// a unique message id). It records every POST attempt and every stored message,
// and can fail or lose responses on demand.
type dedupReceiver struct {
	mu       sync.Mutex
	attempts int
	stored   []noticeSend
	seen     map[string]bool
	mode     atomic.Int32 // 0 ok, 1 reject 403 (not stored), 2 store but drop the response
	// Pause barrier (optional, set before the first post): the first post is
	// counted, then held before it is stored until release is closed. held is
	// set once it is held.
	release chan struct{}
	held    atomic.Bool
}

// holdFirstPost makes the next first post wait inside the receiver, before it
// is stored, until the returned release function is called.
func (r *dedupReceiver) holdFirstPost() (release func()) {
	r.release = make(chan struct{})
	var once sync.Once
	return func() { once.Do(func() { close(r.release) }) }
}

func (r *dedupReceiver) messages() []noticeSend {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]noticeSend(nil), r.stored...)
}

func newDedupReceiver(t *testing.T) (*dedupReceiver, string) {
	t.Helper()
	r := &dedupReceiver{seen: map[string]bool{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/message/send" {
			http.NotFound(w, req)
			return
		}
		var body noticeSend
		_ = json.NewDecoder(req.Body).Decode(&body)
		r.mu.Lock()
		r.attempts++
		first := r.attempts == 1
		r.mu.Unlock()
		if first && r.release != nil {
			r.held.Store(true)
			<-r.release // the first post is in flight, not yet stored
		}
		r.mu.Lock()
		mode := r.mode.Load()
		if mode == 1 {
			r.mu.Unlock()
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if !r.seen[body.ClientMsgNo] {
			r.seen[body.ClientMsgNo] = true
			r.stored = append(r.stored, body)
		}
		r.mu.Unlock()
		if mode == 2 {
			// Stored, but the response never reaches IM.
			hijacker, _ := w.(http.Hijacker)
			conn, _, _ := hijacker.Hijack()
			_ = conn.Close()
		}
	}))
	t.Cleanup(server.Close)
	return r, server.URL
}

func (r *dedupReceiver) counts() (attempts, stored int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts, len(r.stored)
}

// P1: the answer is claimed, WuKong refuses the post (403), and later works
// again. The claimed answer stays owed and is delivered exactly once; the
// question is never dispatched again and no failure notice is posted.
func TestClaimedAnswerSurvivesSendRefusalAndIsDeliveredOnce(t *testing.T) {
	p, db, core := longRequestFixture(t)
	receiver, url := newDedupReceiver(t)
	p.apiAddr = url
	var dispatched atomic.Int32
	core.turns["/api/v1/addons/conversation/im-dispatch"] = func(w http.ResponseWriter, r *http.Request) {
		dispatched.Add(1)
		http.Error(w, "must not be dispatched again", http.StatusTeapot)
	}
	row := longRequestRow(t, db, "claimed-403", 9)
	receiver.mode.Store(1)
	err := p.sendFinal(t.Context(), row, 8, "the answer")
	if err == nil {
		t.Fatal("refused post reported success")
	}
	p.failInbox(row, err, time.Now())
	got := reloadRow(t, db, row.ID)
	if got.State != inboxRetryable || got.FinalCoreMessageID != 8 || got.NoticeDueAt != nil {
		t.Fatalf("after the refused post: %+v, want retryable with the answer claimed", got)
	}
	receiver.mode.Store(0)
	leased, err := leaseInbox(t.Context(), global.PRISM_DB, "w2", time.Now().Add(time.Hour))
	if err != nil || leased == nil {
		t.Fatalf("re-lease: %+v %v", leased, err)
	}
	p.processLeased(t.Context(), leased)
	got = reloadRow(t, db, row.ID)
	attempts, stored := receiver.counts()
	if got.State != inboxCompleted || got.CumulativeAnswer != "the answer" || stored != 1 || attempts != 2 || dispatched.Load() != 0 {
		t.Fatalf("row=%+v attempts=%d stored=%d dispatched=%d, want completed, 2 attempts, 1 message, no re-dispatch",
			got, attempts, stored, dispatched.Load())
	}
}

// P2: WuKong stored the answer but its response was lost. IM retries the same
// claimed post; the receiver keeps one message (POST attempts and stored
// messages counted separately).
func TestLostResponseResendStoresOneMessage(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	receiver, url := newDedupReceiver(t)
	p.apiAddr = url
	row := longRequestRow(t, db, "lost-response", 9)
	receiver.mode.Store(2)
	err := p.sendFinal(t.Context(), row, 8, "the answer")
	if err == nil {
		t.Fatal("lost response reported success")
	}
	p.failInbox(row, err, time.Now())
	receiver.mode.Store(0)
	leased, _ := leaseInbox(t.Context(), global.PRISM_DB, "w2", time.Now().Add(time.Hour))
	if leased == nil {
		t.Fatal("claimed answer not re-leased for delivery")
	}
	p.processLeased(t.Context(), leased)
	attempts, stored := receiver.counts()
	if got := reloadRow(t, db, row.ID); got.State != inboxCompleted || attempts != 2 || stored != 1 {
		t.Fatalf("row=%s attempts=%d stored=%d, want completed after 2 attempts and 1 message", got.State, attempts, stored)
	}
}

// P2 reverse order: the failure is claimed first (continuation timeout, Core
// stopped), then a late answer arrives for the same turn. Only the failure
// notice exists; the late answer is never posted.
func TestFailureClaimedFirstNeverPostsLateAnswer(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	receiver, url := newDedupReceiver(t)
	p.apiAddr = url
	row := longRequestRow(t, db, "failure-first", 9)
	if err := p.endContinuationTimeout(t.Context(), row); err != nil {
		t.Fatalf("timeout: %v", err)
	}
	late := *row
	if err := p.sendFinal(t.Context(), &late, 8, "late answer"); err == nil {
		t.Fatal("late answer accepted for a turn that already failed")
	}
	attempts, stored := receiver.counts()
	got := reloadRow(t, db, row.ID)
	if got.State != inboxDead || got.FinalCoreMessageID != 0 || attempts != 1 || stored != 1 {
		t.Fatalf("row=%+v attempts=%d stored=%d, want dead, only the notice posted", got, attempts, stored)
	}
	receiver.mu.Lock()
	defer receiver.mu.Unlock()
	var payload textPayload
	_ = json.Unmarshal(receiver.stored[0].Payload, &payload)
	if payload.SystemNotice != execFailedNotice {
		t.Fatalf("stored %+v, want the failure notice", payload)
	}
}

// P1: a refused delivery (recipient disabled) of a turn whose answer is
// claimed: nothing is posted, Core's run is stopped first, and only then the
// turn ends.
func TestRefusalAfterClaimDeliversNothingAndStopsCore(t *testing.T) {
	p, db, core := longRequestFixture(t)
	receiver, url := newDedupReceiver(t)
	p.apiAddr = url
	var stops atomic.Int32
	core.turns["/api/v1/addons/conversation/im-turn-stop"] = func(w http.ResponseWriter, r *http.Request) {
		stops.Add(1)
		fmt.Fprint(w, `{"stopped":true}`)
	}
	row := longRequestRow(t, db, "claim-then-refuse", 9)
	receiver.mode.Store(1)
	err := p.sendFinal(t.Context(), row, 8, "the answer")
	p.failInbox(row, err, time.Now()) // claimed, post refused by WuKong: owed
	receiver.mode.Store(0)
	if err := db.Model(&authmodel.User{}).Where("id = ?", 1).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	leased, _ := leaseInbox(t.Context(), global.PRISM_DB, "w2", time.Now().Add(time.Hour))
	if leased == nil {
		t.Fatal("owed answer not re-leased")
	}
	p.processLeased(t.Context(), leased) // policy refusal now
	got := reloadRow(t, db, row.ID)
	_, stored := receiver.counts()
	if got.State != inboxDead || got.LastErrorCode != "im_recipient_disabled" || stored != 0 || stops.Load() != 1 {
		t.Fatalf("row=%+v stored=%d stops=%d, want dead after a Core stop, nothing delivered", got, stored, stops.Load())
	}
}

// P1: SSE refused with 401/403 for a turn Core accepted: Core's run is stopped
// before the turn ends; while the stop fails the turn stays owed.
func TestSSERefusalStopsCoreBeforeEnding(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			p, db, core := longRequestFixture(t)
			var stopOK atomic.Bool
			var stops atomic.Int32
			core.turns["/api/v1/addons/conversation/im-turn-stop"] = func(w http.ResponseWriter, r *http.Request) {
				stops.Add(1)
				if !stopOK.Load() {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				fmt.Fprint(w, `{"stopped":true}`)
			}
			core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}
			row := longRequestRow(t, db, "sse-refused", 9)
			err := p.consumeCoreStream(t.Context(), row)
			p.failInbox(row, err, time.Now())
			if got := reloadRow(t, db, row.ID); got.State == inboxDead || got.CoreStopCode == "" {
				t.Fatalf("SSE %d ended the turn without a confirmed Core stop: %+v", status, got)
			}
			stopOK.Store(true)
			leased, _ := leaseInbox(t.Context(), global.PRISM_DB, "w2", time.Now().Add(time.Hour))
			if leased == nil {
				t.Fatal("owed stop not re-leased")
			}
			p.processLeased(t.Context(), leased)
			if got := reloadRow(t, db, row.ID); got.State != inboxDead || got.CoreStopCode != "" || stops.Load() != 2 {
				t.Fatalf("after the stop: %+v stops=%d", got, stops.Load())
			}
		})
	}
}

// P1: while a re-leased turn waits on its owed Core stop, a browser snapshot
// check that Core lifecycle refuses must not end the turn or release its lock.
func TestSnapshotRefusalDuringOwedStopKeepsDebt(t *testing.T) {
	p, db, core := longRequestFixture(t)
	release := make(chan struct{})
	var refuse atomic.Bool
	core.turns["/api/v1/addons/conversation/im-turn-stop"] = func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusBadGateway)
	}
	row := longRequestRow(t, db, "owed-stop", 9)
	if err := db.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).Updates(map[string]any{
		"core_stop_code": "im_continuation_timeout", "core_stop_notice": true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	row.CoreStopCode, row.CoreStopNotice = "im_continuation_timeout", true
	done := make(chan struct{})
	go func() { p.processLeased(context.Background(), row); close(done) }()
	time.Sleep(50 * time.Millisecond) // the worker is inside its stop call
	refuse.Store(true)
	var refusedChecks atomic.Int32
	core.mu.Lock() // the worker goroutine reads the handler map concurrently
	core.turns[lifecyclePath] = func(w http.ResponseWriter, r *http.Request) {
		if refuse.Load() {
			refusedChecks.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"code":"unauthorized"}`)
			return
		}
		fmt.Fprint(w, `{"allowed":true}`)
	}
	core.mu.Unlock()
	rec := httptest.NewRecorder()
	p.sendAgentSnapshots(t.Context(), rec, rec, 1, row.ChannelID, row.ChannelType, time.Time{}, map[uint]string{})
	if refusedChecks.Load() == 0 {
		t.Fatal("the snapshot path never reached Core's lifecycle refusal")
	}
	if got := reloadRow(t, db, row.ID); got.State != inboxProcessing || got.LeaseOwner != row.LeaseOwner || got.CoreStopCode == "" {
		t.Fatalf("snapshot refusal ended or unlocked the turn: %+v", got)
	}
	close(release)
	<-done
	if got := reloadRow(t, db, row.ID); got.State != inboxRetryable || got.CoreStopCode != "im_continuation_timeout" {
		t.Fatalf("after the failed stop: %+v, want still owed", got)
	}
}
