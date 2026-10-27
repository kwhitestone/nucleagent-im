package im

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// MUYU-160 review 7 (P1-b): the follow-up after each terminal outcome now goes
// through the real worker path — lease, dispatch to Core, stream — against a
// Core that enforces busy exactly like Core's im-dispatch: while the
// conversation is executing a follow-up is refused with 409
// im_conversation_busy. A turn IM gave up on must therefore leave Core idle.

// busyCore models one Core conversation. A turn runs (executing) until its
// script ends it; a delegated turn's continuation keeps it executing until
// stopped. /im-turn-stop cancels the run of the named turn unless a newer turn
// was admitted.
type busyCore struct {
	mu        sync.Mutex
	executing bool
	current   string // source key of the latest admitted turn
	stops     []string
	busy      int
	answers   []string
	script    map[string]string // source key -> "answer" | "delegate-forever" | "fail" | "empty"
}

func (c *busyCore) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if allowLifecycle(w, r) {
			return
		}
		switch r.URL.Path {
		case "/channel/messagesync":
			fmt.Fprint(w, `{"messages":[]}`)
		case "/message/send":
			var body noticeSend
			_ = json.NewDecoder(r.Body).Decode(&body)
			var text textPayload
			_ = json.Unmarshal(body.Payload, &text)
			c.mu.Lock()
			c.answers = append(c.answers, text.Content)
			c.mu.Unlock()
		case "/api/v1/addons/conversation/im-dispatch":
			var input dispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			c.mu.Lock()
			defer c.mu.Unlock()
			if input.SourceKey != c.current {
				if c.executing { // Core: executing or blocked -> ErrConversationBusy
					c.busy++
					w.WriteHeader(http.StatusConflict)
					fmt.Fprint(w, `{"code":"im_conversation_busy"}`)
					return
				}
				c.current, c.executing = input.SourceKey, true
			}
			_ = json.NewEncoder(w).Encode(dispatchResult{ConversationID: 9, SourceKey: input.SourceKey, Status: "accepted"})
		case "/api/v1/addons/conversation/im-turn-stop":
			var input turnStopRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			c.mu.Lock()
			defer c.mu.Unlock()
			c.stops = append(c.stops, input.SourceKey)
			if input.SourceKey != c.current || !c.executing {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"code":"im_turn_not_running"}`)
				return
			}
			c.executing = false
			fmt.Fprint(w, `{"stopped":true}`)
		case "/api/v1/addons/conversation/9/messages/stream":
			c.mu.Lock()
			turn, mode := c.current, c.script[c.current]
			c.mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			emitCoreMessage(w, 7, "user", "text", "q", turn)
			switch mode {
			case "answer":
				c.finish()
				emitCoreMessage(w, 8, "agent", "result", turn+" answer", "")
			case "empty":
				c.finish()
				emitCoreMessage(w, 8, "agent", "result", "", "")
			case "fail":
				c.finish()
				emitCoreMessage(w, 8, "system", "error", "provider failed", "")
			case "delegate-forever":
				// turn 1 ends empty after a delegation; its continuation keeps the
				// conversation executing and never answers.
				emitCoreTool(w, 8, "delegate_task", "completed")
				emitCoreMessage(w, 9, "agent", "result", "", "")
				flusher := w.(http.Flusher)
				for {
					select {
					case <-r.Context().Done():
						return
					case <-time.After(20 * time.Millisecond):
						fmt.Fprint(w, ": ping\n\n")
						flusher.Flush()
					}
				}
			}
		default:
			t.Errorf("unexpected Core/WuKong call %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}
}

func (c *busyCore) finish() { c.mu.Lock(); c.executing = false; c.mu.Unlock() }

func busyFixture(t *testing.T, script map[string]string) (*Plugin, *gorm.DB, *busyCore) {
	t.Helper()
	db := concurrencyDB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	core := &busyCore{script: script}
	server := httptest.NewServer(core.handler(t))
	t.Cleanup(server.Close)
	return &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token", done: make(chan struct{})}, db, core
}

func TestFollowUpThroughDispatchAfterEachOutcome(t *testing.T) {
	previous := coreContinuationWait
	coreContinuationWait = 200 * time.Millisecond
	t.Cleanup(func() { coreContinuationWait = previous })
	for outcome, want := range map[string]string{
		"answer": inboxCompleted, "empty": inboxCompleted, "fail": inboxDead, "delegate-forever": inboxDead,
	} {
		t.Run(outcome, func(t *testing.T) {
			p, db, core := busyFixture(t, map[string]string{"first": outcome, "second": "answer"})
			first := pendingRow(t, db, "first", "1@42", 1)
			ctx, cancel := context.WithCancel(t.Context())
			go p.runWorker(ctx)
			t.Cleanup(func() { cancel(); <-p.done })
			waitState(t, db, first.ID, want, 5*time.Second)
			second := pendingRow(t, db, "second", "1@42", 1)
			got := waitState(t, db, second.ID, inboxCompleted, 5*time.Second)
			if got.CumulativeAnswer != "second answer" {
				t.Fatalf("follow-up answer=%q", got.CumulativeAnswer)
			}
			core.mu.Lock()
			defer core.mu.Unlock()
			if core.busy != 0 {
				t.Fatalf("follow-up was refused as busy %d times: Core still ran the first turn", core.busy)
			}
			if outcome == "delegate-forever" && (len(core.stops) != 1 || core.stops[0] != "first") {
				t.Fatalf("Core stops=%v, want exactly the timed-out turn", core.stops)
			}
			if outcome != "delegate-forever" && len(core.stops) != 0 {
				t.Fatalf("a finished turn was stopped: %v", core.stops)
			}
		})
	}
}

// The stop cannot reach Core: the turn is not ended (no failure notice, still
// processing for a retry), so IM never claims a turn over while Core runs it.
func TestContinuationTimeoutWaitsForCoreStop(t *testing.T) {
	previous := coreContinuationWait
	coreContinuationWait = 100 * time.Millisecond
	t.Cleanup(func() { coreContinuationWait = previous })
	p, db, core := longRequestFixture(t)
	core.turns["/api/v1/addons/conversation/im-turn-stop"] = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "stop-down")
		emitCoreTool(w, 8, "delegate_task", "completed")
		emitCoreMessage(w, 9, "agent", "result", "", "")
		time.Sleep(300 * time.Millisecond)
	}
	row := longRequestRow(t, db, "stop-down", 9)
	if err := p.consumeCoreStream(t.Context(), row); err == nil {
		t.Fatal("turn ended although Core could not be told to stop it")
	}
	if got := reloadRow(t, db, row.ID); got.State != inboxProcessing || got.NoticeDueAt != nil {
		t.Fatalf("row=%+v, want still processing for the retry", got)
	}
	if n := len(core.notices()); n != 0 {
		t.Fatalf("failure notice posted before Core stopped: %d", n)
	}
}

// IM gives up on a dispatched turn after its retries (the Core stream kept
// dropping): Core is told to stop that turn too. A turn Core refused as busy
// was never admitted there and is not stopped.
func TestExhaustedTurnStopsCoreTurn(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		stop bool
	}{
		{"stream-idle", errCoreStreamIdle, true},
		{"busy", &httpStatusError{status: http.StatusConflict, code: conversationBusyCode}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, db, core := longRequestFixture(t)
			var mu sync.Mutex
			var stops []string
			core.turns["/api/v1/addons/conversation/im-turn-stop"] = func(w http.ResponseWriter, r *http.Request) {
				var input turnStopRequest
				_ = json.NewDecoder(r.Body).Decode(&input)
				mu.Lock()
				stops = append(stops, input.SourceKey)
				mu.Unlock()
				fmt.Fprint(w, `{"stopped":true}`)
			}
			row := longRequestRow(t, db, "given-up", 9)
			exhaustRetries(t, db, row)
			p.failInbox(row, tc.err, time.Now())
			if got := reloadRow(t, db, row.ID); got.State != inboxDead {
				t.Fatalf("row=%+v, want dead", got)
			}
			mu.Lock()
			defer mu.Unlock()
			if stopped := len(stops) == 1 && stops[0] == "given-up"; stopped != tc.stop {
				t.Fatalf("stops=%v, want stop=%v", stops, tc.stop)
			}
		})
	}
}
