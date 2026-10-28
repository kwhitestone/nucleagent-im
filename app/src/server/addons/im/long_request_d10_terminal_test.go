package im

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// MUYU-160 D10 (zero-cost technical track): the three non-success outcomes driven end to end through the real
// worker loop (lease -> im-dispatch -> Core stream -> outcome -> notice -> follow-up) against an isolated local
// fake Core (busyCore: enforces busy exactly like Core's im-dispatch). Assertions the earlier tests did not make:
//   1. the turn was ADMITTED and RAN before it failed (dispatch accepted, Core stream opened, row was processing
//      under a lease) — a pre-admission 400 cannot pass;
//   2. processing cleared: the row ends with no lease owner, no lease expiry, no next attempt;
//   3. Core released: not executing any more (the follow-up is not refused as busy);
//   4. exactly ONE post reaches the chat for the failed turn, carrying the failure notice, under the turn's
//      deterministic client number (Core error id for an execution error, id 0 for IM's own timeout), no answer;
//   5. the follow-up is answered once, with its own text, and is not refused.
// Short clocks and a local fake: this is a technical regression only, never a substitute for the real DEV
// execution failure, the real 30-minute continuation timeout or any environment acceptance.

func TestD10TerminalOutcomesThroughWorker(t *testing.T) {
	prevWait, prevIdle, prevDelays := coreContinuationWait, coreStreamIdleTimeout, retryDelays
	coreContinuationWait, coreStreamIdleTimeout = 300*time.Millisecond, 150*time.Millisecond
	retryDelays = []time.Duration{10 * time.Millisecond} // one retry, then exhausted (stream-idle path)
	t.Cleanup(func() { coreContinuationWait, coreStreamIdleTimeout, retryDelays = prevWait, prevIdle, prevDelays })
	for _, tc := range []struct {
		name, mode, code string
		noticeID         uint // Core message id the failure notice is numbered by
		stopped          bool // IM must have stopped the Core turn (im-turn-stop) before ending it
	}{
		{"execution-error", "fail", "im_execution_failed", 8, false},
		{"blocked-stream", "blocked", "im_internal_error", 0, true},
		{"continuation-timeout", "delegate-forever", "im_continuation_timeout", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, db, core := busyFixture(t, map[string]string{"first": tc.mode, "second": "answer"})
			admittedRunning := make(chan IMWebhookInbox, 4)
			core.onStream = func(turn string) {
				if turn != "first" {
					return
				}
				var row IMWebhookInbox
				if err := db.Where("source_key = ?", "first").First(&row).Error; err == nil {
					admittedRunning <- row
				}
			}
			first := pendingRow(t, db, "first", "1@42", 1)
			ctx, cancel := context.WithCancel(t.Context())
			go p.runWorker(ctx)
			t.Cleanup(func() { cancel(); <-p.done })

			// 1. admitted and running
			select {
			case row := <-admittedRunning:
				if row.State != inboxProcessing || row.LeaseOwner == "" || row.LeaseExpiresAt == nil || row.CoreConversationID != 9 {
					t.Fatalf("while Core ran the turn: row=%+v, want processing under a lease, admitted to conversation 9", row)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the turn never reached Core's stream: it was not admitted and run")
			}

			got := waitState(t, db, first.ID, inboxDead, 10*time.Second)
			// 2. processing cleared
			if got.LeaseOwner != "" || got.LeaseExpiresAt != nil || got.NextAttemptAt != nil || got.LastErrorCode != tc.code {
				t.Fatalf("ended row=%+v, want dead with code %s and no lease / no next attempt", got, tc.code)
			}
			if got.CoreStopCode != "" {
				t.Fatalf("ended row still owes a Core stop: %q", got.CoreStopCode)
			}
			// 4. exactly one post for the failed turn: the notice, under the deterministic number
			wantNo := deterministicClientMessageNo("first", tc.noticeID)
			waitPosts(t, core, 1, 5*time.Second)
			core.mu.Lock()
			firstPosts := append([]noticeSend(nil), core.posts...)
			core.mu.Unlock()
			if len(firstPosts) != 1 || firstPosts[0].ClientMsgNo != wantNo {
				t.Fatalf("posts for the failed turn=%v, want exactly one under %s", clientNos(firstPosts), wantNo)
			}
			if text := payloadOf(t, firstPosts[0]); text.SystemNotice == "" || strings.Contains(text.Content, "answer") {
				t.Fatalf("the failed turn's post is not a failure notice: %+v", text)
			}

			// 3 + 5. Core released: the follow-up runs through the real dispatch and is answered once
			second := pendingRow(t, db, "second", "1@42", 1)
			ans := waitState(t, db, second.ID, inboxCompleted, 10*time.Second)
			if ans.CumulativeAnswer != "second answer" || ans.LeaseOwner != "" {
				t.Fatalf("follow-up=%+v", ans)
			}
			waitPosts(t, core, 2, 5*time.Second)
			core.mu.Lock()
			defer core.mu.Unlock()
			if core.busy != 0 || core.executing {
				t.Fatalf("Core busy refusals=%d executing=%v: the failed turn kept Core occupied", core.busy, core.executing)
			}
			if len(core.posts) != 2 || core.posts[1].ClientMsgNo == wantNo {
				t.Fatalf("posts=%v, want the notice then exactly one follow-up answer", clientNos(core.posts))
			}
			if text := payloadOf(t, core.posts[1]); text.Content != "second answer" || text.SystemNotice != "" {
				t.Fatalf("follow-up post=%+v", text)
			}
			if stopped := len(core.stops) == 1 && core.stops[0] == "first"; stopped != tc.stopped {
				t.Fatalf("Core stops=%v, want stopped=%v", core.stops, tc.stopped)
			}
		})
	}
}

func waitPosts(t *testing.T, core *busyCore, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		core.mu.Lock()
		got := len(core.posts)
		core.mu.Unlock()
		if got >= n {
			time.Sleep(200 * time.Millisecond) // a duplicate would land now
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("posts=%d after %v, want %d", got, within, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func payloadOf(t *testing.T, s noticeSend) textPayload {
	t.Helper()
	var p textPayload
	if err := json.Unmarshal(s.Payload, &p); err != nil {
		t.Fatalf("payload: %v", err)
	}
	return p
}

func clientNos(posts []noticeSend) []string {
	out := make([]string, len(posts))
	for i, p := range posts {
		out[i] = p.ClientMsgNo
	}
	return out
}

var _ = context.Background
