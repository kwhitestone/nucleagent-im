package im

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// MUYU-160: an IM long request must always reach a terminal state — completed,
// failed or timed out — leave processing, release the worker, reply once, and
// let the next question in the same chat run normally.

type longRequestCore struct {
	mu    sync.Mutex
	sent  []noticeSend
	turns map[string]func(w http.ResponseWriter, r *http.Request)
}

func (c *longRequestCore) notices() []noticeSend {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]noticeSend(nil), c.sent...)
}

func longRequestFixture(t *testing.T) (*Plugin, *gorm.DB, *longRequestCore) {
	t.Helper()
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	core := &longRequestCore{turns: map[string]func(http.ResponseWriter, *http.Request){}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowLifecycle(w, r) {
			return
		}
		if r.URL.Path == "/message/send" {
			var body noticeSend
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			core.mu.Lock()
			core.sent = append(core.sent, body)
			core.mu.Unlock()
			return
		}
		core.mu.Lock()
		handler := core.turns[r.URL.Path]
		core.mu.Unlock()
		if handler == nil {
			http.NotFound(w, r)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token"}, db, core
}

func longRequestRow(t *testing.T, db *gorm.DB, source string, conversation uint) *IMWebhookInbox {
	t.Helper()
	row := &IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: source, SourceKey: source, TargetAgentUID: 42,
		SenderUID: 1, ChannelID: "1@42", ChannelType: personChannel, Text: "q", State: inboxProcessing,
		ExecutionOwnerUserID: 1, OriginSenderUID: 1, CoreConversationID: conversation, LeaseOwner: "w", Attempts: 1,
	}
	if err := db.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func emitCoreMessage(w http.ResponseWriter, id int, sender, kind, content, source string) {
	data, _ := json.Marshal(map[string]any{"id": id, "senderType": sender, "msgType": kind, "content": content, "sourceKey": source})
	fmt.Fprintf(w, "id: %d\nevent: message-created\ndata: %s\n\n", id, data)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func reloadRow(t *testing.T, db *gorm.DB, id uint) IMWebhookInbox {
	t.Helper()
	var row IMWebhookInbox
	if err := db.First(&row, id).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

// Success after a background delegation: turn 1 may end with an empty result
// while the delegated work continues; the answer arrives in the continuation and
// must be the one delivered, so an empty result is never treated as the reply.
func TestLongRequestEmptyResultWaitsForContinuation(t *testing.T) {
	p, db, core := longRequestFixture(t)
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "delegated-turn")
		emitCoreMessage(w, 8, "agent", "result", "", "")
		fmt.Fprint(w, ": ping\n\n")
		emitCoreMessage(w, 9, "agent", "result", "delegated answer", "")
	}
	row := longRequestRow(t, db, "delegated-turn", 9)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatalf("consume: %v", err)
	}
	got := reloadRow(t, db, row.ID)
	if got.State != inboxCompleted || got.FinalSentAt == nil || got.LeaseOwner != "" || got.LeaseExpiresAt != nil {
		t.Fatalf("row=%+v, want completed and released", got)
	}
	sent := core.notices()
	if len(sent) != 1 || sentPayload(t, sent[0]).Content != "delegated answer" {
		t.Fatalf("sent=%+v, want exactly the continuation answer", sent)
	}
}

// Timeout: a Core stream that goes silent (half-open) must not hold the turn
// processing forever; it ends with a retryable error that releases the worker.
func TestLongRequestIdleCoreStreamReleasesTurn(t *testing.T) {
	previous := coreStreamIdleTimeout
	coreStreamIdleTimeout = 150 * time.Millisecond
	t.Cleanup(func() { coreStreamIdleTimeout = previous })
	p, db, core := longRequestFixture(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		emitCoreMessage(w, 7, "user", "text", "q", "idle-turn")
		select { // hang without bytes, like a dropped proxy connection
		case <-release:
		case <-r.Context().Done():
		}
	}
	row := longRequestRow(t, db, "idle-turn", 9)
	done := make(chan error, 1)
	go func() { done <- p.consumeCoreStream(t.Context(), row) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumeCoreStream still blocked on a silent Core stream")
	}
	if !errors.Is(err, errCoreStreamIdle) {
		t.Fatalf("err=%v, want idle timeout", err)
	}
	p.failInbox(row, err, time.Now())
	got := reloadRow(t, db, row.ID)
	if got.State != inboxRetryable || got.LeaseOwner != "" || got.NextAttemptAt == nil {
		t.Fatalf("row=%+v, want retryable with the lease released", got)
	}
	if got.LastCoreEventID != "7" {
		t.Fatalf("cursor=%q, want resume after the dispatched message", got.LastCoreEventID)
	}
}

// Heartbeats keep a long but healthy run alive: idle detection must count the
// ": ping" comments Core sends while the agent is still working.
func TestLongRequestHeartbeatsKeepStreamAlive(t *testing.T) {
	previous := coreStreamIdleTimeout
	coreStreamIdleTimeout = 200 * time.Millisecond
	t.Cleanup(func() { coreStreamIdleTimeout = previous })
	p, db, core := longRequestFixture(t)
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		emitCoreMessage(w, 7, "user", "text", "q", "slow-turn")
		for range 8 { // 8 x 80ms = 640ms total, well past the 200ms idle limit
			time.Sleep(80 * time.Millisecond)
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
		emitCoreMessage(w, 8, "agent", "result", "the long answer", "")
	}
	row := longRequestRow(t, db, "slow-turn", 9)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if got := reloadRow(t, db, row.ID); got.State != inboxCompleted {
		t.Fatalf("state=%q, want completed", got.State)
	}
}

// Failure after retries: the turn must not go dead silently.
func TestLongRequestRetryExhaustionPostsFailureNotice(t *testing.T) {
	p, db, core := longRequestFixture(t)
	row := longRequestRow(t, db, "exhausted-turn", 9)
	row.Attempts = len(retryDelays) + 1
	p.failInbox(row, errCoreStreamIdle, time.Now())
	got := reloadRow(t, db, row.ID)
	if got.State != inboxDead || got.LeaseOwner != "" || got.LeaseExpiresAt != nil || got.FinalWuKongClientMsgNo == "" {
		t.Fatalf("row=%+v, want dead, released, with the notice client number", got)
	}
	sent := core.notices()
	if len(sent) != 1 || sent[0].ClientMsgNo != got.FinalWuKongClientMsgNo {
		t.Fatalf("sent=%+v, want exactly one notice", sent)
	}
	if payload := sentPayload(t, sent[0]); payload.Content != execFailedText || payload.SystemNotice != execFailedNotice {
		t.Fatalf("payload=%+v", payload)
	}
	// A second failInbox from a stale lease holder does not post again.
	p.failInbox(row, errCoreStreamIdle, time.Now())
	if len(core.notices()) != 1 {
		t.Fatalf("notice repeated: %+v", core.notices())
	}
}

// Busy retries keep their own "queue timed out" reply and are not doubled.
func TestLongRequestBusyExhaustionKeepsBusyNoticeOnly(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	row := longRequestRow(t, db, "busy-turn", 9)
	row.Attempts = len(retryDelays) + 1
	p.failInbox(row, &httpStatusError{status: http.StatusConflict, code: conversationBusyCode}, time.Now())
	if got := reloadRow(t, db, row.ID); got.State != inboxDead || got.FinalWuKongClientMsgNo != "" {
		t.Fatalf("row=%+v, want dead without the execution failure notice", got)
	}
}

// Failure then follow-up: after a failed long turn the next question in the
// same chat is dispatched and answered normally.
func TestLongRequestFollowUpAfterFailureCompletes(t *testing.T) {
	p, db, core := longRequestFixture(t)
	turn := 0
	core.turns["/api/v1/addons/conversation/9/messages/stream"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		core.mu.Lock()
		turn++
		current := turn
		core.mu.Unlock()
		if current == 1 {
			emitCoreMessage(w, 7, "user", "text", "q", "first-turn")
			emitCoreMessage(w, 8, "system", "error", "❌ executor lease timeout (no result)", "")
			return
		}
		emitCoreMessage(w, 7, "user", "text", "q", "first-turn")
		emitCoreMessage(w, 8, "system", "error", "❌ executor lease timeout (no result)", "")
		emitCoreMessage(w, 10, "user", "text", "again", "second-turn")
		emitCoreMessage(w, 11, "agent", "result", "second answer", "")
	}
	first := longRequestRow(t, db, "first-turn", 9)
	if err := p.consumeCoreStream(t.Context(), first); err != nil {
		t.Fatalf("first: %v", err)
	}
	if got := reloadRow(t, db, first.ID); got.State != inboxDead || got.LeaseOwner != "" {
		t.Fatalf("first=%+v, want dead and released", got)
	}
	second := longRequestRow(t, db, "second-turn", 9)
	if err := p.consumeCoreStream(t.Context(), second); err != nil {
		t.Fatalf("second: %v", err)
	}
	got := reloadRow(t, db, second.ID)
	if got.State != inboxCompleted || got.CumulativeAnswer != "second answer" {
		t.Fatalf("second=%+v, want completed with its own answer", got)
	}
	var processing int64
	db.Model(&IMWebhookInbox{}).Where("state = ?", inboxProcessing).Count(&processing)
	if processing != 0 {
		t.Fatalf("processing rows=%d, want 0", processing)
	}
}
