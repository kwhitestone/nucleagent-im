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
	"gorm.io/gorm"
)

// MUYU-160 review 4: a delegated turn waiting for its continuation used to hold
// the only inbox worker, so every other channel on the instance queued behind
// it. These tests drive the real runWorker loop.

// concurrencyDB serializes connections: the shared-cache in-memory SQLite used in
// tests reports "table is locked" under concurrent writers, whereas production
// MySQL uses row locks with SKIP LOCKED. One connection keeps the test about the
// worker's scheduling, not about SQLite's table locks.
func concurrencyDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := m2DB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	return db
}

type concurrencyCore struct {
	mu        sync.Mutex
	answers   map[string]string // reply text by channel_id, in send order
	sendOrder []string
	dispatch  map[string]uint // sourceKey -> conversation
	waitGate  chan struct{}   // released to let the waiting channel's continuation arrive
}

func concurrencyFixture(t *testing.T) (*Plugin, *gorm.DB, *concurrencyCore) {
	t.Helper()
	db := concurrencyDB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	core := &concurrencyCore{answers: map[string]string{}, dispatch: map[string]uint{}, waitGate: make(chan struct{})}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			core.mu.Lock()
			core.answers[body.ChannelID] = text.Content
			core.sendOrder = append(core.sendOrder, body.ChannelID+":"+text.Content)
			core.mu.Unlock()
		case "/api/v1/addons/conversation/im-dispatch":
			var input dispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			conv := uint(9)
			if input.Channel.ID != "1@42" {
				conv = 10
			}
			core.mu.Lock()
			core.dispatch[input.SourceKey] = conv
			core.mu.Unlock()
			_ = json.NewEncoder(w).Encode(dispatchResult{ConversationID: conv, SourceKey: input.SourceKey, Status: "accepted"})
		case "/api/v1/addons/conversation/9/messages/stream":
			// Channel A: a delegated turn that waits for its continuation, then a
			// follow-up turn in the same conversation.
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			emitCoreMessage(w, 7, "user", "text", "long", "a-long")
			emitCoreTool(w, 8, "delegate_task", "completed")
			emitCoreMessage(w, 9, "agent", "result", "", "")
			select {
			case <-core.waitGate:
			case <-r.Context().Done():
				return
			}
			emitCoreMessage(w, 11, "agent", "result", "A long answer", "")
			emitCoreMessage(w, 12, "user", "text", "next", "a-next")
			emitCoreMessage(w, 13, "agent", "result", "A next answer", "")
			flusher.Flush()
		case "/api/v1/addons/conversation/10/messages/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			emitCoreMessage(w, 20, "user", "text", "quick", "b-quick")
			emitCoreMessage(w, 21, "agent", "result", "B quick answer", "")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	p := &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token", done: make(chan struct{})}
	return p, db, core
}

func pendingRow(t *testing.T, db *gorm.DB, source, channel string, sender uint) IMWebhookInbox {
	t.Helper()
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: source, SourceKey: source, TargetAgentUID: 42,
		SenderUID: sender, ChannelID: channel, ChannelType: personChannel, Text: "q", State: inboxPending,
		ExecutionOwnerUserID: sender, OriginSenderUID: sender,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func waitState(t *testing.T, db *gorm.DB, id uint, want string, within time.Duration) IMWebhookInbox {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		var row IMWebhookInbox
		if err := db.First(&row, id).Error; err != nil {
			t.Fatal(err)
		}
		if row.State == want {
			return row
		}
		if time.Now().After(deadline) {
			t.Fatalf("row %d state=%s after %v, want %s (%+v)", id, row.State, within, want, row)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Channel A's delegated turn is waiting for its continuation. Channel B's
// question, sent after it, must be answered without waiting for A, and A's own
// follow-up must not overtake A's long turn.
func TestOtherChannelAnsweredWhileTurnWaitsForContinuation(t *testing.T) {
	p, db, core := concurrencyFixture(t)
	aLong := pendingRow(t, db, "a-long", "1@42", 1)
	ctx, cancel := context.WithCancel(t.Context())
	go p.runWorker(ctx)
	t.Cleanup(func() { cancel(); <-p.done })

	// A is in flight and waiting (empty result after delegate_task persisted).
	deadline := time.Now().Add(3 * time.Second)
	for {
		var row IMWebhookInbox
		db.First(&row, aLong.ID)
		if row.ContinuationDueAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never reached the continuation wait: %+v", row)
		}
		time.Sleep(20 * time.Millisecond)
	}
	aNext := pendingRow(t, db, "a-next", "1@42", 1)
	bQuick := pendingRow(t, db, "b-quick", "2@42", 2)

	waitState(t, db, bQuick.ID, inboxCompleted, 3*time.Second)
	var stillWaiting, nextRow IMWebhookInbox
	db.First(&stillWaiting, aLong.ID)
	db.First(&nextRow, aNext.ID)
	if stillWaiting.State != inboxProcessing || nextRow.State != inboxPending {
		t.Fatalf("A long=%s A next=%s: A's follow-up must wait behind A's own turn", stillWaiting.State, nextRow.State)
	}

	close(core.waitGate)
	waitState(t, db, aLong.ID, inboxCompleted, 3*time.Second)
	waitState(t, db, aNext.ID, inboxCompleted, 3*time.Second)
	core.mu.Lock()
	defer core.mu.Unlock()
	want := []string{"2@42:B quick answer", "1@42:A long answer", "1@42:A next answer"}
	if fmt.Sprint(core.sendOrder) != fmt.Sprint(want) {
		t.Fatalf("send order=%v, want %v", core.sendOrder, want)
	}
	var processing int64
	db.Model(&IMWebhookInbox{}).Where("state = ?", inboxProcessing).Count(&processing)
	if processing != 0 {
		t.Fatalf("processing rows left=%d", processing)
	}
}

// The slot limit holds: no more turns run at once than inboxWorkerSlots.
func TestInboxWorkerSlotsBoundConcurrency(t *testing.T) {
	previous := inboxWorkerSlots
	inboxWorkerSlots = 2
	t.Cleanup(func() { inboxWorkerSlots = previous })
	db := concurrencyDB(t)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	var running, peak atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowLifecycle(w, r) {
			return
		}
		switch r.URL.Path {
		case "/channel/messagesync":
			fmt.Fprint(w, `{"messages":[]}`)
		case "/api/v1/addons/conversation/im-dispatch":
			var input dispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
			running.Add(-1)
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	for i := range 5 {
		uid := uint(100 + i)
		addUser(t, db, uid, authmodel.AccountTypeHuman)
		pendingRow(t, db, fmt.Sprint("slot-", i), fmt.Sprintf("%d@42", uid), uid)
	}
	p := &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token", done: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	go p.runWorker(ctx)
	time.Sleep(400 * time.Millisecond)
	if got := peak.Load(); got != 2 {
		close(release)
		cancel()
		<-p.done
		t.Fatalf("peak concurrent turns=%d, want 2", got)
	}
	close(release)
	cancel()
	<-p.done
}
