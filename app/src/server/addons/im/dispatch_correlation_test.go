package im

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestDispatchTwoTurnsAndReconnect(t *testing.T) {
	db := m2DB(t)
	var mu sync.Mutex
	var answers, cursors []string
	dispatches := map[string]bool{}
	disconnected := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/channel/messagesync":
			fmt.Fprint(w, `{"messages":[]}`)
		case "/message/send":
			var body struct {
				Payload []byte `json:"payload"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			var text textPayload
			if err := json.Unmarshal(body.Payload, &text); err != nil {
				t.Error(err)
				return
			}
			answers = append(answers, text.Content)
			fmt.Fprint(w, `{}`)
		case "/api/v1/addons/conversation/im-dispatch":
			var input dispatchRequest
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			dispatches[input.SourceKey] = true
			_ = json.NewEncoder(w).Encode(dispatchResult{ConversationID: 9, SourceKey: input.SourceKey, Status: "accepted"})
		case "/api/v1/addons/conversation/9/messages/stream":
			cursors = append(cursors, r.Header.Get("Last-Event-ID"))
			w.Header().Set("Content-Type", "text/event-stream")
			emit := func(event string, id int, sender, kind, content, source string) {
				data, _ := json.Marshal(map[string]any{
					"id": id, "senderType": sender, "msgType": kind, "content": content, "sourceKey": source,
				})
				fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, event, data)
			}
			// Replay old results even with a cursor, exercising the local boundary too.
			emit("message-created", 1, "user", "text", "first", "turn-1")
			emit("message-created", 2, "agent", "result", "A", "")
			if !dispatches["turn-2"] {
				return
			}
			emit("message-created", 3, "system", "error", "old failure", "")
			emit("message-created", 4, "user", "text", "second", "turn-2")
			emit("message-created", 2, "agent", "result", "A", "")
			emit("message-updated", 5, "agent", "streaming", "B partial", "")
			if !disconnected {
				disconnected = true
				return
			}
			// Core replays mutable rows as created, including IDs at/below the cursor.
			emit("message-created", 5, "agent", "streaming", "B resumed", "")
			emit("message-updated", 5, "agent", "streaming", "B updated", "")
			emit("message-created", 6, "agent", "result", "B", "")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	p := &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token"}
	for turn := 1; turn <= 2; turn++ {
		row := IMWebhookInbox{
			Event: "msg.notify", MessageIDStr: fmt.Sprint(turn), TargetAgentUID: 42,
			SenderUID: 1, ChannelID: "g1", ChannelType: groupChannel, Text: "prompt",
			State: inboxProcessing, ExecutionOwnerUserID: 1, SourceKey: fmt.Sprintf("turn-%d", turn),
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		err := p.dispatchAndStream(t.Context(), &row)
		if turn == 2 {
			if err == nil {
				t.Fatal("turn 2 consumed an old terminal event instead of awaiting its own final")
			}
			if err := db.First(&row, row.ID).Error; err != nil {
				t.Fatal(err)
			}
			if row.CumulativeAnswer != "B partial" || row.LastCoreEventID != "5" || row.DispatchCoreMessageID != 4 {
				t.Fatalf("snapshot before reconnect: answer=%q cursor=%q", row.CumulativeAnswer, row.LastCoreEventID)
			}
			err = p.dispatchAndStream(t.Context(), &row)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := db.First(&row, row.ID).Error; err != nil {
			t.Fatal(err)
		}
		if row.State != inboxCompleted || row.FinalCoreMessageID != uint(2+(turn-1)*4) {
			t.Fatalf("turn %d: state=%s final=%d", turn, row.State, row.FinalCoreMessageID)
		}
		if turn == 2 && row.Revision != 4 {
			t.Fatalf("turn 2 revision=%d, want all four distinct cumulative snapshots", row.Revision)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(answers, []string{"A", "B"}) || len(dispatches) != 2 {
		t.Fatalf("answers=%v executions=%d", answers, len(dispatches))
	}
	if !slices.Equal(cursors, []string{"0", "2", "5"}) {
		t.Fatalf("resume cursors=%v", cursors)
	}
}

func TestDispatchBusyRemainsTerminalDomainError(t *testing.T) {
	db := m2DB(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"code":"im_conversation_busy"}`)
	}))
	defer server.Close()
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "busy", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "g1", ChannelType: groupChannel, Text: "prompt", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "busy", LeaseOwner: "worker", Attempts: 1,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	p := &Plugin{coreURL: server.URL, serviceJWT: "test-token"}
	_, err := p.dispatchCore(t.Context(), &row, nil)
	if err == nil {
		t.Fatal("busy dispatch accepted")
	}
	p.failInbox(&row, err, time.Now())
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.State != inboxDead || row.LastErrorCode != "im_conversation_busy" || row.NextAttemptAt != nil {
		t.Fatalf("busy handling: state=%s code=%s retry=%v", row.State, row.LastErrorCode, row.NextAttemptAt)
	}
}
