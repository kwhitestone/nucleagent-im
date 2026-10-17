package im

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
)

// IM-AGENT-FAILURE-NOTICE-0830: a run that Core ends with msgType=error leaves
// exactly one durable system notice in the chat. Before, the row only went dead
// and the live "could not complete" line vanished after the 2-minute replay.
func execFailureFixture(t *testing.T, final string, sendStatus int) (*Plugin, *IMWebhookInbox, func() []noticeSend) {
	t.Helper()
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	var mu sync.Mutex
	var sent []noticeSend
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowLifecycle(w, r) {
			return
		}
		switch r.URL.Path {
		case "/message/send":
			var body noticeSend
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			mu.Lock()
			sent = append(sent, body)
			mu.Unlock()
			w.WriteHeader(sendStatus)
		case "/api/v1/addons/conversation/9/messages/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			emit := func(id int, sender, kind, content, source string) {
				data, _ := json.Marshal(map[string]any{"id": id, "senderType": sender, "msgType": kind, "content": content, "sourceKey": source})
				fmt.Fprintf(w, "id: %d\nevent: message-created\ndata: %s\n\n", id, data)
			}
			emit(7, "user", "text", "q", "fail-turn")
			emit(8, "agent", "streaming", "half an answ", "")
			if final == "error" {
				emit(9, "agent", "error", "upstream secret: high demand at provider-1", "")
			} else {
				emit(9, "agent", "result", "the answer", "")
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	row := &IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "fail-turn", SourceKey: "fail-turn", TargetAgentUID: 42,
		SenderUID: 1, ChannelID: "1@42", ChannelType: personChannel, Text: "q", State: inboxProcessing,
		ExecutionOwnerUserID: 1, OriginSenderUID: 1, CoreConversationID: 9, LeaseOwner: "w", Attempts: 1,
	}
	if err := db.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	p := &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token"}
	return p, row, func() []noticeSend { mu.Lock(); defer mu.Unlock(); return append([]noticeSend(nil), sent...) }
}

func sentPayload(t *testing.T, s noticeSend) textPayload {
	t.Helper()
	var p textPayload
	if err := json.Unmarshal(s.Payload, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExecutionFailurePostsOneDurableNotice(t *testing.T) {
	p, row, sent := execFailureFixture(t, "error", http.StatusOK)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	got := sent()
	if len(got) != 1 {
		t.Fatalf("want exactly 1 durable notice, got %d", len(got))
	}
	payload := sentPayload(t, got[0])
	if payload.Type != wuKongTextType || payload.Content != execFailedText || payload.SystemNotice != execFailedNotice {
		t.Fatalf("bad notice payload %+v", payload)
	}
	if strings.Contains(string(got[0].Payload), "secret") || strings.Contains(payload.Content, "half") {
		t.Fatal("notice must not carry the upstream error or the partial answer")
	}
	want := deterministicClientMessageNo("fail-turn", 9)
	if got[0].FromUID != "42" || got[0].ChannelID != "1@42" || got[0].ChannelType != personChannel || got[0].ClientMsgNo != want {
		t.Fatalf("bad notice target %+v", got[0])
	}
	var stored IMWebhookInbox
	readInbox(t, row.ID, &stored)
	if stored.State != inboxDead || stored.LastErrorCode != "im_execution_failed" || stored.FinalWuKongClientMsgNo != want ||
		stored.LeaseOwner != "" || stored.LastCoreEventID != "9" {
		t.Fatalf("row after failure: %+v", stored)
	}

	// Core replays the same error on a reconnect and a second worker re-reads it:
	// the row is already dead, so nothing more is written.
	if err := p.consumeCoreStream(t.Context(), row); err == nil {
		t.Fatal("a dead row must not be streamed again")
	}
	if err := p.failExecution(t.Context(), row, 9); err != nil {
		t.Fatal(err)
	}
	if n := len(sent()); n != 1 {
		t.Fatalf("repeated failure events must write once, got %d", n)
	}
}

func TestExecutionSuccessPostsNoNotice(t *testing.T) {
	p, row, sent := execFailureFixture(t, "result", http.StatusOK)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	got := sent()
	if len(got) != 1 {
		t.Fatalf("success must send only the answer, got %d", len(got))
	}
	if payload := sentPayload(t, got[0]); payload.Content != "the answer" || payload.SystemNotice != "" {
		t.Fatalf("success payload %+v", payload)
	}
	var stored IMWebhookInbox
	readInbox(t, row.ID, &stored)
	if stored.State != inboxCompleted || stored.LastErrorCode != "" {
		t.Fatalf("row after success: %+v", stored)
	}
}

// A WuKong outage must not turn a finished failure into a retry that would run
// the model again; the turn stays dead and the live error line still shows.
func TestExecutionFailureNoticeSendErrorKeepsTurnDead(t *testing.T) {
	p, row, sent := execFailureFixture(t, "error", http.StatusInternalServerError)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatalf("notice send failure must not fail the turn: %v", err)
	}
	var stored IMWebhookInbox
	readInbox(t, row.ID, &stored)
	if stored.State != inboxDead || len(sent()) != 1 {
		t.Fatalf("state=%s sends=%d", stored.State, len(sent()))
	}
}

// The browser's live error line names the durable notice so the client can
// swap one for the other instead of showing both.
func TestAgentStreamErrorCarriesNoticeClientMsgNo(t *testing.T) {
	p, row, _ := execFailureFixture(t, "error", http.StatusOK)
	if err := p.consumeCoreStream(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	if !p.sendAgentSnapshots(t.Context(), rec, rec, 1, "1@42", personChannel, time.Now().Add(-agentStreamReplayWindow), map[uint]string{}) {
		t.Fatal("authorized stream stopped")
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, `"clientMsgNo":"`+deterministicClientMessageNo("fail-turn", 9)+`"`) {
		t.Fatalf("error event without notice id:\n%s", body)
	}
	if strings.Contains(body, "secret") || strings.Contains(body, "half an answ") {
		t.Fatalf("error event leaked content:\n%s", body)
	}
}

func readInbox(t *testing.T, id uint, out *IMWebhookInbox) {
	t.Helper()
	if err := global.PRISM_DB.First(out, id).Error; err != nil {
		t.Fatal(err)
	}
}
