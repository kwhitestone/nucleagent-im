package im

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func m2DB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(
		&authmodel.User{}, &model.AgentInstance{},
		&IMWebhookInbox{}, &IMCoreConversationMap{}, &IMGroup{},
		&IMGroupAgentAllowlist{}, &IMRateWindow{},
	); err != nil {
		t.Fatal(err)
	}
	previous := global.PRISM_DB
	global.PRISM_DB = db
	t.Cleanup(func() {
		global.PRISM_DB = previous
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})
	return db
}

func addUser(t *testing.T, db *gorm.DB, id uint, accountType string) {
	t.Helper()
	if err := db.Create(&authmodel.User{
		ID: id, Username: fmt.Sprintf("user-%d", id), NickName: fmt.Sprintf("User %d", id),
		AccountType: accountType, Enable: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
}

func addBinding(t *testing.T, db *gorm.DB, owner, agent uint) {
	t.Helper()
	if err := db.Create(&model.AgentInstance{
		UserID: owner, AuthAgentUserID: &agent, IMEnabled: true,
		ExecutionBackend: "test", Model: "test",
	}).Error; err != nil {
		t.Fatal(err)
	}
}

func webhookText(id string, sender uint, channel string, channelType uint8, content string, mentions ...uint) webhookMessage {
	uids := make([]string, len(mentions))
	for i := range mentions {
		uids[i] = strconvUint(mentions[i])
	}
	payload, _ := json.Marshal(map[string]any{
		"type": wuKongTextType, "content": content,
		"mention": map[string]any{"all": 0, "uids": uids},
	})
	return webhookMessage{
		MessageIDStr: id, ClientMsgNo: "client-" + id, FromUID: strconvUint(sender),
		ChannelID: channel, ChannelType: channelType, Timestamp: 1, RawPayload: payload,
	}
}

func strconvUint(value uint) string { return fmt.Sprintf("%d", value) }

// acceptHuman exercises the M2 path: with no group-member resolver the agent-sender
// C-rule parser can never fire, so these assertions stay exactly as M2 wrote them.
func acceptHuman(
	ctx context.Context, db *gorm.DB, event string, messages []webhookMessage, now time.Time,
) (int, error) {
	return acceptWebhookBatch(ctx, db, event, messages, now, nil)
}

func inboxCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var count int64
	if err := db.Model(&IMWebhookInbox{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func TestWebhookPrivateReplayOwnerAndAgentGuard(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addBinding(t, db, 1, 42)
	now := time.Unix(1_800_000_000, 0)

	message := webhookText("100", 1, "42@1", personChannel, "hello")
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{message}, now); err != nil || accepted != 1 {
		t.Fatalf("accepted=%d err=%v", accepted, err)
	}
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{message}, now); err != nil || accepted != 0 {
		t.Fatalf("duplicate accepted=%d err=%v", accepted, err)
	}
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify",
		[]webhookMessage{webhookText("101", 2, "2@42", personChannel, "wrong owner")}, now); err != nil || accepted != 0 {
		t.Fatalf("owner mismatch accepted=%d err=%v", accepted, err)
	}
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify",
		[]webhookMessage{webhookText("102", 42, "1@42", personChannel, "loop")}, now); err != nil || accepted != 0 {
		t.Fatalf("agent sender accepted=%d err=%v", accepted, err)
	}
	if count := inboxCount(t, db); count != 1 {
		t.Fatalf("inbox count=%d, want 1", count)
	}
	var row IMWebhookInbox
	if err := db.First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.ChannelID != "1@42" || row.SourceKey != "wukong:msg.notify:100:42" {
		t.Fatalf("row=%+v", row)
	}
}

func TestWebhookGroupMentionsAllowlistAndMultipleAgents(t *testing.T) {
	db := m2DB(t)
	for _, id := range []uint{1, 2} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	for _, id := range []uint{42, 43} {
		addUser(t, db, id, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, id)
	}
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&IMGroupAgentAllowlist{
		GroupID: "g1", AgentUID: 42, MemberUID: 2, ManagedByOwnerUID: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)

	owner := webhookText("200", 1, "g1", groupChannel, "both", 42, 43)
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{owner}, now); err != nil || accepted != 2 {
		t.Fatalf("owner accepted=%d err=%v", accepted, err)
	}
	member := webhookText("201", 2, "g1", groupChannel, "one", 42, 43)
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{member}, now); err != nil || accepted != 1 {
		t.Fatalf("member accepted=%d err=%v", accepted, err)
	}
	allOnly := webhookText("202", 1, "g1", groupChannel, "@all")
	var payload map[string]any
	_ = json.Unmarshal(allOnly.RawPayload, &payload)
	payload["mention"] = map[string]any{"all": 1, "uids": []string{}}
	allOnly.RawPayload, _ = json.Marshal(payload)
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{allOnly}, now); err != nil || accepted != 0 {
		t.Fatalf("@all accepted=%d err=%v", accepted, err)
	}
	if count := inboxCount(t, db); count != 3 {
		t.Fatalf("inbox count=%d, want 3", count)
	}
	var memberRow IMWebhookInbox
	if err := db.Where("message_idstr = ? AND target_agent_uid = ?", "201", 42).First(&memberRow).Error; err != nil {
		t.Fatal(err)
	}
	if memberRow.SenderUID != 2 || memberRow.ExecutionOwnerUserID != 1 {
		t.Fatalf("initiator=%d execution owner=%d", memberRow.SenderUID, memberRow.ExecutionOwnerUserID)
	}
}

func TestWebhookRateLimitBoundaries(t *testing.T) {
	t.Run("sender fifth and sixth", func(t *testing.T) {
		db := m2DB(t)
		addUser(t, db, 1, authmodel.AccountTypeHuman)
		addUser(t, db, 42, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, 42)
		now := time.Unix(1_800_000_000, 0)
		for i := 1; i <= 5; i++ {
			message := webhookText(fmt.Sprintf("s%d", i), 1, "1@42", personChannel, "hello")
			if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{message}, now); err != nil || accepted != 1 {
				t.Fatalf("request %d accepted=%d err=%v", i, accepted, err)
			}
		}
		_, err := acceptHuman(t.Context(), db, "msg.notify",
			[]webhookMessage{webhookText("s6", 1, "1@42", personChannel, "hello")}, now)
		if !errors.Is(err, errAgentRateLimited) {
			t.Fatalf("sixth err=%v", err)
		}
		if count := inboxCount(t, db); count != 5 {
			t.Fatalf("inbox count=%d, want 5", count)
		}
	})

	t.Run("agent twentieth and twenty-first", func(t *testing.T) {
		db := m2DB(t)
		addUser(t, db, 42, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, 42)
		if err := db.Create(&IMGroup{GroupID: "g", CreatorUID: 1}).Error; err != nil {
			t.Fatal(err)
		}
		for sender := uint(1); sender <= 5; sender++ {
			addUser(t, db, sender, authmodel.AccountTypeHuman)
			if sender != 1 {
				if err := db.Create(&IMGroupAgentAllowlist{
					GroupID: "g", AgentUID: 42, MemberUID: sender, ManagedByOwnerUID: 1,
				}).Error; err != nil {
					t.Fatal(err)
				}
			}
		}
		now := time.Unix(1_800_000_000, 0)
		for i := 0; i < 20; i++ {
			sender := uint(i/5 + 1)
			message := webhookText(fmt.Sprintf("a%d", i), sender, "g", groupChannel, "hello", 42)
			if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{message}, now); err != nil || accepted != 1 {
				t.Fatalf("request %d accepted=%d err=%v", i+1, accepted, err)
			}
		}
		_, err := acceptHuman(t.Context(), db, "msg.notify",
			[]webhookMessage{webhookText("a21", 5, "g", groupChannel, "hello", 42)}, now)
		if !errors.Is(err, errAgentRateLimited) {
			t.Fatalf("twenty-first err=%v", err)
		}
		if count := inboxCount(t, db); count != 20 {
			t.Fatalf("inbox count=%d, want 20", count)
		}
	})
}

func TestWebhookRateLimitDoesNotRollbackOtherMentionedAgent(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	for _, agent := range []uint{42, 43} {
		addUser(t, db, agent, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, agent)
	}
	if err := db.Create(&IMGroup{GroupID: "g", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	for i := 0; i < 5; i++ {
		message := webhookText(fmt.Sprintf("limited-%d", i), 1, "g", groupChannel, "hello", 42)
		if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{message}, now); err != nil || accepted != 1 {
			t.Fatalf("setup request %d accepted=%d err=%v", i+1, accepted, err)
		}
	}
	accepted, err := acceptHuman(t.Context(), db, "msg.notify",
		[]webhookMessage{webhookText("mixed", 1, "g", groupChannel, "hello", 42, 43)}, now)
	if !errors.Is(err, errAgentRateLimited) || accepted != 1 {
		t.Fatalf("mixed accepted=%d err=%v", accepted, err)
	}
	var rows []IMWebhookInbox
	if err := db.Where("message_idstr = ?", "mixed").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].TargetAgentUID != 43 {
		t.Fatalf("mixed rows=%+v", rows)
	}
}

func TestWebhookHTTPAuthMalformedEventAndSize(t *testing.T) {
	db := m2DB(t)
	_ = db
	gin.SetMode(gin.TestMode)
	router := gin.New()
	sharedAuthStack(t, router)
	api := humagin.New(router, huma.DefaultConfig("im webhook test", "1"))
	p := &Plugin{webhookCapability: strings.Repeat("c", 32)}
	p.registerWebhook(api)

	request := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	response := request("/api/v1/im/webhooks/wukong?token=bad&event=msg.notify", "[]")
	if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), `"code":"im_webhook_unauthorized"`) {
		t.Fatalf("bad capability response=%d %s", response.Code, response.Body.String())
	}
	if status := request("/api/v1/im/webhooks/wukong?token="+strings.Repeat("c", 32)+"&event=other", "[]").Code; status != http.StatusBadRequest {
		t.Fatalf("event status=%d", status)
	}
	if status := request("/api/v1/im/webhooks/wukong?token="+strings.Repeat("c", 32)+"&event=msg.notify", "{").Code; status < 400 || status >= 500 {
		t.Fatalf("malformed status=%d", status)
	}
	large := `["` + strings.Repeat("x", 300*1024) + `"]`
	if status := request("/api/v1/im/webhooks/wukong?token="+strings.Repeat("c", 32)+"&event=msg.notify", large).Code; status != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d", status)
	}
	p.webhookAdmission = webhookAdmission{windowStart: time.Now().UTC().Truncate(time.Minute), count: 120}
	response = request("/api/v1/im/webhooks/wukong?token="+strings.Repeat("c", 32)+"&event=msg.notify", "[]")
	if response.Code != http.StatusTooManyRequests || !strings.Contains(response.Body.String(), `"code":"im_webhook_rate_limited"`) {
		t.Fatalf("rate response=%d %s", response.Code, response.Body.String())
	}
}

func TestCapabilityLogRedaction(t *testing.T) {
	var output bytes.Buffer
	writer := capabilityRedactingWriter{Writer: &output}
	line := `POST /api/v1/im/webhooks/wukong?token=secret-value&event=msg.notify`
	if _, err := writer.Write([]byte(line)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "secret-value") ||
		!strings.Contains(output.String(), "token=<redacted>&event=msg.notify") {
		t.Fatalf("redacted output=%q", output.String())
	}
}

func TestLeaseRecoveryAndRetryExhaustion(t *testing.T) {
	db := m2DB(t)
	expired := time.Now().Add(-time.Minute)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "lease", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "1@42", ChannelType: personChannel, Text: "hello", State: inboxProcessing,
		LeaseOwner: "old", LeaseExpiresAt: &expired, Attempts: 1,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:lease:42",
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	leased, err := leaseInbox(t.Context(), db, "new", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if leased == nil || leased.LeaseOwner != "new" || leased.Attempts != 2 {
		t.Fatalf("leased=%+v", leased)
	}
	leased.Attempts = len(retryDelays) + 1
	(&Plugin{}).failInbox(leased, errors.New("network"), time.Now())
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.State != inboxDead {
		t.Fatalf("state=%q, want dead", row.State)
	}
}

func TestCoreStreamResumeSnapshotsFinalAndIdempotency(t *testing.T) {
	db := m2DB(t)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "stream", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "1@42", ChannelType: personChannel, Text: "hello", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:stream:42",
		CoreConversationID: 9, LastCoreEventID: "7", DispatchCoreMessageID: 7,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	wuKong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/message/send" {
			t.Fatalf("WuKong path=%s", r.URL.Path)
		}
		sends.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer wuKong.Close()
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Last-Event-ID"); got != "7" {
			t.Fatalf("Last-Event-ID=%q", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: 8\nevent: message-updated\ndata: {\"id\":8,\"senderType\":\"agent\",\"msgType\":\"streaming\",\"content\":\"Hel\"}\n\n")
		fmt.Fprint(w, "id: 8\nevent: message-updated\ndata: {\"id\":8,\"senderType\":\"agent\",\"msgType\":\"streaming\",\"content\":\"Hello\"}\n\n")
		fmt.Fprint(w, "id: 9\nevent: message-deleted\ndata: {\"id\":8}\n\n")
		fmt.Fprint(w, "id: 10\nevent: message-created\ndata: {\"id\":10,\"senderType\":\"agent\",\"msgType\":\"text\",\"content\":\"Hello world\"}\n\n")
	}))
	defer core.Close()
	p := &Plugin{apiAddr: wuKong.URL, coreURL: core.URL, serviceJWT: "test-token"}
	if err := p.consumeCoreStream(t.Context(), &row); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.State != inboxCompleted || row.CumulativeAnswer != "Hello world" || row.Revision != 3 ||
		row.LastCoreEventID != "10" || row.FinalWuKongClientMsgNo == "" {
		t.Fatalf("row=%+v", row)
	}
	if err := p.sendFinal(t.Context(), &row, 10, "Hello world"); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 1 {
		t.Fatalf("final sends=%d, want 1", sends.Load())
	}
}

func TestCoreStreamRetriesFinalEventAfterWuKongFailure(t *testing.T) {
	db := m2DB(t)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "retry-final", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "1@42", ChannelType: personChannel, Text: "hello", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:retry-final:42",
		CoreConversationID: 9, LastCoreEventID: "7", DispatchCoreMessageID: 7,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	var clientNumbers []string
	wuKong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ClientMessageNo string `json:"client_msg_no"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		clientNumbers = append(clientNumbers, body.ClientMessageNo)
		if sends.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer wuKong.Close()
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Last-Event-ID"); got != "7" {
			t.Fatalf("Last-Event-ID=%q, want 7", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: 10\nevent: message-created\ndata: {\"id\":10,\"senderType\":\"agent\",\"msgType\":\"text\",\"content\":\"done\"}\n\n")
	}))
	defer core.Close()
	p := &Plugin{apiAddr: wuKong.URL, coreURL: core.URL, serviceJWT: "test-token"}
	if err := p.consumeCoreStream(t.Context(), &row); err == nil {
		t.Fatal("first final send unexpectedly succeeded")
	}
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.LastCoreEventID != "7" || row.FinalWuKongClientMsgNo == "" {
		t.Fatalf("row after failed send=%+v", row)
	}
	if err := p.consumeCoreStream(t.Context(), &row); err != nil {
		t.Fatal(err)
	}
	if len(clientNumbers) != 2 || clientNumbers[0] != clientNumbers[1] {
		t.Fatalf("client message numbers=%v", clientNumbers)
	}
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.State != inboxCompleted || row.LastCoreEventID != "10" {
		t.Fatalf("row after retry=%+v", row)
	}
}

func TestTerminalFailureDoesNotSendAgentMessage(t *testing.T) {
	db := m2DB(t)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "error", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "1@42", ChannelType: personChannel, Text: "hello", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:error:42", CoreConversationID: 9,
		DispatchCoreMessageID: 7,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	wuKong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer wuKong.Close()
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "id: 11\nevent: message-created\ndata: {\"id\":11,\"senderType\":\"system\",\"msgType\":\"error\",\"content\":\"secret failure\"}\n\n")
	}))
	defer core.Close()
	p := &Plugin{apiAddr: wuKong.URL, coreURL: core.URL, serviceJWT: "test-token"}
	if err := p.consumeCoreStream(t.Context(), &row); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.State != inboxDead || row.LastErrorCode != "im_execution_failed" || sends.Load() != 0 {
		t.Fatalf("row=%+v sends=%d", row, sends.Load())
	}
}

func TestParticipantAuthorization(t *testing.T) {
	p := &Plugin{}
	if allowed, err := p.channelParticipant(t.Context(), 1, "42@1", personChannel); err != nil || !allowed {
		t.Fatalf("direct allowed=%v err=%v", allowed, err)
	}
	if allowed, err := p.channelParticipant(t.Context(), 2, "1@42", personChannel); err != nil || allowed {
		t.Fatalf("direct outsider allowed=%v err=%v", allowed, err)
	}

	wuKong := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/channel/messagesync" {
			http.NotFound(w, r)
			return
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != "admin" || password != "password" {
			t.Fatalf("membership request basic auth invalid")
		}
		var body struct {
			LoginUID string `json:"login_uid"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.LoginUID == "3" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(wuKongSyncResponse{Messages: []struct {
			MessageIDStr string `json:"message_idstr"`
			MessageSeq   uint64 `json:"message_seq"`
			FromUID      string `json:"from_uid"`
			Setting      uint8  `json:"setting"`
			Payload      []byte `json:"payload"`
		}{}})
	}))
	defer wuKong.Close()
	p.apiAddr = wuKong.URL
	p.wuKongAdminUser = "admin"
	p.wuKongAdminPassword = "password"
	if allowed, err := p.channelParticipant(t.Context(), 2, "g1", groupChannel); err != nil || !allowed {
		t.Fatalf("group allowed=%v err=%v", allowed, err)
	}
	if allowed, err := p.channelParticipant(t.Context(), 3, "g1", groupChannel); err != nil || allowed {
		t.Fatalf("group outsider allowed=%v err=%v", allowed, err)
	}
}

func TestInitialHistoryUsesAuthOrdersAndFilters(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	text := func(content string) []byte {
		payload, _ := json.Marshal(map[string]any{"type": wuKongTextType, "content": content})
		return payload
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "admin" || password != "password" {
			t.Fatalf("history request basic auth invalid")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": []map[string]any{
			{"message_idstr": "3", "message_seq": 3, "from_uid": "42", "payload": text("third")},
			{"message_idstr": "2", "message_seq": 2, "from_uid": "2", "setting": 2, "payload": text("stream")},
			{"message_idstr": "4", "message_seq": 4, "from_uid": "1", "payload": text("trigger")},
			{"message_idstr": "1", "message_seq": 1, "from_uid": "1", "payload": text("first")},
		}})
	}))
	defer server.Close()
	row := &IMWebhookInbox{
		MessageIDStr: "4", SenderUID: 1, TargetAgentUID: 42, ChannelID: "g1",
		ChannelType: groupChannel, ExecutionOwnerUserID: 1,
	}
	p := &Plugin{
		apiAddr: server.URL, wuKongAdminUser: "admin", wuKongAdminPassword: "password",
	}
	turns, err := p.initialHistory(t.Context(), row)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 2 || turns[0].Content != "first" || turns[1].Content != "third" ||
		turns[0].SpeakerName != "User 1" || turns[1].Role != "assistant" {
		t.Fatalf("turns=%+v", turns)
	}
}

func TestScanSSECumulativeReplacement(t *testing.T) {
	stream := bytes.NewBufferString(
		"id: 1\nevent: message-updated\ndata: first\n\n" +
			": ping\n\n" +
			"id: 2\nevent: message-updated\ndata: second\n\n",
	)
	var snapshots []string
	if err := scanSSE(stream, func(event, id string, data []byte) (bool, error) {
		snapshots = append(snapshots, id+":"+string(data))
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshots, ","); got != "1:first,2:second" {
		t.Fatalf("snapshots=%q", got)
	}
}

func TestFinalWuKongPayloadIsBase64Text(t *testing.T) {
	db := m2DB(t)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "payload", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "1@42", ChannelType: personChannel, Text: "hello", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:payload:42",
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(body.Payload)
		if err != nil || !strings.Contains(string(decoded), `"content":"answer"`) {
			t.Fatalf("payload=%q err=%v", decoded, err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := (&Plugin{apiAddr: server.URL}).sendFinal(context.Background(), &row, 7, "answer"); err != nil {
		t.Fatal(err)
	}
}
