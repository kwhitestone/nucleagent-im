package im

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
)

type noticeSend struct {
	FromUID     string `json:"from_uid"`
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	ClientMsgNo string `json:"client_msg_no"`
	Payload     []byte `json:"payload"`
}

// noticeServer records /message/send calls; failFirst makes the first call fail.
func noticeServer(t *testing.T, failFirst bool) (*[]noticeSend, func() int) {
	t.Helper()
	var mu sync.Mutex
	var sent []noticeSend
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path != "/message/send" {
			t.Errorf("unexpected WuKong call %s", r.URL.Path)
			return
		}
		calls++
		if failFirst && calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var body noticeSend
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		sent = append(sent, body)
	}))
	t.Cleanup(server.Close)
	previous := wuKongAPIAddr
	wuKongAPIAddr = server.URL
	t.Cleanup(func() { wuKongAPIAddr = previous })
	return &sent, func() int { mu.Lock(); defer mu.Unlock(); return len(sent) }
}

// fixedNoticeTime sits mid-window so two calls never straddle a 10-minute boundary.
var fixedNoticeTime = time.Date(2026, 10, 13, 12, 3, 0, 0, time.UTC)

func setEnable(t *testing.T, db *gorm.DB, uid uint, enable int) {
	t.Helper()
	if err := db.Model(&authmodel.User{}).Where("id = ?", uid).Update("enable", enable).Error; err != nil {
		t.Fatal(err)
	}
}

// addDefinition binds a definition-level identity; state is "active", "stopped" or "deleted".
// Stopping or deleting also disables the account, exactly like agentidentity.Project.
func addNoticeDefinition(t *testing.T, db *gorm.DB, agent uint, state string) {
	t.Helper()
	tpl := model.AgentTemplate{Name: "d", Slug: "d-" + strconvUint(agent), AuthAgentUserID: &agent, IsActive: state == "active"}
	if state == "deleted" {
		now := fixedNoticeTime
		tpl.DeletedAt = &now
	}
	if err := db.Create(&tpl).Error; err != nil {
		t.Fatal(err)
	}
	if state != "active" {
		setEnable(t, db, agent, 2)
	}
}

func decodeNotice(t *testing.T, s noticeSend) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(s.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestAgentUnavailableDirectReplyWindowAndWording(t *testing.T) {
	for _, state := range []string{"stopped", "deleted"} {
		t.Run(state, func(t *testing.T) {
			db := m2DB(t)
			sent, count := noticeServer(t, false)
			addUser(t, db, 1, authmodel.AccountTypeHuman)
			addUser(t, db, 42, authmodel.AccountTypeAgent)
			addNoticeDefinition(t, db, 42, state)
			now := time.Date(2026, 10, 13, 12, 1, 0, 0, time.UTC)
			ctx := context.Background()
			if _, err := acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("m1", 1, "42@1", personChannel, "hi")}, now); err != nil {
				t.Fatal(err)
			}
			if inboxCount(t, db) != 0 {
				t.Fatal("a disabled agent must never get a core task")
			}
			if count() != 1 {
				t.Fatalf("want 1 notice, got %d", count())
			}
			got := (*sent)[0]
			if got.FromUID != "42" || got.ChannelID != "1@42" || got.ChannelType != personChannel || got.ClientMsgNo == "" {
				t.Fatalf("bad notice target %+v", got)
			}
			payload := decodeNotice(t, got)
			if payload["type"] != float64(wuKongTextType) || payload["systemNotice"] != agentUnavailableNotice ||
				payload["content"] != agentUnavailableText {
				t.Fatalf("bad notice payload %v", payload)
			}
			// Same window: silent. Next window: one more, with a different client_msg_no.
			_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("m2", 1, "1@42", personChannel, "again")}, now.Add(8*time.Minute))
			if count() != 1 {
				t.Fatalf("second message inside the 10-minute window must not reply, got %d", count())
			}
			_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("m3", 1, "1@42", personChannel, "later")}, now.Add(10*time.Minute))
			if count() != 2 || (*sent)[1].ClientMsgNo == got.ClientMsgNo {
				t.Fatalf("next window must reply once with a new client_msg_no, got %d", count())
			}
		})
	}
}

func TestAgentUnavailableClientMsgNoIsDeterministic(t *testing.T) {
	w := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	a := agentUnavailableClientMsgNo(w, personChannel, "1@42", 42)
	if a != agentUnavailableClientMsgNo(w, personChannel, "1@42", 42) {
		t.Fatal("retry inside a window must reuse the client_msg_no")
	}
	if a == agentUnavailableClientMsgNo(w, personChannel, "1@42", 43) || a == agentUnavailableClientMsgNo(w.Add(10*time.Minute), personChannel, "1@42", 42) {
		t.Fatal("agent or window change must change the client_msg_no")
	}
}

func TestAgentUnavailableSkipsHumansAndEnabledAgents(t *testing.T) {
	db := m2DB(t)
	_, count := noticeServer(t, false)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addNoticeDefinition(t, db, 42, "active")
	setEnable(t, db, 2, 2)
	now := fixedNoticeTime
	ctx := context.Background()
	_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("h", 1, "1@2", personChannel, "to a disabled human")}, now)
	if _, err := acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("a", 1, "1@42", personChannel, "to a live agent")}, now); err != nil {
		t.Fatal(err)
	}
	if count() != 0 {
		t.Fatalf("disabled humans and enabled agents never get a notice, got %d", count())
	}
	if inboxCount(t, db) != 1 {
		t.Fatal("an enabled agent keeps its normal task")
	}
}

func TestAgentUnavailableEnabledAccountInactiveDefinition(t *testing.T) {
	db := m2DB(t)
	_, count := noticeServer(t, false)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addNoticeDefinition(t, db, 42, "active")
	db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 42).Update("is_active", false)
	_, _ = acceptHuman(context.Background(), db, "msg.notify", []webhookMessage{webhookText("m", 1, "1@42", personChannel, "hi")}, time.Now())
	if count() != 1 || inboxCount(t, db) != 0 {
		t.Fatalf("an inactive definition replies even if its account lags, got %d", count())
	}
}

func TestAgentUnavailableGroupMentions(t *testing.T) {
	db := m2DB(t)
	sent, count := noticeServer(t, false)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	for _, uid := range []uint{42, 43, 44} {
		addUser(t, db, uid, authmodel.AccountTypeAgent)
		addNoticeDefinition(t, db, uid, "deleted")
	}
	lifecycleGroup(t, db, 1, 42, 43) // 44 is not a member
	now := fixedNoticeTime
	ctx := context.Background()
	_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("g1", 1, "g1", groupChannel, "@a @b @c", 42, 43, 44)}, now)
	if count() != 2 {
		t.Fatalf("each disabled member agent replies once, a non-member never; got %d", count())
	}
	for _, s := range *sent {
		if s.ChannelID != "g1" || s.ChannelType != groupChannel || s.FromUID == "44" {
			t.Fatalf("bad group notice %+v", s)
		}
	}
	_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("g2", 1, "g1", groupChannel, "@a", 42)}, now)
	if count() != 2 {
		t.Fatal("same group + agent inside the window must stay silent")
	}
}

func TestAgentUnavailableGroupTextMentionFromAgent(t *testing.T) {
	db := m2DB(t)
	_, count := noticeServer(t, false)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 41, authmodel.AccountTypeAgent)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addNoticeDefinition(t, db, 42, "stopped")
	lifecycleGroup(t, db, 1, 41, 42)
	payload, _ := json.Marshal(map[string]any{
		"type": wuKongTextType, "content": "ask @user-42",
		"a2a": map[string]any{"chainId": "chain:x", "depth": 1, "originUid": 1},
	})
	msg := webhookMessage{MessageIDStr: "t1", FromUID: "41", ChannelID: "g1", ChannelType: groupChannel, RawPayload: payload}
	_, _ = acceptWebhookBatch(context.Background(), db, "msg.notify", []webhookMessage{msg}, fixedNoticeTime, groupMembers)
	if count() != 1 || inboxCount(t, db) != 0 {
		t.Fatalf("a text @ of a disabled agent replies once, got %d", count())
	}
}

func TestAgentUnavailableDeadLetterReplyWithinWindow(t *testing.T) {
	db := m2DB(t)
	_, count := noticeServer(t, false)
	row := lifecycleRow(t, db)
	addNoticeDefinition(t, db, 42, "stopped")
	p := &Plugin{}
	disabled := &httpStatusError{status: http.StatusForbidden, code: "im_agent_binding_disabled"}
	p.failInbox(&row, disabled, fixedNoticeTime)
	if count() != 1 {
		t.Fatalf("a dead letter for a disabled agent replies once, got %d", count())
	}
	var stored IMWebhookInbox
	db.First(&stored, row.ID)
	if stored.State != inboxDead {
		t.Fatalf("dead letter state must stay dead, got %s", stored.State)
	}
	second := row
	second.ID = 0
	second.MessageIDStr, second.SourceKey = "lifecycle-2", "lifecycle-2"
	db.Create(&second)
	p.failInbox(&second, disabled, fixedNoticeTime)
	if count() != 1 {
		t.Fatal("a second dead letter inside the window must stay silent")
	}
}

func TestAgentUnavailableDeadLetterSkipsReenabledAgentAndOtherErrors(t *testing.T) {
	db := m2DB(t)
	_, count := noticeServer(t, false)
	row := lifecycleRow(t, db)
	addNoticeDefinition(t, db, 42, "active")
	p := &Plugin{}
	p.failInbox(&row, &httpStatusError{status: http.StatusForbidden, code: "im_agent_binding_disabled"}, time.Now())
	p.failInbox(&row, &httpStatusError{status: http.StatusBadGateway, code: "upstream_http_502"}, time.Now())
	if count() != 0 {
		t.Fatalf("only a still-disabled agent gets a notice, got %d", count())
	}
}

func TestAgentUnavailableSendFailureReleasesWindow(t *testing.T) {
	db := m2DB(t)
	_, count := noticeServer(t, true)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addNoticeDefinition(t, db, 42, "deleted")
	now := fixedNoticeTime
	ctx := context.Background()
	_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("m1", 1, "1@42", personChannel, "hi")}, now)
	_, _ = acceptHuman(ctx, db, "msg.notify", []webhookMessage{webhookText("m2", 1, "1@42", personChannel, "hi")}, now)
	if count() != 1 {
		t.Fatalf("a failed send must not burn the window, got %d", count())
	}
}
