package im

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// agentGroup builds the standard M3 fixture: human 1 owns agents 42 and 43, both are
// members of group "g1", and the group creator is the human so invocation is allowed.
func agentGroup(t *testing.T, db *gorm.DB) groupMemberResolver {
	t.Helper()
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	for _, agent := range []uint{42, 43} {
		addUser(t, db, agent, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, agent)
	}
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	return func(context.Context, string) ([]uint, error) { return []uint{1, 42, 43}, nil }
}

// agentMessage is an agent's durable message as sendFinal writes it: no mention
// metadata, visible @name text, and the A2A provenance stamp.
func agentMessage(id string, sender uint, content string, provenance *a2aProvenance) webhookMessage {
	payload, _ := json.Marshal(textPayload{Type: wuKongTextType, Content: content, A2A: provenance})
	return webhookMessage{
		MessageIDStr: id, ClientMsgNo: "client-" + id, FromUID: strconvUint(sender),
		ChannelID: "g1", ChannelType: groupChannel, Timestamp: 1, RawPayload: payload,
	}
}

func chainOf(depth int) *a2aProvenance {
	return &a2aProvenance{ChainID: "chain:origin", Depth: depth, OriginUID: 1, ViaUID: 42}
}

func TestA2AOneHopTriggerFromAgentMentionText(t *testing.T) {
	db := m2DB(t)
	members := agentGroup(t, db)
	now := time.Unix(1_800_000_000, 0)

	message := agentMessage("hop-1", 42, "转给 @user-43：请回答这个问题", chainOf(1))
	accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify", []webhookMessage{message}, now, members)
	if err != nil || accepted != 1 {
		t.Fatalf("accepted=%d err=%v", accepted, err)
	}
	var row IMWebhookInbox
	if err := db.Where("message_idstr = ?", "hop-1").First(&row).Error; err != nil {
		t.Fatal(err)
	}
	if row.TargetAgentUID != 43 || row.SenderUID != 42 {
		t.Fatalf("target=%d sender=%d", row.TargetAgentUID, row.SenderUID)
	}
	// Depth advances one hop, the chain and the authorizing human are carried forward,
	// and the sending agent is recorded for the "via @agentA" badge.
	if row.ChainID != "chain:origin" || row.ChainDepth != 2 ||
		row.OriginSenderUID != 1 || row.SourceAgentUID != 42 {
		t.Fatalf("provenance=%+v", row)
	}
}

func TestA2ACRuleParserBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		content  string
		setup    func(t *testing.T, db *gorm.DB)
		members  []uint
		accepted int
	}{
		{name: "exact username match", content: "ask @user-43 please", accepted: 1},
		{name: "multiple targets all dispatched (D18-A)", content: "@user-43 and @user-44", accepted: 2,
			setup: func(t *testing.T, db *gorm.DB) {
				addUser(t, db, 44, authmodel.AccountTypeAgent)
				addBinding(t, db, 1, 44)
			},
			members: []uint{1, 42, 43, 44}},
		{name: "case sensitive, no fuzzy match", content: "ask @USER-43 please"},
		{name: "substring is not a match", content: "ask @user-4 please"},
		{name: "prefix extension is not a match", content: "ask @user-433 please"},
		{name: "display name is not a username", content: "ask @User 43 please"},
		{name: "non member ignored", content: "ask @user-43 please", members: []uint{1, 42}},
		{name: "disabled agent ignored", content: "ask @user-43 please", setup: func(t *testing.T, db *gorm.DB) {
			if err := db.Model(&authmodel.User{}).Where("id = ?", 43).Update("enable", 2).Error; err != nil {
				t.Fatal(err)
			}
		}},
		{name: "human member is not an agent target", content: "ask @user-1 please"},
		{name: "self mention never re-triggers", content: "ask @user-42 please"},
		{name: "no mention text at all", content: "just an answer, no targets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := m2DB(t)
			members := agentGroup(t, db)
			if tc.setup != nil {
				tc.setup(t, db)
			}
			if tc.members != nil {
				uids := tc.members
				members = func(context.Context, string) ([]uint, error) { return uids, nil }
			}
			message := agentMessage("parse", 42, tc.content, chainOf(1))
			accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
				[]webhookMessage{message}, time.Unix(1_800_000_000, 0), members)
			if err != nil {
				t.Fatal(err)
			}
			if accepted != tc.accepted {
				t.Fatalf("accepted=%d, want %d", accepted, tc.accepted)
			}
			if count := inboxCount(t, db); count != int64(tc.accepted) {
				t.Fatalf("inbox count=%d, want %d", count, tc.accepted)
			}
		})
	}
}

func TestA2AHumanSenderKeepsM2MentionRules(t *testing.T) {
	db := m2DB(t)
	members := agentGroup(t, db)
	now := time.Unix(1_800_000_000, 0)

	// A human's visible @name text is still NOT a trigger: the D9 amendment covers
	// agent senders only, so structured mention.uids remain mandatory for humans.
	text := webhookText("human-text", 1, "g1", groupChannel, "转给 @user-43：请回答")
	if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{text}, now, members); err != nil || accepted != 0 {
		t.Fatalf("human @name accepted=%d err=%v", accepted, err)
	}

	structured := webhookText("human-uids", 1, "g1", groupChannel, "no at-sign here", 43)
	if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{structured}, now, members); err != nil || accepted != 1 {
		t.Fatalf("human mention.uids accepted=%d err=%v", accepted, err)
	}
	var row IMWebhookInbox
	if err := db.Where("message_idstr = ?", "human-uids").First(&row).Error; err != nil {
		t.Fatal(err)
	}
	// A human trigger starts a fresh chain at depth 1 with no source agent.
	if row.ChainDepth != 1 || row.OriginSenderUID != 1 || row.SourceAgentUID != 0 ||
		row.ChainID != "chain:human-uids" {
		t.Fatalf("human provenance=%+v", row)
	}
}

func TestA2AUnstampedAgentMessageIsNotATrigger(t *testing.T) {
	db := m2DB(t)
	members := agentGroup(t, db)
	// An agent message with no provenance was not produced by a dispatch this bridge
	// owns, so it stays ignored exactly as in M2 rather than starting a new chain.
	message := agentMessage("unstamped", 42, "ask @user-43 please", nil)
	accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{message}, time.Unix(1_800_000_000, 0), members)
	if err != nil || accepted != 0 {
		t.Fatalf("accepted=%d err=%v", accepted, err)
	}
}

func TestA2APrivateChannelAgentSenderStillRejected(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addBinding(t, db, 1, 42)
	payload, _ := json.Marshal(textPayload{
		Type: wuKongTextType, Content: "ask @user-42", A2A: chainOf(1),
	})
	message := webhookMessage{
		MessageIDStr: "private-a2a", ClientMsgNo: "c", FromUID: "42",
		ChannelID: "1@42", ChannelType: personChannel, Timestamp: 1, RawPayload: payload,
	}
	members := func(context.Context, string) ([]uint, error) { return []uint{1, 42}, nil }
	accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{message}, time.Unix(1_800_000_000, 0), members)
	if err != nil || accepted != 0 {
		t.Fatalf("private A2A accepted=%d err=%v", accepted, err)
	}
}

func TestA2ADepthGuardRejectsTheFourthHop(t *testing.T) {
	db := m2DB(t)
	members := agentGroup(t, db)
	now := time.Unix(1_800_000_000, 0)

	// depth 1 (human) -> 2 -> 3 accepted; the hop that would land at depth 4 is rejected.
	for _, depth := range []int{1, 2} {
		message := agentMessage(fmt.Sprintf("d%d", depth), 42, "ask @user-43 please", chainOf(depth))
		if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
			[]webhookMessage{message}, now, members); err != nil || accepted != 1 {
			t.Fatalf("depth %d accepted=%d err=%v", depth+1, accepted, err)
		}
	}
	tooDeep := agentMessage("d3", 42, "ask @user-43 please", chainOf(maxChainDepth))
	accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify", []webhookMessage{tooDeep}, now, members)
	if !errors.Is(err, errA2ADepthExceeded) || accepted != 0 {
		t.Fatalf("depth 4 accepted=%d err=%v", accepted, err)
	}
	// Rejected at the guard: no inbox row for the over-deep hop.
	var count int64
	if err := db.Model(&IMWebhookInbox{}).Where("message_idstr = ?", "d3").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("over-deep inbox rows=%d, want 0", count)
	}
}

func TestA2AChainBudgetBreaker(t *testing.T) {
	db := m2DB(t)
	members := agentGroup(t, db)
	now := time.Unix(1_800_000_000, 0)

	// The breaker is hourly and per chain, so it binds across minutes and senders where
	// the M2 minute limits do not. Charge the chain to its ceiling directly...
	for i := 0; i < chainHourlyBudget; i++ {
		if err := db.Transaction(func(tx *gorm.DB) error {
			return takeChainBudget(tx, "chain:origin", now.Add(time.Duration(i)*time.Minute))
		}); err != nil {
			t.Fatalf("budget charge %d err=%v", i+1, err)
		}
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return takeChainBudget(tx, "chain:origin", now.Add(45*time.Minute))
	}); !errors.Is(err, errA2ABudgetExceeded) {
		t.Fatalf("twenty-first charge err=%v", err)
	}

	// ...then assert the accept path is actually wired to it: a hop on the exhausted
	// chain is refused with the 429-domain code and leaves no inbox row.
	message := agentMessage("budget", 42, "ask @user-43 please", chainOf(1))
	accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{message}, now.Add(50*time.Minute), members)
	if !errors.Is(err, errA2ABudgetExceeded) || accepted != 0 {
		t.Fatalf("exhausted chain accepted=%d err=%v", accepted, err)
	}
	if count := inboxCount(t, db); count != 0 {
		t.Fatalf("inbox count=%d, want 0", count)
	}

	// A different chain is unaffected — the breaker is per origin chain, not global.
	other := agentMessage("other-chain", 42, "ask @user-43 please",
		&a2aProvenance{ChainID: "chain:other", Depth: 1, OriginUID: 1, ViaUID: 42})
	if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{other}, now.Add(50*time.Minute), members); err != nil || accepted != 1 {
		t.Fatalf("other chain accepted=%d err=%v", accepted, err)
	}
}

func TestA2ARateLimitsRemainAdditiveForAgentSenders(t *testing.T) {
	db := m2DB(t)
	members := agentGroup(t, db)
	now := time.Unix(1_800_000_000, 0)

	// The M2 per-(sender x agent x channel) minute limit applies unchanged to an agent
	// sender: the sixth hop in one minute is refused.
	for i := 1; i <= senderMinuteLimit; i++ {
		message := agentMessage(fmt.Sprintf("r%d", i), 42, "ask @user-43 please", chainOf(1))
		if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
			[]webhookMessage{message}, now, members); err != nil || accepted != 1 {
			t.Fatalf("hop %d accepted=%d err=%v", i, accepted, err)
		}
	}
	over := agentMessage("r6", 42, "ask @user-43 please", chainOf(1))
	accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify", []webhookMessage{over}, now, members)
	if !errors.Is(err, errAgentRateLimited) || accepted != 0 {
		t.Fatalf("sixth hop accepted=%d err=%v", accepted, err)
	}
}

func TestA2AAgentHopUsesOriginHumanAuthorization(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman) // group creator, owns nothing here
	addUser(t, db, 2, authmodel.AccountTypeHuman) // allowlisted for agent 43 only
	for _, agent := range []uint{42, 43} {
		addUser(t, db, agent, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, agent)
	}
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	members := func(context.Context, string) ([]uint, error) { return []uint{1, 2, 42, 43}, nil }
	now := time.Unix(1_800_000_000, 0)

	// Chain started by human 2, who is NOT allowlisted for agent 43: the hop is refused
	// because an agent carries no invocation rights of its own.
	unauthorized := agentMessage("auth-no", 42, "ask @user-43 please",
		&a2aProvenance{ChainID: "chain:x", Depth: 1, OriginUID: 2, ViaUID: 42})
	if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{unauthorized}, now, members); err != nil || accepted != 0 {
		t.Fatalf("unauthorized origin accepted=%d err=%v", accepted, err)
	}

	if err := db.Create(&IMGroupAgentAllowlist{
		GroupID: "g1", AgentUID: 43, MemberUID: 2, ManagedByOwnerUID: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	authorized := agentMessage("auth-yes", 42, "ask @user-43 please",
		&a2aProvenance{ChainID: "chain:x", Depth: 1, OriginUID: 2, ViaUID: 42})
	if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify",
		[]webhookMessage{authorized}, now, members); err != nil || accepted != 1 {
		t.Fatalf("authorized origin accepted=%d err=%v", accepted, err)
	}
}

func TestDispatchCarriesProvenanceToCore(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addUser(t, db, 43, authmodel.AccountTypeAgent)
	row := &IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "carry", TargetAgentUID: 43, SenderUID: 42,
		ChannelID: "g1", ChannelType: groupChannel, Text: "hello", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:carry:43",
		ChainID: "chain:carry", ChainDepth: 2, OriginSenderUID: 1, SourceAgentUID: 42,
	}
	if err := db.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	var got *a2aProvenance
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input dispatchRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		got = input.Provenance
		_ = json.NewEncoder(w).Encode(dispatchResult{
			ConversationID: 9, SourceKey: input.SourceKey, Status: "accepted",
		})
	}))
	defer core.Close()

	p := &Plugin{coreURL: core.URL, serviceJWT: "test-token"}
	if _, err := p.dispatchCore(t.Context(), row, nil); err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ChainID != "chain:carry" || got.Depth != 2 ||
		got.OriginUID != 1 || got.ViaUID != 42 {
		t.Fatalf("dispatched provenance=%+v", got)
	}
}

func TestSendFinalStampsProvenanceReadableByTheNextHop(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "stamp", TargetAgentUID: 42, SenderUID: 1,
		ChannelID: "g1", ChannelType: groupChannel, Text: "hello", State: inboxProcessing,
		ExecutionOwnerUserID: 1, SourceKey: "wukong:msg.notify:stamp:42",
		ChainID: "chain:stamp", ChainDepth: 2, OriginSenderUID: 1, SourceAgentUID: 43,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	var sent []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(body.Payload)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sent = decoded
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := (&Plugin{apiAddr: server.URL}).sendFinal(t.Context(), &row, 7, "转给 @user-43"); err != nil {
		t.Fatal(err)
	}

	// The captured payload is exactly what the next webhook hop parses.
	var payload textPayload
	if err := json.Unmarshal(sent, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Type != wuKongTextType || payload.Content != "转给 @user-43" {
		t.Fatalf("payload=%+v", payload)
	}
	// ViaUID is the agent that TRIGGERED this answer (43), not its author (42): that is
	// what the client renders as "via @agentA".
	if payload.A2A == nil || payload.A2A.ChainID != "chain:stamp" || payload.A2A.Depth != 2 ||
		payload.A2A.OriginUID != 1 || payload.A2A.ViaUID != 43 {
		t.Fatalf("provenance=%+v", payload.A2A)
	}
	// Agent messages carry no mention metadata; the amendment exists precisely because
	// of this, so assert the shape rather than assume it.
	if strings.Contains(string(sent), `"mention"`) {
		t.Fatalf("agent payload must not carry mention metadata: %s", sent)
	}
}
