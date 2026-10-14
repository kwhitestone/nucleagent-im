package im

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
)

func TestWebhookHTTPBeta21Envelope(t *testing.T) {
	// beta.21 internal/runtime/webhook/mapper.go emits these 12 fields.
	const fixture = `{"header":{"no_persist":0,"red_dot":1,"sync_once":0},
		"setting":0,"expire":0,"message_id":1234567890123456789,
		"message_idstr":"1234567890123456789","client_msg_no":"client-beta21",
		"message_seq":17,"from_uid":"2","channel_id":"g1","channel_type":2,
		"timestamp":1790000000,"payload":%q}`
	capability := strings.Repeat("c", 32)
	for _, tc := range []struct {
		name     string
		token    string
		payload  string
		edit     func(map[string]any)
		status   int
		accepted int
		debug    bool
	}{
		{name: "real envelope", token: capability, status: 200, accepted: 1},
		{name: "raw capture opt in", token: capability, status: 200, accepted: 1, debug: true},
		{name: "capture redacts capability", token: capability, status: 200, accepted: 1, debug: true, edit: func(m map[string]any) {
			m["topic"] = "url?token=" + capability
		}},
		{name: "legacy seven fields", token: capability, status: 200, accepted: 1, edit: func(m map[string]any) {
			for _, key := range []string{"header", "setting", "expire", "message_id", "message_seq"} {
				delete(m, key)
			}
		}},
		{name: "optional topic", token: capability, status: 200, accepted: 1, edit: func(m map[string]any) {
			m["topic"] = "topic-a"
		}},
		{name: "wrong capability", token: "wrong", status: 401, debug: true},
		{name: "missing capability", status: 401},
		{name: "non text ignored", token: capability, status: 200, payload: `{"type":2,"content":"hello","mention":{"all":0,"uids":["42"]}}`},
		{name: "agent ignored", token: capability, status: 200, edit: func(m map[string]any) {
			m["from_uid"] = "42"
		}},
		{name: "unknown property rejected", token: capability, status: 422, edit: func(m map[string]any) {
			m["unexpected"] = true
		}},
		{name: "unknown header property rejected", token: capability, status: 422, edit: func(m map[string]any) {
			m["header"].(map[string]any)["unexpected"] = true
		}},
		{name: "numeric ID required", token: capability, status: 422, edit: func(m map[string]any) {
			m["message_id"] = "1234567890123456789"
		}},
		{name: "setting byte range", token: capability, status: 422, edit: func(m map[string]any) {
			m["setting"] = 256
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("IM_WEBHOOK_DEBUG_BODY", "")
			if tc.debug {
				t.Setenv("IM_WEBHOOK_DEBUG_BODY", "1")
			}
			db := m2DB(t)
			addUser(t, db, 2, authmodel.AccountTypeHuman)
			addUser(t, db, 42, authmodel.AccountTypeAgent)
			addBinding(t, db, 1, 42)
			if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&IMGroupAgentAllowlist{
				GroupID: "g1", AgentUID: 42, MemberUID: 2, ManagedByOwnerUID: 1,
			}).Error; err != nil {
				t.Fatal(err)
			}
			gin.SetMode(gin.TestMode)
			router := gin.New()
			sharedAuthStack(t, router)
			api := humagin.New(router, huma.DefaultConfig("webhook contract test", "1"))
			(&Plugin{webhookCapability: capability}).registerWebhook(api)
			if api.OpenAPI().Components.Schemas.Map()["WebhookMessage"].AdditionalProperties != false {
				t.Fatal("webhook schema must remain strict")
			}
			payload := tc.payload
			if payload == "" {
				payload = `{"type":1,"content":"hello","mention":{"all":0,"uids":["42"]}}`
			}
			body := fmt.Sprintf(fixture, base64.StdEncoding.EncodeToString([]byte(payload)))
			var fields map[string]any
			decoder := json.NewDecoder(strings.NewReader(body))
			decoder.UseNumber()
			if err := decoder.Decode(&fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 12 {
				t.Fatalf("fixture fields=%d, want 12", len(fields))
			}
			if tc.edit != nil {
				tc.edit(fields)
				encoded, err := json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				body = string(encoded)
			}
			req := httptest.NewRequest(http.MethodPost,
				"/api/v1/im/webhooks/wukong?event=msg.notify&token="+tc.token,
				strings.NewReader("["+body+"]"))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout := os.Stdout
			os.Stdout = writer
			t.Cleanup(func() { os.Stdout = stdout; reader.Close(); writer.Close() })
			router.ServeHTTP(rec, req)
			os.Stdout = stdout
			writer.Close()
			logged, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if tc.debug && tc.status == http.StatusOK {
				var record struct {
					Level string `json:"level"`
					Body  string `json:"body"`
				}
				if err := json.Unmarshal(logged, &record); err != nil {
					t.Fatal(err)
				}
				want := strings.ReplaceAll("["+body+"]", capability, "[REDACTED]")
				if record.Level != "DEBUG" || record.Body != want || strings.Contains(string(logged), capability) {
					t.Fatal("capture must retain raw bytes except capability redaction")
				}
			} else if len(logged) != 0 {
				t.Fatal("capture must be opt-in and authenticated")
			}
			if rec.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if count := inboxCount(t, db); count != int64(tc.accepted) {
				t.Fatalf("inbox count=%d, want %d", count, tc.accepted)
			}
			if rec.Code == http.StatusOK {
				var response webhookOutput
				if err := json.Unmarshal(rec.Body.Bytes(), &response.Body); err != nil {
					t.Fatal(err)
				}
				if response.Body.Data.Accepted != tc.accepted {
					t.Fatalf("accepted=%d, want %d", response.Body.Data.Accepted, tc.accepted)
				}
			}
		})
	}
}

// C15: resolvePayer is the single execution/billing-owner entry; 0 means reject,
// never fall back to the sender.
func TestResolvePayerPriority(t *testing.T) {
	const instanceOwner, groupOwner, creator, origin = 11, 22, 33, 44
	for _, tc := range []struct {
		name        string
		kind        payerKind
		channelType uint8
		group, crt  uint
		want        uint
	}{
		{"instance in group", payerInstance, groupChannel, groupOwner, 0, instanceOwner},
		{"instance in dm", payerInstance, personChannel, 0, 0, instanceOwner},
		{"private agent in dm", payerPrivateAgent, personChannel, 0, creator, creator},
		{"private agent in group pays creator", payerPrivateAgent, groupChannel, groupOwner, creator, creator},
		{"private agent without creator", payerPrivateAgent, groupChannel, groupOwner, 0, 0},
		{"definition in group pays group owner", payerDefinition, groupChannel, groupOwner, 0, groupOwner},
		{"definition in group without owner", payerDefinition, groupChannel, 0, 0, 0},
		{"definition in dm pays origin", payerDefinition, personChannel, groupOwner, 0, origin},
		{"unknown kind", 0, groupChannel, groupOwner, creator, 0},
	} {
		if got := resolvePayer(tc.kind, tc.channelType, instanceOwner, tc.group, tc.crt, origin); got != tc.want {
			t.Errorf("%s: payer=%d want %d", tc.name, got, tc.want)
		}
	}
	if got := resolvePayer(payerInstance, groupChannel, 0, groupOwner, creator, origin); got != 0 {
		t.Errorf("instance without owner must be 0, got %d", got)
	}
}

// G1: a definition agent in a group runs (and bills) as the group owner, whoever @s
// it; the allowlist still gates by the chain's human origin (C9, C12).
func TestGroupDefinitionRunsAsGroupOwner(t *testing.T) {
	db := m2DB(t)
	for _, id := range []uint{1, 2, 3} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addUser(t, db, 50, authmodel.AccountTypeAgent)
	addDefinition(t, db, 42, true)
	addDefinition(t, db, 50, true)
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for _, agent := range []uint{42, 50} {
		if err := db.Create(&IMGroupAgentAllowlist{GroupID: "g1", AgentUID: agent, MemberUID: 2, ManagedByOwnerUID: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(1_800_000_000, 0)
	accept := func(m webhookMessage) int {
		n, err := acceptWebhookBatch(t.Context(), db, "msg.notify", []webhookMessage{m}, now,
			func(context.Context, string) ([]uint, error) { return []uint{1, 2, 3, 42, 50}, nil })
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if accept(webhookText("o1", 1, "g1", groupChannel, "hi", 42)) != 1 ||
		accept(webhookText("m1", 2, "g1", groupChannel, "hi", 42)) != 1 {
		t.Fatal("owner and allowlisted member must both be accepted")
	}
	if ownerOf(t, db, "o1") != 1 || ownerOf(t, db, "m1") != 1 {
		t.Fatalf("owners o1=%d m1=%d, want group owner 1 (one shared mapping key)", ownerOf(t, db, "o1"), ownerOf(t, db, "m1"))
	}
	if accept(webhookText("x1", 3, "g1", groupChannel, "hi", 42)) != 0 {
		t.Fatal("a member outside the allowlist must be refused")
	}
	// A2A: member 2 started the chain; the hop to 50 still bills the group owner and
	// is allowed because the ORIGIN (2) is allowlisted for 50.
	hop := agentMessage("h1", 42, "ask @user-50", &a2aProvenance{ChainID: "chain:m1", Depth: 1, OriginUID: 2})
	if accept(hop) != 1 || ownerOf(t, db, "h1") != 1 {
		t.Fatal("A2A hop must run as the group owner")
	}
	// Private chat with the same definition stays the sender's own.
	if accept(webhookText("d1", 2, "2@42", personChannel, "hi")) != 1 || ownerOf(t, db, "d1") != 2 {
		t.Fatal("a direct chat keeps the sender as owner")
	}
}

func TestGroupDefinitionWithoutGroupHasNoTarget(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addDefinition(t, db, 42, true)
	got, err := resolveAgentTargets(db, []uint{42}, groupChannel, "missing", 2, 2)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing group must yield no target (never the sender): %+v %v", got, err)
	}
}

// Instance bindings keep their owner in a group (unchanged by G1).
func TestGroupInstanceBindingKeepsInstanceOwner(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addBinding(t, db, 7, 42)
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	got, err := resolveAgentTargets(db, []uint{42}, groupChannel, "g1", 2, 2)
	if err != nil || len(got) != 1 || got[0].ownerUID != 7 {
		t.Fatalf("instance binding owner: %+v %v", got, err)
	}
}
