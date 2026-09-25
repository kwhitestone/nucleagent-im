package im

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

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
