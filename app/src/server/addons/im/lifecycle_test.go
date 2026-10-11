package im

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/humatest"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

const lifecyclePath = "/api/v1/addons/conversation/im-lifecycle"

func lifecycleRow(t *testing.T, db *gorm.DB) IMWebhookInbox {
	t.Helper()
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "lifecycle", SourceKey: "lifecycle",
		SenderUID: 1, TargetAgentUID: 42, ExecutionOwnerUserID: 1, OriginSenderUID: 1,
		ChannelID: "1@42", ChannelType: personChannel, State: inboxProcessing,
		LeaseOwner: "worker", Attempts: 1, CoreConversationID: 9, DispatchCoreMessageID: 7,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func TestLifecycleBlocksDispatchFinalAndSnapshots(t *testing.T) {
	for _, stage := range []string{"dispatch", "final", "core-stream", "browser-stream"} {
		for _, refusal := range []struct {
			name   string
			status int
			body   string
		}{
			{"disabled", 403, `{"code":"im_agent_binding_disabled"}`},
			{"restored-old-work", 409, `{"code":"im_agent_lifecycle_stale"}`},
			{"unavailable", 503, `{}`},
			{"missing-decision", 200, `{}`},
		} {
			t.Run(stage+"/"+refusal.name, func(t *testing.T) {
				db := m2DB(t)
				row := lifecycleRow(t, db)
				row.CumulativeAnswer, row.Revision = "private-answer", 1
				db.Save(&row)
				checks, sends, dispatches := 0, 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case lifecyclePath:
						checks++
						var body struct {
							AgentUID uint `json:"agentUid"`
							Owner uint `json:"executionOwnerUserId"`
							SourceKey string `json:"sourceKey"`
							AdmittedAt time.Time `json:"admittedAt"`
						}
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
							body.AgentUID != 42 || body.Owner != 1 || body.SourceKey != row.SourceKey ||
							!body.AdmittedAt.Equal(row.CreatedAt) || r.Method != http.MethodPost {
							t.Error("lifecycle request lost immutable admission identity")
						}
						w.WriteHeader(refusal.status)
						fmt.Fprint(w, refusal.body)
					case "/api/v1/addons/conversation/im-dispatch":
						dispatches++
						fmt.Fprint(w, `{"conversationId":9,"sourceKey":"lifecycle","status":"accepted"}`)
					case "/message/send":
						sends++
						fmt.Fprint(w, `{}`)
					default:
						fmt.Fprint(w, "id: 8\nevent: message-updated\ndata: {\"id\":8,\"senderType\":\"agent\",\"msgType\":\"streaming\",\"content\":\"new-private-answer\"}\n\n")
					}
				}))
				defer server.Close()
				p := &Plugin{coreURL: server.URL, apiAddr: server.URL, serviceJWT: "test-token"}
				var err error
				switch stage {
				case "dispatch":
					_, err = p.dispatchCore(t.Context(), &row, nil)
				case "final":
					err = p.sendFinal(t.Context(), &row, 8, "new-private-answer")
				case "core-stream":
					err = p.consumeCoreStream(t.Context(), &row)
				case "browser-stream":
					// Cancellation bounds the old implementation without timers or sleep.
					ctx, cancel := context.WithCancel(context.WithValue(t.Context(), userIDKey, uint(1)))
					cancel()
					req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
					rec := httptest.NewRecorder()
					p.serveAgentStream(humatest.NewContext(&huma.Operation{}, req, rec), row.ChannelID, row.ChannelType)
					if strings.Contains(rec.Body.String(), "private-answer") {
						t.Fatal("browser stream disclosed revoked snapshot")
					}
					return
				}
				if err == nil || sends != 0 || dispatches != 0 {
					t.Fatalf("revoked work escaped guard: err=%v sends=%d dispatches=%d", err, sends, dispatches)
				}
				if checks == 0 {
					t.Fatal("Core lifecycle authority was not checked")
				}
				var saved IMWebhookInbox
				db.First(&saved, row.ID)
				if saved.CumulativeAnswer != "private-answer" || saved.FinalSentAt != nil {
					t.Fatal("revoked work changed delivery state")
				}
			})
		}
	}
}

func TestLifecycleParticipantRecheck(t *testing.T) {
	for _, revoked := range []string{"sender-membership", "target-membership", "origin-membership", "allowlist", "origin-account", "owner-account", "target-type"} {
		t.Run(revoked, func(t *testing.T) {
			db := m2DB(t)
			row := lifecycleRow(t, db)
			addUser(t, db, 2, authmodel.AccountTypeHuman)
			addUser(t, db, 3, authmodel.AccountTypeHuman)
			addUser(t, db, 43, authmodel.AccountTypeAgent)
			row.SenderUID, row.SourceAgentUID, row.OriginSenderUID, row.ExecutionOwnerUserID = 43, 43, 2, 3
			row.ChannelID, row.ChannelType = "g1", groupChannel
			db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1})
			insertGroupMembers(db, "g1", []uint{1, 2, 3, 42, 43})
			db.Create(&IMGroupAgentAllowlist{GroupID: "g1", AgentUID: 42, MemberUID: 2, ManagedByOwnerUID: 1})
			if err := checkDispatchParticipants(t.Context(), &row); err != nil {
				t.Fatalf("valid group rejected: %v", err)
			}
			switch revoked {
			case "sender-membership", "target-membership", "origin-membership":
				uid := map[string]uint{"sender-membership": 43, "target-membership": 42, "origin-membership": 2}[revoked]
				db.Where("group_id = ? AND uid = ?", "g1", uid).Delete(&IMGroupMember{})
			case "allowlist":
				db.Where("group_id = ?", "g1").Delete(&IMGroupAgentAllowlist{})
			case "origin-account", "owner-account":
				uid := map[string]uint{"origin-account": 2, "owner-account": 3}[revoked]
				db.Model(&authmodel.User{}).Where("id = ?", uid).Update("enable", 2)
			case "target-type":
				db.Model(&authmodel.User{}).Where("id = ?", 42).Update("account_type", authmodel.AccountTypeHuman)
			}
			if err := checkDispatchParticipants(t.Context(), &row); err == nil {
				t.Fatal("revoked participant still authorized")
			}
		})
	}
}

func TestLifecyclePolicyFailureIsNeverLeasedAfterRestore(t *testing.T) {
	db := m2DB(t)
	row := lifecycleRow(t, db)
	p := &Plugin{}
	p.failInbox(&row, &httpStatusError{status: 409, code: "im_agent_lifecycle_stale"}, time.Now())
	leased, err := leaseInbox(t.Context(), db, "next", time.Now().Add(time.Hour))
	if err != nil || leased != nil {
		t.Fatalf("terminal work replayed: leased=%v err=%v", leased, err)
	}
	if err := p.sendFinal(t.Context(), &row, 8, "late answer"); err == nil {
		t.Fatal("terminal work accepted a late final")
	} else {
		var status *httpStatusError
		if !errors.As(err, &status) {
			t.Fatalf("expected terminal policy error, got %v", err)
		}
	}
}
