package im

import (
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

const lifecyclePath = "/api/v1/addons/conversation/im-lifecycle"

func allowLifecycle(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != lifecyclePath {
		return false
	}
	fmt.Fprint(w, `{"allowed":true}`)
	return true
}

func lifecycleGroup(t *testing.T, db *gorm.DB, members ...uint) {
	t.Helper()
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := insertGroupMembers(db, "g1", members); err != nil {
		t.Fatal(err)
	}
}

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
							AgentUID   uint      `json:"agentUid"`
							Owner      uint      `json:"executionOwnerUserId"`
							SourceKey  string    `json:"sourceKey"`
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
					rec := httptest.NewRecorder()
					p.sendAgentSnapshots(t.Context(), rec, rec, 1, row.ChannelID, row.ChannelType, time.Time{}, map[uint]string{})
					if strings.Contains(rec.Body.String(), "private-answer") {
						t.Fatal("browser stream disclosed revoked snapshot")
					}
					if checks == 0 {
						t.Fatal("browser stream skipped Core lifecycle authority")
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

func TestLifecycleRechecksDuringCoreStream(t *testing.T) {
	db := m2DB(t)
	row := lifecycleRow(t, db)
	checks, sends := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case lifecyclePath:
			checks++
			if checks > 2 {
				w.WriteHeader(http.StatusConflict)
				fmt.Fprint(w, `{"code":"im_agent_lifecycle_stale"}`)
				return
			}
			fmt.Fprint(w, `{"allowed":true}`)
		case "/message/send":
			sends++
		default:
			for i, text := range []string{"allowed", "revoked"} {
				fmt.Fprintf(w, "id: %d\nevent: message-updated\ndata: {\"id\":%d,\"senderType\":\"agent\",\"msgType\":\"streaming\",\"content\":%q}\n\n", 8+i, 8+i, text)
			}
			fmt.Fprint(w, "id: 10\nevent: message-created\ndata: {\"id\":10,\"senderType\":\"agent\",\"msgType\":\"text\",\"content\":\"revoked final\"}\n\n")
		}
	}))
	defer server.Close()
	p := &Plugin{coreURL: server.URL, apiAddr: server.URL, serviceJWT: "test-token"}
	if err := p.consumeCoreStream(t.Context(), &row); err == nil {
		t.Fatal("stream continued after lifecycle revocation")
	}
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if row.State != inboxDead || row.CumulativeAnswer != "allowed" || row.Revision != 1 || sends != 0 {
		t.Fatalf("revoked stream persisted or delivered output: state=%s revision=%d sends=%d", row.State, row.Revision, sends)
	}
}

func TestLifecycleBrowserRechecksViewer(t *testing.T) {
	for _, revoke := range []string{"membership-between-polls", "account-between-polls", "membership-during-check"} {
		t.Run(revoke, func(t *testing.T) {
			db := m2DB(t)
			row := lifecycleRow(t, db)
			addUser(t, db, 2, authmodel.AccountTypeHuman)
			lifecycleGroup(t, db, 1, 2, 42)
			row.ChannelID, row.ChannelType = "g1", groupChannel
			row.CumulativeAnswer, row.Revision = "before", 1
			if err := db.Save(&row).Error; err != nil {
				t.Fatal(err)
			}
			checks := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				checks++
				if revoke == "membership-during-check" && checks == 2 {
					if err := db.Where("group_id = ? AND uid = ?", "g1", 2).Delete(&IMGroupMember{}).Error; err != nil {
						t.Error(err)
					}
				}
				allowLifecycle(w, r)
			}))
			defer server.Close()
			p := &Plugin{coreURL: server.URL, serviceJWT: "test-token"}
			rec := httptest.NewRecorder()
			seen := map[uint]string{}
			if !p.sendAgentSnapshots(t.Context(), rec, rec, 2, "g1", groupChannel, time.Time{}, seen) ||
				!strings.Contains(rec.Body.String(), "before") {
				t.Fatal("initial authorized snapshot missing")
			}
			switch revoke {
			case "membership-between-polls":
				db.Where("group_id = ? AND uid = ?", "g1", 2).Delete(&IMGroupMember{})
			case "account-between-polls":
				db.Model(&authmodel.User{}).Where("id = ?", 2).Update("enable", 2)
			}
			db.Model(&row).Updates(map[string]any{"revision": 2, "cumulative_answer": "after-revocation"})
			before := rec.Body.Len()
			if p.sendAgentSnapshots(t.Context(), rec, rec, 2, "g1", groupChannel, time.Time{}, seen) ||
				rec.Body.Len() != before {
				t.Fatal("revoked viewer continued receiving events")
			}
		})
	}
}

func TestLifecycleLocalRevocationDuringCoreCheck(t *testing.T) {
	for _, revoke := range []string{"account", "terminal-state"} {
		t.Run(revoke, func(t *testing.T) {
			db := m2DB(t)
			row := lifecycleRow(t, db)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				if revoke == "account" {
					err = db.Model(&authmodel.User{}).Where("id = ?", 42).Update("enable", 2).Error
				} else {
					err = markTerminalFailure(row.ID, "im_agent_lifecycle_stale")
				}
				if err != nil {
					t.Error(err)
				}
				allowLifecycle(w, r)
			}))
			defer server.Close()
			p := &Plugin{coreURL: server.URL, serviceJWT: "test-token"}
			if err := p.checkDelivery(t.Context(), &row); err == nil {
				t.Fatal("local revocation during Core check was ignored")
			}
		})
	}
}

func TestLifecycleRestoreRejectsOldAdmissionButAllowsNew(t *testing.T) {
	db := m2DB(t)
	old := lifecycleRow(t, db)
	barrier := old.CreatedAt.Add(time.Minute)
	checks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checks++
		var input lifecycleRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if !input.AdmittedAt.After(barrier) {
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"code":"im_agent_lifecycle_stale"}`)
			return
		}
		allowLifecycle(w, r)
	}))
	defer server.Close()
	p := &Plugin{coreURL: server.URL, serviceJWT: "test-token"}
	// IM observed neither disable nor restore. Auth is currently enabled.
	if err := p.checkDelivery(t.Context(), &old); err == nil {
		t.Fatal("old admission replayed after restore")
	}
	// A retry cannot substitute its current time for the original admission.
	old.CreatedAt = barrier.Add(time.Hour)
	if err := p.checkDelivery(t.Context(), &old); err == nil || checks != 1 {
		t.Fatal("terminal work was reauthorized")
	}
	if leased, err := leaseInbox(t.Context(), db, "next", barrier.Add(time.Hour)); err != nil || leased != nil {
		t.Fatalf("revoked row became leasable: %v", err)
	}
	fresh := old
	fresh.ID, fresh.SourceKey, fresh.MessageIDStr = 0, "fresh", "fresh"
	if err := db.Create(&fresh).Error; err != nil {
		t.Fatal(err)
	}
	if err := p.checkDelivery(t.Context(), &fresh); err != nil {
		t.Fatalf("new admission after re-enable refused: %v", err)
	}
}
