package im

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	authservice "github.com/kwhitestone/prism-fusion/addons/auth/service"
)

func TestRecipientStatusFreshAndAuthenticated(t *testing.T) {
	db := m2DB(t)
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 33, authmodel.AccountTypeAgent)
	router := gin.New()
	sharedAuthStack(t, router)
	(&Plugin{}).registerRecipients(humagin.New(router, huma.DefaultConfig("recipients", "1")))
	token := issueSessionToken(t, db, 1, "recipients-session")
	path := "/api/v1/im/recipients/33"
	serviceToken, err := (&authservice.JwtService{}).GenerateToken(0, "nucleagent-im", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, denied := range []string{"", serviceToken} {
		if got := authorizedRequest(t, router, http.MethodGet, path, denied).Code; got != http.StatusUnauthorized {
			t.Fatalf("unauthorized status=%d", got)
		}
	}
	check := func(path string, want bool) {
		t.Helper()
		response := authorizedRequest(t, router, http.MethodGet, path, token)
		var body envelope[recipientData]
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil ||
			body.Data.Enabled != want || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("status=%d body=%s cache=%s", response.Code, response.Body, response.Header().Get("Cache-Control"))
		}
	}
	check(path, true)
	for _, enable := range []int{2, 1} {
		if err := db.Model(&authmodel.User{}).Where("id = ?", 33).Update("enable", enable).Error; err != nil {
			t.Fatal(err)
		}
		check(path, enable == 1)
	}
	if err := db.Delete(&authmodel.User{}, 33).Error; err != nil {
		t.Fatal(err)
	}
	check(path, false)
	check("/api/v1/im/recipients/999", false)
	if response := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/recipients/invalid", token); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("invalid UID status=%d", response.Code)
	}
	if err := db.Migrator().DropTable(&authmodel.User{}); err != nil {
		t.Fatal(err)
	}
	if _, err := recipientEnabled(t.Context(), db, 33); err == nil {
		t.Fatal("database failure must not permit a send")
	}
}

func TestDisabledRecipientWebhook(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	for _, uid := range []uint{33, 42} {
		addUser(t, db, uid, authmodel.AccountTypeAgent)
		addBinding(t, db, 1, uid)
	}
	if err := db.Model(&authmodel.User{}).Where("id = ?", 33).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		message webhookMessage
		want    int
	}{
		{webhookText("disabled", 1, "1@33", personChannel, "blocked"), 0},
		{webhookText("enabled", 1, "1@42", personChannel, "allowed"), 1},
		{webhookText("mixed", 1, "g1", groupChannel, "both", 33, 42), 1},
	} {
		got, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{tc.message}, time.Now())
		if err != nil || got != tc.want {
			t.Fatalf("%s: accepted=%d want=%d err=%v", tc.message.MessageIDStr, got, tc.want, err)
		}
	}
	var disabled int64
	db.Model(&IMWebhookInbox{}).Where("target_agent_uid = ?", 33).Count(&disabled)
	if disabled != 0 {
		t.Fatal("disabled agent was enqueued")
	}
}

func TestQueuedRecipientDisableBlocksDispatchAndFinal(t *testing.T) {
	for _, uid := range []uint{1, 33} {
		t.Run(strconvUint(uid), func(t *testing.T) {
			db := m2DB(t)
			addUser(t, db, 1, authmodel.AccountTypeHuman)
			addUser(t, db, 33, authmodel.AccountTypeAgent)
			row := IMWebhookInbox{
				Event: "msg.notify", MessageIDStr: "queued", SourceKey: "queued",
				SenderUID: 1, TargetAgentUID: 33, ChannelID: "1@33", ChannelType: personChannel,
				State: inboxProcessing, Attempts: 1, LeaseOwner: "worker",
			}
			if err := db.Create(&row).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&authmodel.User{}).Where("id = ?", uid).Update("enable", 2).Error; err != nil {
				t.Fatal(err)
			}
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()
			p := &Plugin{apiAddr: server.URL, coreURL: server.URL, serviceJWT: "test-token"}
			_, dispatchErr := p.dispatchCore(t.Context(), &row, nil)
			finalErr := p.sendFinal(t.Context(), &row, 7, "answer")
			for _, err := range []error{dispatchErr, finalErr} {
				var denied *httpStatusError
				if !errors.As(err, &denied) || denied.code != "im_recipient_disabled" {
					t.Fatalf("want policy refusal, got %v", err)
				}
			}
			if calls != 0 {
				t.Fatalf("upstream calls=%d", calls)
			}
			p.failInbox(&row, finalErr, time.Now())
			if err := db.First(&row, row.ID).Error; err != nil {
				t.Fatal(err)
			}
			if row.State != inboxDead || row.NextAttemptAt != nil || row.FinalSentAt != nil {
				t.Fatalf("disabled work was retried or sent: %+v", row)
			}
		})
	}
}
