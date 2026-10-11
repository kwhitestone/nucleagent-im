package im

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
)

// UNI-P-BATCH1 P3 (IM-REDESIGN O1): opening a chat must not replay replies and
// failures that finished long ago. They are history: a completed row is already
// the durable message, and a dead row from yesterday is not news. Replaying them
// showed every old answer a second time as a live bubble (DEV) and stacked old
// "could not complete" lines under the newest reply (TEST/PREPROD).
func TestAgentStreamSkipsLongFinishedRows(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	old := time.Now().Add(-time.Hour)
	rows := []IMWebhookInbox{
		{SourceKey: "old-complete", State: inboxCompleted, CumulativeAnswer: "2+3 is 5", Revision: 3, FinalWuKongClientMsgNo: "im-old"},
		{SourceKey: "old-dead", State: inboxDead, LastErrorCode: "im_invalid_provider_model"},
		{SourceKey: "fresh-complete", State: inboxCompleted, CumulativeAnswer: "just now", Revision: 2, FinalWuKongClientMsgNo: "im-new"},
		{SourceKey: "fresh-dead", State: inboxDead, LastErrorCode: "im_execution_failed", CumulativeAnswer: "never disclose failed content"},
		{SourceKey: "in-flight", State: inboxProcessing, CumulativeAnswer: "typing", Revision: 1},
	}
	for i := range rows {
		rows[i].Event, rows[i].MessageIDStr, rows[i].TargetAgentUID, rows[i].SenderUID = "msg.notify", rows[i].SourceKey, 42, 1
		rows[i].ChannelID, rows[i].ChannelType, rows[i].Text, rows[i].ExecutionOwnerUserID = "1@42", personChannel, "q", 1
		if err := db.Create(&rows[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	// in-flight is old too: a long agent run that finishes while the chat is open.
	for _, key := range []string{"old-complete", "old-dead", "in-flight"} {
		if err := db.Model(&IMWebhookInbox{}).Where("source_key = ?", key).UpdateColumn("updated_at", old).Error; err != nil {
			t.Fatal(err)
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !allowLifecycle(w, r) {
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	p := &Plugin{coreURL: server.URL, serviceJWT: "test-token"}
	rec := httptest.NewRecorder()
	seen := map[uint]string{}
	finishedSince := time.Now().Add(-agentStreamReplayWindow)
	send := func() {
		t.Helper()
		if !p.sendAgentSnapshots(t.Context(), rec, rec, 1, "1@42", personChannel, finishedSince, seen) {
			t.Fatal("authorized stream stopped")
		}
	}
	send()
	// Finishes between polls: a live event, never history.
	if err := db.Model(&IMWebhookInbox{}).Where("source_key = ?", "in-flight").
		Updates(map[string]any{"state": inboxCompleted, "cumulative_answer": "done", "revision": 2, "final_wu_kong_client_msg_no": "im-live"}).Error; err != nil {
		t.Fatal(err)
	}
	send()
	body := rec.Body.String()

	for _, stale := range []string{"old-complete", "old-dead"} {
		if strings.Contains(body, `"`+stale+`"`) {
			t.Errorf("replayed long-finished row %s:\n%s", stale, body)
		}
	}
	for _, want := range []string{
		`event: error`,
		`event: complete` + "\n" + `data: {"clientMsgNo":"im-new"`,
		`event: snapshot` + "\n" + `data: {"revision":1,"sourceKey":"in-flight"`,
		`event: complete` + "\n" + `data: {"clientMsgNo":"im-live"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	if strings.Contains(body, "never disclose failed content") {
		t.Fatal("failed row disclosed content")
	}
}
