package im

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/humatest"
)

// UNI-P-BATCH1 P3 (IM-REDESIGN O1): opening a chat must not replay replies and
// failures that finished long ago. They are history: a completed row is already
// the durable message, and a dead row from yesterday is not news. Replaying them
// showed every old answer a second time as a live bubble (DEV) and stacked old
// "could not complete" lines under the newest reply (TEST/PREPROD).
func TestAgentStreamSkipsLongFinishedRows(t *testing.T) {
	db := m2DB(t)
	old := time.Now().Add(-time.Hour)
	rows := []IMWebhookInbox{
		{SourceKey: "old-complete", State: inboxCompleted, CumulativeAnswer: "2+3 is 5", Revision: 3, FinalWuKongClientMsgNo: "im-old"},
		{SourceKey: "old-dead", State: inboxDead, LastErrorCode: "im_invalid_provider_model"},
		{SourceKey: "fresh-complete", State: inboxCompleted, CumulativeAnswer: "just now", Revision: 2, FinalWuKongClientMsgNo: "im-new"},
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

	body := streamFor(t, 700*time.Millisecond, func() {
		// Finishes while the chat is open: a live event, never history.
		time.Sleep(200 * time.Millisecond)
		db.Model(&IMWebhookInbox{}).Where("source_key = ?", "in-flight").
			Updates(map[string]any{"state": inboxCompleted, "cumulative_answer": "done", "revision": 2, "final_wu_kong_client_msg_no": "im-live"})
	})

	for _, stale := range []string{"old-complete", "old-dead"} {
		if strings.Contains(body, `"`+stale+`"`) {
			t.Errorf("replayed long-finished row %s:\n%s", stale, body)
		}
	}
	for _, want := range []string{
		`event: complete` + "\n" + `data: {"clientMsgNo":"im-new"`,
		`event: snapshot` + "\n" + `data: {"revision":1,"sourceKey":"in-flight"`,
		`event: complete` + "\n" + `data: {"clientMsgNo":"im-live"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}

func streamFor(t *testing.T, d time.Duration, during func()) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/im/agent-streams", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	go during()
	(&Plugin{}).serveAgentStream(humatest.NewContext(&huma.Operation{}, req, rec), "1@42", personChannel)
	return rec.Body.String()
}
