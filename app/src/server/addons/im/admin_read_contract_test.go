package im

import (
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

// MUYU-163: nucleagent-core's all-users conversation viewer (core
// addons/admin/router/conversations_admin.go) reads these im tables read-only
// from the shared database. Renaming or dropping any column below breaks that
// viewer; change both sides together.
func TestAdminConversationViewerColumnContract(t *testing.T) {
	want := map[string]struct {
		model   any
		table   string
		columns []string
	}{
		"conversations": {&IMConversation{}, "im_conversations", []string{"id", "uid", "channel_type", "channel_key", "last_message_id", "hidden_at", "updated_at"}},
		"messages":      {&IMMessage{}, "im_messages", []string{"id", "message_idstr", "channel_key", "channel_type", "from_uid", "payload_type", "payload", "wk_timestamp"}},
		"map":           {&IMCoreConversationMap{}, "im_core_conversation_map", []string{"id", "channel_id", "channel_type", "agent_uid", "execution_owner_user_id", "core_conversation_id"}},
		"groups":        {&IMGroup{}, "im_groups", []string{"group_id", "title"}},
		"inbox":         {&IMWebhookInbox{}, "im_webhook_inbox", []string{"channel_id", "channel_type", "state"}},
	}
	for name, w := range want {
		parsed, err := schema.Parse(w.model, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if parsed.Table != w.table {
			t.Fatalf("%s table=%q want %q", name, parsed.Table, w.table)
		}
		for _, column := range w.columns {
			if parsed.LookUpField(column) == nil {
				t.Fatalf("%s.%s is gone; core's admin conversation viewer reads it", w.table, column)
			}
		}
	}
	// The viewer's in-flight states must stay real inbox states.
	if inboxPending != "pending" || inboxProcessing != "processing" || inboxRetryable != "retryable" {
		t.Fatal("inbox in-flight state names changed; update core's imInFlightStates")
	}
	if personChannel != 1 || groupChannel != 2 || filePayloadType != 8 {
		t.Fatal("channel/payload type numbers changed; update core's viewer constants")
	}
}
