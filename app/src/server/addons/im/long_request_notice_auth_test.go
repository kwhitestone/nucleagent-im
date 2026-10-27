package im

import (
	"sync/atomic"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// MUYU-160 review 7 (P2): an owed failure notice is posted as the agent, maybe
// days after the turn ended. If the agent was removed from the group (or turned
// off) meanwhile, a resend must not post into that channel; the debt stays, so
// it is delivered only if the right to post comes back.

func owedGroupNotice(t *testing.T, db *gorm.DB) *IMWebhookInbox {
	t.Helper()
	if err := db.Create(&IMGroup{GroupID: "g-notice", CreatorUID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := insertGroupMembers(db, "g-notice", []uint{1, 42}); err != nil {
		t.Fatal(err)
	}
	row := &IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: "notice-auth", SourceKey: "notice-auth", TargetAgentUID: 42,
		SenderUID: 1, ChannelID: "g-notice", ChannelType: groupChannel, Text: "q", State: inboxDead,
		ExecutionOwnerUserID: 1, OriginSenderUID: 1, CoreConversationID: 9, Attempts: 1,
		FinalWuKongClientMsgNo: deterministicClientMessageNo("notice-auth", 0),
	}
	now := time.Now()
	row.NoticeDueAt = &now
	if err := db.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func TestOwedNoticeNotPostedAfterAgentLeftGroup(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := owedGroupNotice(t, db)
	// WuKong was down for the first attempt; meanwhile the agent left the group.
	failing.Store(true)
	p.deliverNotice(t.Context(), row, time.Now())
	if sends["failed"] != 1 {
		t.Fatalf("first attempt sends=%v", sends)
	}
	if err := db.Where("group_id = ? AND uid = ?", "g-notice", 42).Delete(&IMGroupMember{}).Error; err != nil {
		t.Fatal(err)
	}
	failing.Store(false)
	at := time.Now()
	for range 3 {
		at = at.Add(noticeSlowRetry + time.Hour)
		p.resendDueNotices(t.Context(), at)
	}
	if sends["ok"] != 0 {
		t.Fatalf("notice posted into a group the agent left: %v", sends)
	}
	if got := reloadRow(t, db, row.ID); got.NoticeDueAt == nil || got.NoticeSentAt != nil {
		t.Fatalf("debt must stay owed (not sent, not dropped): %+v", got)
	}
	// The agent is added back: the owed notice is delivered once.
	if err := insertGroupMembers(db, "g-notice", []uint{42}); err != nil {
		t.Fatal(err)
	}
	p.resendDueNotices(t.Context(), at.Add(noticeSlowRetry+time.Hour))
	if got := reloadRow(t, db, row.ID); sends["ok"] != 1 || got.NoticeSentAt == nil || got.NoticeDueAt != nil {
		t.Fatalf("after re-adding the agent: sends=%v row=%+v", sends, got)
	}
}

func TestOwedNoticeNotPostedForDisabledAgent(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := owedGroupNotice(t, db)
	if err := db.Model(&authmodel.User{}).Where("id = ?", 42).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	p.deliverNotice(t.Context(), row, time.Now())
	if sends["ok"]+sends["failed"] != 0 {
		t.Fatalf("disabled agent posted: %v", sends)
	}
	if got := reloadRow(t, db, row.ID); got.NoticeDueAt == nil || got.NoticeSentAt != nil {
		t.Fatalf("debt must stay owed: %+v", got)
	}
}

// The authorization check itself fails (database unavailable): nothing is sent
// and the notice stays owed for a later attempt.
func TestOwedNoticeKeptWhenAuthorizationCheckFails(t *testing.T) {
	p, db, _ := longRequestFixture(t)
	var failing atomic.Bool
	sends := map[string]int{}
	p.apiAddr = wrapSend(t, &failing, sends)
	row := owedGroupNotice(t, db)
	if err := db.Migrator().DropTable(&IMGroupMember{}); err != nil {
		t.Fatal(err)
	}
	p.deliverNotice(t.Context(), row, time.Now())
	if sends["ok"]+sends["failed"] != 0 {
		t.Fatalf("posted without a completed authorization check: %v", sends)
	}
	if got := reloadRow(t, db, row.ID); got.NoticeDueAt == nil || got.NoticeSentAt != nil {
		t.Fatalf("debt lost by a failed check: %+v", got)
	}
}
