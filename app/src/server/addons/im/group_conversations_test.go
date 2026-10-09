package im

import (
	"fmt"
	"net/http"
	"slices"
	"testing"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// IM4: dissolving a group, removing a member and leaving drop the affected
// im_conversations rows (visible and hidden), in the membership transaction. im_messages stay.

func groupMessage(t *testing.T, db *gorm.DB, channelType uint8, key string, from uint, n int) {
	t.Helper()
	if _, err := persistMessage(db, webhookMessage{
		MessageIDStr: fmt.Sprintf("im4-%s-%d", key, n), FromUID: fmt.Sprint(from),
		ChannelID: key, ChannelType: channelType, Timestamp: int64(1000 + n),
		RawPayload: []byte(`{"type":1,"content":"hi"}`),
	}); err != nil {
		t.Fatal(err)
	}
}

func allListIDs(t *testing.T, router http.Handler, user uint) []string {
	t.Helper()
	return append(listIDs(t, router, user, false), listIDs(t, router, user, true)...)
}

func im4Fixture(t *testing.T) (*gorm.DB, *fakeWuKong, http.Handler, http.Handler, groupData, groupData) {
	t.Helper()
	db := m2DB(t)
	for _, id := range []uint{1, 2, 3} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	fake := newFakeWuKong(t)
	groups := groupRouter(t, fake.plugin(), true)
	reads := readsRouter(t)
	gone := createGroupForTest(t, groups, "Gone", 2, 3)
	kept := createGroupForTest(t, groups, "Kept", 2, 3)
	groupMessage(t, db, groupChannel, gone.WuKongChannelID, 1, 1)
	groupMessage(t, db, groupChannel, kept.WuKongChannelID, 1, 2)
	groupMessage(t, db, personChannel, "1@2", 1, 3)
	batch(t, reads, 3, "hide", ch(gone.WuKongChannelID, groupChannel)) // uid 3 has it in the hidden view
	for _, uid := range []uint{1, 2, 3} {
		if !slices.Contains(allListIDs(t, reads, uid), gone.WuKongChannelID) {
			t.Fatalf("setup: uid %d has no row for the group", uid)
		}
	}
	return db, fake, groups, reads, gone, kept
}

func assertListed(t *testing.T, reads http.Handler, uid uint, channel string, want bool) {
	t.Helper()
	if got := slices.Contains(allListIDs(t, reads, uid), channel); got != want {
		t.Fatalf("uid %d lists %s = %v, want %v", uid, channel, got, want)
	}
}

func TestDissolveGroupDropsMembersConversations(t *testing.T) {
	db, _, groups, reads, gone, kept := im4Fixture(t)
	before := messageCount(t, db)
	rec := groupRequest(t, groups, http.MethodDelete, fmt.Sprintf("/api/v1/im/groups/%d", gone.ID), 1, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status=%d %s", rec.Code, rec.Body.String())
	}
	for _, uid := range []uint{1, 2, 3} {
		assertListed(t, reads, uid, gone.WuKongChannelID, false)
		assertListed(t, reads, uid, kept.WuKongChannelID, true)
	}
	assertListed(t, reads, 1, "2", true) // the DM is untouched
	assertListed(t, reads, 2, "1", true)
	if after := messageCount(t, db); after != before {
		t.Fatalf("im_messages %d -> %d; history must be kept", before, after)
	}
}

func TestRemoveAndLeaveDropOnlyThatMembersConversation(t *testing.T) {
	db, _, groups, reads, gone, kept := im4Fixture(t)
	before := messageCount(t, db)
	path := fmt.Sprintf("/api/v1/im/groups/%d/members/", gone.ID)
	if rec := groupRequest(t, groups, http.MethodDelete, path+"3", 1, nil); rec.Code != http.StatusOK { // removed
		t.Fatalf("remove status=%d %s", rec.Code, rec.Body.String())
	}
	assertListed(t, reads, 3, gone.WuKongChannelID, false)
	assertListed(t, reads, 2, gone.WuKongChannelID, true)
	if rec := groupRequest(t, groups, http.MethodDelete, path+"2", 2, nil); rec.Code != http.StatusOK { // leaves
		t.Fatalf("leave status=%d %s", rec.Code, rec.Body.String())
	}
	assertListed(t, reads, 2, gone.WuKongChannelID, false)
	assertListed(t, reads, 1, gone.WuKongChannelID, true)
	for _, uid := range []uint{2, 3} {
		assertListed(t, reads, uid, kept.WuKongChannelID, true)
	}
	if after := messageCount(t, db); after != before {
		t.Fatalf("im_messages %d -> %d; history must be kept", before, after)
	}
}

// A WuKong failure rolls the membership change back, conversation rows included.
func TestGroupConversationCleanupRollsBackOnWuKongFailure(t *testing.T) {
	_, fake, groups, reads, gone, _ := im4Fixture(t)
	fake.failPath = "/channel/subscriber_remove"
	rec := groupRequest(t, groups, http.MethodDelete, fmt.Sprintf("/api/v1/im/groups/%d/members/3", gone.ID), 1, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("remove status=%d %s", rec.Code, rec.Body.String())
	}
	fake.failPath = "/channel/delete"
	rec = groupRequest(t, groups, http.MethodDelete, fmt.Sprintf("/api/v1/im/groups/%d", gone.ID), 1, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("delete status=%d %s", rec.Code, rec.Body.String())
	}
	for _, uid := range []uint{1, 2, 3} {
		assertListed(t, reads, uid, gone.WuKongChannelID, true)
	}
}
