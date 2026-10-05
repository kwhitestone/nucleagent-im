package im

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// persistHarness posts msg.notify batches through the real webhook handler.
func persistHarness(t *testing.T) (*gorm.DB, *Plugin, func(...webhookMessage) *httptest.ResponseRecorder) {
	t.Helper()
	db := m2DB(t)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	sharedAuthStack(t, router)
	api := humagin.New(router, huma.DefaultConfig("im persist test", "1"))
	capability := strings.Repeat("c", 32)
	p := &Plugin{webhookCapability: capability}
	p.registerWebhook(api)
	post := func(batch ...webhookMessage) *httptest.ResponseRecorder {
		body, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost,
			"/api/v1/im/webhooks/wukong?event=msg.notify&token="+capability, strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	return db, p, post
}

func messageCount(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	if err := db.Model(&IMMessage{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

// conversations returns uid -> (unread, last_message_id) for one channel.
func conversations(t *testing.T, db *gorm.DB, channelType uint8, key string) map[uint][2]uint64 {
	t.Helper()
	var rows []IMConversation
	if err := db.Where("channel_type = ? AND channel_key = ?", channelType, key).Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	out := map[uint][2]uint64{}
	for _, r := range rows {
		out[r.UID] = [2]uint64{uint64(r.Unread), r.LastMessageID}
	}
	return out
}

func lastID(t *testing.T, db *gorm.DB, idStr string) uint64 {
	t.Helper()
	var m IMMessage
	if err := db.Where("message_idstr = ?", idStr).First(&m).Error; err != nil {
		t.Fatal(err)
	}
	return m.ID
}

func mustOK(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

// The duplicate webhook is one row, and the DM fans out to exactly 2 conversation rows with
// unread counted once for the receiver only.
func TestPersistIdempotentDM(t *testing.T) {
	db, _, post := persistHarness(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	msg := webhookText("9001", 1, "2@1", personChannel, "hi")
	msg.MessageSeq = 7
	mustOK(t, post(msg))
	mustOK(t, post(msg)) // WK redelivery
	mustOK(t, post(msg, msg))
	if n := messageCount(t, db); n != 1 {
		t.Fatalf("messages=%d, want 1", n)
	}
	var m IMMessage
	if err := db.First(&m).Error; err != nil {
		t.Fatal(err)
	}
	if m.ChannelKey != "1@2" || m.FromUID != 1 || m.PayloadType != wuKongTextType || m.WKMessageSeq != 7 ||
		m.ClientMsgNo != "client-9001" || !strings.Contains(m.Payload, `"content":"hi"`) {
		t.Fatalf("row=%+v", m)
	}
	want := map[uint][2]uint64{1: {0, m.ID}, 2: {1, m.ID}}
	if got := conversations(t, db, personChannel, "1@2"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("conversations=%v want %v", got, want)
	}
}

// All senders and types are saved before the agent filters: an agent's DM reply, a disabled
// user, an unknown sender and a non-text payload are persisted but never dispatched.
// Q2 ruling: the agent reply (red_dot:0, as WK delivers API sends) counts as unread.
func TestPersistAllSendersAndAgentReplyUnread(t *testing.T) {
	db, _, post := persistHarness(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addUser(t, db, 5, authmodel.AccountTypeHuman)
	addBinding(t, db, 1, 42)
	if err := db.Model(&authmodel.User{}).Where("id = ?", 5).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	reply := webhookText("100", 42, "1@42", personChannel, "agent answer")
	reply.ClientMsgNo = "im-abc"
	reply.Header = &webhookHeader{RedDot: 0}
	image := webhookText("101", 1, "42@1", personChannel, "")
	image.RawPayload = []byte(`{"type":2,"url":"x.png"}`)
	mustOK(t, post(reply, image,
		webhookText("102", 5, "5@1", personChannel, "from disabled user"),
		webhookText("103", 77, "77@1", personChannel, "unknown sender")))
	if n := messageCount(t, db); n != 4 {
		t.Fatalf("messages=%d, want 4", n)
	}
	if n := inboxCount(t, db); n != 0 {
		t.Fatalf("inbox=%d, want 0 (nothing here may trigger an agent)", n)
	}
	got := conversations(t, db, personChannel, "1@42")
	if got[1] != [2]uint64{1, lastID(t, db, "101")} || got[42] != [2]uint64{1, lastID(t, db, "101")} {
		t.Fatalf("agent DM conversations=%v (human: agent reply unread; agent: image unread)", got)
	}
	var image2 IMMessage
	if err := db.Where("message_idstr = ?", "101").First(&image2).Error; err != nil || image2.PayloadType != 2 {
		t.Fatalf("non-text row=%+v err=%v", image2, err)
	}
	// Unread accumulates per viewer; the sender's own counter never moves.
	mustOK(t, post(webhookText("104", 42, "1@42", personChannel, "second answer")))
	if got := conversations(t, db, personChannel, "1@42"); got[1][0] != 2 || got[42][0] != 1 || got[1][1] != lastID(t, db, "104") {
		t.Fatalf("after second reply=%v", got)
	}
	// Skipped: no_persist / sync_once headers, a malformed DM channel, an empty id.
	noPersist := webhookText("105", 1, "1@42", personChannel, "x")
	noPersist.Header = &webhookHeader{NoPersist: 1}
	syncOnce := webhookText("106", 1, "1@42", personChannel, "x")
	syncOnce.Header = &webhookHeader{SyncOnce: 1}
	mustOK(t, post(noPersist, syncOnce, webhookText("107", 1, "3@4", personChannel, "x"),
		webhookText("", 1, "1@42", personChannel, "x")))
	if n := messageCount(t, db); n != 5 {
		t.Fatalf("messages=%d, want 5", n)
	}
}

// A group message fans out to one row per im_group_members row (N), including agent members;
// an unknown group gets the message row only.
func TestPersistGroupFanOut(t *testing.T) {
	db, _, post := persistHarness(t)
	const n = 100 // maxGroupMembers
	members := make([]IMGroupMember, n)
	for i := range members {
		members[i] = IMGroupMember{GroupID: "g1", UID: uint(i + 1)}
	}
	if err := db.Create(&members).Error; err != nil {
		t.Fatal(err)
	}
	mustOK(t, post(webhookText("200", 3, "g1", groupChannel, "hello group")))
	mustOK(t, post(webhookText("201", 3, "g1", groupChannel, "again")))
	mustOK(t, post(webhookText("202", 4, "g1", groupChannel, "reply")))
	got := conversations(t, db, groupChannel, "g1")
	if len(got) != n {
		t.Fatalf("group conversation rows=%d, want %d", len(got), n)
	}
	last := lastID(t, db, "202")
	for uid, c := range got {
		want := uint64(3)
		switch uid {
		case 3:
			want = 1
		case 4:
			want = 2
		}
		if c != [2]uint64{want, last} {
			t.Fatalf("uid %d conversation=%v want unread %d last %d", uid, c, want, last)
		}
	}
	mustOK(t, post(webhookText("203", 3, "ghost", groupChannel, "nobody home")))
	if messageCount(t, db) != 4 || len(conversations(t, db, groupChannel, "ghost")) != 0 {
		t.Fatal("unknown group must store the message row only")
	}
}

// The admission cap (120/min) and the per-sender agent limit gate agent triggers only:
// a limited trigger still saved its message.
func TestPersistRunsBeforeRateLimits(t *testing.T) {
	db, p, post := persistHarness(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addBinding(t, db, 1, 42)
	for i := 0; i < senderMinuteLimit; i++ {
		mustOK(t, post(webhookText(fmt.Sprint(300+i), 1, "1@42", personChannel, "ask")))
	}
	rec := post(webhookText("399", 1, "1@42", personChannel, "one too many"))
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), "im_agent_rate_limited") {
		t.Fatalf("agent limit status=%d %s", rec.Code, rec.Body.String())
	}
	if messageCount(t, db) != senderMinuteLimit+1 || inboxCount(t, db) != senderMinuteLimit {
		t.Fatalf("messages=%d inbox=%d", messageCount(t, db), inboxCount(t, db))
	}

	p.webhookAdmission = webhookAdmission{windowStart: time.Now().UTC().Truncate(time.Minute), count: 120}
	rec = post(webhookText("400", 1, "1@42", personChannel, "over the global cap"),
		webhookText("401", 9, "g9", groupChannel, "group chatter"))
	if rec.Code != http.StatusTooManyRequests || !strings.Contains(rec.Body.String(), "im_webhook_rate_limited") {
		t.Fatalf("admission status=%d %s", rec.Code, rec.Body.String())
	}
	if messageCount(t, db) != senderMinuteLimit+3 || inboxCount(t, db) != senderMinuteLimit {
		t.Fatalf("over cap: messages=%d inbox=%d", messageCount(t, db), inboxCount(t, db))
	}
}

// A DB outage answers 503 and records nothing (no message, no trigger). WK's resend after
// recovery saves the message once and dispatches it once.
func TestPersistRetryAfterOutage(t *testing.T) {
	db, _, post := persistHarness(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addBinding(t, db, 1, 42)
	msg := webhookText("500", 1, "1@42", personChannel, "during the outage")
	if err := db.Migrator().DropTable(&IMMessage{}); err != nil {
		t.Fatal(err)
	}
	rec := post(msg)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "im_inbox_unavailable") {
		t.Fatalf("outage status=%d %s", rec.Code, rec.Body.String())
	}
	if inboxCount(t, db) != 0 {
		t.Fatal("a failed save must not dispatch")
	}
	if err := db.AutoMigrate(&IMMessage{}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, post(msg)) // WK resend (beta.21: up to 3 attempts)
	mustOK(t, post(msg)) // and a late duplicate
	if messageCount(t, db) != 1 || inboxCount(t, db) != 1 {
		t.Fatalf("after recovery messages=%d inbox=%d", messageCount(t, db), inboxCount(t, db))
	}
	if got := conversations(t, db, personChannel, "1@42"); got[42][0] != 1 || got[1][0] != 0 {
		t.Fatalf("conversations=%v", got)
	}
}
