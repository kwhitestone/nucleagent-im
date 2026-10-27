package im

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
)

// MUYU-160 review 9 / acceptance: the receiver's idempotency contract checked
// against a REAL WuKongIM (opt-in: IM_TEST_WUKONG_API=http://127.0.0.1:port of a
// disposable WuKongIM). A proxy in front of it counts every POST attempt, can
// refuse (403) or swallow the response after WuKong stored the message; stored
// messages are read back from WuKong's own /channel/messagesync.

type wukongProxy struct {
	mu       sync.Mutex
	attempts int
	mode     atomic.Int32 // 0 pass, 1 refuse 403 without forwarding, 2 forward then drop the response
}

func realWuKong(t *testing.T) (string, *wukongProxy) {
	t.Helper()
	target := os.Getenv("IM_TEST_WUKONG_API")
	if target == "" {
		t.Skip("IM_TEST_WUKONG_API not set (real WuKongIM)")
	}
	upstream, err := url.Parse(target)
	if err != nil {
		t.Fatal(err)
	}
	proxy := &wukongProxy{}
	reverse := httputil.NewSingleHostReverseProxy(upstream)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/message/send" {
			reverse.ServeHTTP(w, r)
			return
		}
		proxy.mu.Lock()
		proxy.attempts++
		proxy.mu.Unlock()
		switch proxy.mode.Load() {
		case 1:
			w.WriteHeader(http.StatusForbidden)
		case 2:
			body, _ := io.ReadAll(r.Body)
			response, err := http.Post(target+"/message/send", "application/json", bytes.NewReader(body))
			if err == nil {
				_ = response.Body.Close()
			}
			conn, _, _ := w.(http.Hijacker).Hijack() // stored in WuKong, response lost
			_ = conn.Close()
		default:
			reverse.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, proxy
}

func (p *wukongProxy) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.attempts }

// storedInWuKong reads the channel back from WuKong: messages per client number.
// WuKong stores asynchronously after acknowledging a send, so the read is
// repeated until two reads 500ms apart agree (at most 10s).
func storedInWuKong(t *testing.T, row *IMWebhookInbox) map[string]int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := readWuKong(t, row)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		next := readWuKong(t, row)
		if len(next) > 0 && fmt.Sprint(next) == fmt.Sprint(last) {
			return next
		}
		last = next
	}
	return last
}

func readWuKong(t *testing.T, row *IMWebhookInbox) map[string]int {
	t.Helper()
	// A person channel is read as the asker with the agent's uid as channel id.
	body, _ := json.Marshal(map[string]any{"login_uid": strconv.FormatUint(uint64(row.SenderUID), 10),
		"channel_id": strconv.FormatUint(uint64(row.TargetAgentUID), 10), "channel_type": personChannel,
		"start_message_seq": 0, "end_message_seq": 0, "limit": 100, "pull_mode": 1})
	response, err := http.Post(os.Getenv("IM_TEST_WUKONG_API")+"/channel/messagesync", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var out struct {
		Messages []struct {
			ClientMsgNo string `json:"client_msg_no"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(response.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, m := range out.Messages {
		counts[m.ClientMsgNo]++
	}
	return counts
}

// Each case uses its own person channel, so WuKong's history is per case.
func realRow(t *testing.T, source string, uid uint) *IMWebhookInbox {
	t.Helper()
	addUser(t, global.PRISM_DB, uid, authmodel.AccountTypeHuman)
	channel := personChannelOf(uid, 42)
	row := &IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: source, SourceKey: source, TargetAgentUID: 42,
		SenderUID: uid, ChannelID: channel, ChannelType: personChannel, Text: "q", State: inboxProcessing,
		ExecutionOwnerUserID: uid, OriginSenderUID: uid, CoreConversationID: 9, LeaseOwner: "w", Attempts: 1,
	}
	if err := global.PRISM_DB.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

func personChannelOf(a, b uint) string {
	if a > b {
		a, b = b, a
	}
	return strconv.FormatUint(uint64(a), 10) + "@" + strconv.FormatUint(uint64(b), 10)
}

func relet(t *testing.T) *IMWebhookInbox {
	t.Helper()
	leased, err := leaseInbox(t.Context(), global.PRISM_DB, "w2", time.Now().Add(time.Hour))
	if err != nil || leased == nil {
		t.Fatalf("re-lease: %+v %v", leased, err)
	}
	return leased
}

func TestRealWuKongLostResponseKeepsOneMessage(t *testing.T) {
	apiAddr, proxy := realWuKong(t)
	p, db, _ := longRequestFixture(t)
	p.apiAddr = apiAddr
	row := realRow(t, "real-lost-"+time.Now().Format("150405.000000"), 101)
	proxy.mode.Store(2)
	err := p.sendFinal(t.Context(), row, 8, "the answer")
	if err == nil {
		t.Fatal("lost response reported success")
	}
	p.failInbox(row, err, time.Now())
	proxy.mode.Store(0)
	p.processLeased(t.Context(), relet(t))
	stored := storedInWuKong(t, row)
	got := reloadRow(t, db, row.ID)
	t.Logf("POST attempts=%d stored=%v state=%s", proxy.count(), stored, got.State)
	if got.State != inboxCompleted || proxy.count() != 2 || stored[got.FinalWuKongClientMsgNo] != 1 || len(stored) != 1 {
		t.Fatalf("want completed after 2 POSTs and exactly 1 message in WuKong")
	}
}

func TestRealWuKongClaimRefusedThenRecoveredKeepsOneMessage(t *testing.T) {
	apiAddr, proxy := realWuKong(t)
	p, db, _ := longRequestFixture(t)
	p.apiAddr = apiAddr
	row := realRow(t, "real-403-"+time.Now().Format("150405.000000"), 102)
	proxy.mode.Store(1)
	err := p.sendFinal(t.Context(), row, 8, "the answer")
	if err == nil {
		t.Fatal("refused post reported success")
	}
	p.failInbox(row, err, time.Now())
	proxy.mode.Store(0)
	p.processLeased(t.Context(), relet(t))
	stored := storedInWuKong(t, row)
	got := reloadRow(t, db, row.ID)
	t.Logf("POST attempts=%d stored=%v state=%s", proxy.count(), stored, got.State)
	if got.State != inboxCompleted || proxy.count() != 2 || stored[got.FinalWuKongClientMsgNo] != 1 || len(stored) != 1 {
		t.Fatalf("want completed after 2 POSTs (1 refused) and exactly 1 message in WuKong")
	}
}

func TestRealWuKongFailureFirstNeverStoresLateAnswer(t *testing.T) {
	apiAddr, proxy := realWuKong(t)
	p, db, _ := longRequestFixture(t)
	p.apiAddr = apiAddr
	row := realRow(t, "real-failfirst-"+time.Now().Format("150405.000000"), 103)
	if err := p.endContinuationTimeout(t.Context(), row); err != nil {
		t.Fatalf("timeout: %v", err)
	}
	late := *row
	if err := p.sendFinal(t.Context(), &late, 8, "late answer"); err == nil {
		t.Fatal("late answer accepted")
	}
	stored := storedInWuKong(t, row)
	got := reloadRow(t, db, row.ID)
	t.Logf("POST attempts=%d stored=%v state=%s", proxy.count(), stored, got.State)
	if got.State != inboxDead || proxy.count() != 1 || len(stored) != 1 || stored[deterministicClientMessageNo(row.SourceKey, 0)] != 1 {
		t.Fatalf("want only the failure notice in WuKong after 1 POST")
	}
}
