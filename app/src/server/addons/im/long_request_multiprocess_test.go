package im

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// MUYU-160 review 6: real multi-process restart takeover on MySQL 8. The test
// binary re-executes itself as separate OS processes that each run the real
// runWorker loop; one is SIGKILLed mid-turn and the other must take the turn
// over after the lease expires, finish it, and then run the conversation's
// next turn in order. Opt-in: IM_TEST_MYSQL_DSN (see long_request_mysql_test.go).

const (
	mpWorkerEnv = "IM_MP_WORKER"      // set in child processes
	mpSchemaEnv = "IM_MP_SCHEMA"      // schema shared by parent and children
	mpCoreEnv   = "IM_MP_CORE_URL"    // fake Core/WuKong served by the parent
	mpNameEnv   = "IM_MP_WORKER_NAME" // label for logs
)

// TestMultiProcessWorker is the child entry point; it only runs as a child.
func TestMultiProcessWorker(t *testing.T) {
	if os.Getenv(mpWorkerEnv) != "1" {
		t.Skip("child process entry point")
	}
	dsn := os.Getenv("IM_TEST_MYSQL_DSN") + os.Getenv(mpSchemaEnv) + "?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	global.PRISM_DB = db
	// Diagnostics (MUYU-160 85 follow-up): every worker log line, debug level,
	// goes to stderr, which the parent keeps and prints with the test output.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).
		With("worker", os.Getenv(mpNameEnv)))
	core := os.Getenv(mpCoreEnv)
	p := &Plugin{apiAddr: core, coreURL: core, serviceJWT: "test-token", done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	fmt.Fprintf(os.Stderr, "worker %s pid=%d started\n", os.Getenv(mpNameEnv), os.Getpid())
	p.runWorker(ctx) // returns on ctx timeout; the parent kills or stops us earlier
}

func TestMultiProcessRestartTakeover(t *testing.T) {
	if os.Getenv(mpWorkerEnv) == "1" {
		t.Skip("parent only")
	}
	db := mysqlInboxDB(t)
	if err := db.AutoMigrate(&authmodel.User{}, &model.AgentInstance{}, &model.AgentTemplate{},
		&IMCoreConversationMap{}, &IMGroupMember{}, &IMMessage{}); err != nil {
		t.Fatal(err)
	}
	for _, u := range []struct {
		id   uint
		kind string
	}{{1, authmodel.AccountTypeHuman}, {42, authmodel.AccountTypeAgent}} {
		if err := db.Create(&authmodel.User{ID: u.id, Username: fmt.Sprint("u", u.id), AccountType: u.kind, Enable: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var schema string
	db.Raw("SELECT DATABASE()").Scan(&schema)

	// Fake Core + WuKong. Turn a1's stream stalls (an executor still working)
	// until released; a2 answers at once. Every dispatch and reply is recorded.
	var mu sync.Mutex
	var dispatches, replies []string
	stall := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowLifecycle(w, r) {
			return
		}
		switch r.URL.Path {
		case "/channel/messagesync":
			fmt.Fprint(w, `{"messages":[]}`)
		case "/message/send":
			var body noticeSend
			_ = json.NewDecoder(r.Body).Decode(&body)
			var text textPayload
			_ = json.Unmarshal(body.Payload, &text)
			mu.Lock()
			replies = append(replies, text.Content)
			mu.Unlock()
		case "/api/v1/addons/conversation/im-dispatch":
			var input dispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			mu.Lock()
			dispatches = append(dispatches, input.SourceKey)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(dispatchResult{ConversationID: 9, SourceKey: input.SourceKey, Status: "accepted"})
		case "/api/v1/addons/conversation/9/messages/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			emitCoreMessage(w, 7, "user", "text", "long", "a1")
			mu.Lock()
			a2Dispatched := false
			for _, d := range dispatches {
				a2Dispatched = a2Dispatched || d == "a2"
			}
			mu.Unlock()
			if !a2Dispatched {
				select { // a1 still executing; pings keep the stream alive
				case <-stall:
				case <-r.Context().Done():
					return
				}
				emitCoreMessage(w, 8, "agent", "result", "a1 answer", "")
				flusher.Flush()
				return
			}
			emitCoreMessage(w, 8, "agent", "result", "a1 answer", "")
			emitCoreMessage(w, 10, "user", "text", "next", "a2")
			emitCoreMessage(w, 11, "agent", "result", "a2 answer", "")
			flusher.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	a1 := mysqlRow(t, db, "a1", "1@42", 1, inboxPending)
	a2 := mysqlRow(t, db, "a2", "1@42", 1, inboxPending)

	start := func(name string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestMultiProcessWorker$", "-test.v")
		cmd.Env = append(os.Environ(), mpWorkerEnv+"=1", mpSchemaEnv+"="+schema, mpCoreEnv+"="+server.URL, mpNameEnv+"="+name)
		var out strings.Builder
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Logf("started %s pid=%d", name, cmd.Process.Pid)
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Logf("%s output:\n%s", name, out.String())
		})
		return cmd
	}

	first := start("proc-1")
	held := waitRow(t, db, a1.ID, func(r IMWebhookInbox) bool { return r.State == inboxProcessing && r.LeaseOwner != "" }, 15*time.Second)
	owner1 := held.LeaseOwner
	if strings.HasSuffix(owner1, fmt.Sprintf(":%d", os.Getpid())) {
		t.Fatalf("a1 leased by the parent (%s), want the child process", owner1)
	}
	if r := reloadRow(t, db, a2.ID); r.State != inboxPending {
		t.Fatalf("a2=%s while a1 runs; the conversation must stay serial", r.State)
	}

	second := start("proc-2")
	time.Sleep(2 * time.Second) // proc-2 is live but must not touch a1 or a2 while proc-1 holds a1
	if r := reloadRow(t, db, a1.ID); r.LeaseOwner != owner1 {
		t.Fatalf("live lease stolen: owner %s -> %s", owner1, r.LeaseOwner)
	}
	if r := reloadRow(t, db, a2.ID); r.State != inboxPending {
		t.Fatalf("a2=%s on another process while a1 runs", r.State)
	}

	// Hard crash of the process holding a1 (no graceful release).
	if err := first.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	t.Logf("SIGKILLed proc-1 (pid %d) holding a1 owner=%s", first.Process.Pid, owner1)
	crashAt := time.Now()

	// The lease (30s, renewed every 10s by the dead process) must expire and
	// proc-2 must take a1 over.
	taken := waitRow(t, db, a1.ID, func(r IMWebhookInbox) bool { return r.LeaseOwner != "" && r.LeaseOwner != owner1 }, 45*time.Second)
	t.Logf("a1 taken over by %s after %v (attempts=%d)", taken.LeaseOwner, time.Since(crashAt).Round(time.Second), taken.Attempts)
	if taken.Attempts < 2 {
		t.Fatalf("takeover did not count as a new attempt: %+v", taken)
	}
	close(stall)

	waitRow(t, db, a1.ID, func(r IMWebhookInbox) bool { return r.State == inboxCompleted }, 15*time.Second)
	done2 := waitRow(t, db, a2.ID, func(r IMWebhookInbox) bool { return r.State == inboxCompleted }, 15*time.Second)
	_ = second
	if done2.CumulativeAnswer != "a2 answer" {
		t.Fatalf("a2 answer=%q", done2.CumulativeAnswer)
	}
	var processing int64
	db.Model(&IMWebhookInbox{}).Where("state = ?", inboxProcessing).Count(&processing)
	mu.Lock()
	defer mu.Unlock()
	t.Logf("dispatches=%v replies=%v", dispatches, replies)
	if processing != 0 {
		t.Fatalf("processing rows left=%d", processing)
	}
	if last := dispatches[len(dispatches)-1]; last != "a2" || indexOf(dispatches, "a2") < indexOf(dispatches, "a1") {
		t.Fatalf("dispatch order %v: a2 must follow a1", dispatches)
	}
	if fmt.Sprint(replies) != fmt.Sprint([]string{"a1 answer", "a2 answer"}) {
		t.Fatalf("replies=%v, want each answer once and in order", replies)
	}
}

// dumpInbox logs every inbox row's key state (diagnostics for a failed wait).
func dumpInbox(t *testing.T, db *gorm.DB) {
	t.Helper()
	var rows []IMWebhookInbox
	db.Order("id").Find(&rows)
	for _, r := range rows {
		t.Logf("inbox id=%d source=%s state=%s owner=%s attempts=%d lease_expires=%v next=%v err=%s final_core=%d final_sent=%v core_stop=%q",
			r.ID, r.SourceKey, r.State, r.LeaseOwner, r.Attempts, r.LeaseExpiresAt, r.NextAttemptAt, r.LastErrorCode,
			r.FinalCoreMessageID, r.FinalSentAt, r.CoreStopCode)
	}
}

func waitRow(t *testing.T, db *gorm.DB, id uint, ok func(IMWebhookInbox) bool, within time.Duration) IMWebhookInbox {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		var row IMWebhookInbox
		if err := db.First(&row, id).Error; err != nil {
			t.Fatal(err)
		}
		if ok(row) {
			return row
		}
		if time.Now().After(deadline) {
			dumpInbox(t, db)
			t.Fatalf("row %d not as expected after %v: %+v", id, within, row)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func indexOf(items []string, want string) int {
	for i, item := range items {
		if item == want {
			return i
		}
	}
	return -1
}

// MUYU-160 review 7 (P1-a): the old holder is not killed but paused (SIGSTOP)
// past its lease, another process takes the turn over, and then the old holder
// resumes (SIGCONT) holding an answer it read before the pause. It must neither
// post that answer nor end the turn; the new holder alone finishes it.
func TestMultiProcessPausedHolderResumesAfterTakeover(t *testing.T) {
	if os.Getenv(mpWorkerEnv) == "1" {
		t.Skip("parent only")
	}
	db := mysqlInboxDB(t)
	if err := db.AutoMigrate(&authmodel.User{}, &model.AgentInstance{}, &model.AgentTemplate{},
		&IMCoreConversationMap{}, &IMGroupMember{}, &IMMessage{}); err != nil {
		t.Fatal(err)
	}
	for _, u := range []struct {
		id   uint
		kind string
	}{{1, authmodel.AccountTypeHuman}, {42, authmodel.AccountTypeAgent}} {
		if err := db.Create(&authmodel.User{ID: u.id, Username: fmt.Sprint("u", u.id), AccountType: u.kind, Enable: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	var schema string
	db.Raw("SELECT DATABASE()").Scan(&schema)

	var mu sync.Mutex
	var dispatches, replies []string
	streams := 0
	staleAnswer := make(chan struct{}) // releases the answer to the paused holder's stream
	release := make(chan struct{})     // releases the answer to the new holder's stream
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowLifecycle(w, r) {
			return
		}
		switch r.URL.Path {
		case "/channel/messagesync":
			fmt.Fprint(w, `{"messages":[]}`)
		case "/message/send":
			var body noticeSend
			_ = json.NewDecoder(r.Body).Decode(&body)
			var text textPayload
			_ = json.Unmarshal(body.Payload, &text)
			mu.Lock()
			replies = append(replies, text.Content)
			mu.Unlock()
		case "/api/v1/addons/conversation/im-dispatch":
			var input dispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&input)
			mu.Lock()
			dispatches = append(dispatches, input.SourceKey)
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(dispatchResult{ConversationID: 9, SourceKey: input.SourceKey, Status: "accepted"})
		case "/api/v1/addons/conversation/9/messages/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			mu.Lock()
			streams++
			connection := streams
			mu.Unlock()
			emitCoreMessage(w, 7, "user", "text", "long", "a1")
			gate := release
			if connection == 1 {
				gate = staleAnswer // the first holder's connection
			}
			if connection <= 2 {
				select {
				case <-gate:
				case <-r.Context().Done():
					return
				}
				emitCoreMessage(w, 8, "agent", "result", "a1 answer", "")
				flusher.Flush()
				<-r.Context().Done()
				return
			}
			emitCoreMessage(w, 8, "agent", "result", "a1 answer", "")
			emitCoreMessage(w, 10, "user", "text", "next", "a2")
			emitCoreMessage(w, 11, "agent", "result", "a2 answer", "")
			flusher.Flush()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	a1 := mysqlRow(t, db, "a1", "1@42", 1, inboxPending)
	a2 := mysqlRow(t, db, "a2", "1@42", 1, inboxPending)
	start := func(name string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestMultiProcessWorker$", "-test.v")
		cmd.Env = append(os.Environ(), mpWorkerEnv+"=1", mpSchemaEnv+"="+schema, mpCoreEnv+"="+server.URL, mpNameEnv+"="+name)
		var out strings.Builder
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Logf("started %s pid=%d", name, cmd.Process.Pid)
		t.Cleanup(func() {
			_ = cmd.Process.Signal(syscall.SIGCONT)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Logf("%s output:\n%s", name, out.String())
		})
		return cmd
	}
	streamCount := func() int { mu.Lock(); defer mu.Unlock(); return streams }

	first := start("proc-1")
	held := waitRow(t, db, a1.ID, func(r IMWebhookInbox) bool {
		return r.State == inboxProcessing && r.LeaseOwner != "" && r.DispatchCoreMessageID != 0
	}, 15*time.Second)
	owner1 := held.LeaseOwner
	if err := first.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	t.Logf("SIGSTOPped proc-1 (pid %d) holding a1 owner=%s attempts=%d", first.Process.Pid, owner1, held.Attempts)
	pausedAt := time.Now()

	start("proc-2")
	taken := waitRow(t, db, a1.ID, func(r IMWebhookInbox) bool { return r.LeaseOwner != "" && r.LeaseOwner != owner1 }, 45*time.Second)
	t.Logf("a1 taken over by %s after %v (attempts=%d)", taken.LeaseOwner, time.Since(pausedAt).Round(time.Second), taken.Attempts)
	deadline := time.Now().Add(10 * time.Second)
	for streamCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("new holder never opened its Core stream")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The paused holder's connection now carries an answer, then it resumes.
	close(staleAnswer)
	time.Sleep(500 * time.Millisecond)
	if err := first.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	t.Logf("SIGCONTed proc-1 after %v paused", time.Since(pausedAt).Round(time.Second))
	time.Sleep(5 * time.Second) // let the resumed holder act on what it read
	if r := reloadRow(t, db, a1.ID); r.State != inboxProcessing || r.LeaseOwner != taken.LeaseOwner || r.Attempts != taken.Attempts {
		t.Fatalf("resumed old holder touched the new holder's turn: state=%s owner=%s attempts=%d", r.State, r.LeaseOwner, r.Attempts)
	}
	if r := reloadRow(t, db, a2.ID); r.State != inboxPending {
		t.Fatalf("a2=%s while the new holder still runs a1", r.State)
	}
	mu.Lock()
	early := append([]string(nil), replies...)
	mu.Unlock()
	if len(early) != 0 {
		t.Fatalf("resumed old holder posted %v", early)
	}

	close(release)
	waitRow(t, db, a1.ID, func(r IMWebhookInbox) bool { return r.State == inboxCompleted }, 15*time.Second)
	waitRow(t, db, a2.ID, func(r IMWebhookInbox) bool { return r.State == inboxCompleted }, 20*time.Second)
	mu.Lock()
	defer mu.Unlock()
	t.Logf("dispatches=%v replies=%v", dispatches, replies)
	if fmt.Sprint(replies) != fmt.Sprint([]string{"a1 answer", "a2 answer"}) {
		t.Fatalf("replies=%v, want each answer once and in order", replies)
	}
	var processing int64
	db.Model(&IMWebhookInbox{}).Where("state = ?", inboxProcessing).Count(&processing)
	if processing != 0 {
		t.Fatalf("processing rows left=%d", processing)
	}
}
