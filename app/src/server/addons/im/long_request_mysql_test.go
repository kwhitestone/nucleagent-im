package im

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// MUYU-160 review 5: inbox leasing on a real MySQL 8 (SKIP LOCKED, row locks,
// several processes). Opt-in like search_mysql_test.go: IM_TEST_MYSQL_DSN=
// user:pass@tcp(host:port)/ — each test gets its own schema, dropped after.

func mysqlInboxDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("IM_TEST_MYSQL_DSN not set (MySQL integration)")
	}
	schema := "im_t_" + strings.ToLower(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name()))
	raw, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("DROP DATABASE IF EXISTS " + schema); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("CREATE DATABASE " + schema + " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(mysql.Open(dsn+schema+"?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&IMWebhookInbox{}); err != nil {
		t.Fatal(err)
	}
	previous := global.PRISM_DB
	global.PRISM_DB = db
	t.Cleanup(func() {
		global.PRISM_DB = previous
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
		_, _ = raw.Exec("DROP DATABASE IF EXISTS " + schema)
		_ = raw.Close()
	})
	return db
}

func mysqlRow(t *testing.T, db *gorm.DB, source, channel string, owner uint, state string) IMWebhookInbox {
	t.Helper()
	row := IMWebhookInbox{
		Event: "msg.notify", MessageIDStr: source, SourceKey: source, TargetAgentUID: 42,
		SenderUID: owner, ChannelID: channel, ChannelType: personChannel, Text: "q", State: state,
		ExecutionOwnerUserID: owner, OriginSenderUID: owner,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row
}

// The 5 MUYU-160 columns are created by AutoMigrate on MySQL with the declared types.
func TestMySQLInboxMigrationAddsNoticeAndContinuationColumns(t *testing.T) {
	db := mysqlInboxDB(t)
	for _, column := range []string{"notice_due_at", "notice_attempts", "notice_sent_at", "delegation_started", "continuation_due_at"} {
		if !db.Migrator().HasColumn(&IMWebhookInbox{}, column) {
			t.Fatalf("column %s missing after AutoMigrate", column)
		}
	}
}

// Two IM instances (separate connection pools, as separate processes would
// have) lease concurrently: every due row is leased exactly once.
func TestMySQLConcurrentLeasesNeverShareARow(t *testing.T) {
	db := mysqlInboxDB(t)
	for i := range 40 {
		mysqlRow(t, db, fmt.Sprint("r", i), fmt.Sprintf("%d@42", 100+i), uint(100+i), inboxPending)
	}
	dsn := os.Getenv("IM_TEST_MYSQL_DSN")
	var schema string
	db.Raw("SELECT DATABASE()").Scan(&schema)
	instance := func() *gorm.DB {
		other, err := gorm.Open(mysql.Open(dsn+schema+"?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{Logger: logger.Discard})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s, _ := other.DB(); _ = s.Close() })
		return other
	}
	pools := []*gorm.DB{instance(), instance()}
	var mu sync.Mutex
	leasedBy := map[uint][]string{}
	var wg sync.WaitGroup
	for p, pool := range pools {
		for g := range 4 { // each instance leases with 4 slots in parallel
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker := fmt.Sprintf("inst%d-slot%d", p, g)
				for {
					row, err := leaseInboxExcluding(context.Background(), pool, worker, time.Now(), nil)
					if err != nil {
						t.Error(err)
						return
					}
					if row == nil {
						return
					}
					mu.Lock()
					leasedBy[row.ID] = append(leasedBy[row.ID], worker)
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if len(leasedBy) != 40 {
		t.Fatalf("leased %d distinct rows, want 40", len(leasedBy))
	}
	for id, owners := range leasedBy {
		if len(owners) != 1 {
			t.Fatalf("row %d leased by %v, want exactly one", id, owners)
		}
	}
}

// Within an instance the in-flight conversation is excluded: its next turn is
// not leased while its earlier turn runs, but other conversations are.
func TestMySQLLeaseSkipsInFlightConversation(t *testing.T) {
	db := mysqlInboxDB(t)
	first := mysqlRow(t, db, "a1", "1@42", 1, inboxPending)
	mysqlRow(t, db, "a2", "1@42", 1, inboxPending)
	other := mysqlRow(t, db, "b1", "2@42", 2, inboxPending)
	got, err := leaseInboxExcluding(t.Context(), db, "w", time.Now(), nil)
	if err != nil || got == nil || got.ID != first.ID {
		t.Fatalf("first lease=%+v err=%v", got, err)
	}
	next, err := leaseInboxExcluding(t.Context(), db, "w", time.Now(), []inboxTurnKey{turnKeyOf(got)})
	if err != nil || next == nil || next.ID != other.ID {
		t.Fatalf("lease while a1 in flight=%+v, want b1", next)
	}
}

// Same conversation, earlier turn waiting on a busy retry: the conversation's
// later turn must not overtake it once the instance-level exclusion is gone
// (e.g. after a restart, where leases are only ordered by id).
func TestMySQLRetryKeepsConversationOrder(t *testing.T) {
	db := mysqlInboxDB(t)
	first := mysqlRow(t, db, "a1", "1@42", 1, inboxPending)
	second := mysqlRow(t, db, "a2", "1@42", 1, inboxPending)
	row, _ := leaseInboxExcluding(t.Context(), db, "w", time.Now(), nil)
	if row.ID != first.ID {
		t.Fatalf("leased %d first, want %d", row.ID, first.ID)
	}
	now := time.Now()
	(&Plugin{}).failInbox(row, &httpStatusError{status: 409, code: conversationBusyCode}, now)
	// a1 is in busy-retry backoff; a2 is due but must not overtake it.
	if next, _ := leaseInboxExcluding(t.Context(), db, "w", now, nil); next != nil {
		t.Fatalf("during a1's backoff the lease returned id %d; the conversation's later turn (id %d) must wait", next.ID, second.ID)
	}
	// Once the backoff has passed, a1 goes first.
	next, _ := leaseInboxExcluding(t.Context(), db, "w", now.Add(time.Minute), nil)
	if next == nil || next.ID != first.ID {
		t.Fatalf("after the backoff the lease was %+v, want a1 (id %d)", next, first.ID)
	}
}

// A process died mid-turn: once its lease expires another instance takes the
// row over (restart takeover), and a live lease is never stolen.
func TestMySQLExpiredLeaseIsTakenOver(t *testing.T) {
	db := mysqlInboxDB(t)
	row := mysqlRow(t, db, "crash", "1@42", 1, inboxPending)
	leased, _ := leaseInboxExcluding(t.Context(), db, "dead-instance", time.Now(), nil)
	if leased == nil || leased.ID != row.ID {
		t.Fatalf("initial lease=%+v", leased)
	}
	if again, _ := leaseInboxExcluding(t.Context(), db, "other", time.Now(), nil); again != nil {
		t.Fatalf("live lease stolen: %+v", again)
	}
	taken, _ := leaseInboxExcluding(t.Context(), db, "other", time.Now().Add(31*time.Second), nil)
	if taken == nil || taken.ID != row.ID || taken.LeaseOwner != "other" {
		t.Fatalf("expired lease not taken over: %+v", taken)
	}
}

// Two instances, one conversation with two pending turns: across instances only
// the earlier turn is leasable; the later one waits until the earlier is done.
func TestMySQLMultiInstanceSameConversationIsSerial(t *testing.T) {
	db := mysqlInboxDB(t)
	first := mysqlRow(t, db, "a1", "1@42", 1, inboxPending)
	second := mysqlRow(t, db, "a2", "1@42", 1, inboxPending)
	got1, _ := leaseInboxExcluding(t.Context(), db, "inst-1", time.Now(), nil)
	got2, _ := leaseInboxExcluding(t.Context(), db, "inst-2", time.Now(), nil) // another instance, no local exclusion
	if got1 == nil || got1.ID != first.ID || got2 != nil {
		t.Fatalf("inst-1=%+v inst-2=%+v: only a1 may run", got1, got2)
	}
	if err := db.Model(&IMWebhookInbox{}).Where("id = ?", first.ID).Update("state", inboxCompleted).Error; err != nil {
		t.Fatal(err)
	}
	got3, _ := leaseInboxExcluding(t.Context(), db, "inst-2", time.Now(), nil)
	if got3 == nil || got3.ID != second.ID {
		t.Fatalf("after a1 completed inst-2 leased %+v, want a2", got3)
	}
}

// A crashed instance's expired turn is taken over before the conversation's
// later pending turn (restart takeover keeps order).
func TestMySQLTakeoverPrecedesLaterTurn(t *testing.T) {
	db := mysqlInboxDB(t)
	first := mysqlRow(t, db, "a1", "1@42", 1, inboxPending)
	mysqlRow(t, db, "a2", "1@42", 1, inboxPending)
	if l, _ := leaseInboxExcluding(t.Context(), db, "dead", time.Now(), nil); l == nil || l.ID != first.ID {
		t.Fatalf("initial lease=%+v", l)
	}
	taken, _ := leaseInboxExcluding(t.Context(), db, "survivor", time.Now().Add(31*time.Second), nil)
	if taken == nil || taken.ID != first.ID {
		t.Fatalf("survivor leased %+v, want the expired a1 first", taken)
	}
}
