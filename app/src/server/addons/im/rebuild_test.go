package im

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

// rebuildFixture seeds the table the rebuild reads: two filled groups and one not yet
// filled (no member rows), whose WuKong members must survive the rebuild untouched.
func rebuildFixture(t *testing.T, db *gorm.DB, fake *fakeWuKong) {
	t.Helper()
	for _, group := range []IMGroup{
		{GroupID: "g-a", CreatorUID: 1}, {GroupID: "g-b", CreatorUID: 2}, {GroupID: "g-unfilled", CreatorUID: 3},
	} {
		if err := db.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := insertGroupMembers(db, "g-a", []uint{1, 4, 5}); err != nil {
		t.Fatal(err)
	}
	if err := insertGroupMembers(db, "g-b", []uint{2, 6}); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.members["g-unfilled"] = []uint{3, 8}
	fake.mu.Unlock()
}

func wkMembers(f *fakeWuKong) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fmt.Sprint(f.members["g-a"], f.members["g-b"], f.members["g-unfilled"])
}

const wantConverged = "[1 4 5] [2 6] [3 8]"

func memberRows(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	if err := db.Model(&IMGroupMember{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, label string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// countingWuKong fronts the fake: counts /channel posts per group and fails the groups in fail.
type countingWuKong struct {
	mu    sync.Mutex
	posts map[string]int
	fail  map[string]bool
	down  bool
}

func (c *countingWuKong) wrap(t *testing.T, next http.Handler) string {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		c.mu.Lock()
		down := c.down
		failed := false
		if r.URL.Path == "/channel" {
			for id := range c.fail {
				failed = failed || strings.Contains(string(body), `"`+id+`"`)
			}
			if !down && !failed {
				for _, id := range []string{"g-a", "g-b", "g-unfilled"} {
					if strings.Contains(string(body), `"`+id+`"`) {
						c.posts[id]++
					}
				}
			}
		}
		c.mu.Unlock()
		if down || failed {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func (c *countingWuKong) counts() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprint(c.posts["g-a"], c.posts["g-b"], c.posts["g-unfilled"])
}

func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// Boot rebuild + the loop: Start projects the table into an empty WuKong before the first
// tick, and a group wiped from WuKong later comes back on the next tick with its members.
func TestGroupRebuildOnBootAndLoopRestoresWipedGroup(t *testing.T) {
	db := m2DB(t)
	fake := newFakeWuKong(t)
	rebuildFixture(t, db, fake)
	previous := groupRebuildInterval
	groupRebuildInterval = 50 * time.Millisecond
	t.Cleanup(func() { groupRebuildInterval = previous })

	p := fake.plugin()
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Stop(ctx)
	})
	waitFor(t, "boot rebuild", func() bool { return wkMembers(fake) == wantConverged })

	// WuKong loses g-a (reset/wipe) and drifts on g-b.
	fake.mu.Lock()
	delete(fake.members, "g-a")
	fake.members["g-b"] = []uint{2, 6, 99}
	fake.mu.Unlock()
	waitFor(t, "loop restore", func() bool { return wkMembers(fake) == wantConverged })
	// 5 fixture rows + the 2 W1's boot fill took from WuKong for g-unfilled; the rebuild writes none.
	if rows := memberRows(t, db); rows != 7 {
		t.Fatalf("member rows=%d: the rebuild must not write the DB", rows)
	}
}

// Idempotent re-run: each tick posts exactly one reset:1 per filled group, and a converged
// WuKong is left identical (no duplicate members). An unfilled group is never posted.
func TestGroupRebuildIdempotent(t *testing.T) {
	db := m2DB(t)
	fake := newFakeWuKong(t)
	rebuildFixture(t, db, fake)
	counter := &countingWuKong{posts: map[string]int{}}
	p := &Plugin{apiAddr: counter.wrap(t, http.HandlerFunc(fake.serveHTTP))}

	for tick := 1; tick <= 3; tick++ {
		p.reconcileWuKongGroups(t.Context(), db)
		if got := wkMembers(fake); got != wantConverged {
			t.Fatalf("tick %d WuKong=%s want %s", tick, got, wantConverged)
		}
		if got, want := counter.counts(), fmt.Sprint(tick, tick, 0); got != want {
			t.Fatalf("tick %d posts(g-a g-b g-unfilled)=%s want %s", tick, got, want)
		}
	}
	if rows := memberRows(t, db); rows != 5 {
		t.Fatalf("member rows=%d", rows)
	}
}

// A WuKong outage: logged, the DB is unchanged, and the next tick heals. A failure on one
// group does not stop the others.
func TestGroupRebuildWuKongOutage(t *testing.T) {
	db := m2DB(t)
	fake := newFakeWuKong(t)
	rebuildFixture(t, db, fake)
	logs := captureLogs(t)
	counter := &countingWuKong{posts: map[string]int{}, fail: map[string]bool{}}
	p := &Plugin{apiAddr: counter.wrap(t, http.HandlerFunc(fake.serveHTTP)), wuKongAdminPassword: "s3cret-pw"}

	counter.down = true
	p.reconcileWuKongGroups(t.Context(), db)
	if got := wkMembers(fake); got != "[] [] [3 8]" {
		t.Fatalf("WuKong during outage=%s", got)
	}
	if out := logs.String(); strings.Count(out, "im group rebuild failed") != 2 ||
		!strings.Contains(out, "channel_id=g-a") || strings.Contains(out, "s3cret-pw") {
		t.Fatalf("outage log=%q", out)
	}
	if rows := memberRows(t, db); rows != 5 {
		t.Fatalf("member rows=%d after outage", rows)
	}

	// Partial: g-a still failing, g-b must still be restored.
	counter.mu.Lock()
	counter.down, counter.fail["g-a"] = false, true
	counter.mu.Unlock()
	p.reconcileWuKongGroups(t.Context(), db)
	if got := wkMembers(fake); got != "[] [2 6] [3 8]" {
		t.Fatalf("WuKong with g-a failing=%s", got)
	}

	// Recovered: the next tick converges.
	counter.mu.Lock()
	delete(counter.fail, "g-a")
	counter.mu.Unlock()
	p.reconcileWuKongGroups(t.Context(), db)
	if got := wkMembers(fake); got != wantConverged {
		t.Fatalf("WuKong after recovery=%s", got)
	}
	if rows := memberRows(t, db); rows != 5 {
		t.Fatalf("member rows=%d after recovery", rows)
	}
}
