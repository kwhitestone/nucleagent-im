package im

import (
	"context"
	"testing"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"gorm.io/gorm"
)

// D2=A (D82): once its owner is disabled or deleted, a private agent is out of
// service: not in the owner's directory, not dispatchable, recipient status
// off, and the unavailable (stopped) branch applies. Non-owners keep the
// unknown-uid answer. Restoring the owner restores the agent.
func TestUserAgentD2OwnerOffStopsPrivateAgent(t *testing.T) {
	db := uaFixture(t)
	ctx := context.Background()
	expect := func(state string, on bool) {
		t.Helper()
		agents, err := listDirectoryAgents(ctx, db, uaOwner)
		if err != nil {
			t.Fatal(err)
		}
		listed, system := false, false
		for _, a := range agents {
			listed = listed || a.UID == uaPrivate
			system = system || a.UID == uaSystem
		}
		targets, err := resolveAgentTargets(db, []uint{uaPrivate}, personChannel, uaOwner, uaOwner)
		if err != nil {
			t.Fatal(err)
		}
		enabled, err := recipientEnabled(ctx, db, uaPrivate)
		if err != nil {
			t.Fatal(err)
		}
		off, err := agentUnavailable(db, uaPrivate)
		if err != nil {
			t.Fatal(err)
		}
		hidden, err := privateAgentHidden(db, uaPrivate, uaOther)
		if err != nil {
			t.Fatal(err)
		}
		if !system || !hidden {
			t.Fatalf("%s: system agent must stay listed (%v), non-owner must stay hidden (%v)", state, system, hidden)
		}
		if listed != on || (len(targets) == 1) != on || enabled != on || off == on {
			t.Fatalf("%s: private agent in service want %v: listed=%v targets=%+v recipientEnabled=%v unavailable=%v",
				state, on, listed, targets, enabled, off)
		}
		if sysOn, err := recipientEnabled(ctx, db, uaSystem); err != nil || !sysOn {
			t.Fatalf("%s: system agent recipient status must stay on: %v %v", state, sysOn, err)
		}
	}
	setOwner := func(update map[string]any) {
		t.Helper()
		if err := db.Session(&gorm.Session{}).Unscoped().Model(&authmodel.User{}).Where("id = ?", uaOwner).
			Updates(update).Error; err != nil {
			t.Fatal(err)
		}
	}
	expect("owner active", true)
	setOwner(map[string]any{"enable": 2})
	expect("owner disabled", false)
	setOwner(map[string]any{"enable": 1})
	expect("owner restored", true)
	setOwner(map[string]any{"deleted_at": fixedNoticeTime})
	expect("owner deleted", false)
	setOwner(map[string]any{"deleted_at": nil})
	expect("owner undeleted", true)
}
