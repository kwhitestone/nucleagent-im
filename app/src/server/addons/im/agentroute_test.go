package im

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
)

func addDefinition(t *testing.T, db *gorm.DB, agent uint, active bool) {
	t.Helper()
	tpl := model.AgentTemplate{Name: "hr", Slug: fmt.Sprintf("hr-%d", agent), AuthAgentUserID: &agent, IsActive: true}
	if err := db.Create(&tpl).Error; err != nil {
		t.Fatal(err)
	}
	if !active { // gorm skips a false bool on Create
		if err := db.Model(&tpl).Update("is_active", false).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func ownerOf(t *testing.T, db *gorm.DB, messageID string) uint {
	t.Helper()
	var row IMWebhookInbox
	if err := db.Where("message_idstr = ?", messageID).First(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row.ExecutionOwnerUserID
}

// O-2: a definition identity is shared by everyone; each sender runs in their own instance.
func TestDefinitionIdentityRoutesToSender(t *testing.T) {
	db := m2DB(t)
	for _, id := range []uint{1, 2} {
		addUser(t, db, id, authmodel.AccountTypeHuman)
	}
	addUser(t, db, 50, authmodel.AccountTypeAgent)
	addDefinition(t, db, 50, true)
	if err := db.Create(&IMGroup{GroupID: "g1", CreatorUID: 2}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)

	for _, m := range []webhookMessage{
		webhookText("d1", 1, "1@50", personChannel, "hi"),
		webhookText("d2", 2, "50@2", personChannel, "hi"),
		webhookText("g1", 2, "g1", groupChannel, "hi", 50),
	} {
		if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{m}, now); err != nil || accepted != 1 {
			t.Fatalf("%s accepted=%d err=%v", m.MessageIDStr, accepted, err)
		}
	}
	for id, want := range map[string]uint{"d1": 1, "d2": 2, "g1": 2} {
		if got := ownerOf(t, db, id); got != want {
			t.Fatalf("%s owner=%d want %d", id, got, want)
		}
	}

	// A2A hop to a definition identity runs as the chain's human origin, not the agent.
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addBinding(t, db, 2, 42)
	hop := agentMessage("h1", 42, "ask @user-50", &a2aProvenance{ChainID: "chain:x", Depth: 1, OriginUID: 2})
	if accepted, err := acceptWebhookBatch(t.Context(), db, "msg.notify", []webhookMessage{hop}, now,
		func(context.Context, string) ([]uint, error) { return []uint{2, 42, 50}, nil }); err != nil || accepted != 1 {
		t.Fatalf("hop accepted=%d err=%v", accepted, err)
	}
	if got := ownerOf(t, db, "h1"); got != 2 {
		t.Fatalf("hop owner=%d want origin 2", got)
	}
}

func TestDefinitionIdentityGuards(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	addUser(t, db, 50, authmodel.AccountTypeAgent)
	addUser(t, db, 51, authmodel.AccountTypeAgent)
	addUser(t, db, 52, authmodel.AccountTypeAgent)
	addDefinition(t, db, 50, false) // disabled definition
	addDefinition(t, db, 51, true)
	addBinding(t, db, 2, 51) // instance binding wins over the definition (M2)
	addDefinition(t, db, 52, true)
	if err := db.Model(&authmodel.User{}).Where("id = ?", 52).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	for _, m := range []webhookMessage{
		webhookText("x50", 1, "1@50", personChannel, "hi"),
		webhookText("x51", 1, "1@51", personChannel, "hi"), // owned by 2, not routed to 1
		webhookText("x52", 1, "1@52", personChannel, "hi"),
	} {
		if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{m}, now); err != nil || accepted != 0 {
			t.Fatalf("%s accepted=%d err=%v", m.MessageIDStr, accepted, err)
		}
	}
}

// The agent ceiling is per (agent, owner): one user exhausting it leaves others untouched.
func TestDefinitionIdentityRateLimitPerOwner(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 50, authmodel.AccountTypeAgent)
	addDefinition(t, db, 50, true)
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 2, authmodel.AccountTypeHuman)
	for _, g := range []string{"g0", "g1", "g2", "g3", "gx"} {
		if err := db.Create(&IMGroup{GroupID: g, CreatorUID: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(1_800_000_000, 0)
	// Owner 1: 20 accepted across 4 groups (the sender limit is per channel), 21st refused.
	for i := 0; i < agentMinuteCeiling; i++ {
		m := webhookText(fmt.Sprintf("o1-%d", i), 1, fmt.Sprintf("g%d", i/senderMinuteLimit), groupChannel, "hi", 50)
		if accepted, err := acceptHuman(t.Context(), db, "msg.notify", []webhookMessage{m}, now); err != nil || accepted != 1 {
			t.Fatalf("owner1 #%d accepted=%d err=%v", i+1, accepted, err)
		}
	}
	if _, err := acceptHuman(t.Context(), db, "msg.notify",
		[]webhookMessage{webhookText("o1-over", 1, "gx", groupChannel, "hi", 50)}, now); !errors.Is(err, errAgentRateLimited) {
		t.Fatalf("owner1 21st err=%v", err)
	}
	// Owner 2 talks to the same identity in the same minute and is not affected.
	if accepted, err := acceptHuman(t.Context(), db, "msg.notify",
		[]webhookMessage{webhookText("o2", 2, "2@50", personChannel, "hi")}, now); err != nil || accepted != 1 {
		t.Fatalf("owner2 accepted=%d err=%v", accepted, err)
	}
}
