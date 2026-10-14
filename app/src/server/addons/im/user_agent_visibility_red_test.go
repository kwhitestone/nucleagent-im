package im

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
)

// USERAGENT U4 (review USERAGENT-REVIEW-1 section 2, I1-I6): a private
// (user-created) agent is visible and callable only by its owner in a DM.
// For everyone else it must behave exactly like an agent that does not exist.

const (
	uaOwner   = uint(1)  // A: owner of the private agent
	uaOther   = uint(2)  // B: another human
	uaPrivate = uint(70) // A's private agent account
	uaSystem  = uint(71) // admin-maintained system agent account
)

// addPrivateDefinition creates A's private definition bound to agent uid and
// asserts the row really is private+owned (guards against a vacuous pass).
func addPrivateDefinition(t *testing.T, db *gorm.DB, owner, agent uint, active bool) model.AgentTemplate {
	t.Helper()
	tpl := model.AgentTemplate{Name: fmt.Sprintf("private-%d", agent), Slug: fmt.Sprintf("private-%d", agent),
		AuthAgentUserID: &agent, IsActive: true, OwnerUserID: &owner, Visibility: model.AgentVisibilityPrivate}
	if err := db.Create(&tpl).Error; err != nil {
		t.Fatal(err)
	}
	if !active {
		if err := db.Model(&tpl).Update("is_active", false).Error; err != nil {
			t.Fatal(err)
		}
	}
	var got model.AgentTemplate
	if err := db.First(&got, tpl.ID).Error; err != nil || got.Visibility != model.AgentVisibilityPrivate ||
		got.OwnerUserID == nil || *got.OwnerUserID != owner || got.IsActive != active {
		t.Fatalf("precondition: private definition not created as wanted: %+v err=%v", got, err)
	}
	return got
}

// uaFixture: humans A and B, A's private agent and one system agent, both active.
func uaFixture(t *testing.T) *gorm.DB {
	t.Helper()
	db := m2DB(t)
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}
	addUser(t, db, uaOwner, authmodel.AccountTypeHuman)
	addUser(t, db, uaOther, authmodel.AccountTypeHuman)
	addUser(t, db, uaPrivate, authmodel.AccountTypeAgent)
	addUser(t, db, uaSystem, authmodel.AccountTypeAgent)
	addPrivateDefinition(t, db, uaOwner, uaPrivate, true)
	addDefinition(t, db, uaSystem, true)
	return db
}

func directoryUIDs(t *testing.T, db *gorm.DB, viewer uint) map[uint]bool {
	t.Helper()
	router := gin.New()
	sharedAuthStack(t, router)
	(&Plugin{}).registerAgents(humagin.New(router, huma.DefaultConfig("agents", "1")))
	response := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/agents",
		issueSessionToken(t, db, viewer, fmt.Sprintf("ua-dir-%d-%s", viewer, t.Name())))
	var body envelope[[]directoryAgent]
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatalf("viewer %d status=%d body=%s", viewer, response.Code, response.Body)
	}
	out := map[uint]bool{}
	for _, a := range body.Data {
		out[a.UID] = true
	}
	return out
}

// I1 agents.go listDirectoryAgents: the IM directory hides others' private agents.
func TestUserAgentI1DirectoryHidesOthersPrivate(t *testing.T) {
	db := uaFixture(t)
	owner := directoryUIDs(t, db, uaOwner)
	if !owner[uaPrivate] || !owner[uaSystem] {
		t.Fatalf("precondition: owner directory must list own private and system agents, got %v", owner)
	}
	other := directoryUIDs(t, db, uaOther)
	if !other[uaSystem] {
		t.Fatalf("system agent must stay visible to everyone, got %v", other)
	}
	if other[uaPrivate] {
		t.Fatalf("I1: B's IM directory lists A's private agent %d", uaPrivate)
	}
}

// I2 webhook.go resolveAgentTargets: a private agent uid resolves only for its
// owner in a direct chat; never for another sender, never in a group.
func TestUserAgentI2ResolveOnlyOwnerDirect(t *testing.T) {
	db := uaFixture(t)
	got, err := resolveAgentTargets(db, []uint{uaPrivate}, personChannel, "", uaOwner, uaOwner)
	if err != nil || len(got) != 1 || got[0].agentUID != uaPrivate || got[0].ownerUID != uaOwner {
		t.Fatalf("precondition: owner DM must resolve own private agent, got %+v err=%v", got, err)
	}
	if got, _ := resolveAgentTargets(db, []uint{uaSystem}, groupChannel, "ua-g1", uaOther, uaOther); len(got) != 1 {
		t.Fatalf("system agent must stay callable in a group, got %+v", got)
	}
	if got, err := resolveAgentTargets(db, []uint{uaPrivate}, personChannel, "", uaOther, uaOther); err != nil || len(got) != 0 {
		t.Fatalf("I2: B's DM resolves A's private agent: %+v err=%v", got, err)
	}
	if got, err := resolveAgentTargets(db, []uint{uaPrivate}, groupChannel, "ua-g1", uaOther, uaOther); err != nil || len(got) != 0 {
		t.Fatalf("I2: B's group @ resolves A's private agent: %+v err=%v", got, err)
	}
	if got, err := resolveAgentTargets(db, []uint{uaPrivate}, groupChannel, "ua-g1", uaOwner, uaOwner); err != nil || len(got) != 0 {
		t.Fatalf("I2: phase 1 private agents never run in a group, even for the owner: %+v err=%v", got, err)
	}
	// End to end: B's DM to A's private agent must create no Core task.
	ctx := context.Background()
	if _, err := acceptHuman(ctx, db, "msg.notify", []webhookMessage{
		webhookText("ua-i2-a", uaOwner, "1@70", personChannel, "hi")}, fixedNoticeTime); err != nil || inboxCount(t, db) != 1 {
		t.Fatalf("precondition: owner DM must create one task, inbox=%d err=%v", inboxCount(t, db), err)
	}
	if _, err := acceptHuman(ctx, db, "msg.notify", []webhookMessage{
		webhookText("ua-i2-b", uaOther, "2@70", personChannel, "hi")}, fixedNoticeTime); err != nil {
		t.Fatal(err)
	}
	if n := inboxCount(t, db); n != 1 {
		t.Fatalf("I2: B's DM to A's private agent created a task (inbox=%d)", n)
	}
}

// I3 agent_unavailable_reply.go: B messaging A's stopped/deleted private agent
// gets the same silence as an unknown uid; only A sees the "disabled" notice.
func TestUserAgentI3DisabledNoticeDoesNotLeakPrivate(t *testing.T) {
	for _, state := range []string{"stopped", "deleted"} {
		t.Run(state, func(t *testing.T) {
			db := uaFixture(t)
			update := map[string]any{"is_active": false}
			if state == "deleted" {
				update["deleted_at"] = fixedNoticeTime
			}
			if err := db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", uaPrivate).Updates(update).Error; err != nil {
				t.Fatal(err)
			}
			setEnable(t, db, uaPrivate, 2) // as agentidentity.Project does on stop/delete
			sent, count := noticeServer(t, false)
			ctx := context.Background()
			if _, err := acceptHuman(ctx, db, "msg.notify", []webhookMessage{
				webhookText("ua-i3-a", uaOwner, "1@70", personChannel, "hi")}, fixedNoticeTime); err != nil {
				t.Fatal(err)
			}
			if count() != 1 || (*sent)[0].ChannelID != "1@70" {
				t.Fatalf("precondition: owner must get exactly one disabled notice, got %d", count())
			}
			if _, err := acceptHuman(ctx, db, "msg.notify", []webhookMessage{
				webhookText("ua-i3-b", uaOther, "2@70", personChannel, "hi")}, fixedNoticeTime); err != nil {
				t.Fatal(err)
			}
			if count() != 1 {
				t.Fatalf("I3: B got a disabled notice from A's private agent (existence leak), sends=%d", count())
			}
			if off, err := agentUnavailable(db, uaPrivate); err != nil || !off {
				t.Fatalf("precondition: private agent must be unavailable, off=%v err=%v", off, err)
			}
		})
	}
}

// I4 groups.go createGroup/addGroupMembers validateUsers: a private agent account
// cannot be pulled into any group (phase 1 server-side rule), not even by its
// owner, and the refusal equals the one for a nonexistent uid.
func TestUserAgentI4GroupsRejectPrivateAgentMembers(t *testing.T) {
	db := uaFixture(t)
	addUser(t, db, 3, authmodel.AccountTypeHuman)
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)
	team := createGroupForTest(t, router, "Team", uaOther, uaSystem) // precondition: system agent may join
	missing := groupRequest(t, router, http.MethodPost, "/api/v1/im/groups", uaOwner,
		map[string]any{"title": "Ghost", "memberUids": []uint{999}})
	if missing.Code != http.StatusNotFound {
		t.Fatalf("precondition: unknown member must be 404, got %d %s", missing.Code, missing.Body)
	}
	for _, creator := range []uint{uaOwner, uaOther} {
		create := groupRequest(t, router, http.MethodPost, "/api/v1/im/groups", creator,
			map[string]any{"title": "Leak", "memberUids": []uint{uaPrivate}})
		if create.Code != missing.Code || responseCode(t, create) != responseCode(t, missing) {
			t.Fatalf("I4: creator %d put private agent into a new group: %d %s", creator, create.Code, create.Body)
		}
	}
	add := groupRequest(t, router, http.MethodPost, fmt.Sprintf("/api/v1/im/groups/%d/members", team.ID), uaOwner,
		map[string]any{"memberUids": []uint{uaPrivate}})
	if add.Code != missing.Code || responseCode(t, add) != responseCode(t, missing) {
		t.Fatalf("I4: owner added private agent to a group: %d %s", add.Code, add.Body)
	}
}

// I5 groups.go replaceAllowlist/getAllowlist validateAgent: a private agent is
// "agent not found" for group allowlists even if a stale membership row exists.
func TestUserAgentI5AllowlistRejectsPrivateAgent(t *testing.T) {
	db := uaFixture(t)
	fake := newFakeWuKong(t)
	router := groupRouter(t, fake.plugin(), true)
	team := createGroupForTest(t, router, "Team", uaOther, uaSystem)
	ok := groupRequest(t, router, http.MethodPut,
		fmt.Sprintf("/api/v1/im/groups/%d/agents/%d/allowlist", team.ID, uaSystem), uaOwner, map[string]any{"memberUids": []uint{uaOther}})
	if ok.Code != http.StatusOK {
		t.Fatalf("precondition: system agent allowlist must work, got %d %s", ok.Code, ok.Body)
	}
	// A stale row (e.g. written before this rule) must not open the allowlist.
	if err := db.Create(&IMGroupMember{GroupID: team.WuKongChannelID, UID: uaPrivate}).Error; err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/api/v1/im/groups/%d/agents/%d/allowlist", team.ID, uaPrivate)
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		var body any
		if method == http.MethodPut {
			body = map[string]any{"memberUids": []uint{uaOther}}
		}
		got := groupRequest(t, router, method, path, uaOwner, body)
		if got.Code != http.StatusNotFound || responseCode(t, got) != "agent_not_found" {
			t.Fatalf("I5: %s allowlist for private agent: %d %s", method, got.Code, got.Body)
		}
	}
}

// I6 recipients.go: B asking for A's private agent's status gets exactly the
// answer for a nonexistent uid; A still sees it enabled.
func TestUserAgentI6RecipientStatusHidesPrivate(t *testing.T) {
	db := uaFixture(t)
	router := gin.New()
	sharedAuthStack(t, router)
	(&Plugin{}).registerRecipients(humagin.New(router, huma.DefaultConfig("recipients", "1")))
	get := func(viewer, uid uint) string {
		t.Helper()
		r := authorizedRequest(t, router, http.MethodGet, fmt.Sprintf("/api/v1/im/recipients/%d", uid),
			issueSessionToken(t, db, viewer, fmt.Sprintf("ua-rcpt-%d-%d", viewer, uid)))
		if r.Code != http.StatusOK {
			t.Fatalf("viewer %d uid %d status=%d %s", viewer, uid, r.Code, r.Body)
		}
		return r.Body.String()
	}
	owner := get(uaOwner, uaPrivate)
	if !strings.Contains(owner, `"enabled":true`) {
		t.Fatalf("precondition: owner sees own private agent enabled: %s", owner)
	}
	if missing, other := get(uaOther, 999), get(uaOther, uaPrivate); other != missing {
		t.Fatalf("I6: B's status for A's private agent %s differs from nonexistent %s", other, missing)
	}
}
