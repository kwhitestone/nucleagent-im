package im

import (
	"encoding/json"
	"fmt"
	"net/http"
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
