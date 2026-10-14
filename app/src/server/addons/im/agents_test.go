package im

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humagin"
	"github.com/gin-gonic/gin"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/nucleagent/nucleagent-shared/model"
)

// UNI-IM-REDESIGN: the directory lists exactly the definitions a DM would reach.
func TestAgentDirectory(t *testing.T) {
	db := m2DB(t)
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	for _, uid := range []uint{50, 51, 52, 53} {
		addUser(t, db, uid, authmodel.AccountTypeAgent)
	}
	addDefinition(t, db, 51, false) // inactive definition
	addDefinition(t, db, 52, true)  // account disabled below
	if err := db.Model(&authmodel.User{}).Where("id = ?", 52).Update("enable", 2).Error; err != nil {
		t.Fatal(err)
	}
	agent := uint(50)
	if err := db.Create(&model.AgentTemplate{Name: "HR helper", Slug: "hr-helper", AuthAgentUserID: &agent, IsActive: true,
		Config: model.JSON(`{"description":"Answers HR questions","prompt":"SECRET-PERSONA"}`)}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&model.AgentTemplate{Name: "no identity", Slug: "no-identity", IsActive: true}).Error; err != nil {
		t.Fatal(err)
	}

	router := gin.New()
	sharedAuthStack(t, router)
	(&Plugin{}).registerAgents(humagin.New(router, huma.DefaultConfig("agents", "1")))
	if got := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/agents", "").Code; got != http.StatusUnauthorized {
		t.Fatalf("anonymous status=%d", got)
	}
	response := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/agents", issueSessionToken(t, db, 1, "agents-session"))
	var body envelope[[]directoryAgent]
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	want := []directoryAgent{{UID: 50, Name: "HR helper", Description: "Answers HR questions"}}
	if len(body.Data) != 1 || body.Data[0].UID != want[0].UID ||
		body.Data[0].Name != want[0].Name || body.Data[0].Description != want[0].Description {
		t.Fatalf("data=%+v want %+v", body.Data, want)
	}
	if strings.Contains(response.Body.String(), "SECRET-PERSONA") {
		t.Fatal("persona prompt leaked")
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("directory can be served from a stale cache")
	}
	if err := db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 50).
		Update("is_active", false).Error; err != nil {
		t.Fatal(err)
	}
	response = authorizedRequest(t, router, http.MethodGet, "/api/v1/im/agents", issueSessionToken(t, db, 1, "agents-session-next"))
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || len(body.Data) != 0 {
		t.Fatal("directory retained disabled definition")
	}
}

func TestAgentDirectoryFreshLifecycle(t *testing.T) {
	db := m2DB(t)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addDefinition(t, db, 42, true)
	assertCount := func(want int) {
		t.Helper()
		rows, err := listDirectoryAgents(t.Context(), db, 0)
		if err != nil || len(rows) != want {
			t.Fatalf("directory count=%d want=%d err=%v", len(rows), want, err)
		}
	}
	assertCount(1)
	// Tombstones must win even if a stale writer left is_active=true.
	if err := db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 42).
		Update("deleted_at", time.Now()).Error; err != nil {
		t.Fatal(err)
	}
	assertCount(0)
	if err := db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 42).
		Updates(map[string]any{"deleted_at": nil, "is_active": false}).Error; err != nil {
		t.Fatal(err)
	}
	assertCount(0)
	db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 42).Update("is_active", true)
	assertCount(1)
	db.Model(&authmodel.User{}).Where("id = ?", 42).Update("enable", 2)
	assertCount(0)
	db.Model(&authmodel.User{}).Where("id = ?", 42).Update("enable", 1)
	assertCount(1)
	db.Model(&authmodel.User{}).Where("id = ?", 42).Update("account_type", authmodel.AccountTypeHuman)
	assertCount(0)
}

func TestAgentDirectoryPublicInvalidation(t *testing.T) {
	db := m2DB(t)
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}
	addUser(t, db, 1, authmodel.AccountTypeHuman)
	addUser(t, db, 42, authmodel.AccountTypeAgent)
	addDefinition(t, db, 42, true)
	if err := db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 42).
		Update("config", model.JSON(`{"avatar":"/avatar.png","description":"Public","prompt":"NEVER-PUBLIC","unknown":"ALSO-PRIVATE"}`)).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	sharedAuthStack(t, router)
	(&Plugin{}).registerAgents(humagin.New(router, huma.DefaultConfig("public directory", "1")))
	token := issueSessionToken(t, db, 1, "directory-events")
	response := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/agents", token)
	var directory envelope[[]map[string]any]
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &directory) != nil || len(directory.Data) != 1 {
		t.Fatal("public directory unavailable")
	}
	item := directory.Data[0]
	if item["avatar"] != "/avatar.png" || item["effectiveSource"] != "definition" ||
		item["revision"] == nil || item["revision"] == "" || item["modelRule"] != "im-next-admission" {
		t.Fatalf("missing public metadata: fields=%v", item)
	}
	before := response.Header().Get("X-IM-Directory-Revision")
	if before == "" {
		t.Fatal("directory has no invalidation cursor")
	}
	path := "/api/v1/im/agents/events?after=" + url.QueryEscape(before)
	if code := authorizedRequest(t, router, http.MethodGet, path, "").Code; code != http.StatusUnauthorized {
		t.Fatalf("anonymous events status=%d", code)
	}
	assertChanged := func(want bool) string {
		t.Helper()
		response := authorizedRequest(t, router, http.MethodGet, path, token)
		var body envelope[struct {
			Revision string `json:"revision"`
			Changed  bool   `json:"changed"`
		}]
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil ||
			body.Data.Changed != want || body.Data.Revision == "" || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("invalidation response status=%d changed=%v want=%v", response.Code, body.Data.Changed, want)
		}
		return body.Data.Revision
	}
	assertChanged(false)
	if err := db.Model(&model.AgentTemplate{}).Where("auth_agent_user_id = ?", 42).
		Update("name", "Renamed").Error; err != nil {
		t.Fatal(err)
	}
	after := assertChanged(true)
	path = "/api/v1/im/agents/events?after=" + url.QueryEscape(after)
	assertChanged(false)
	db.Model(&authmodel.User{}).Where("id = ?", 42).Update("enable", 2)
	assertChanged(true)
	db.Model(&authmodel.User{}).Where("id = ?", 1).Update("enable", 2)
	if code := authorizedRequest(t, router, http.MethodGet, path, token).Code; code < 400 {
		t.Fatal("disabled viewer retained directory subscription")
	}
	if strings.Contains(response.Body.String(), "NEVER-PUBLIC") || strings.Contains(response.Body.String(), "ALSO-PRIVATE") {
		t.Fatal("directory disclosed private fields")
	}
}
