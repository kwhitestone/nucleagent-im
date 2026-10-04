package im

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

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
	if len(body.Data) != 1 || body.Data[0] != want[0] {
		t.Fatalf("data=%+v want %+v", body.Data, want)
	}
	if strings.Contains(response.Body.String(), "SECRET-PERSONA") {
		t.Fatal("persona prompt leaked")
	}
}
