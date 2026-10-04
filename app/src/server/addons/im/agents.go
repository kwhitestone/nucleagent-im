package im

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
)

// directoryAgent is one agent a user can DM: an active definition with an IM
// identity whose agent account is enabled. The persona prompt is admin-authored
// instructions, not user-facing copy, so only the description is exposed.
type directoryAgent struct {
	UID         uint   `json:"uid"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type agentsOutput struct {
	Body envelope[[]directoryAgent]
}

// listDirectoryAgents is the set resolveAgentTargets admits for a DM by
// definition: active template, IM identity set, enabled agent account.
func listDirectoryAgents(ctx context.Context, db *gorm.DB) ([]directoryAgent, error) {
	var templates []model.AgentTemplate
	if err := db.WithContext(ctx).
		Where("is_active = ? AND auth_agent_user_id IS NOT NULL", true).
		Order("name ASC").Find(&templates).Error; err != nil {
		return nil, err
	}
	uids := make([]uint, 0, len(templates))
	for _, t := range templates {
		uids = append(uids, *t.AuthAgentUserID)
	}
	var enabled []uint
	if len(uids) > 0 {
		if err := db.WithContext(ctx).Model(&authmodel.User{}).
			Where("id IN ? AND enable = ? AND account_type = ?", uids, 1, authmodel.AccountTypeAgent).
			Pluck("id", &enabled).Error; err != nil {
			return nil, err
		}
	}
	ok := make(map[uint]bool, len(enabled))
	for _, uid := range enabled {
		ok[uid] = true
	}
	out := make([]directoryAgent, 0, len(enabled))
	for _, t := range templates {
		if !ok[*t.AuthAgentUserID] {
			continue
		}
		var config struct {
			Description string `json:"description"`
		}
		_ = json.Unmarshal(t.Config, &config) // a malformed config just has no description
		out = append(out, directoryAgent{UID: *t.AuthAgentUserID, Name: t.Name, Description: config.Description})
	}
	return out, nil
}

func (p *Plugin) registerAgents(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "imListAgents",
		Method:      http.MethodGet,
		Path:        "/api/v1/im/agents",
		Summary:     "List agents a user can message in IM",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, _ *struct{}) (*agentsOutput, error) {
		agents, err := listDirectoryAgents(ctx, global.PRISM_DB)
		if err != nil {
			return nil, newIMProblem(http.StatusServiceUnavailable, "im_agents_unavailable", "agent directory is unavailable")
		}
		return &agentsOutput{Body: envelope[[]directoryAgent]{Code: 0, Message: "success", Data: agents}}, nil
	})
}
