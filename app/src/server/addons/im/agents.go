package im

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
)

// directoryAgent is one agent a user can DM: an active definition with an IM
// identity whose agent account is enabled. The persona prompt is admin-authored
// instructions, not user-facing copy; only explicit public metadata is exposed.
type directoryAgent struct {
	UID             uint   `json:"uid"`
	Name            string `json:"name"`
	Description     string `json:"description"`
	Avatar          string `json:"avatar"`
	Revision        string `json:"revision"`
	EffectiveSource string `json:"effectiveSource"`
	ModelRule       string `json:"modelRule"`
	PromptRule      string `json:"promptRule"`
	OverrideRule    string `json:"overrideRule"`
	InFlightRule    string `json:"inFlightRule"`
}

type agentsOutput struct {
	CacheControl string `header:"Cache-Control"`
	Revision     string `header:"X-IM-Directory-Revision"`
	Body         envelope[[]directoryAgent]
}

type directoryEventsInput struct {
	After string `query:"after" maxLength:"128"`
}

type directoryChange struct {
	Revision string `json:"revision"`
	Changed  bool   `json:"changed"`
}

type directoryEventsOutput struct {
	CacheControl string `header:"Cache-Control"`
	Body         envelope[directoryChange]
}

// Only public projection data and update times enter this invalidation token,
// never private config hashes or a claimed Core execution revision.
func directoryRevision(value any) string {
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// listDirectoryAgents is the set resolveAgentTargets admits for a DM by
// definition: active template, IM identity set, enabled agent account.
func listDirectoryAgents(ctx context.Context, db *gorm.DB) ([]directoryAgent, error) {
	var templates []model.AgentTemplate
	if err := db.WithContext(ctx).
		Where("is_active = ? AND deleted_at IS NULL AND auth_agent_user_id IS NOT NULL", true).
		Order("name ASC, id ASC").Find(&templates).Error; err != nil {
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
			Avatar      string `json:"avatar"`
		}
		_ = json.Unmarshal(t.Config, &config) // a malformed config just has no description
		agent := directoryAgent{
			UID: *t.AuthAgentUserID, Name: t.Name, Description: config.Description, Avatar: config.Avatar,
			EffectiveSource: "definition", ModelRule: "im-next-admission", PromptRule: "next-admission",
			OverrideRule: "instance-explicit", InFlightRule: "pinned",
		}
		agent.Revision = directoryRevision(struct {
			Agent     directoryAgent
			UpdatedAt string
		}{agent, t.UpdatedAt.UTC().Format(time.RFC3339Nano)})
		out = append(out, agent)
	}
	return out, nil
}

func authorizedDirectory(ctx context.Context) ([]directoryAgent, error) {
	userID, _ := ctx.Value(userIDKey).(uint)
	enabled, err := recipientEnabled(ctx, global.PRISM_DB, userID)
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_agents_unavailable", "agent directory is unavailable")
	}
	if !enabled {
		return nil, newIMProblem(http.StatusForbidden, "im_recipient_disabled", "account is disabled")
	}
	agents, err := listDirectoryAgents(ctx, global.PRISM_DB)
	if err != nil {
		return nil, newIMProblem(http.StatusServiceUnavailable, "im_agents_unavailable", "agent directory is unavailable")
	}
	return agents, nil
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
		agents, err := authorizedDirectory(ctx)
		if err != nil {
			return nil, err
		}
		return &agentsOutput{
			CacheControl: "no-store", Revision: directoryRevision(agents),
			Body: envelope[[]directoryAgent]{Code: 0, Message: "success", Data: agents},
		}, nil
	})
	huma.Register(api, huma.Operation{
		OperationID: "imAgentDirectoryEvents",
		Method:      http.MethodGet,
		Path:        "/api/v1/im/agents/events",
		Summary:     "Poll public agent directory invalidation",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, input *directoryEventsInput) (*directoryEventsOutput, error) {
		agents, err := authorizedDirectory(ctx)
		if err != nil {
			return nil, err
		}
		revision := directoryRevision(agents)
		return &directoryEventsOutput{
			CacheControl: "no-store",
			Body: envelope[directoryChange]{Code: 0, Message: "success",
				Data: directoryChange{Revision: revision, Changed: input.After != revision}},
		}, nil
	})
}
