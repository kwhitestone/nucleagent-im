package im

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
)

type recipientInput struct {
	UID uint `path:"uid" minimum:"1"`
}

type recipientData struct {
	Enabled bool `json:"enabled"`
}

type recipientOutput struct {
	CacheControl string `header:"Cache-Control"`
	Body         envelope[recipientData]
}

func recipientEnabled(ctx context.Context, db *gorm.DB, uid uint) (bool, error) {
	var count int64
	err := db.WithContext(ctx).Model(&authmodel.User{}).
		Where("id = ? AND enable = ?", uid, 1).Count(&count).Error
	return count == 1, err
}

func (p *Plugin) registerRecipients(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "imRecipientStatus",
		Method:      http.MethodGet,
		Path:        "/api/v1/im/recipients/{uid}",
		Summary:     "Check whether a direct-message recipient is enabled",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, input *recipientInput) (*recipientOutput, error) {
		enabled, err := recipientEnabled(ctx, global.PRISM_DB, input.UID)
		if err == nil && enabled {
			// Another user's private agent reads exactly like a nonexistent uid.
			var hidden bool
			viewer, _ := ctx.Value(userIDKey).(uint)
			hidden, err = privateAgentHidden(global.PRISM_DB.WithContext(ctx), input.UID, viewer)
			enabled = !hidden
		}
		if err != nil {
			return nil, newIMProblem(http.StatusServiceUnavailable, "im_recipient_unavailable", "recipient status is unavailable")
		}
		return &recipientOutput{
			CacheControl: "no-store",
			Body:         envelope[recipientData]{Code: 0, Message: "success", Data: recipientData{Enabled: enabled}},
		}, nil
	})
}

// Recheck queued work: account state can change after webhook admission.
func checkDispatchParticipants(ctx context.Context, row *IMWebhookInbox) error {
	origin := row.OriginSenderUID
	if origin == 0 && row.SourceAgentUID == 0 {
		origin = row.SenderUID
	}
	seen := map[uint]bool{}
	for _, uid := range []uint{row.TargetAgentUID, row.SenderUID, row.ExecutionOwnerUserID, origin} {
		if seen[uid] {
			continue
		}
		seen[uid] = true
		enabled, err := recipientEnabled(ctx, global.PRISM_DB, uid)
		if err != nil {
			return err
		}
		if !enabled {
			return &httpStatusError{status: http.StatusForbidden, code: "im_recipient_disabled"}
		}
	}
	db := global.PRISM_DB.WithContext(ctx)
	var agents int64
	if err := db.Model(&authmodel.User{}).
		Where("id = ? AND account_type = ?", row.TargetAgentUID, authmodel.AccountTypeAgent).
		Count(&agents).Error; err != nil {
		return err
	}
	if agents != 1 {
		return &httpStatusError{status: http.StatusForbidden, code: "im_agent_identity_invalid"}
	}
	for uid := range map[uint]bool{origin: true, row.ExecutionOwnerUserID: true} {
		var humans int64
		if err := db.Model(&authmodel.User{}).
			Where("id = ? AND account_type = ?", uid, authmodel.AccountTypeHuman).Count(&humans).Error; err != nil {
			return err
		}
		if humans != 1 {
			return &httpStatusError{status: http.StatusForbidden, code: "im_execution_owner_invalid"}
		}
	}
	switch row.ChannelType {
	case personChannel:
		_, peer, ok := canonicalPersonChannel(row.ChannelID, row.SenderUID)
		if !ok || peer != row.TargetAgentUID || row.ExecutionOwnerUserID != row.SenderUID {
			return &httpStatusError{status: http.StatusForbidden, code: "channel_forbidden"}
		}
	case groupChannel:
		for uid := range map[uint]bool{row.SenderUID: true, row.TargetAgentUID: true, origin: true} {
			member, err := isGroupMember(ctx, row.ChannelID, uid)
			if err != nil {
				return err
			}
			if !member {
				return &httpStatusError{status: http.StatusForbidden, code: "channel_forbidden"}
			}
		}
		allowed, err := groupInvocationAllowed(db, row.ChannelID, row.TargetAgentUID, origin)
		if err != nil {
			return err
		}
		if !allowed {
			return &httpStatusError{status: http.StatusForbidden, code: "im_group_invocation_forbidden"}
		}
	default:
		return &httpStatusError{status: http.StatusForbidden, code: "channel_forbidden"}
	}
	return nil
}
