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
	for _, uid := range []uint{row.TargetAgentUID, row.SenderUID} {
		enabled, err := recipientEnabled(ctx, global.PRISM_DB, uid)
		if err != nil {
			return err
		}
		if !enabled {
			return &httpStatusError{status: http.StatusForbidden, code: "im_recipient_disabled"}
		}
	}
	return nil
}
