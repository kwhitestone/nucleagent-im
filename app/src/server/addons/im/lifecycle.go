package im

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/kwhitestone/prism-fusion/global"
)

type lifecycleRequest struct {
	AgentUID             uint      `json:"agentUid"`
	ExecutionOwnerUserID uint      `json:"executionOwnerUserId"`
	SourceKey            string    `json:"sourceKey"`
	AdmittedAt           time.Time `json:"admittedAt"`
}

// Never cache an authorization decision: events and the directory are only hints.
func (p *Plugin) checkDelivery(ctx context.Context, row *IMWebhookInbox) error {
	err := p.deliveryAllowed(ctx, row)
	var denied *httpStatusError
	if errors.As(err, &denied) && denied.status >= 400 && denied.status < 500 &&
		denied.status != http.StatusTooManyRequests {
		// Keep completed history intact; a restore must not revive unsent work.
		// The row is ended only as the caller saw it (state, holder and lease
		// generation): a holder that was paused after the refusal and came back
		// once another instance took the turn over must not end that turn
		// (MUYU-160 review 8).
		result := global.PRISM_DB.WithContext(ctx).Model(&IMWebhookInbox{}).
			Where("id = ? AND state = ? AND lease_owner = ? AND attempts = ?", row.ID, row.State, row.LeaseOwner, row.Attempts).
			Where("state IN ?", []string{inboxPending, inboxProcessing, inboxRetryable}).
			Updates(map[string]any{
				"state": inboxDead, "last_error_code": denied.code, "lease_owner": "",
				"lease_expires_at": nil, "next_attempt_at": nil,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 && row.State == inboxProcessing {
			// Not ended by this call: either it is already over (the refusal
			// stands), or another holder runs the turn now (leave it alone).
			var current IMWebhookInbox
			if global.PRISM_DB.WithContext(ctx).Select("state").First(&current, row.ID).Error == nil &&
				(current.State == inboxProcessing || current.State == inboxPending || current.State == inboxRetryable) {
				return errLeaseLost
			}
		}
	}
	return err
}

func (p *Plugin) deliveryAllowed(ctx context.Context, row *IMWebhookInbox) error {
	if err := checkDispatchParticipants(ctx, row); err != nil {
		return err
	}
	var current IMWebhookInbox
	if err := global.PRISM_DB.WithContext(ctx).Select("state", "created_at").First(&current, row.ID).Error; err != nil {
		return err
	}
	if current.State == inboxDead || current.State == inboxIgnored || current.CreatedAt.IsZero() {
		return &httpStatusError{status: http.StatusConflict, code: "im_work_terminal"}
	}
	token, err := p.serviceToken()
	if err != nil {
		return err
	}
	var decision struct {
		Allowed bool `json:"allowed"`
	}
	input := lifecycleRequest{
		AgentUID: row.TargetAgentUID, ExecutionOwnerUserID: row.ExecutionOwnerUserID,
		SourceKey: row.SourceKey, AdmittedAt: current.CreatedAt.UTC(),
	}
	if err := doJSON(ctx, http.MethodPost, p.coreURL+"/api/v1/addons/conversation/im-lifecycle", token, input, &decision); err != nil {
		return err
	}
	if !decision.Allowed {
		return &httpStatusError{status: http.StatusServiceUnavailable, code: "im_lifecycle_unavailable"}
	}
	if err := global.PRISM_DB.WithContext(ctx).Select("state").First(&current, row.ID).Error; err != nil {
		return err
	}
	if current.State == inboxDead || current.State == inboxIgnored {
		return &httpStatusError{status: http.StatusConflict, code: "im_work_terminal"}
	}
	row.CreatedAt = current.CreatedAt
	// Local revocation may have committed while Core was answering.
	return checkDispatchParticipants(ctx, row)
}

func (p *Plugin) streamViewerAllowed(ctx context.Context, userID uint, channelID string, channelType uint8) (bool, error) {
	enabled, err := recipientEnabled(ctx, global.PRISM_DB, userID)
	if err != nil || !enabled {
		return false, err
	}
	return p.channelParticipant(ctx, userID, channelID, channelType)
}
