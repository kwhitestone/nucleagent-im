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
		// A policy refusal: nothing of this turn may be delivered any more.
		refusal := &deliveryRefusal{denied}
		// Keep completed history intact; a restore must not revive unsent work.
		// The row is ended here only as the caller saw it (state, holder and
		// lease generation), and only while Core holds nothing for it: a turn
		// Core accepted, owes a Core stop, or has a claimed answer is ended by
		// the worker's single exit (failInbox), which stops the Core run first
		// and never delivers after a refusal (MUYU-160 reviews 8 and 9).
		result := global.PRISM_DB.WithContext(ctx).Model(&IMWebhookInbox{}).
			Where("id = ? AND state = ? AND lease_owner = ? AND attempts = ?", row.ID, row.State, row.LeaseOwner, row.Attempts).
			// last_core_event_id is seeded right before the first dispatch, so an
			// empty cursor means Core never saw this turn.
			Where("core_conversation_id = 0 AND core_stop_code = '' AND COALESCE(last_core_event_id, '') = '' AND (final_core_message_id IS NULL OR final_core_message_id = 0)").
			Where("state IN ?", []string{inboxPending, inboxProcessing, inboxRetryable}).
			Updates(map[string]any{
				"state": inboxDead, "last_error_code": denied.code, "lease_owner": "",
				"lease_expires_at": nil, "next_attempt_at": nil,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 && row.State == inboxProcessing && verifyLease(global.PRISM_DB, row) != nil {
			// Not ended by this call and not ours any more: either it is already
			// over (the refusal stands) or another holder runs it (leave it alone).
			var current IMWebhookInbox
			if global.PRISM_DB.WithContext(ctx).Select("state").First(&current, row.ID).Error == nil &&
				(current.State == inboxProcessing || current.State == inboxPending || current.State == inboxRetryable) {
				return errLeaseLost
			}
		}
		return refusal
	}
	return err
}

// deliveryRefusal marks a policy refusal (agent stopped, recipient disabled,
// lifecycle stale, ...): the turn ends without delivering anything, not even an
// answer that was already claimed. It is distinct from a transport failure of
// WuKong (an answer whose post failed stays owed and is resent).
type deliveryRefusal struct{ *httpStatusError }

func (e *deliveryRefusal) Unwrap() error { return e.httpStatusError }

func isDeliveryRefusal(err error) bool {
	var refusal *deliveryRefusal
	return errors.As(err, &refusal)
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
