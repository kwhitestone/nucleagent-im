package im

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
)

// D50 (AGENT-REPLY-1): a stopped or deleted agent answers with one fixed system
// notice instead of silence. No model call, no Core task, zero cost. The same
// wording covers stop and delete and never carries a time or an operator.
const (
	agentUnavailableNotice = "agent_unavailable"
	agentUnavailableText   = "该智能体已停用\n如需继续使用，请联系它的创建者或管理员"
	agentUnavailableScope  = "disabled_reply"
	agentUnavailableWindow = 10 * time.Minute
)

// wuKongAPIAddr lets the webhook path (a free function) reach WuKong; set in Initialize.
var wuKongAPIAddr string

// agentUnavailable reports whether uid is an agent account that must not run:
// account disabled, or (account enabled but) its definition stopped or deleted.
// Humans, unknown uids and live agents are false.
func agentUnavailable(db *gorm.DB, uid uint) (bool, error) {
	var user authmodel.User
	err := db.Select("id", "enable", "account_type").Where("id = ?", uid).Limit(1).Find(&user).Error
	if err != nil || user.ID == 0 || user.AccountType != authmodel.AccountTypeAgent {
		return false, err
	}
	if user.Enable != 1 {
		return true, nil
	}
	var definition model.AgentTemplate
	if err := db.Select("id", "is_active", "deleted_at").Where("auth_agent_user_id = ?", uid).
		Limit(1).Find(&definition).Error; err != nil || definition.ID == 0 {
		return false, err
	}
	return !definition.IsActive || definition.DeletedAt != nil, nil
}

func agentUnavailableClientMsgNo(window time.Time, channelType uint8, channelID string, agentUID uint) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s:%d:%s:%d", window.UTC().Format(time.RFC3339), channelType, channelID, agentUID))
	return "agent-unavailable-" + hex.EncodeToString(sum[:16])
}

// replyAgentUnavailable posts the notice as the agent, at most once per
// (channel, agent) per 10-minute window. The window is taken in the same
// transaction as the send, so a failed send releases it for the next message,
// and the deterministic client_msg_no makes a retried send land once.
// Failures are logged, never returned: a notice must not fail message intake.
func replyAgentUnavailable(ctx context.Context, db *gorm.DB, channelType uint8, channelID string, agentUID uint, now time.Time) {
	window := now.UTC().Truncate(agentUnavailableWindow)
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := takeWindow(tx, agentUnavailableScope, rateKey(channelType, channelID, agentUID), 1, window); err != nil {
			return err
		}
		payload, _ := json.Marshal(textPayload{Type: wuKongTextType, Content: agentUnavailableText, SystemNotice: agentUnavailableNotice})
		return postWuKong(ctx, wuKongAPIAddr, "/message/send", map[string]any{
			"from_uid":   fmt.Sprint(agentUID),
			"channel_id": channelID, "channel_type": channelType,
			"client_msg_no": agentUnavailableClientMsgNo(window, channelType, channelID, agentUID),
			"payload":       base64.StdEncoding.EncodeToString(payload),
		}, nil)
	})
	if err != nil && err != errAgentRateLimited {
		slog.Warn("im agent unavailable notice failed", "agent_uid", agentUID, "channel_type", channelType, "error", err)
	}
}

// replyUnresolvedAgents sends the notice for every addressed agent that
// resolveAgentTargets dropped because it is stopped or deleted. In a group the
// agent must still be a member, so a stray @uid cannot make any agent post there.
func replyUnresolvedAgents(
	ctx context.Context, db *gorm.DB, targets []uint, resolved []agentTarget,
	channelType uint8, channelID string, senderUID uint, now time.Time,
) {
	for _, uid := range targets {
		if uid == senderUID || slices.ContainsFunc(resolved, func(t agentTarget) bool { return t.agentUID == uid }) {
			continue
		}
		off, err := agentUnavailable(db, uid)
		if err == nil && off {
			// Another user's private agent answers like an unknown uid: silence.
			viewer := uint(0)
			if channelType == personChannel {
				viewer = senderUID
			}
			var hidden bool
			hidden, err = privateAgentHidden(db, uid, viewer)
			off = !hidden
		}
		if err == nil && off && channelType == groupChannel {
			off, err = isGroupMember(ctx, channelID, uid)
		}
		if err != nil {
			slog.Warn("im agent unavailable check failed", "agent_uid", uid, "error", err)
			continue
		}
		if off {
			replyAgentUnavailable(ctx, db, channelType, channelID, uid, now)
		}
	}
}
