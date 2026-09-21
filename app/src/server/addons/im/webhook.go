package im

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/danielgtaylor/huma/v2"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/nucleagent/nucleagent-shared/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	wuKongTextType = 1
	personChannel  = 1
	groupChannel   = 2
)

var errAgentRateLimited = errors.New("agent rate limit exceeded")

type webhookHeader struct {
	NoPersist uint8 `json:"no_persist"`
	RedDot    uint8 `json:"red_dot"`
	SyncOnce  uint8 `json:"sync_once"`
}

type webhookMessage struct {
	Header       *webhookHeader `json:"header,omitempty"`
	Setting      uint8          `json:"setting,omitempty"`
	Topic        string         `json:"topic,omitempty"`
	Expire       int64          `json:"expire,omitempty"`
	MessageID    int64          `json:"message_id,omitempty"`
	MessageSeq   int64          `json:"message_seq,omitempty"`
	MessageIDStr string         `json:"message_idstr"`
	ClientMsgNo  string         `json:"client_msg_no"`
	FromUID      string         `json:"from_uid"`
	ChannelID    string         `json:"channel_id"`
	ChannelType  uint8          `json:"channel_type"`
	Timestamp    int64          `json:"timestamp"`
	RawPayload   []byte         `json:"payload"`
}

type textPayload struct {
	Type    int    `json:"type"`
	Content string `json:"content"`
	Mention struct {
		All  int      `json:"all"`
		UIDs []string `json:"uids"`
	} `json:"mention"`
}

type webhookInput struct {
	Event   string `query:"event"`
	Body    []webhookMessage
	RawBody []byte
}

type webhookData struct {
	Accepted int `json:"accepted"`
}

type webhookOutput struct {
	Body envelope[webhookData]
}

type webhookProblem struct {
	huma.ErrorModel
	Code string `json:"code"`
}

type webhookAdmission struct {
	mu          sync.Mutex
	windowStart time.Time
	count       int
}

func (a *webhookAdmission) allow(now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	window := now.UTC().Truncate(time.Minute)
	if !window.Equal(a.windowStart) {
		a.windowStart, a.count = window, 0
	}
	if a.count >= 120 {
		return false
	}
	a.count++
	return true
}

func (p *Plugin) registerWebhook(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID:     "imWuKongWebhook",
		Method:          http.MethodPost,
		Path:            "/api/v1/im/webhooks/wukong",
		Summary:         "Receive WuKongIM message notifications",
		Tags:            []string{"IM"},
		MaxBodyBytes:    256 * 1024,
		BodyReadTimeout: 5 * time.Second,
	}, func(ctx context.Context, input *webhookInput) (*webhookOutput, error) {
		capability, _ := ctx.Value(webhookCapabilityKey).(string)
		if !capabilityMatches(capability, p.webhookCapability) {
			return nil, newWebhookProblem(http.StatusUnauthorized, "im_webhook_unauthorized", "invalid webhook capability")
		}
		if !p.webhookAdmission.allow(time.Now()) {
			return nil, newWebhookProblem(http.StatusTooManyRequests, "im_webhook_rate_limited", "webhook rate limit exceeded")
		}
		if input.Event != "msg.notify" {
			return nil, newWebhookProblem(http.StatusBadRequest, "im_webhook_event_unsupported", "unsupported webhook event")
		}
		// Temporary acceptance capture: authenticated raw bytes, never the capability URL.
		if os.Getenv("IM_WEBHOOK_DEBUG_BODY") == "1" {
			body := capabilityLogPattern.ReplaceAllString(string(input.RawBody), "${1}[REDACTED]")
			body = strings.ReplaceAll(body, p.webhookCapability, "[REDACTED]")
			slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})).
				Debug("im_webhook_debug_body", "event", input.Event, "body", body)
		}
		accepted, err := acceptWebhookBatch(ctx, global.PRISM_DB, input.Event, input.Body, time.Now())
		if errors.Is(err, errAgentRateLimited) {
			return nil, newWebhookProblem(http.StatusTooManyRequests, "im_agent_rate_limited", "agent rate limit exceeded")
		}
		if err != nil {
			return nil, newWebhookProblem(http.StatusServiceUnavailable, "im_inbox_unavailable", "webhook could not be recorded")
		}
		return &webhookOutput{Body: envelope[webhookData]{
			Code: 0, Message: "success", Data: webhookData{Accepted: accepted},
		}}, nil
	})
}

func capabilityMatches(got, want string) bool {
	gotHash := sha256.Sum256([]byte(got))
	wantHash := sha256.Sum256([]byte(want))
	return hmac.Equal(gotHash[:], wantHash[:])
}

func newWebhookProblem(status int, code, detail string) error {
	return &webhookProblem{
		ErrorModel: huma.ErrorModel{Status: status, Title: http.StatusText(status), Detail: detail},
		Code:       code,
	}
}

func acceptWebhookBatch(ctx context.Context, db *gorm.DB, event string, messages []webhookMessage, now time.Time) (int, error) {
	accepted := 0
	rateLimited := false
	for i := range messages {
		count, err := acceptWebhookMessage(db.WithContext(ctx), event, messages[i], now)
		accepted += count
		if errors.Is(err, errAgentRateLimited) {
			rateLimited = true
			continue
		}
		if err != nil {
			return accepted, err
		}
	}
	if rateLimited {
		return accepted, errAgentRateLimited
	}
	return accepted, nil
}

func acceptWebhookMessage(db *gorm.DB, event string, message webhookMessage, now time.Time) (int, error) {
	senderUID, err := strconv.ParseUint(strings.TrimSpace(message.FromUID), 10, 64)
	if err != nil || senderUID == 0 || strings.TrimSpace(message.MessageIDStr) == "" {
		return 0, nil
	}
	var sender authmodel.User
	if err := db.Select("id", "account_type", "enable").First(&sender, uint(senderUID)).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if sender.Enable != 1 || sender.AccountType == authmodel.AccountTypeAgent {
		return 0, nil
	}

	var payload textPayload
	if json.Unmarshal(message.RawPayload, &payload) != nil || payload.Type != wuKongTextType ||
		strings.TrimSpace(payload.Content) == "" {
		return 0, nil
	}

	channelID := strings.TrimSpace(message.ChannelID)
	bindings := make([]model.AgentInstance, 0, len(payload.Mention.UIDs))
	switch message.ChannelType {
	case personChannel:
		canonical, peer, ok := canonicalPersonChannel(channelID, uint(senderUID))
		if !ok {
			return 0, nil
		}
		channelID = canonical
		var binding model.AgentInstance
		err := db.Where("auth_agent_user_id = ? AND user_id = ? AND im_enabled = ?", peer, uint(senderUID), true).
			First(&binding).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		bindings = append(bindings, binding)
	case groupChannel:
		agentUIDs := parseUIDs(payload.Mention.UIDs)
		if len(agentUIDs) == 0 {
			return 0, nil
		}
		if err := db.Where("auth_agent_user_id IN ? AND im_enabled = ?", agentUIDs, true).Find(&bindings).Error; err != nil {
			return 0, err
		}
	default:
		return 0, nil
	}

	mentions, _ := json.Marshal(payload.Mention.UIDs)
	accepted, rateLimited := 0, false
	for i := range bindings {
		if bindings[i].AuthAgentUserID == nil {
			continue
		}
		agentUID := *bindings[i].AuthAgentUserID
		sourceKey := fmt.Sprintf("wukong:msg.notify:%s:%d", message.MessageIDStr, agentUID)
		row := IMWebhookInbox{
			Event: event, MessageIDStr: message.MessageIDStr, TargetAgentUID: agentUID,
			ClientMessageNo: message.ClientMsgNo, SenderUID: uint(senderUID), ChannelID: channelID,
			ChannelType: message.ChannelType, MessageTimestamp: message.Timestamp,
			Text: strings.TrimSpace(payload.Content), MentionUIDs: string(mentions), State: inboxPending,
			ExecutionOwnerUserID: bindings[i].UserID, SourceKey: sourceKey,
		}
		inserted := false
		err := db.Transaction(func(tx *gorm.DB) error {
			if message.ChannelType == groupChannel {
				allowed, err := groupInvocationAllowed(tx, channelID, agentUID, uint(senderUID))
				if err != nil {
					return err
				}
				if !allowed {
					return nil
				}
			}
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
			if result.Error != nil || result.RowsAffected == 0 {
				return result.Error
			}
			if err := takeRateLimit(tx, "sender", rateKey(uint(senderUID), agentUID, message.ChannelType, channelID), 5, now); err != nil {
				return err
			}
			if err := takeRateLimit(tx, "agent", rateKey(agentUID), 20, now); err != nil {
				return err
			}
			inserted = true
			return nil
		})
		if errors.Is(err, errAgentRateLimited) {
			rateLimited = true
			continue
		}
		if err != nil {
			return accepted, err
		}
		if inserted {
			accepted++
		}
	}
	if rateLimited {
		return accepted, errAgentRateLimited
	}
	return accepted, nil
}

func canonicalPersonChannel(channelID string, senderUID uint) (string, uint, bool) {
	parts := strings.Split(channelID, "@")
	if len(parts) != 2 {
		return "", 0, false
	}
	left, leftErr := strconv.ParseUint(parts[0], 10, 64)
	right, rightErr := strconv.ParseUint(parts[1], 10, 64)
	if leftErr != nil || rightErr != nil || left == 0 || right == 0 || left == right {
		return "", 0, false
	}
	ids := []uint64{left, right}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if senderUID != uint(left) && senderUID != uint(right) {
		return "", 0, false
	}
	peer := uint(left)
	if senderUID == uint(left) {
		peer = uint(right)
	}
	return fmt.Sprintf("%d@%d", ids[0], ids[1]), peer, true
}

func parseUIDs(raw []string) []uint {
	seen := map[uint]struct{}{}
	out := make([]uint, 0, len(raw))
	for _, value := range raw {
		uid, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		if err != nil || uid == 0 {
			continue
		}
		if _, ok := seen[uint(uid)]; ok {
			continue
		}
		seen[uint(uid)] = struct{}{}
		out = append(out, uint(uid))
	}
	return out
}

func groupInvocationAllowed(tx *gorm.DB, groupID string, agentUID, senderUID uint) (bool, error) {
	var group IMGroup
	if err := tx.Where("group_id = ?", groupID).First(&group).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if group.CreatorUID == senderUID {
		return true, nil
	}
	var count int64
	err := tx.Model(&IMGroupAgentAllowlist{}).
		Where("group_id = ? AND agent_uid = ? AND member_uid = ?", groupID, agentUID, senderUID).
		Count(&count).Error
	return count != 0, err
}

func takeRateLimit(tx *gorm.DB, scope, key string, limit int, now time.Time) error {
	window := now.UTC().Truncate(time.Minute)
	row := IMRateWindow{Scope: scope, WindowKey: key, WindowStart: window, Count: 0}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
		return err
	}
	var current IMRateWindow
	query := tx.Where("scope = ? AND window_key = ? AND window_start = ?", scope, key, window)
	if tx.Dialector.Name() != "sqlite" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.First(&current).Error; err != nil {
		return err
	}
	if current.Count >= limit {
		return errAgentRateLimited
	}
	return tx.Model(&current).UpdateColumn("count", gorm.Expr("count + 1")).Error
}

func rateKey(parts ...any) string {
	encoded, _ := json.Marshal(parts)
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}
