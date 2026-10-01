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
	"regexp"
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

	// M3 A2A guards (coordinator-approved defaults).
	maxChainDepth      = 3
	chainHourlyBudget  = 20
	senderMinuteLimit  = 5
	agentMinuteCeiling = 20
)

var (
	errAgentRateLimited    = errors.New("agent rate limit exceeded")
	errA2ADepthExceeded    = errors.New("a2a chain depth exceeded")
	errA2ABudgetExceeded   = errors.New("a2a chain budget exceeded")
	agentMentionTextParser = regexp.MustCompile(`@([A-Za-z0-9_.\-]+)`)
)

// groupMemberResolver reports the WuKong subscribers of a group channel. Only the
// agent-sender path needs it, so human triggers keep the M2 database-only cost.
type groupMemberResolver func(ctx context.Context, channelID string) ([]uint, error)

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

type mentionPayload struct {
	All  int      `json:"all"`
	UIDs []string `json:"uids"`
}

type textPayload struct {
	Type    int            `json:"type"`
	Content string         `json:"content"`
	Mention mentionPayload `json:"mention,omitzero"`
	// A2A is the provenance nucleagent-im stamps on an agent's own durable message
	// (see sendFinal). WuKong stores the payload verbatim, so the next hop reads a
	// real chain depth instead of inferring one.
	A2A *a2aProvenance `json:"a2a,omitempty"`
}

func (m mentionPayload) IsZero() bool { return m.All == 0 && len(m.UIDs) == 0 }

type a2aProvenance struct {
	ChainID   string `json:"chainId"`
	Depth     int    `json:"depth"`
	OriginUID uint   `json:"originUid"`
	ViaUID    uint   `json:"viaUid"`
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
		accepted, err := acceptWebhookBatch(ctx, global.PRISM_DB, input.Event, input.Body, time.Now(), p.wuKongGroupMembers)
		if errors.Is(err, errA2ADepthExceeded) {
			return nil, newWebhookProblem(http.StatusTooManyRequests, "a2a_depth_exceeded",
				"agent-to-agent chain depth exceeded")
		}
		if errors.Is(err, errA2ABudgetExceeded) {
			return nil, newWebhookProblem(http.StatusTooManyRequests, "a2a_budget_exceeded",
				"agent-to-agent chain budget exceeded")
		}
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

func acceptWebhookBatch(
	ctx context.Context, db *gorm.DB, event string,
	messages []webhookMessage, now time.Time, members groupMemberResolver,
) (int, error) {
	accepted := 0
	var deferred error
	for i := range messages {
		count, err := acceptWebhookMessage(ctx, db.WithContext(ctx), event, messages[i], now, members)
		accepted += count
		switch {
		case errors.Is(err, errAgentRateLimited), errors.Is(err, errA2ADepthExceeded),
			errors.Is(err, errA2ABudgetExceeded):
			if deferred == nil {
				deferred = err
			}
		case err != nil:
			return accepted, err
		}
	}
	return accepted, deferred
}

// a2aOrigin carries the provenance the accepted rows inherit. For a human sender it
// starts a fresh chain at depth 1; for an agent sender it continues the triggering
// message's chain one hop deeper, keeping the original human as the authorizer.
type a2aOrigin struct {
	chainID     string
	depth       int
	originUID   uint
	sourceAgent uint
}

func acceptWebhookMessage(
	ctx context.Context, db *gorm.DB, event string,
	message webhookMessage, now time.Time, members groupMemberResolver,
) (int, error) {
	senderUID, err := strconv.ParseUint(strings.TrimSpace(message.FromUID), 10, 64)
	if err != nil || senderUID == 0 || strings.TrimSpace(message.MessageIDStr) == "" {
		return 0, nil
	}
	var sender authmodel.User
	if err := db.Select("id", "account_type", "enable", "username").First(&sender, uint(senderUID)).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if sender.Enable != 1 {
		return 0, nil
	}
	agentSender := sender.AccountType == authmodel.AccountTypeAgent
	// D16-A held for M2: every agent-sent message was ignored. M3 un-rejects exactly
	// one case — a group message from an agent — and only in the group branch below.
	if agentSender && message.ChannelType != groupChannel {
		return 0, nil
	}

	var payload textPayload
	if json.Unmarshal(message.RawPayload, &payload) != nil || payload.Type != wuKongTextType ||
		strings.TrimSpace(payload.Content) == "" {
		return 0, nil
	}

	channelID := strings.TrimSpace(message.ChannelID)
	origin := a2aOrigin{chainID: chainIDFor(message.MessageIDStr), depth: 1, originUID: uint(senderUID)}
	var targets []uint
	switch message.ChannelType {
	case personChannel:
		canonical, peer, ok := canonicalPersonChannel(channelID, uint(senderUID))
		if !ok {
			return 0, nil
		}
		channelID = canonical
		targets = []uint{peer}
	case groupChannel:
		if agentSender {
			// Only a message this bridge stamped is a tracked chain link. An agent
			// message without provenance was not produced by a dispatch we own, so it
			// is ignored exactly as in M2 rather than silently starting a new chain.
			if payload.A2A == nil || payload.A2A.ChainID == "" {
				return 0, nil
			}
			origin = a2aOrigin{
				chainID: payload.A2A.ChainID, depth: payload.A2A.Depth + 1,
				originUID: payload.A2A.OriginUID, sourceAgent: uint(senderUID),
			}
			if origin.depth > maxChainDepth {
				return 0, errA2ADepthExceeded
			}
			targets, err = agentMentionTargets(ctx, db, channelID, uint(senderUID), payload.Content, members)
			if err != nil {
				return 0, err
			}
		} else {
			targets = parseUIDs(payload.Mention.UIDs)
		}
		if len(targets) == 0 {
			return 0, nil
		}
	default:
		return 0, nil
	}

	var bindings []model.AgentInstance
	enabledAgents := db.Model(&authmodel.User{}).Select("id").
		Where("enable = ? AND account_type = ?", 1, authmodel.AccountTypeAgent)
	query := db.Where("auth_agent_user_id IN ? AND im_enabled = ?", targets, true).
		Where("auth_agent_user_id IN (?)", enabledAgents)
	if message.ChannelType == personChannel {
		// A direct channel only triggers the peer agent the sender itself owns.
		query = query.Where("user_id = ?", uint(senderUID))
	}
	if err := query.Find(&bindings).Error; err != nil {
		return 0, err
	}

	mentions, _ := json.Marshal(payload.Mention.UIDs)
	accepted := 0
	var deferred error
	for i := range bindings {
		if bindings[i].AuthAgentUserID == nil {
			continue
		}
		agentUID := *bindings[i].AuthAgentUserID
		if agentUID == uint(senderUID) {
			// An agent never re-triggers itself, whatever the text says.
			continue
		}
		sourceKey := fmt.Sprintf("wukong:msg.notify:%s:%d", message.MessageIDStr, agentUID)
		row := IMWebhookInbox{
			Event: event, MessageIDStr: message.MessageIDStr, TargetAgentUID: agentUID,
			ClientMessageNo: message.ClientMsgNo, SenderUID: uint(senderUID), ChannelID: channelID,
			ChannelType: message.ChannelType, MessageTimestamp: message.Timestamp,
			Text: strings.TrimSpace(payload.Content), MentionUIDs: string(mentions), State: inboxPending,
			ExecutionOwnerUserID: bindings[i].UserID, SourceKey: sourceKey,
			ChainID: origin.chainID, ChainDepth: origin.depth,
			OriginSenderUID: origin.originUID, SourceAgentUID: origin.sourceAgent,
		}
		inserted := false
		err := db.Transaction(func(tx *gorm.DB) error {
			if message.ChannelType == groupChannel {
				// Agent hops inherit the original human's authorization: the allowlist is
				// checked against the human who started the chain, not the sending agent
				// (which holds no invocation rights of its own).
				allowed, err := groupInvocationAllowed(tx, channelID, agentUID, origin.originUID)
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
			if err := takeChainBudget(tx, origin.chainID, now); err != nil {
				return err
			}
			if err := takeRateLimit(tx, "sender",
				rateKey(uint(senderUID), agentUID, message.ChannelType, channelID), senderMinuteLimit, now); err != nil {
				return err
			}
			if err := takeRateLimit(tx, "agent", rateKey(agentUID), agentMinuteCeiling, now); err != nil {
				return err
			}
			inserted = true
			return nil
		})
		switch {
		case errors.Is(err, errAgentRateLimited), errors.Is(err, errA2ABudgetExceeded):
			if deferred == nil {
				deferred = err
			}
			continue
		case err != nil:
			return accepted, err
		}
		if inserted {
			accepted++
		}
	}
	return accepted, deferred
}

// chainIDFor derives a stable chain identifier from the message that started a chain.
func chainIDFor(messageIDStr string) string { return "chain:" + messageIDStr }

// agentMentionTargets resolves the agents an AGENT-sent message addresses.
//
// COORDINATOR RULING 2026-09-22 (M3-PH1-C) — explicit, recorded AMENDMENT to M2
// invariant D9 (which chose D9-A: structured `mention.uids` only, and F.2: "text
// parsing alone must never decide invocation"). The amendment applies to AGENT
// SENDERS ONLY: an agent's durable message carries no mention metadata (sendFinal
// emits `{type,content}`), so the visible `@name` text MAY be parsed to resolve
// targets, restricted to:
//   - EXACT, case-sensitive username match (no fuzzy, no substring, no nickname),
//   - against ENABLED agents that are MEMBERS of the same group,
//   - all matches dispatched (D18-A).
//
// Human-sender messages keep the M2 rules UNCHANGED: structured mention.uids only.
func agentMentionTargets(
	ctx context.Context, db *gorm.DB, channelID string, senderUID uint,
	content string, members groupMemberResolver,
) ([]uint, error) {
	matches := agentMentionTextParser.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 || members == nil {
		return nil, nil
	}
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, match[1])
	}
	groupMembers, err := members(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if len(groupMembers) == 0 {
		return nil, nil
	}
	// Select the group's enabled agents and match usernames in Go. MySQL's default
	// collation is case-INsensitive, so a SQL `username IN (...)` would quietly break
	// the ruling's case-sensitivity requirement; group membership is bounded at
	// maxGroupMembers, so this stays one small query either way.
	var candidates []authmodel.User
	if err := db.Select("id", "username").
		Where("id IN ? AND enable = ? AND account_type = ?", groupMembers, 1, authmodel.AccountTypeAgent).
		Find(&candidates).Error; err != nil {
		return nil, err
	}
	targets := make([]uint, 0, len(candidates))
	for i := range candidates {
		if candidates[i].ID != senderUID && containsString(names, candidates[i].Username) {
			targets = appendUnique(targets, candidates[i].ID)
		}
	}
	return targets, nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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

// takeChainBudget is the per-origin-chain circuit breaker: 20 messages per chain per
// hour. It reuses the same SQL fixed-window table as the minute limits, only with an
// hour granule, so no new machinery is introduced.
func takeChainBudget(tx *gorm.DB, chainID string, now time.Time) error {
	if chainID == "" {
		return nil
	}
	err := takeWindow(tx, "chain", rateKey(chainID), chainHourlyBudget, now.UTC().Truncate(time.Hour))
	if errors.Is(err, errAgentRateLimited) {
		return errA2ABudgetExceeded
	}
	return err
}

func takeRateLimit(tx *gorm.DB, scope, key string, limit int, now time.Time) error {
	return takeWindow(tx, scope, key, limit, now.UTC().Truncate(time.Minute))
}

func takeWindow(tx *gorm.DB, scope, key string, limit int, window time.Time) error {
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
