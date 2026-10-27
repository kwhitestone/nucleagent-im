package im

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	authservice "github.com/kwhitestone/prism-fusion/addons/auth/service"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var retryDelays = []time.Duration{
	time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
}

const (
	conversationBusyCode  = "im_conversation_busy"
	busyTimeoutScope      = "busy_timeout_reply"
	busyTimeoutText       = "排队超时，请稍后重新 @"
	ownerNotFoundCode     = "im_execution_owner_not_found"
	ownerUnavailableScope = "owner_unavailable_reply"
	ownerUnavailableText  = "群主账号当前不可用，本群暂时不能调用智能体，请联系群主。"
)

type httpStatusError struct {
	status int
	code   string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("upstream returned HTTP %d (%s)", e.status, e.code)
}

type dispatchRequest struct {
	SourceKey            string          `json:"sourceKey"`
	ExecutionOwnerUserID uint            `json:"executionOwnerUserId"`
	AgentUID             uint            `json:"agentUid"`
	Channel              dispatchChannel `json:"channel"`
	Input                string          `json:"input"`
	InitialHistory       []historyTurn   `json:"initialHistory"`
	Provenance           *a2aProvenance  `json:"provenance,omitempty"`
	AdmittedAt           time.Time       `json:"admittedAt"`
}

type dispatchChannel struct {
	ID   string `json:"id"`
	Type int    `json:"type"`
}

type dispatchResult struct {
	ConversationID uint   `json:"conversationId"`
	SourceKey      string `json:"sourceKey"`
	Status         string `json:"status"`
}

type historyTurn struct {
	Role        string `json:"role"`
	SpeakerUID  string `json:"speakerUid"`
	SpeakerName string `json:"speakerName"`
	Content     string `json:"content"`
}

// inboxWorkerSlots bounds how many turns one IM instance runs at once. A long
// turn (or a delegated turn waiting for its continuation) used to hold the only
// worker, so every other channel on the instance queued behind it (MUYU-160).
var inboxWorkerSlots = 8

// inboxLeaseDuration is how long a lease holds without renewal; the holder renews
// every inboxLeaseRenewInterval. Another instance may take a turn over only after
// its lease expired.
var (
	inboxLeaseDuration      = 30 * time.Second
	inboxLeaseRenewInterval = 10 * time.Second
)

// errLeaseLost: this instance no longer holds the turn's lease (another instance
// took it over, or the holder could not confirm it in time). The turn must stop
// without writing, sending or ending anything (MUYU-160 review 7).
var errLeaseLost = errors.New("im turn lease lost")

// leaseScope selects the row only while this holder still owns its lease. Every
// write a turn makes goes through it, so a holder that was paused past its lease
// and came back cannot touch a turn another instance now runs. attempts is the
// lease generation: every lease, takeover included, increments it, so even a
// re-lease by the same host:pid fences off the earlier holder.
func leaseScope(db *gorm.DB, row *IMWebhookInbox) *gorm.DB {
	return db.Model(&IMWebhookInbox{}).
		Where("id = ? AND state = ? AND lease_owner = ? AND attempts = ?", row.ID, inboxProcessing, row.LeaseOwner, row.Attempts)
}

// verifyLease reports errLeaseLost unless this holder still owns the row.
func verifyLease(db *gorm.DB, row *IMWebhookInbox) error {
	var held int64
	if err := leaseScope(db, row).Count(&held).Error; err != nil {
		return err
	}
	if held == 0 {
		return errLeaseLost
	}
	return nil
}

// updateLeased writes to the row only under the holder's lease. Zero affected rows
// is re-checked, because MySQL reports a write of unchanged values as 0 rows.
func updateLeased(db *gorm.DB, row *IMWebhookInbox, updates map[string]any) error {
	result := leaseScope(db, row).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return verifyLease(db, row)
	}
	return nil
}

// renewLease extends the holder's lease from now.
func renewLease(row *IMWebhookInbox, now time.Time) error {
	return updateLeased(global.PRISM_DB, row, map[string]any{"lease_expires_at": now.Add(inboxLeaseDuration)})
}

// inboxTurnKey is the per-conversation serial lock: one IM conversation (channel,
// agent, owner) never has two turns in flight on this instance, so its turns keep
// their order while other conversations proceed in parallel.
type inboxTurnKey struct {
	channelID   string
	channelType uint8
	agentUID    uint
	ownerUID    uint
}

func turnKeyOf(row *IMWebhookInbox) inboxTurnKey {
	return inboxTurnKey{row.ChannelID, row.ChannelType, row.TargetAgentUID, row.ExecutionOwnerUserID}
}

func (p *Plugin) runWorker(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var (
		mu       sync.Mutex
		inFlight = map[inboxTurnKey]bool{}
		turns    sync.WaitGroup
	)
	slots := make(chan struct{}, max(inboxWorkerSlots, 1))
	defer turns.Wait() // shutdown lets running turns finish their own cancellation
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		mu.Lock()
		busy := make([]inboxTurnKey, 0, len(inFlight))
		for key := range inFlight {
			busy = append(busy, key)
		}
		mu.Unlock()
		row, err := leaseInboxExcluding(ctx, global.PRISM_DB, p.workerID(), time.Now(), busy)
		if err != nil || row == nil {
			<-slots
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		key := turnKeyOf(row)
		mu.Lock()
		inFlight[key] = true
		mu.Unlock()
		turns.Add(1)
		go func() {
			defer turns.Done()
			defer func() { <-slots }()
			defer func() { mu.Lock(); delete(inFlight, key); mu.Unlock() }()
			p.processLeased(ctx, row)
		}()
	}
}

func (p *Plugin) workerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func leaseInbox(ctx context.Context, db *gorm.DB, worker string, now time.Time) (*IMWebhookInbox, error) {
	return leaseInboxExcluding(ctx, db, worker, now, nil)
}

// leaseInboxExcluding leases the oldest due row whose conversation has no turn
// in flight on this instance, so a busy conversation never blocks the others.
func leaseInboxExcluding(ctx context.Context, db *gorm.DB, worker string, now time.Time, busy []inboxTurnKey) (*IMWebhookInbox, error) {
	var leased IMWebhookInbox
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Where(
			"(state IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR "+
				"(state = ? AND lease_expires_at < ?)",
			[]string{inboxPending, inboxRetryable}, now, inboxProcessing, now,
		)
		for _, key := range busy {
			query = query.Where("NOT (channel_id = ? AND channel_type = ? AND target_agent_uid = ? AND execution_owner_user_id = ?)",
				key.channelID, key.channelType, key.agentUID, key.ownerUID)
		}
		// A conversation's turns run in order on every instance: a turn is never
		// leased while an earlier turn of the same conversation is still open
		// (pending, waiting on a retry, or processing anywhere). This also keeps a
		// later turn from overtaking an earlier one that is in busy-retry backoff.
		query = query.Where("NOT EXISTS (SELECT 1 FROM im_webhook_inbox AS earlier WHERE "+
			"earlier.channel_id = im_webhook_inbox.channel_id AND earlier.channel_type = im_webhook_inbox.channel_type AND "+
			"earlier.target_agent_uid = im_webhook_inbox.target_agent_uid AND "+
			"earlier.execution_owner_user_id = im_webhook_inbox.execution_owner_user_id AND "+
			"earlier.id < im_webhook_inbox.id AND earlier.state IN ?)",
			[]string{inboxPending, inboxRetryable, inboxProcessing})
		query = query.Order("id ASC")
		if tx.Dialector.Name() != "sqlite" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
		}
		if err := query.First(&leased).Error; err != nil {
			return err
		}
		expires := now.Add(inboxLeaseDuration)
		return tx.Model(&leased).Updates(map[string]any{
			"state": inboxProcessing, "lease_owner": worker, "lease_expires_at": expires,
			"attempts": gorm.Expr("attempts + 1"), "next_attempt_at": nil, "last_error_code": "",
		}).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	leased.State = inboxProcessing
	leased.LeaseOwner = worker
	leased.Attempts++
	return &leased, nil
}

func (p *Plugin) processLeased(ctx context.Context, row *IMWebhookInbox) {
	turnCtx, stopTurn := context.WithCancelCause(ctx)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		keepLease(turnCtx, row, stopTurn)
	}()
	var err error
	if row.CoreStopCode != "" {
		// A turn IM already gave up on: only its owed Core stop remains.
		err = p.giveUpTurn(turnCtx, row, row.CoreStopCode, row.CoreStopNotice)
	} else {
		err = p.dispatchAndStream(turnCtx, row)
	}
	lost := errors.Is(err, errLeaseLost) || errors.Is(context.Cause(turnCtx), errLeaseLost)
	stopTurn(nil)
	if errors.Is(err, errTurnStopPending) {
		return // parked for the next stop attempt
	}
	if lost {
		// Another instance owns the turn now; it alone may end, retry or answer it.
		slog.Warn("im turn lease lost; leaving the turn to its new holder",
			"inbox_id", row.ID, "lease_owner", row.LeaseOwner)
		return
	}
	if err == nil {
		return
	}
	p.failInbox(row, err, time.Now())
}

// keepLease renews the turn's lease and stops the turn as soon as the lease is
// lost, or cannot be confirmed for so long that it may have expired: a paused
// holder must not keep running a turn another instance may already have taken.
func keepLease(ctx context.Context, row *IMWebhookInbox, stopTurn context.CancelCauseFunc) {
	ticker := time.NewTicker(inboxLeaseRenewInterval)
	defer ticker.Stop()
	renewed := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			err := renewLease(row, now)
			switch {
			case err == nil:
				renewed = now
			case errors.Is(err, errLeaseLost), now.Sub(renewed) >= inboxLeaseDuration-inboxLeaseRenewInterval:
				stopTurn(errLeaseLost)
				return
			}
		}
	}
}

func (p *Plugin) dispatchAndStream(ctx context.Context, row *IMWebhookInbox) error {
	if row.LastCoreEventID == "" {
		// Seed before publishing, never from a later turn or on a retry. The
		// stream still waits for this sourceKey before accepting any answer.
		var previous IMWebhookInbox
		if err := global.PRISM_DB.Select("last_core_event_id").
			Where("id < ? AND channel_id = ? AND channel_type = ? AND target_agent_uid = ? AND execution_owner_user_id = ? AND state = ?",
				row.ID, row.ChannelID, row.ChannelType, row.TargetAgentUID, row.ExecutionOwnerUserID, inboxCompleted).
			Order("id DESC").Limit(1).Find(&previous).Error; err != nil {
			return err
		}
		cursor := previous.LastCoreEventID
		if cursor == "" {
			cursor = "0"
		}
		if err := updateLeased(global.PRISM_DB, row, map[string]any{"last_core_event_id": cursor}); err != nil {
			return err
		}
		row.LastCoreEventID = cursor
	}
	history, err := p.initialHistory(ctx, row)
	if err != nil {
		return err
	}
	// The lease is re-checked right before Core is asked to run: a holder that
	// lost it must not start (or restart) Core work for a turn it no longer owns.
	if err := verifyLease(global.PRISM_DB, row); err != nil {
		return err
	}
	result, err := p.dispatchCore(ctx, row, history)
	if err != nil {
		return err
	}
	row.CoreConversationID = result.ConversationID
	now := time.Now()
	if err := global.PRISM_DB.Transaction(func(tx *gorm.DB) error {
		if err := updateLeased(tx, row, map[string]any{
			"core_conversation_id": result.ConversationID,
			"lease_expires_at":     now.Add(inboxLeaseDuration),
		}); err != nil {
			return err
		}
		mapping := IMCoreConversationMap{
			ChannelID: row.ChannelID, ChannelType: row.ChannelType, AgentUID: row.TargetAgentUID,
			ExecutionOwnerUserID: row.ExecutionOwnerUserID, CoreConversationID: result.ConversationID,
			ImportedAt: now, LatestSourceMessageID: row.MessageIDStr,
		}
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "channel_id"}, {Name: "channel_type"}, {Name: "agent_uid"}, {Name: "execution_owner_user_id"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"core_conversation_id":     result.ConversationID,
				"latest_source_message_id": row.MessageIDStr,
				"updated_at":               now,
			}),
		}).Create(&mapping).Error
	}); err != nil {
		return err
	}
	return p.consumeCoreStream(ctx, row)
}

func (p *Plugin) serviceToken() (string, error) {
	if p.serviceJWT != "" {
		return bareToken(p.serviceJWT), nil
	}
	return (&authservice.JwtService{}).GenerateToken(0, "nucleagent-im", 0)
}

func (p *Plugin) dispatchCore(ctx context.Context, row *IMWebhookInbox, history []historyTurn) (*dispatchResult, error) {
	if err := p.checkDelivery(ctx, row); err != nil {
		return nil, err
	}
	token, err := p.serviceToken()
	if err != nil {
		return nil, err
	}
	text := row.Text
	if row.ChannelType != personChannel {
		// G3: a shared group conversation must know who is speaking. Same "[name]: " form
		// Core already uses for group history; the inbox keeps the raw text.
		text = "[" + userLabel(ctx, row.SenderUID) + "]: " + text
	}
	input := dispatchRequest{
		SourceKey: row.SourceKey, ExecutionOwnerUserID: row.ExecutionOwnerUserID,
		AgentUID: row.TargetAgentUID, Channel: dispatchChannel{ID: row.ChannelID, Type: int(row.ChannelType)},
		Input: text, InitialHistory: history,
		AdmittedAt: row.CreatedAt.UTC(),
		Provenance: &a2aProvenance{
			ChainID: row.ChainID, Depth: row.ChainDepth,
			OriginUID: row.OriginSenderUID, ViaUID: row.SourceAgentUID,
		},
	}
	var result dispatchResult
	if err := doJSON(ctx, http.MethodPost, p.coreURL+"/api/v1/addons/conversation/im-dispatch", token, input, &result); err != nil {
		return nil, err
	}
	if result.ConversationID == 0 || result.SourceKey != row.SourceKey || result.Status != "accepted" {
		return nil, errors.New("core returned an invalid IM dispatch response")
	}
	return &result, nil
}

func (p *Plugin) initialHistory(ctx context.Context, row *IMWebhookInbox) ([]historyTurn, error) {
	var count int64
	if err := global.PRISM_DB.Model(&IMCoreConversationMap{}).
		Where("channel_id = ? AND channel_type = ? AND agent_uid = ? AND execution_owner_user_id = ?",
			row.ChannelID, row.ChannelType, row.TargetAgentUID, row.ExecutionOwnerUserID).
		Count(&count).Error; err != nil || count != 0 {
		return nil, err
	}
	// W4: the starting context reads MySQL (the latest 30 of the channel), not WuKong.
	recent, _, err := channelMessages(global.PRISM_DB.WithContext(ctx), row.ChannelType, row.ChannelID, 0, 0, 0, 30)
	if err != nil {
		return nil, err
	}
	names := map[uint]string{}
	var users []authmodel.User
	if err := global.PRISM_DB.Select("id", "nick_name", "username").Find(&users).Error; err == nil {
		for i := range users {
			names[users[i].ID] = speakerLabel(displayName(users[i]), users[i].ID)
		}
	}
	candidates := make([]historyTurn, 0, len(recent))
	for _, message := range recent {
		if message.MessageIDStr == row.MessageIDStr || message.Setting == 2 {
			continue
		}
		var payload textPayload
		if json.Unmarshal([]byte(message.Payload), &payload) != nil || payload.Type != wuKongTextType ||
			strings.TrimSpace(payload.Content) == "" || payload.SystemNotice != "" {
			continue
		}
		role := "user"
		if message.FromUID == row.TargetAgentUID {
			role = "assistant"
		}
		candidates = append(candidates, historyTurn{
			Role: role, SpeakerUID: strconv.FormatUint(uint64(message.FromUID), 10),
			SpeakerName: names[message.FromUID], Content: strings.TrimSpace(payload.Content),
		})
	}
	turns := make([]historyTurn, 0, len(candidates))
	total := 0
	for i := len(candidates) - 1; i >= 0; i-- {
		if total+len(candidates[i].Content) > 64*1024 {
			continue
		}
		total += len(candidates[i].Content)
		turns = append(turns, candidates[i])
	}
	for left, right := 0, len(turns)-1; left < right; left, right = left+1, right-1 {
		turns[left], turns[right] = turns[right], turns[left]
	}
	return turns, nil
}

func displayName(user authmodel.User) string {
	if name := strings.TrimSpace(user.NickName); name != "" {
		return name
	}
	return user.Username
}

// userLabel is the cleaned speaker label for uid; the uid itself when the user is unknown.
func userLabel(ctx context.Context, uid uint) string {
	var user authmodel.User
	if err := global.PRISM_DB.WithContext(ctx).Select("id", "nick_name", "username").
		Where("id = ?", uid).Limit(1).Find(&user).Error; err != nil || user.ID == 0 {
		return speakerLabel("", uid)
	}
	return speakerLabel(displayName(user), uid)
}

// speakerLabel makes a nickname safe inside "[name]: ": one line, no brackets or colons
// (C14: a nickname must not fake another speaker or a role), at most 32 runes.
func speakerLabel(name string, uid uint) string {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsSpace(r):
			return ' '
		case unicode.IsControl(r), r == '[', r == ']', r == ':', r == '：':
			return -1
		}
		return r
	}, name)
	runes := []rune(strings.Join(strings.Fields(cleaned), " "))
	if len(runes) > 32 {
		runes = []rune(strings.TrimSpace(string(runes[:32])))
	}
	if len(runes) == 0 {
		return strconv.FormatUint(uint64(uid), 10)
	}
	return string(runes)
}

func (p *Plugin) failInbox(row *IMWebhookInbox, err error, now time.Time) {
	code := "im_internal_error"
	retryable := true
	var upstream *httpStatusError
	if errors.As(err, &upstream) {
		code = upstream.code
		retryable = upstream.status == http.StatusTooManyRequests || upstream.status >= 500 ||
			upstream.code == conversationBusyCode // G2: wait in line behind the running turn
	}
	updates := map[string]any{
		"state": inboxDead, "lease_owner": "", "lease_expires_at": nil,
		"next_attempt_at": nil, "last_error_code": code,
	}
	if retryable && row.Attempts <= len(retryDelays) {
		updates["state"] = inboxRetryable
		updates["next_attempt_at"] = now.Add(retryDelays[row.Attempts-1])
	}
	// MUYU-160: a turn that ran out of retries on an infrastructure failure (Core
	// stream dropped or idle, 5xx) used to go dead with no reply at all.
	// A stopped or deleted agent keeps its own notice below. The client number is
	// recorded only when this notice is really sent: the browser hands its live
	// error line over to the durable message that carries it.
	exhausted := retryable && updates["state"] == inboxDead && code != conversationBusyCode
	if exhausted {
		if off, err := agentUnavailable(global.PRISM_DB, row.TargetAgentUID); err == nil && off {
			exhausted = false
		}
	}
	exhaustedClientNo := ""
	if exhausted {
		// Core message IDs start at 1, so 0 never collides with a reply's number.
		exhaustedClientNo = deterministicClientMessageNo(row.SourceKey, 0)
		updates["final_wu_kong_client_msg_no"] = exhaustedClientNo
		// Owed in the same write that ends the turn, so a failed send is retried.
		updates["notice_due_at"] = now
		updates["notice_attempts"] = 0
	}
	if updates["state"] == inboxDead && retryable && code != conversationBusyCode && row.CoreConversationID != 0 {
		// IM gives up on a turn Core accepted and may still be running (its stream
		// kept dropping). The turn ends only once Core confirms the run is stopped;
		// until then the stop is a durable debt retried on later leases, so the
		// next question is never sent into a conversation Core still holds busy
		// (MUYU-160 review 8). A claimed answer is delivered instead of failing.
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := p.giveUpTurn(stopCtx, row, code, exhausted)
		switch {
		case err == nil:
			if !exhausted && row.State == inboxDead {
				if off, err := agentUnavailable(global.PRISM_DB, row.TargetAgentUID); err == nil && off {
					replyAgentUnavailable(context.Background(), global.PRISM_DB, row.ChannelType, row.ChannelID, row.TargetAgentUID, now)
				}
			}
		case errors.Is(err, errTurnStopPending), errors.Is(err, errLeaseLost):
		default:
			slog.Warn("im give-up of a turn failed; it stays leased until its lease expires",
				"inbox_id", row.ID, "error", err)
		}
		return
	}
	result := leaseScope(global.PRISM_DB, row).Updates(updates)
	// Only the call that moved this lease's row posts anything; a stale holder,
	// whose turn another instance took over, ends here without a notice.
	if result.Error != nil || result.RowsAffected != 1 {
		return
	}
	if exhausted {
		row.FinalWuKongClientMsgNo = exhaustedClientNo
		p.deliverNotice(context.Background(), row, now)
		return
	}
	if updates["state"] == inboxDead && code == conversationBusyCode {
		// ponytail: retry order is not first-come-first-served; a per-(group,agent,owner) lease is P1.
		replyNotice(context.Background(), global.PRISM_DB, busyTimeoutScope, busyTimeoutText, row.ChannelType, row.ChannelID, row.TargetAgentUID, now)
		return
	}
	if updates["state"] == inboxDead && code == ownerNotFoundCode && row.ChannelType != personChannel {
		// G4: Core refused before any model call (zero cost); tell the group why.
		replyNotice(context.Background(), global.PRISM_DB, ownerUnavailableScope, ownerUnavailableText, row.ChannelType, row.ChannelID, row.TargetAgentUID, now)
		return
	}
	// Dead because the agent was stopped or deleted after admission: say so once.
	if updates["state"] == inboxDead {
		if off, err := agentUnavailable(global.PRISM_DB, row.TargetAgentUID); err == nil && off {
			replyAgentUnavailable(context.Background(), global.PRISM_DB, row.ChannelType, row.ChannelID, row.TargetAgentUID, now)
		}
	}
}

// bareToken accepts a token with or without the `Bearer ` scheme and returns it bare.
func bareToken(value string) string {
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(value), "Bearer "))
}

func doJSON(ctx context.Context, method, url, bearer string, input, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		request.Header.Set("Authorization", bearer) // A-16: bare; PROD gateway rejects Bearer
	}
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem struct {
			Code string `json:"code"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&problem)
		if problem.Code == "" {
			problem.Code = "upstream_http_" + strconv.Itoa(response.StatusCode)
		}
		return &httpStatusError{status: response.StatusCode, code: problem.Code}
	}
	if output == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	return json.NewDecoder(io.LimitReader(response.Body, 2*1024*1024)).Decode(output)
}

func deterministicClientMessageNo(sourceKey string, coreMessageID uint) string {
	sum := sha256.Sum256([]byte(sourceKey + ":" + strconv.FormatUint(uint64(coreMessageID), 10)))
	return "im-" + hex.EncodeToString(sum[:16])
}

// sendFinal delivers the turn's answer. A turn has exactly one outcome
// (MUYU-160 review 8): the answer is first CLAIMED on the row under the
// holder's lease (final_core_message_id, client number and text, written once
// and never changed), and only then sent. Every failure path of a turn refuses
// to end a turn whose answer is claimed and delivers that answer instead. So a
// holder paused between its last lease check and the send can only ever post
// the claimed answer with the claimed client number — the same message the new
// holder posts — which WuKong keeps once; it can never contradict a failure
// notice, because no failure notice is owed for a turn with a claimed answer.
// coreMessageID 0 means "deliver the answer an earlier holder claimed".
func (p *Plugin) sendFinal(ctx context.Context, row *IMWebhookInbox, coreMessageID uint, answer string) error {
	var current IMWebhookInbox
	if err := global.PRISM_DB.Select(
		"state", "final_sent_at", "final_wu_kong_client_msg_no", "final_core_message_id", "cumulative_answer", "revision",
	).
		First(&current, row.ID).Error; err != nil {
		return err
	}
	if current.State == inboxCompleted && current.FinalSentAt != nil {
		return nil
	}
	if err := p.checkDelivery(ctx, row); err != nil {
		return err
	}
	if err := verifyLease(global.PRISM_DB, row); err != nil {
		return err
	}
	row.CumulativeAnswer = current.CumulativeAnswer
	row.Revision = current.Revision
	if current.FinalCoreMessageID == 0 || current.FinalWuKongClientMsgNo == "" {
		if coreMessageID == 0 {
			return errors.New("no claimed answer to deliver")
		}
		clientNo := deterministicClientMessageNo(row.SourceKey, coreMessageID)
		result := leaseScope(global.PRISM_DB, row).
			Where("final_core_message_id IS NULL OR final_core_message_id = 0").
			Updates(map[string]any{
				"final_wu_kong_client_msg_no": clientNo, "final_core_message_id": coreMessageID,
				"cumulative_answer": answer, "revision": gorm.Expr("revision + 1"),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			if err := verifyLease(global.PRISM_DB, row); err != nil {
				return err
			}
			if err := global.PRISM_DB.Select("final_wu_kong_client_msg_no", "final_core_message_id", "cumulative_answer").
				First(&current, row.ID).Error; err != nil {
				return err
			}
		} else {
			current.FinalWuKongClientMsgNo, current.FinalCoreMessageID, current.CumulativeAnswer = clientNo, coreMessageID, answer
			row.Revision++
		}
	}
	// The claimed answer is what is delivered, whoever claimed it.
	clientNo, answer := current.FinalWuKongClientMsgNo, current.CumulativeAnswer
	row.FinalWuKongClientMsgNo, row.CumulativeAnswer = clientNo, answer
	// Last ownership check before the answer leaves.
	if err := verifyLease(global.PRISM_DB, row); err != nil {
		return err
	}
	// Stamp A2A provenance on the durable message so the NEXT webhook hop reads a real
	// chain id and depth instead of inferring one from history. ViaUID names the agent
	// that triggered this answer (zero when a human did), which is what the client
	// renders as "via @agentA".
	payload, _ := json.Marshal(textPayload{
		Type: wuKongTextType, Content: answer,
		A2A: &a2aProvenance{
			ChainID: row.ChainID, Depth: row.ChainDepth,
			OriginUID: row.OriginSenderUID, ViaUID: row.SourceAgentUID,
		},
	})
	if err := postWuKong(ctx, p.apiAddr, "/message/send", map[string]any{
		"from_uid":   strconv.FormatUint(uint64(row.TargetAgentUID), 10),
		"channel_id": row.ChannelID, "channel_type": row.ChannelType, "client_msg_no": clientNo,
		"payload": base64.StdEncoding.EncodeToString(payload),
	}, nil); err != nil {
		return err
	}
	now := time.Now()
	if err := updateLeased(global.PRISM_DB, row, map[string]any{
		"state": inboxCompleted, "final_sent_at": now, "lease_owner": "", "lease_expires_at": nil,
		"next_attempt_at": nil, "last_error_code": "", "core_stop_code": "",
	}); err != nil {
		return err
	}
	row.State = inboxCompleted
	return nil
}

func scanSSE(response io.Reader, handle func(event, id string, data []byte) (bool, error)) error {
	scanner := bufio.NewScanner(response)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	var event, id string
	var data []string
	flush := func() (bool, error) {
		if event == "" && id == "" && len(data) == 0 {
			return false, nil
		}
		stop, err := handle(event, id, []byte(strings.Join(data, "\n")))
		event, id, data = "", "", nil
		return stop, err
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			stop, err := flush()
			if err != nil || stop {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if ok {
			value = strings.TrimPrefix(value, " ")
		}
		switch name {
		case "event":
			event = value
		case "id":
			id = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	_, err := flush()
	return err
}
