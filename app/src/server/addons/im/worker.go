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
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	authservice "github.com/kwhitestone/prism-fusion/addons/auth/service"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var retryDelays = []time.Duration{
	time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute,
}

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

type wuKongSyncResponse struct {
	Messages []struct {
		MessageIDStr string `json:"message_idstr"`
		MessageSeq   uint64 `json:"message_seq"`
		FromUID      string `json:"from_uid"`
		Setting      uint8  `json:"setting"`
		Payload      []byte `json:"payload"`
	} `json:"messages"`
}

func (p *Plugin) runWorker(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		row, err := leaseInbox(ctx, global.PRISM_DB, p.workerID(), time.Now())
		if err == nil && row != nil {
			p.processLeased(ctx, row)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *Plugin) workerID() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func leaseInbox(ctx context.Context, db *gorm.DB, worker string, now time.Time) (*IMWebhookInbox, error) {
	var leased IMWebhookInbox
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Where(
			"(state IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)) OR "+
				"(state = ? AND lease_expires_at < ?)",
			[]string{inboxPending, inboxRetryable}, now, inboxProcessing, now,
		).Order("id ASC")
		if tx.Dialector.Name() != "sqlite" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
		}
		if err := query.First(&leased).Error; err != nil {
			return err
		}
		expires := now.Add(30 * time.Second)
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
	leaseCtx, cancel := context.WithCancel(ctx)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case now := <-ticker.C:
				expires := now.Add(30 * time.Second)
				global.PRISM_DB.Model(&IMWebhookInbox{}).
					Where("id = ? AND state = ? AND lease_owner = ?", row.ID, inboxProcessing, row.LeaseOwner).
					Update("lease_expires_at", expires)
			}
		}
	}()
	err := p.dispatchAndStream(ctx, row)
	cancel()
	if err == nil {
		return
	}
	p.failInbox(row, err, time.Now())
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
		if err := global.PRISM_DB.Model(row).Update("last_core_event_id", cursor).Error; err != nil {
			return err
		}
		row.LastCoreEventID = cursor
	}
	history, err := p.initialHistory(ctx, row)
	if err != nil {
		return err
	}
	result, err := p.dispatchCore(ctx, row, history)
	if err != nil {
		return err
	}
	row.CoreConversationID = result.ConversationID
	now := time.Now()
	if err := global.PRISM_DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(row).Updates(map[string]any{
			"core_conversation_id": result.ConversationID,
			"lease_expires_at":     now.Add(30 * time.Second),
		}).Error; err != nil {
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
		return p.serviceJWT, nil
	}
	return (&authservice.JwtService{}).GenerateToken(0, "nucleagent-im", 0)
}

func (p *Plugin) dispatchCore(ctx context.Context, row *IMWebhookInbox, history []historyTurn) (*dispatchResult, error) {
	token, err := p.serviceToken()
	if err != nil {
		return nil, err
	}
	input := dispatchRequest{
		SourceKey: row.SourceKey, ExecutionOwnerUserID: row.ExecutionOwnerUserID,
		AgentUID: row.TargetAgentUID, Channel: dispatchChannel{ID: row.ChannelID, Type: int(row.ChannelType)},
		Input: row.Text, InitialHistory: history,
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
	var synced wuKongSyncResponse
	err := postWuKongWithAuth(ctx, p.apiAddr, "/channel/messagesync",
		p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
			"login_uid": strconv.FormatUint(uint64(row.SenderUID), 10), "channel_id": row.ChannelID,
			"channel_type": row.ChannelType, "limit": 30, "pull_mode": 1,
		}, &synced)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(synced.Messages, func(i, j int) bool {
		return synced.Messages[i].MessageSeq < synced.Messages[j].MessageSeq
	})
	if len(synced.Messages) > 30 {
		synced.Messages = synced.Messages[len(synced.Messages)-30:]
	}
	names := map[uint]string{}
	var users []authmodel.User
	if err := global.PRISM_DB.Select("id", "nick_name", "username").Find(&users).Error; err == nil {
		for i := range users {
			names[users[i].ID] = strings.TrimSpace(users[i].NickName)
			if names[users[i].ID] == "" {
				names[users[i].ID] = users[i].Username
			}
		}
	}
	candidates := make([]historyTurn, 0, len(synced.Messages))
	for i := range synced.Messages {
		message := synced.Messages[i]
		if message.MessageIDStr == row.MessageIDStr || message.Setting == 2 {
			continue
		}
		var payload textPayload
		if json.Unmarshal(message.Payload, &payload) != nil || payload.Type != wuKongTextType ||
			strings.TrimSpace(payload.Content) == "" {
			continue
		}
		uid64, _ := strconv.ParseUint(message.FromUID, 10, 64)
		role := "user"
		if uint(uid64) == row.TargetAgentUID {
			role = "assistant"
		}
		content := strings.TrimSpace(payload.Content)
		candidates = append(candidates, historyTurn{
			Role: role, SpeakerUID: message.FromUID, SpeakerName: names[uint(uid64)], Content: content,
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

func (p *Plugin) failInbox(row *IMWebhookInbox, err error, now time.Time) {
	code := "im_internal_error"
	retryable := true
	var upstream *httpStatusError
	if errors.As(err, &upstream) {
		code = upstream.code
		retryable = upstream.status == http.StatusTooManyRequests || upstream.status >= 500
	}
	updates := map[string]any{
		"state": inboxDead, "lease_owner": "", "lease_expires_at": nil,
		"next_attempt_at": nil, "last_error_code": code,
	}
	if retryable && row.Attempts <= len(retryDelays) {
		updates["state"] = inboxRetryable
		updates["next_attempt_at"] = now.Add(retryDelays[row.Attempts-1])
	}
	global.PRISM_DB.Model(&IMWebhookInbox{}).
		Where("id = ? AND lease_owner = ?", row.ID, row.LeaseOwner).Updates(updates)
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
		request.Header.Set("Authorization", "Bearer "+bearer)
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

func (p *Plugin) sendFinal(ctx context.Context, row *IMWebhookInbox, coreMessageID uint, answer string) error {
	var current IMWebhookInbox
	if err := global.PRISM_DB.Select(
		"state", "final_sent_at", "final_wu_kong_client_msg_no", "cumulative_answer", "revision",
	).
		First(&current, row.ID).Error; err != nil {
		return err
	}
	if current.State == inboxCompleted && current.FinalSentAt != nil {
		return nil
	}
	if row.FinalWuKongClientMsgNo == "" {
		row.FinalWuKongClientMsgNo = current.FinalWuKongClientMsgNo
	}
	row.CumulativeAnswer = current.CumulativeAnswer
	row.Revision = current.Revision
	if err := persistSnapshot(row, answer); err != nil {
		return err
	}
	clientNo := row.FinalWuKongClientMsgNo
	if clientNo == "" {
		clientNo = deterministicClientMessageNo(row.SourceKey, coreMessageID)
		if err := global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).Updates(map[string]any{
			"final_wu_kong_client_msg_no": clientNo, "final_core_message_id": coreMessageID,
			"cumulative_answer": answer,
		}).Error; err != nil {
			return err
		}
		row.FinalWuKongClientMsgNo = clientNo
	}
	// Stamp A2A provenance on the durable message so the NEXT webhook hop reads a real
	// chain id and depth instead of inferring one from history.
	payload, _ := json.Marshal(textPayload{
		Type: wuKongTextType, Content: answer,
		A2A: &a2aProvenance{
			ChainID: row.ChainID, Depth: row.ChainDepth,
			OriginUID: row.OriginSenderUID, ViaUID: row.TargetAgentUID,
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
	return global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).Updates(map[string]any{
		"state": inboxCompleted, "final_sent_at": now, "lease_owner": "", "lease_expires_at": nil,
		"next_attempt_at": nil, "last_error_code": "", "cumulative_answer": answer,
	}).Error
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
