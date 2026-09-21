package im

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
)

type coreMessage struct {
	ID         uint   `json:"id"`
	SenderType string `json:"senderType"`
	MsgType    string `json:"msgType"`
	Content    string `json:"content"`
}

func (p *Plugin) consumeCoreStream(ctx context.Context, row *IMWebhookInbox) error {
	token, err := p.serviceToken()
	if err != nil {
		return err
	}
	streamURL := fmt.Sprintf("%s/api/v1/addons/conversation/%d/messages/stream?executionOwnerUserId=%d",
		p.coreURL, row.CoreConversationID, row.ExecutionOwnerUserID)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Authorization", "Bearer "+token)
	if row.LastCoreEventID != "" {
		request.Header.Set("Last-Event-ID", row.LastCoreEventID)
	}
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		return &httpStatusError{status: response.StatusCode, code: "core_stream_http_" + strconv.Itoa(response.StatusCode)}
	}
	completed := false
	err = scanSSE(response.Body, func(event, id string, data []byte) (bool, error) {
		if event != "message-created" && event != "message-updated" {
			return false, persistCoreEventID(row, id)
		}
		var message coreMessage
		if json.Unmarshal(data, &message) != nil {
			return false, persistCoreEventID(row, id)
		}
		if message.MsgType == "error" {
			if err := markTerminalFailure(row.ID, "im_execution_failed"); err != nil {
				return false, err
			}
			if err := persistCoreEventID(row, id); err != nil {
				return false, err
			}
			completed = true
			return true, nil
		}
		if message.SenderType != "agent" {
			return false, persistCoreEventID(row, id)
		}
		switch {
		case event == "message-updated" && message.MsgType == "streaming":
			if err := persistSnapshot(row, message.Content); err != nil {
				return false, err
			}
		case event == "message-created" && (message.MsgType == "text" || message.MsgType == "result") &&
			strings.TrimSpace(message.Content) != "":
			if err := p.sendFinal(ctx, row, message.ID, message.Content); err != nil {
				return false, err
			}
			if err := persistCoreEventID(row, id); err != nil {
				return false, err
			}
			completed = true
			return true, nil
		}
		return false, persistCoreEventID(row, id)
	})
	if err != nil {
		return err
	}
	if !completed {
		return errors.New("core stream ended before a terminal event")
	}
	return nil
}

func persistCoreEventID(row *IMWebhookInbox, id string) error {
	if id == "" {
		return nil
	}
	next, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid Core SSE event ID %q", id)
	}
	current, _ := strconv.ParseUint(row.LastCoreEventID, 10, 64)
	if next <= current {
		return nil
	}
	if err := global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).
		Update("last_core_event_id", id).Error; err != nil {
		return err
	}
	row.LastCoreEventID = id
	return nil
}

func persistSnapshot(row *IMWebhookInbox, answer string) error {
	if answer == row.CumulativeAnswer {
		return nil
	}
	if err := global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).Updates(map[string]any{
		"cumulative_answer": answer, "revision": gorm.Expr("revision + 1"),
	}).Error; err != nil {
		return err
	}
	row.CumulativeAnswer = answer
	row.Revision++
	return nil
}

func markTerminalFailure(id uint, code string) error {
	return global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", id).Updates(map[string]any{
		"state": inboxDead, "last_error_code": code, "lease_owner": "",
		"lease_expires_at": nil, "next_attempt_at": nil,
	}).Error
}

type agentStreamInput struct {
	ChannelID   string `query:"channel_id"`
	ChannelType uint8  `query:"channel_type"`
}

func (p *Plugin) registerAgentStreams(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "imAgentStreams",
		Method:      http.MethodGet,
		Path:        "/api/v1/im/agent-streams",
		Summary:     "Stream agent response snapshots for an IM channel",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, input *agentStreamInput) (*huma.StreamResponse, error) {
		userID, _ := ctx.Value(userIDKey).(uint)
		if userID == 0 || strings.TrimSpace(input.ChannelID) == "" {
			return nil, newIMProblem(http.StatusBadRequest, "invalid_channel", "invalid channel")
		}
		allowed, err := p.channelParticipant(ctx, userID, input.ChannelID, input.ChannelType)
		if err != nil {
			return nil, newIMProblem(http.StatusServiceUnavailable, "wukong_unavailable", "channel membership is unavailable")
		}
		if !allowed {
			return nil, newIMProblem(http.StatusForbidden, "channel_forbidden", "channel membership is required")
		}
		channelID := input.ChannelID
		channelType := input.ChannelType
		return &huma.StreamResponse{Body: func(hctx huma.Context) {
			p.serveAgentStream(hctx, channelID, channelType)
		}}, nil
	})
}

func (p *Plugin) channelParticipant(ctx context.Context, userID uint, channelID string, channelType uint8) (bool, error) {
	switch channelType {
	case personChannel:
		_, _, ok := canonicalPersonChannel(channelID, userID)
		return ok, nil
	case groupChannel:
		return p.wuKongGroupMember(ctx, channelID, userID)
	default:
		return false, nil
	}
}

func (p *Plugin) wuKongGroupMember(ctx context.Context, channelID string, userID uint) (bool, error) {
	var synced wuKongSyncResponse
	err := postWuKongWithAuth(ctx, p.apiAddr, "/channel/messagesync",
		p.wuKongAdminUser, p.wuKongAdminPassword, map[string]any{
			"login_uid":  strconv.FormatUint(uint64(userID), 10),
			"channel_id": channelID, "channel_type": groupChannel, "limit": 1, "pull_mode": 1,
		}, &synced)
	var upstream *httpStatusError
	if errors.As(err, &upstream) && upstream.status == http.StatusForbidden {
		return false, nil
	}
	return err == nil, err
}

func (p *Plugin) serveAgentStream(hctx huma.Context, channelID string, channelType uint8) {
	hctx.SetHeader("Content-Type", "text/event-stream")
	hctx.SetHeader("Cache-Control", "no-cache")
	hctx.SetHeader("Connection", "keep-alive")
	hctx.SetHeader("X-Accel-Buffering", "no")
	writer := hctx.BodyWriter()
	flusher, _ := writer.(http.Flusher)
	ctx := hctx.Context()
	seen := map[uint]string{}
	ticker := time.NewTicker(500 * time.Millisecond)
	heartbeat := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer heartbeat.Stop()
	send := func() bool {
		var rows []IMWebhookInbox
		if err := global.PRISM_DB.Where("channel_id = ? AND channel_type = ?", channelID, channelType).
			Where("state IN ?", []string{inboxProcessing, inboxCompleted, inboxDead}).
			Order("id DESC").Limit(100).Find(&rows).Error; err != nil {
			return false
		}
		for i := len(rows) - 1; i >= 0; i-- {
			signature := fmt.Sprintf("%s:%d:%s", rows[i].State, rows[i].Revision, rows[i].FinalWuKongClientMsgNo)
			if seen[rows[i].ID] == signature {
				continue
			}
			if _, ok := seen[rows[i].ID]; !ok {
				writeAgentEvent(writer, flusher, rows[i], "open", map[string]any{
					"sourceKey": rows[i].SourceKey, "agentUid": rows[i].TargetAgentUID,
					"channel": map[string]any{"id": rows[i].ChannelID, "type": rows[i].ChannelType},
				})
			}
			switch rows[i].State {
			case inboxProcessing:
				if rows[i].Revision != 0 {
					writeAgentEvent(writer, flusher, rows[i], "snapshot", map[string]any{
						"sourceKey": rows[i].SourceKey, "text": rows[i].CumulativeAnswer,
						"revision": rows[i].Revision,
					})
				}
			case inboxCompleted:
				writeAgentEvent(writer, flusher, rows[i], "complete", map[string]any{
					"sourceKey": rows[i].SourceKey, "text": rows[i].CumulativeAnswer,
					"revision": rows[i].Revision, "clientMsgNo": rows[i].FinalWuKongClientMsgNo,
				})
			case inboxDead:
				writeAgentEvent(writer, flusher, rows[i], "error", map[string]any{
					"sourceKey": rows[i].SourceKey, "code": rows[i].LastErrorCode,
					"message": "The agent could not complete this request.",
				})
			}
			seen[rows[i].ID] = signature
		}
		return true
	}
	if !send() {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !send() {
				return
			}
		case <-heartbeat.C:
			_, _ = fmt.Fprint(writer, ": ping\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

func writeAgentEvent(writer io.Writer, flusher http.Flusher, row IMWebhookInbox, event string, data any) {
	payload, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(writer, "id: %d:%d\nevent: %s\ndata: %s\n\n", row.ID, row.Revision, event, payload)
	if flusher != nil {
		flusher.Flush()
	}
}
