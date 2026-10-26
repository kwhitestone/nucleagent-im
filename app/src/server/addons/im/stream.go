package im

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
)

type coreMessage struct {
	ID         uint            `json:"id"`
	SenderType string          `json:"senderType"`
	MsgType    string          `json:"msgType"`
	Content    string          `json:"content"`
	SourceKey  string          `json:"sourceKey"`
	Metadata   json.RawMessage `json:"metadata"`
}

// startedDelegation reports a delegate_task call that Core accepted: the run
// then ends without the answer, which arrives later in a continuation run.
func (m coreMessage) startedDelegation() bool {
	if m.MsgType != "tool_call" || len(m.Metadata) == 0 {
		return false
	}
	var meta struct {
		Tool   string `json:"tool"`
		Status string `json:"tool_status"`
	}
	return json.Unmarshal(m.Metadata, &meta) == nil && meta.Tool == "delegate_task" && meta.Status == "completed"
}

func (p *Plugin) consumeCoreStream(ctx context.Context, row *IMWebhookInbox) error {
	if err := p.checkDelivery(ctx, row); err != nil {
		return err
	}
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
	request.Header.Set("Authorization", token) // A-16: bare; PROD gateway rejects Bearer
	if row.LastCoreEventID != "" {
		request.Header.Set("Last-Event-ID", row.LastCoreEventID)
	}
	// MUYU-160: a half-open Core stream used to hang this turn forever while the
	// lease kept renewing, so the row stayed processing and the turn never ended.
	// Core pings every 15s; silence past the idle limit drops the stream and the
	// retry resumes from the persisted Last-Event-ID.
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	request = request.WithContext(streamCtx)
	idle := time.AfterFunc(coreStreamIdleTimeout, cancelStream)
	defer idle.Stop()
	// Set when a delegated run ended empty and the continuation answer did not
	// arrive by the persisted deadline; Core pings keep the stream alive, so this
	// bounds the wait. The deadline lives on the row, so a reconnect, a restart or
	// a cursor that already moved past the empty result cannot reset it.
	continuationExpired := make(chan struct{})
	var continuation *time.Timer
	armContinuation := func(due time.Time) {
		if continuation == nil {
			continuation = time.AfterFunc(max(time.Until(due), 0), func() {
				close(continuationExpired)
				cancelStream()
			})
		}
	}
	defer func() {
		if continuation != nil {
			continuation.Stop()
		}
	}()
	if row.ContinuationDueAt != nil {
		if !time.Now().Before(*row.ContinuationDueAt) {
			return p.endTurnWithFailure(ctx, row, "im_continuation_timeout", deterministicClientMessageNo(row.SourceKey, 0))
		}
		armContinuation(*row.ContinuationDueAt)
	}
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		return coreStreamError(ctx, streamCtx, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		return &httpStatusError{status: response.StatusCode, code: "core_stream_http_" + strconv.Itoa(response.StatusCode)}
	}
	completed := false
	body := &idleResetReader{reader: response.Body, timer: idle, limit: coreStreamIdleTimeout}
	err = scanSSE(body, func(event, id string, data []byte) (bool, error) {
		if event != "message-created" && event != "message-updated" {
			return false, persistCoreEventID(row, id)
		}
		var message coreMessage
		if json.Unmarshal(data, &message) != nil {
			return false, persistCoreEventID(row, id)
		}
		eventID, err := strconv.ParseUint(id, 10, 64)
		if err != nil || eventID == 0 || eventID != uint64(message.ID) {
			return false, errors.New("invalid Core SSE message identity")
		}
		// The source message is a fixed dispatch boundary. The resume cursor is
		// separate because Core updates streaming messages in place at the same ID.
		if row.DispatchCoreMessageID == 0 {
			if message.SenderType == "user" && message.SourceKey == row.SourceKey {
				if err := global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).
					Update("dispatch_core_message_id", message.ID).Error; err != nil {
					return false, err
				}
				row.DispatchCoreMessageID = message.ID
			}
			return false, persistCoreEventID(row, id)
		}
		if message.ID <= row.DispatchCoreMessageID {
			return false, nil
		}
		if message.MsgType == "error" {
			if err := p.failExecution(ctx, row, message.ID); err != nil {
				return false, err
			}
			if err := persistCoreEventID(row, id); err != nil {
				return false, err
			}
			completed = true
			return true, nil
		}
		if message.startedDelegation() && !row.DelegationStarted {
			// Persisted before the cursor moves past this row: Core replays at
			// most 64 tool rows on a resume, so replay alone could lose it.
			if err := global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", row.ID).
				Update("delegation_started", true).Error; err != nil {
				return false, err
			}
			row.DelegationStarted = true
		}
		if message.SenderType != "agent" {
			return false, persistCoreEventID(row, id)
		}
		switch {
		case message.MsgType == "streaming":
			if err := p.checkDelivery(ctx, row); err != nil {
				return false, err
			}
			if err := persistSnapshot(row, message.Content); err != nil {
				return false, err
			}
		case event == "message-created" && message.MsgType == "result" && strings.TrimSpace(message.Content) == "":
			// Core finalizes every successful run with a result row, even one
			// with no text (MUYU-160). Without a delegation that row is the end
			// of the turn; ignoring it left the turn processing forever while
			// Core's pings kept the stream alive.
			if !row.DelegationStarted {
				if err := p.sendFinal(ctx, row, message.ID, emptyResultText); err != nil {
					return false, err
				}
				if err := persistCoreEventID(row, id); err != nil {
					return false, err
				}
				completed = true
				return true, nil
			}
			// After a delegation the answer arrives in a continuation run. Wait
			// for it until a deadline stored once on the row (first empty result
			// wins), so the cursor may move on safely.
			if row.ContinuationDueAt == nil {
				due := time.Now().Add(coreContinuationWait)
				if err := global.PRISM_DB.Model(&IMWebhookInbox{}).
					Where("id = ? AND continuation_due_at IS NULL", row.ID).
					Update("continuation_due_at", due).Error; err != nil {
					return false, err
				}
				var stored IMWebhookInbox
				if err := global.PRISM_DB.Select("continuation_due_at").First(&stored, row.ID).Error; err != nil {
					return false, err
				}
				row.ContinuationDueAt = stored.ContinuationDueAt
			}
			if row.ContinuationDueAt != nil {
				armContinuation(*row.ContinuationDueAt)
			}
			return false, persistCoreEventID(row, id)
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
	select {
	case <-continuationExpired:
		if !completed {
			return p.endTurnWithFailure(ctx, row, "im_continuation_timeout", deterministicClientMessageNo(row.SourceKey, 0))
		}
	default:
	}
	if err != nil {
		return coreStreamError(ctx, streamCtx, err)
	}
	if !completed {
		return errors.New("core stream ended before a terminal event")
	}
	return nil
}

// coreStreamIdleTimeout is how long the Core stream may stay silent before it is
// treated as dead. Core sends a ": ping" every 15s, so this is several missed pings.
var coreStreamIdleTimeout = 75 * time.Second

var errCoreStreamIdle = errors.New("core stream idle timeout")

// coreContinuationWait bounds how long a delegated turn that ended without text
// waits for its continuation answer before it fails with a notice.
var coreContinuationWait = 30 * time.Minute

// emptyResultText answers a run that Core finished without any text, so the
// question is never left without a reply.
const emptyResultText = "智能体已完成处理，但没有返回文字内容。"

// coreStreamError keeps worker shutdown distinct from an idle cutoff, so an idle
// stream is a retryable failure while shutdown keeps its context error.
func coreStreamError(parent, stream context.Context, err error) error {
	if parent.Err() == nil && stream.Err() != nil {
		return errCoreStreamIdle
	}
	return err
}

// idleResetReader pushes the idle deadline forward whenever Core sends bytes,
// including the ": ping" heartbeat comments that scanSSE skips.
type idleResetReader struct {
	reader io.Reader
	timer  *time.Timer
	limit  time.Duration
}

func (r *idleResetReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if n > 0 {
		r.timer.Reset(r.limit)
	}
	return n, err
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

// IM-AGENT-FAILURE-NOTICE: a failed run leaves one durable system line, so the
// question is not left unanswered once the 2-minute live replay is over. The
// upstream error text never reaches the chat.
const (
	execFailedNotice = "execution_failed"
	execFailedText   = "智能体这次没能完成回答，请稍后重试"
)

// failExecution marks the turn dead and posts its notice once per failure: only
// the call that moves the row out of processing sends, and the client_msg_no is
// stored on the row so the browser stream can hand its live error line over to
// the durable notice. Not replyNotice: its 10-minute window would swallow the
// notice for a retry that fails again, and its code renders as "agent disabled".
func (p *Plugin) failExecution(ctx context.Context, row *IMWebhookInbox, coreMessageID uint) error {
	return p.endTurnWithFailure(ctx, row, "im_execution_failed", deterministicClientMessageNo(row.SourceKey, coreMessageID))
}

// endTurnWithFailure moves a processing turn to dead and owes its durable
// notice in the same write, so a WuKong outage can delay the notice but never
// lose it (MUYU-160): deliverNotice and the worker's resend loop clear the debt
// only once WuKong accepted the message.
func (p *Plugin) endTurnWithFailure(ctx context.Context, row *IMWebhookInbox, code, clientNo string) error {
	now := time.Now()
	result := global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ? AND state = ?", row.ID, inboxProcessing).Updates(map[string]any{
		"state": inboxDead, "last_error_code": code, "lease_owner": "",
		"lease_expires_at": nil, "next_attempt_at": nil, "final_wu_kong_client_msg_no": clientNo,
		"notice_due_at": now, "notice_attempts": 0,
	})
	if result.Error != nil || result.RowsAffected == 0 {
		return result.Error
	}
	row.State = inboxDead
	row.FinalWuKongClientMsgNo = clientNo
	p.deliverNotice(ctx, row, now)
	return nil
}

// Resend schedule for an owed failure notice. Each claim moves notice_due_at to
// the next step BEFORE sending, so a send that fails or a process that dies in
// between leaves the notice due again later — never cleared. Only WuKong's
// acceptance clears it. After the fast schedule it retries every noticeSlowRetry.
var noticeRetryDelays = []time.Duration{
	5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, 2 * time.Hour,
}

// noticeSlowRetry is the retry period once the fast schedule is used up.
var noticeSlowRetry = 6 * time.Hour

// deliverNotice claims the current due slot (so two senders never send the same
// attempt), sends with the turn's deterministic client number (WuKong dedups a
// resend), and marks it sent on success.
func (p *Plugin) deliverNotice(ctx context.Context, row *IMWebhookInbox, now time.Time) {
	var current IMWebhookInbox
	if err := global.PRISM_DB.Select("id", "state", "notice_due_at", "notice_attempts", "final_wu_kong_client_msg_no").
		First(&current, row.ID).Error; err != nil || current.State != inboxDead || current.NoticeDueAt == nil ||
		current.FinalWuKongClientMsgNo == "" || now.Before(*current.NoticeDueAt) {
		return
	}
	// The notice is never dropped (MUYU-160 review 3): past the fast schedule it
	// keeps being retried at noticeSlowRetry until WuKong accepts it, so a long
	// WuKong outage only delays it. The ERROR log marks a notice that is overdue.
	next := noticeSlowRetry
	if current.NoticeAttempts < len(noticeRetryDelays) {
		next = noticeRetryDelays[current.NoticeAttempts]
	} else if current.NoticeAttempts == len(noticeRetryDelays) {
		slog.Error("im execution failure notice still undelivered after fast retries; continuing slow retries",
			"inbox_id", current.ID, "agent_uid", row.TargetAgentUID, "attempts", current.NoticeAttempts)
	}
	claim := global.PRISM_DB.Model(&IMWebhookInbox{}).
		Where("id = ? AND notice_attempts = ? AND notice_due_at IS NOT NULL", current.ID, current.NoticeAttempts).
		Updates(map[string]any{
			"notice_attempts": gorm.Expr("notice_attempts + 1"),
			"notice_due_at":   now.Add(next),
		})
	if claim.Error != nil || claim.RowsAffected != 1 {
		return
	}
	payload, _ := json.Marshal(textPayload{Type: wuKongTextType, Content: execFailedText, SystemNotice: execFailedNotice})
	if err := postWuKong(ctx, p.apiAddr, "/message/send", map[string]any{
		"from_uid":   strconv.FormatUint(uint64(row.TargetAgentUID), 10),
		"channel_id": row.ChannelID, "channel_type": row.ChannelType, "client_msg_no": current.FinalWuKongClientMsgNo,
		"payload": base64.StdEncoding.EncodeToString(payload),
	}, nil); err != nil {
		// The turn stays over; the notice stays due at the claimed next step.
		slog.Warn("im execution failure notice failed", "agent_uid", row.TargetAgentUID, "channel_type", row.ChannelType,
			"attempt", current.NoticeAttempts+1, "error", err)
		return
	}
	global.PRISM_DB.Model(&IMWebhookInbox{}).Where("id = ?", current.ID).
		Updates(map[string]any{"notice_due_at": nil, "notice_sent_at": time.Now()})
}

// noticeResendInterval is how often owed notices are checked.
var noticeResendInterval = 5 * time.Second

// runNoticeResender resends owed failure notices on its own goroutine: the inbox
// worker blocks for a whole long turn, so it cannot guarantee the cadence.
func (p *Plugin) runNoticeResender(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		p.resendDueNotices(ctx, time.Now())
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// resendDueNotices retries owed failure notices whose backoff has passed.
func (p *Plugin) resendDueNotices(ctx context.Context, now time.Time) {
	var due []IMWebhookInbox
	if err := global.PRISM_DB.WithContext(ctx).Where("state = ? AND notice_due_at IS NOT NULL AND notice_due_at <= ?", inboxDead, now).
		Order("notice_due_at ASC").Limit(20).Find(&due).Error; err != nil {
		return
	}
	for i := range due {
		if ctx.Err() != nil {
			return
		}
		p.deliverNotice(ctx, &due[i], now)
	}
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
		allowed, err := p.streamViewerAllowed(ctx, userID, input.ChannelID, input.ChannelType)
		if err != nil {
			return nil, newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "channel membership is unavailable")
		}
		if !allowed {
			return nil, newIMProblem(http.StatusForbidden, "channel_forbidden", "channel membership is required")
		}
		channelID := input.ChannelID
		channelType := input.ChannelType
		return &huma.StreamResponse{Body: func(hctx huma.Context) {
			// Huma's streaming context need not retain the handler context values.
			p.serveAgentStreamForUser(hctx, channelID, channelType, userID)
		}}, nil
	})
}

func (p *Plugin) channelParticipant(ctx context.Context, userID uint, channelID string, channelType uint8) (bool, error) {
	switch channelType {
	case personChannel:
		_, _, ok := canonicalPersonChannel(channelID, userID)
		return ok, nil
	case groupChannel:
		return isGroupMember(ctx, channelID, userID)
	default:
		return false, nil
	}
}

// agentStreamReplayWindow still delivers a reply or failure that lands just
// before the chat opens or across a stream reconnect.
const agentStreamReplayWindow = 2 * time.Minute

func (p *Plugin) serveAgentStreamForUser(hctx huma.Context, channelID string, channelType uint8, userID uint) {
	hctx.SetHeader("Content-Type", "text/event-stream")
	hctx.SetHeader("Cache-Control", "no-store")
	hctx.SetHeader("Connection", "keep-alive")
	hctx.SetHeader("X-Accel-Buffering", "no")
	writer := hctx.BodyWriter()
	flusher, _ := writer.(http.Flusher)
	ctx := hctx.Context()
	seen := map[uint]string{}
	// Rows that finished before this window are history, not live news: a
	// completed one is already the durable message, a dead one an old failure.
	// Replaying them doubled past replies (the replay reaches further back than
	// the 30 loaded messages) and resurfaced old "could not complete" lines on
	// every open (UNI-P-BATCH1 P3 / IM-REDESIGN O1).
	finishedSince := time.Now().Add(-agentStreamReplayWindow)
	ticker := time.NewTicker(500 * time.Millisecond)
	heartbeat := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer heartbeat.Stop()
	send := func() bool {
		return p.sendAgentSnapshots(ctx, writer, flusher, userID, channelID, channelType, finishedSince, seen)
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

func (p *Plugin) sendAgentSnapshots(ctx context.Context, writer io.Writer, flusher http.Flusher,
	userID uint, channelID string, channelType uint8, finishedSince time.Time, seen map[uint]string) bool {
	allowed, err := p.streamViewerAllowed(ctx, userID, channelID, channelType)
	if err != nil || !allowed {
		return false
	}
	var rows []IMWebhookInbox
	if err := global.PRISM_DB.WithContext(ctx).Where("channel_id = ? AND channel_type = ?", channelID, channelType).
		Where("state = ? OR (state IN ? AND updated_at >= ?)",
			inboxProcessing, []string{inboxCompleted, inboxDead}, finishedSince).
		Order("id DESC").Limit(100).Find(&rows).Error; err != nil {
		return false
	}
	for i := len(rows) - 1; i >= 0; i-- {
		signature := fmt.Sprintf("%s:%d:%s", rows[i].State, rows[i].Revision, rows[i].FinalWuKongClientMsgNo)
		if seen[rows[i].ID] == signature {
			continue
		}
		// Terminal notifications are not agent content and must never replay the
		// saved answer or an arbitrary upstream error string.
		if rows[i].State == inboxDead {
			allowed, err := p.streamViewerAllowed(ctx, userID, channelID, channelType)
			if err != nil || !allowed {
				return false
			}
			writeAgentEvent(writer, flusher, rows[i], "error", map[string]any{
				"sourceKey": rows[i].SourceKey, "code": "im_execution_failed",
				"message": "The agent could not complete this request.",
				// Set only for an execution failure: the durable notice that replaces this line.
				"clientMsgNo": rows[i].FinalWuKongClientMsgNo,
			})
			seen[rows[i].ID] = signature
			continue
		}
		if err := p.checkDelivery(ctx, &rows[i]); err != nil {
			var denied *httpStatusError
			if errors.As(err, &denied) && denied.status >= 400 && denied.status < 500 &&
				denied.status != http.StatusTooManyRequests {
				continue
			}
			return false
		}
		allowed, err := p.streamViewerAllowed(ctx, userID, channelID, channelType)
		if err != nil || !allowed {
			return false
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
		}
		seen[rows[i].ID] = signature
	}
	return true
}

func writeAgentEvent(writer io.Writer, flusher http.Flusher, row IMWebhookInbox, event string, data any) {
	payload, _ := json.Marshal(data)
	_, _ = fmt.Fprintf(writer, "id: %d:%d\nevent: %s\ndata: %s\n\n", row.ID, row.Revision, event, payload)
	if flusher != nil {
		flusher.Flush()
	}
}
