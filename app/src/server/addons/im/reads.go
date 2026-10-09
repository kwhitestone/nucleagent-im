package im

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/kwhitestone/prism-fusion/global"
	"gorm.io/gorm"
)

// UNI-IM-DB W4: history and the conversation list are answered from MySQL in WuKong's
// JSON shape, so im-web parses them unchanged. Authorization moved here from WuKong.

const (
	conversationPageDefault = 50
	pageMax                 = 100
)

type ConversationReadInput struct {
	Body struct {
		ChannelID   string `json:"channel_id" required:"true"`
		ChannelType uint8  `json:"channel_type" minimum:"1"`
	}
}

type syncedHeader struct {
	NoPersist int `json:"no_persist"`
	RedDot    int `json:"red_dot"`
	SyncOnce  int `json:"sync_once"`
}

// syncedMessage is WuKong's legacyMessageResp; message_seq carries our id (the cursor).
type syncedMessage struct {
	Header       syncedHeader `json:"header"`
	Setting      uint8        `json:"setting"`
	MessageID    int64        `json:"message_id"`
	MessageIDStr string       `json:"message_idstr"`
	ClientMsgNo  string       `json:"client_msg_no"`
	MessageSeq   uint64       `json:"message_seq"`
	FromUID      string       `json:"from_uid"`
	ChannelID    string       `json:"channel_id"`
	ChannelType  uint8        `json:"channel_type"`
	Expire       uint32       `json:"expire"`
	Timestamp    int64        `json:"timestamp"`
	Payload      []byte       `json:"payload"`
}

type messageSyncPage struct {
	StartMessageSeq uint64          `json:"start_message_seq"`
	EndMessageSeq   uint64          `json:"end_message_seq"`
	More            int             `json:"more"`
	Messages        []syncedMessage `json:"messages"`
}

type conversationItem struct {
	ChannelID    string             `json:"channel_id"`
	ChannelType  uint8              `json:"channel_type"`
	ActiveAt     int64              `json:"active_at"`
	ReadSeq      uint64             `json:"read_seq"`
	DeletedToSeq uint64             `json:"deleted_to_seq"`
	Unread       int                `json:"unread"`
	LastMessage  conversationLastMs `json:"last_message"`
}

type conversationLastMs struct {
	MessageID         int64  `json:"message_id"`
	MessageIDStr      string `json:"message_idstr"`
	MessageSeq        uint64 `json:"message_seq"`
	FromUID           string `json:"from_uid"`
	ClientMsgNo       string `json:"client_msg_no"`
	Setting           uint8  `json:"setting"`
	Timestamp         int64  `json:"timestamp"`
	ServerTimestampMS int64  `json:"server_timestamp_ms"`
	Payload           []byte `json:"payload"`
}

type conversationPage struct {
	Conversations []conversationItem `json:"conversations"`
	Deletes       []struct{}         `json:"deletes"`
	NextCursor    string             `json:"next_cursor"`
	Done          bool               `json:"done"`
}

// viewerChannelKey maps what a client sends (a DM peer uid, or "a@b"; a group id) to the
// stored key, and reports whether viewer may read it. A DM needs the viewer in the pair.
func viewerChannelKey(ctx context.Context, channelID string, channelType uint8, viewer uint) (string, bool, error) {
	channelID = strings.TrimSpace(channelID)
	switch channelType {
	case personChannel:
		if !strings.Contains(channelID, "@") {
			channelID = strconv.FormatUint(uint64(viewer), 10) + "@" + channelID
		}
		key, _, ok := canonicalPersonChannel(channelID, viewer)
		return key, ok, nil
	case groupChannel:
		if channelID == "" {
			return "", false, nil
		}
		ok, err := isGroupMember(ctx, channelID, viewer)
		return channelID, ok, err
	default:
		return "", false, nil
	}
}

// channelIDFor renders a stored key as WuKong does for viewer: a DM is the peer uid.
func channelIDFor(key string, channelType uint8, viewer uint) string {
	if channelType != personChannel {
		return key
	}
	pair := strings.SplitN(key, "@", 2)
	if pair[0] == strconv.FormatUint(uint64(viewer), 10) {
		return pair[1]
	}
	return pair[0]
}

func clampLimit(limit, fallback int) int {
	if limit <= 0 {
		return fallback
	}
	return min(limit, pageMax)
}

func toSynced(m IMMessage, viewer uint) syncedMessage {
	id, _ := strconv.ParseInt(m.MessageIDStr, 10, 64)
	return syncedMessage{
		Header: syncedHeader{RedDot: 1}, Setting: m.Setting, MessageID: id, MessageIDStr: m.MessageIDStr,
		ClientMsgNo: m.ClientMsgNo, MessageSeq: m.ID, FromUID: strconv.FormatUint(uint64(m.FromUID), 10),
		ChannelID: channelIDFor(m.ChannelKey, m.ChannelType, viewer), ChannelType: m.ChannelType,
		Timestamp: m.WKTimestamp, Payload: []byte(m.Payload),
	}
}

// channelMessages is WuKong's messagesync over im_messages (idx_im_msg_channel):
// start 0 = the latest n; pull_mode 0 = at or before start (older); 1 = at or after start.
// The result is ascending by id. more reports a further page in the pull direction.
func channelMessages(db *gorm.DB, channelType uint8, key string, start, end uint64, pullMode, limit int) ([]IMMessage, bool, error) {
	query := db.Where("channel_type = ? AND channel_key = ?", channelType, key)
	up := start != 0 && pullMode == 1
	switch {
	case up:
		query = query.Where("id >= ?", start).Order("id ASC")
		if end != 0 {
			query = query.Where("id <= ?", end)
		}
	case start != 0:
		query = query.Where("id <= ?", start).Order("id DESC")
		if end != 0 {
			query = query.Where("id >= ?", end)
		}
	default:
		query = query.Order("id DESC")
	}
	var rows []IMMessage
	if err := query.Limit(limit + 1).Find(&rows).Error; err != nil {
		return nil, false, err
	}
	more := len(rows) > limit
	rows = rows[:min(len(rows), limit)]
	if !up {
		slices.Reverse(rows)
	}
	return rows, more, nil
}

// conversationList is one keyset page on idx_im_conv_recent: last_message_id < cursor, newest first.
// last_message_id is unique per viewer (a message lives in one channel), so pages never overlap.
// hidden picks the hidden view (Q3 §2) instead of the default visible one.
// ponytail: the hidden_at filter runs on rows already found by idx_im_conv_recent, so a page
// reads past the viewer's hidden rows. Add index (uid, hidden_at, last_message_id) once one
// viewer has >1k hidden rows (EXPLAIN rows/limit ≫ 1 on this query); not needed before.
func conversationList(db *gorm.DB, viewer uint, cursor uint64, limit int, hidden bool) (conversationPage, error) {
	query := db.Where("uid = ?", viewer).Order("last_message_id DESC").Limit(limit + 1)
	if hidden {
		query = query.Where("hidden_at IS NOT NULL")
	} else {
		query = query.Where("hidden_at IS NULL")
	}
	if cursor != 0 {
		query = query.Where("last_message_id < ?", cursor)
	}
	var rows []IMConversation
	if err := query.Find(&rows).Error; err != nil {
		return conversationPage{}, err
	}
	page := conversationPage{Conversations: []conversationItem{}, Deletes: []struct{}{}, Done: len(rows) <= limit}
	rows = rows[:min(len(rows), limit)]
	if !page.Done {
		page.NextCursor = strconv.FormatUint(rows[len(rows)-1].LastMessageID, 10)
	}
	ids := make([]uint64, len(rows))
	for i := range rows {
		ids[i] = rows[i].LastMessageID
	}
	var messages []IMMessage
	if err := db.Where("id IN ?", ids).Find(&messages).Error; err != nil {
		return conversationPage{}, err
	}
	byID := make(map[uint64]IMMessage, len(messages))
	for _, m := range messages {
		byID[m.ID] = m
	}
	for _, row := range rows {
		m := toSynced(byID[row.LastMessageID], viewer)
		page.Conversations = append(page.Conversations, conversationItem{
			ChannelID: channelIDFor(row.ChannelKey, row.ChannelType, viewer), ChannelType: row.ChannelType,
			ActiveAt: m.Timestamp, Unread: row.Unread,
			LastMessage: conversationLastMs{
				MessageID: m.MessageID, MessageIDStr: m.MessageIDStr, MessageSeq: m.MessageSeq, FromUID: m.FromUID,
				ClientMsgNo: m.ClientMsgNo, Setting: m.Setting, Timestamp: m.Timestamp,
				ServerTimestampMS: m.Timestamp * 1000, Payload: m.Payload,
			},
		})
	}
	return page, nil
}

const batchMax = 100

type batchChannel struct {
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
}

type ConversationBatchInput struct {
	Body struct {
		Action   string         `json:"action,omitempty" doc:"hide | unhide | read"`
		Channels []batchChannel `json:"channels,omitempty" doc:"1–100 channels"`
	}
}

type batchResult struct {
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	OK          bool   `json:"ok"`
	Code        string `json:"code"` // "" | not_found | error
}

var batchUpdates = map[string]func(now time.Time) map[string]any{
	"hide":   func(now time.Time) map[string]any { return map[string]any{"hidden_at": now, "unread": 0} },
	"unhide": func(time.Time) map[string]any { return map[string]any{"hidden_at": nil} },
	"read":   func(time.Time) map[string]any { return map[string]any{"unread": 0} },
}

// ownChannelKey maps a client channel to the stored key without a membership lookup: the
// batch only touches the caller's own im_conversations rows, so "no row for uid=viewer" is
// the authorization (not_found), and a foreign DM key never canonicalizes for the viewer.
func ownChannelKey(c batchChannel, viewer uint) (string, bool) {
	id := strings.TrimSpace(c.ChannelID)
	switch c.ChannelType {
	case personChannel:
		if !strings.Contains(id, "@") {
			id = strconv.FormatUint(uint64(viewer), 10) + "@" + id
		}
		key, _, ok := canonicalPersonChannel(id, viewer)
		return key, ok
	case groupChannel:
		return id, id != ""
	}
	return "", false
}

// conversationBatch applies action to the caller's rows (Q3 §2): one SELECT, one UPDATE.
// Every action is idempotent; channels without a row of the caller's are not_found.
func conversationBatch(db *gorm.DB, viewer uint, action string, channels []batchChannel) ([]batchResult, error) {
	type ref struct {
		typ uint8
		key string
	}
	refs := make([]ref, len(channels))
	wanted := map[ref]bool{}
	var keys []string
	for i, c := range channels {
		if key, ok := ownChannelKey(c, viewer); ok {
			refs[i] = ref{c.ChannelType, key}
			wanted[refs[i]] = true
			keys = append(keys, key)
		}
	}
	var rows []IMConversation
	if len(keys) > 0 {
		if err := db.Select("id, channel_type, channel_key").
			Where("uid = ? AND channel_key IN ?", viewer, keys).Find(&rows).Error; err != nil {
			return nil, err
		}
	}
	found := make(map[ref]uint64, len(rows))
	ids := make([]uint64, 0, len(rows))
	for _, r := range rows {
		if k := (ref{r.ChannelType, r.ChannelKey}); wanted[k] {
			found[k] = r.ID
			ids = append(ids, r.ID)
		}
	}
	code := ""
	if len(ids) > 0 {
		if err := db.Model(&IMConversation{}).Where("uid = ? AND id IN ?", viewer, ids).
			Updates(batchUpdates[action](time.Now())).Error; err != nil {
			slog.Warn("im conversation batch update failed", "uid", viewer, "action", action, "error", err)
			code = "error"
		}
	}
	results := make([]batchResult, len(channels))
	for i, c := range channels {
		results[i] = batchResult{ChannelID: c.ChannelID, ChannelType: c.ChannelType, Code: "not_found"}
		if _, ok := found[refs[i]]; ok {
			results[i].Code, results[i].OK = code, code == ""
		}
	}
	return results, nil
}

func (p *Plugin) registerReads(api huma.API) {
	security := []map[string][]string{{"AuthTokenAuth": {}}}
	unavailable := func() error {
		return newIMProblem(http.StatusServiceUnavailable, "im_unavailable", "IM storage is unavailable")
	}

	huma.Register(api, huma.Operation{
		OperationID: "imConversationList", Method: http.MethodPost, Path: "/api/v1/im/conversation/list",
		Summary: "List the caller's conversations (cursor-paged)", Tags: []string{"IM"}, Security: security,
	}, func(ctx context.Context, input *ConversationListInput) (*ProxyOutput, error) {
		cursor, err := strconv.ParseUint(strings.TrimSpace(input.Body.Cursor), 10, 64)
		if err != nil && strings.TrimSpace(input.Body.Cursor) != "" {
			return nil, newIMProblem(http.StatusBadRequest, "invalid_cursor", "cursor is invalid")
		}
		viewer := ctx.Value(userIDKey).(uint)
		page, err := conversationList(global.PRISM_DB.WithContext(ctx), viewer, cursor, clampLimit(input.Body.Limit, conversationPageDefault), input.Body.Hidden)
		if err != nil {
			return nil, unavailable()
		}
		return &ProxyOutput{Body: page}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "imConversationBatch", Method: http.MethodPost, Path: "/api/v1/im/conversation/batch",
		Summary: "Hide, unhide or mark read 1–100 of the caller's conversations", Tags: []string{"IM"}, Security: security,
	}, func(ctx context.Context, input *ConversationBatchInput) (*ProxyOutput, error) {
		n := len(input.Body.Channels)
		if batchUpdates[input.Body.Action] == nil || n < 1 || n > batchMax {
			return nil, newIMProblem(http.StatusBadRequest, "invalid_request", "action must be hide, unhide or read with 1–100 channels")
		}
		viewer := ctx.Value(userIDKey).(uint)
		results, err := conversationBatch(global.PRISM_DB.WithContext(ctx), viewer, input.Body.Action, input.Body.Channels)
		if err != nil {
			return nil, unavailable()
		}
		ok := 0
		for _, r := range results {
			if r.OK {
				ok++
			}
		}
		slog.Info("im conversation batch", "uid", viewer, "action", input.Body.Action, "n", n, "ok", ok)
		return &ProxyOutput{Body: map[string]any{"results": results}}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "imChannelMessageSync", Method: http.MethodPost, Path: "/api/v1/im/channel/messagesync",
		Summary: "Page a channel's message history", Tags: []string{"IM"}, Security: security,
	}, func(ctx context.Context, input *MessageSyncInput) (*ProxyOutput, error) {
		viewer := ctx.Value(userIDKey).(uint)
		key, ok, err := viewerChannelKey(ctx, input.Body.ChannelID, input.Body.ChannelType, viewer)
		if err != nil {
			return nil, unavailable()
		}
		if !ok {
			return nil, newIMProblem(http.StatusForbidden, "channel_forbidden", "channel membership is required")
		}
		rows, more, err := channelMessages(global.PRISM_DB.WithContext(ctx), input.Body.ChannelType, key,
			input.Body.StartMessageSeq, input.Body.EndMessageSeq, input.Body.PullMode, clampLimit(input.Body.Limit, pageMax))
		if err != nil {
			return nil, unavailable()
		}
		page := messageSyncPage{
			StartMessageSeq: input.Body.StartMessageSeq, EndMessageSeq: input.Body.EndMessageSeq,
			Messages: make([]syncedMessage, len(rows)),
		}
		if more {
			page.More = 1
		}
		for i := range rows {
			page.Messages[i] = toSynced(rows[i], viewer)
		}
		return &ProxyOutput{Body: page}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "imConversationRead", Method: http.MethodPost, Path: "/api/v1/im/conversation/read",
		Summary: "Clear the caller's unread count on a channel", Tags: []string{"IM"}, Security: security,
	}, func(ctx context.Context, input *ConversationReadInput) (*ProxyOutput, error) {
		viewer := ctx.Value(userIDKey).(uint)
		key, ok, err := viewerChannelKey(ctx, input.Body.ChannelID, input.Body.ChannelType, viewer)
		if err != nil {
			return nil, unavailable()
		}
		if !ok {
			return nil, newIMProblem(http.StatusForbidden, "channel_forbidden", "channel membership is required")
		}
		if err := global.PRISM_DB.WithContext(ctx).Model(&IMConversation{}).
			Where("uid = ? AND channel_type = ? AND channel_key = ?", viewer, input.Body.ChannelType, key).
			Update("unread", 0).Error; err != nil {
			return nil, unavailable()
		}
		return &ProxyOutput{Body: envelope[any]{Code: 0, Message: "success"}}, nil
	})
}

// UNI-IM-DB Q1 option 2: WuKong drops a msg.notify batch after 3 back-to-back failures, so
// an im restart or roll (or a DB blip) loses messages. The backfill pulls each recently
// active channel's latest page from WuKong and saves what MySQL lacks, through the same
// idempotent persistMessage. A WuKong reset loses them in WuKong too; that stays accepted.
const (
	backfillWindow      = 7 * 24 * time.Hour
	backfillMaxChannels = 200
	backfillPerChannel  = 100
)

// backfillMessages returns false when WuKong was unreachable, so the caller retries next tick.
// ponytail: one WuKong call per active channel per pass, and the channel pick scans im_messages
// (no wk_timestamp index); gaps older than the latest 100 per channel, in channels idle for
// 7 days, or in a channel whose every message was lost are not covered. Add the index / widen
// when the table or a real loss shows up there.
func (p *Plugin) backfillMessages(ctx context.Context, db *gorm.DB) bool {
	var channels []struct {
		ChannelType uint8
		ChannelKey  string
		LastID      uint64
		FirstTS     int64
	}
	if err := db.WithContext(ctx).Model(&IMMessage{}).
		Select("channel_type, channel_key, MAX(id) AS last_id, MIN(wk_timestamp) AS first_ts").
		Where("wk_timestamp >= ?", time.Now().Add(-backfillWindow).Unix()).
		Group("channel_type, channel_key").Order("last_id DESC").Limit(backfillMaxChannels).
		Scan(&channels).Error; err != nil {
		slog.Warn("im message backfill skipped", "error", err)
		return true
	}
	added, failed := 0, 0
	for _, ch := range channels {
		if ctx.Err() != nil {
			return true
		}
		n, err := p.backfillChannel(ctx, db, ch.ChannelType, ch.ChannelKey, ch.LastID, ch.FirstTS)
		added += n
		var status *httpStatusError
		if err != nil && !errors.As(err, &status) && ctx.Err() == nil {
			// Transport failure: WuKong is down, every other channel would time out too.
			slog.Warn("im message backfill failed: WuKong unreachable, retrying next tick", "added", added)
			return false
		}
		if err != nil {
			failed++
			slog.Warn("im message backfill failed", "channel_id", ch.ChannelKey)
		}
	}
	if added > 0 || failed > 0 {
		slog.Info("im message backfill", "channels", len(channels), "added", added, "failed", failed)
	}
	return true
}

// Only messages at or after the channel's first saved one are taken: the backfill repairs
// webhook-loss gaps; history from before persistence began is the accepted one-time reset
// (spec §2.5), and importing it would count it all as unread at the cutover.
func (p *Plugin) backfillChannel(ctx context.Context, db *gorm.DB, channelType uint8, key string, lastID uint64, firstTS int64) (int, error) {
	// Read as someone WuKong certainly counts as a member: a DM's last sender, a group's DB member.
	var reader uint
	var err error
	if channelType == personChannel {
		err = db.WithContext(ctx).Model(&IMMessage{}).Where("id = ?", lastID).Pluck("from_uid", &reader).Error
	} else {
		err = db.WithContext(ctx).Model(&IMGroupMember{}).Where("group_id = ?", key).Order("uid ASC").Limit(1).Pluck("uid", &reader).Error
	}
	if err != nil || reader == 0 {
		return 0, err
	}
	var synced struct {
		Messages []webhookMessage `json:"messages"`
	}
	if err := postWuKongWithAuth(ctx, p.apiAddr, "/channel/messagesync", p.wuKongAdminUser, p.wuKongAdminPassword,
		map[string]any{
			"login_uid": strconv.FormatUint(uint64(reader), 10), "channel_id": key,
			"channel_type": channelType, "limit": backfillPerChannel, "pull_mode": 1,
		}, &synced); err != nil {
		return 0, err
	}
	ids := make([]string, len(synced.Messages))
	for i, m := range synced.Messages {
		ids[i] = m.MessageIDStr
	}
	var have []string
	if len(ids) > 0 {
		if err := db.WithContext(ctx).Model(&IMMessage{}).Where("message_idstr IN ?", ids).Pluck("message_idstr", &have).Error; err != nil {
			return 0, err
		}
	}
	// Oldest first, so the backfilled rows keep their relative order. By timestamp: WuKong's seq
	// restarts at 1 on a reset, so a page spanning one would interleave (seq breaks same-second ties).
	slices.SortStableFunc(synced.Messages, func(a, b webhookMessage) int {
		return cmp.Or(cmp.Compare(a.Timestamp, b.Timestamp), cmp.Compare(a.MessageSeq, b.MessageSeq))
	})
	added := 0
	for _, m := range synced.Messages {
		if m.Timestamp < firstTS || slices.Contains(have, m.MessageIDStr) {
			continue
		}
		m.ChannelID = key // WuKong renders a DM as the peer uid; persist wants the pair
		var inserted bool
		if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var txErr error
			inserted, txErr = persistMessage(tx, m)
			return txErr
		}); err != nil {
			return added, err
		}
		if inserted {
			added++
		}
	}
	return added, nil
}
