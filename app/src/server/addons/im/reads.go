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
func conversationList(db *gorm.DB, viewer uint, cursor uint64, limit int) (conversationPage, error) {
	query := db.Where("uid = ?", viewer).Order("last_message_id DESC").Limit(limit + 1)
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
		page, err := conversationList(global.PRISM_DB.WithContext(ctx), viewer, cursor, clampLimit(input.Body.Limit, conversationPageDefault))
		if err != nil {
			return nil, unavailable()
		}
		return &ProxyOutput{Body: page}, nil
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
// (no wk_timestamp index); gaps older than the latest 100 per channel or in channels idle for
// 7 days are not covered. Add the index / widen when the table or a real loss shows up there.
func (p *Plugin) backfillMessages(ctx context.Context, db *gorm.DB) bool {
	var channels []struct {
		ChannelType uint8
		ChannelKey  string
		LastID      uint64
	}
	if err := db.WithContext(ctx).Model(&IMMessage{}).
		Select("channel_type, channel_key, MAX(id) AS last_id").
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
		n, err := p.backfillChannel(ctx, db, ch.ChannelType, ch.ChannelKey, ch.LastID)
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

func (p *Plugin) backfillChannel(ctx context.Context, db *gorm.DB, channelType uint8, key string, lastID uint64) (int, error) {
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
		if slices.Contains(have, m.MessageIDStr) {
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
