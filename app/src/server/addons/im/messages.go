package im

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// persistWebhookMessages saves every message of a msg.notify batch (UNI-IM-DB W2). It runs
// before the agent filters and the admission cap, so it covers all senders and payload types.
// Each message is its own transaction; on error the caller answers 503 and WuKong's resend
// is safe, because message_idstr makes the save idempotent.
func persistWebhookMessages(ctx context.Context, db *gorm.DB, messages []webhookMessage) error {
	for i := range messages {
		if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			_, err := persistMessage(tx, messages[i])
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// persistMessage reports whether the message was new (false: skipped or already saved).
func persistMessage(tx *gorm.DB, message webhookMessage) (bool, error) {
	idStr := strings.TrimSpace(message.MessageIDStr)
	fromUID, err := strconv.ParseUint(strings.TrimSpace(message.FromUID), 10, 64)
	if err != nil || fromUID == 0 || idStr == "" {
		return false, nil
	}
	if h := message.Header; h != nil && (h.NoPersist == 1 || h.SyncOnce == 1) {
		return false, nil
	}
	sender := uint(fromUID)
	key := strings.TrimSpace(message.ChannelID)
	switch message.ChannelType {
	case personChannel:
		canonical, _, ok := canonicalPersonChannel(key, sender)
		if !ok {
			return false, nil
		}
		key = canonical
	case groupChannel:
		if key == "" {
			return false, nil
		}
	default:
		return false, nil
	}

	var head struct {
		Type int `json:"type"`
	}
	_ = json.Unmarshal(message.RawPayload, &head) // non-JSON payloads are kept raw with type 0
	row := IMMessage{
		MessageIDStr: idStr, ClientMsgNo: message.ClientMsgNo, ChannelKey: key,
		ChannelType: message.ChannelType, FromUID: sender, PayloadType: head.Type,
		Payload: string(message.RawPayload), Setting: message.Setting,
		WKTimestamp: message.Timestamp, WKMessageSeq: message.MessageSeq,
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if result.Error != nil || result.RowsAffected == 0 {
		return false, result.Error // 0 rows: a redelivery, already persisted and fanned out
	}

	var viewers []uint
	if message.ChannelType == personChannel {
		pair := strings.SplitN(key, "@", 2)
		left, _ := strconv.ParseUint(pair[0], 10, 64)
		right, _ := strconv.ParseUint(pair[1], 10, 64)
		viewers = []uint{uint(left), uint(right)}
	} else if err := tx.Model(&IMGroupMember{}).Where("group_id = ?", key).Pluck("uid", &viewers).Error; err != nil {
		return true, err
	}
	if len(viewers) == 0 {
		return true, nil // unknown group: the message row only
	}
	rows := make([]IMConversation, len(viewers))
	for i, uid := range viewers {
		rows[i] = IMConversation{UID: uid, ChannelType: message.ChannelType, ChannelKey: key, LastMessageID: row.ID}
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error; err != nil {
		return true, err
	}
	// Q2 ruling: every message not sent by the viewer counts as unread, agent replies
	// included (WK's red_dot:0 on API sends is ignored). The CASE keeps the newest id when
	// concurrent batches commit out of order; it is portable where GREATEST is not. A message
	// older than the current last one (a W4 backfill gets a fresh, higher id) never becomes
	// the preview. A message from someone else also unhides the row (Q3 §2: hidden rows come
	// back on new messages); the viewer's own message from another device does not.
	return true, tx.Model(&IMConversation{}).
		Where("channel_type = ? AND channel_key = ? AND uid IN ?", message.ChannelType, key, viewers).
		Updates(map[string]any{
			"unread":    gorm.Expr("unread + CASE WHEN uid = ? THEN 0 ELSE 1 END", sender),
			"hidden_at": gorm.Expr("CASE WHEN uid = ? THEN hidden_at ELSE NULL END", sender),
			"last_message_id": gorm.Expr("CASE WHEN last_message_id < ? AND COALESCE((SELECT wk_timestamp FROM im_messages WHERE id = last_message_id), 0) <= ? THEN ? ELSE last_message_id END",
				row.ID, row.WKTimestamp, row.ID),
		}).Error
}
