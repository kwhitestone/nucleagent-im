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
			return persistMessage(tx, messages[i])
		}); err != nil {
			return err
		}
	}
	return nil
}

func persistMessage(tx *gorm.DB, message webhookMessage) error {
	idStr := strings.TrimSpace(message.MessageIDStr)
	fromUID, err := strconv.ParseUint(strings.TrimSpace(message.FromUID), 10, 64)
	if err != nil || fromUID == 0 || idStr == "" {
		return nil
	}
	if h := message.Header; h != nil && (h.NoPersist == 1 || h.SyncOnce == 1) {
		return nil
	}
	sender := uint(fromUID)
	key := strings.TrimSpace(message.ChannelID)
	switch message.ChannelType {
	case personChannel:
		canonical, _, ok := canonicalPersonChannel(key, sender)
		if !ok {
			return nil
		}
		key = canonical
	case groupChannel:
		if key == "" {
			return nil
		}
	default:
		return nil
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
		return result.Error // 0 rows: a redelivery, already persisted and fanned out
	}

	var viewers []uint
	if message.ChannelType == personChannel {
		pair := strings.SplitN(key, "@", 2)
		left, _ := strconv.ParseUint(pair[0], 10, 64)
		right, _ := strconv.ParseUint(pair[1], 10, 64)
		viewers = []uint{uint(left), uint(right)}
	} else if err := tx.Model(&IMGroupMember{}).Where("group_id = ?", key).Pluck("uid", &viewers).Error; err != nil {
		return err
	}
	if len(viewers) == 0 {
		return nil // unknown group: the message row only
	}
	rows := make([]IMConversation, len(viewers))
	for i, uid := range viewers {
		rows[i] = IMConversation{UID: uid, ChannelType: message.ChannelType, ChannelKey: key, LastMessageID: row.ID}
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error; err != nil {
		return err
	}
	// Q2 ruling: every message not sent by the viewer counts as unread, agent replies
	// included (WK's red_dot:0 on API sends is ignored). The CASE keeps the newest id when
	// concurrent batches commit out of order; it is portable where GREATEST is not.
	return tx.Model(&IMConversation{}).
		Where("channel_type = ? AND channel_key = ? AND uid IN ?", message.ChannelType, key, viewers).
		Updates(map[string]any{
			"unread":          gorm.Expr("unread + CASE WHEN uid = ? THEN 0 ELSE 1 END", sender),
			"last_message_id": gorm.Expr("CASE WHEN last_message_id < ? THEN ? ELSE last_message_id END", row.ID, row.ID),
		}).Error
}
