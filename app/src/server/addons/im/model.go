package im

import "time"

const (
	inboxPending    = "pending"
	inboxProcessing = "processing"
	inboxCompleted  = "completed"
	inboxIgnored    = "ignored"
	inboxRetryable  = "retryable"
	inboxDead       = "dead"
)

type IMWebhookInbox struct {
	ID                     uint       `gorm:"primaryKey"`
	Event                  string     `gorm:"size:32;not null;uniqueIndex:uidx_im_inbox_target"`
	MessageIDStr           string     `gorm:"column:message_idstr;size:64;not null;uniqueIndex:uidx_im_inbox_target"`
	TargetAgentUID         uint       `gorm:"not null;uniqueIndex:uidx_im_inbox_target;index"`
	ClientMessageNo        string     `gorm:"size:128"`
	SenderUID              uint       `gorm:"not null;index"`
	ChannelID              string     `gorm:"size:191;not null;index:idx_im_inbox_channel"`
	ChannelType            uint8      `gorm:"not null;index:idx_im_inbox_channel"`
	MessageTimestamp       int64      `gorm:"not null"`
	Text                   string     `gorm:"type:text;not null"`
	MentionUIDs            string     `gorm:"type:text"`
	State                  string     `gorm:"size:16;not null;index"`
	LeaseOwner             string     `gorm:"size:64"`
	LeaseExpiresAt         *time.Time `gorm:"index"`
	Attempts               int        `gorm:"not null;default:0"`
	NextAttemptAt          *time.Time `gorm:"index"`
	LastErrorCode          string     `gorm:"size:64"`
	ExecutionOwnerUserID   uint       `gorm:"not null"`
	SourceKey              string     `gorm:"size:191;not null;uniqueIndex"`
	CoreConversationID     uint       `gorm:"index"`
	CumulativeAnswer       string     `gorm:"type:longtext"`
	Revision               uint64     `gorm:"not null;default:0"`
	LastCoreEventID        string     `gorm:"size:64"`
	DispatchCoreMessageID  uint       `gorm:"not null;default:0"`
	FinalCoreMessageID     uint
	FinalWuKongClientMsgNo string `gorm:"size:128"`
	FinalSentAt            *time.Time
	// M3 A2A provenance (additive). ChainID identifies the origin chain that this
	// row belongs to, ChainDepth is 1 for a human-originated trigger and +1 per
	// agent hop, and OriginSenderUID is the human who started the chain — agent
	// hops are authorized as that human, never as themselves.
	ChainID         string `gorm:"size:191;not null;default:'';index"`
	ChainDepth      int    `gorm:"not null;default:1"`
	OriginSenderUID uint   `gorm:"not null;default:0"`
	SourceAgentUID  uint   `gorm:"not null;default:0"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

func (IMWebhookInbox) TableName() string { return "im_webhook_inbox" }

type IMCoreConversationMap struct {
	ID                    uint      `gorm:"primaryKey"`
	ChannelID             string    `gorm:"size:191;not null;uniqueIndex:uidx_im_conversation_map"`
	ChannelType           uint8     `gorm:"not null;uniqueIndex:uidx_im_conversation_map"`
	AgentUID              uint      `gorm:"not null;uniqueIndex:uidx_im_conversation_map"`
	ExecutionOwnerUserID  uint      `gorm:"not null;uniqueIndex:uidx_im_conversation_map"`
	CoreConversationID    uint      `gorm:"not null;index"`
	ImportedAt            time.Time `gorm:"not null"`
	LatestSourceMessageID string    `gorm:"size:64"`
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

func (IMCoreConversationMap) TableName() string { return "im_core_conversation_map" }

type IMGroup struct {
	ID         uint   `gorm:"primaryKey"`
	GroupID    string `gorm:"size:191;not null;uniqueIndex"`
	Title      string `gorm:"size:255"`
	CreatorUID uint   `gorm:"not null;index"`
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (IMGroup) TableName() string { return "im_groups" }

// IMGroupMember is group membership, owned by im (UNI-IM-DB W1); WuKong subscribers are
// a projection of it. (group_id, uid) serves member lists and membership checks; the uid
// index serves "the caller's groups".
type IMGroupMember struct {
	ID        uint   `gorm:"primaryKey"`
	GroupID   string `gorm:"size:191;not null;uniqueIndex:uidx_im_group_member,priority:1"`
	UID       uint   `gorm:"not null;uniqueIndex:uidx_im_group_member,priority:2;index"`
	CreatedAt time.Time
}

func (IMGroupMember) TableName() string { return "im_group_members" }

// IMMessage is every persisted message (UNI-IM-DB W2). ID is the history cursor (JS-safe,
// survives WK resets); message_idstr is the dedup key. idx_im_msg_channel
// (channel_type, channel_key, id) serves every history query.
type IMMessage struct {
	ID           uint64 `gorm:"primaryKey"`
	MessageIDStr string `gorm:"column:message_idstr;size:32;not null;uniqueIndex"`
	ClientMsgNo  string `gorm:"size:128;index"`
	ChannelKey   string `gorm:"size:191;not null;index:idx_im_msg_channel,priority:2"`
	ChannelType  uint8  `gorm:"not null;index:idx_im_msg_channel,priority:1"`
	FromUID      uint   `gorm:"not null"`
	PayloadType  int    `gorm:"not null"`
	Payload      string `gorm:"type:mediumtext;not null"`
	Setting      uint8  `gorm:"not null;default:0"`
	WKTimestamp  int64  `gorm:"column:wk_timestamp;not null"`
	WKMessageSeq int64  `gorm:"column:wk_message_seq;not null;default:0"`
	// SearchText is the searchable text (content, file name; IM2-D2), NULL until the backfill
	// reaches a row saved before it existed. The FULLTEXT index is built at boot (search.go),
	// not here: it needs the ngram parser, MySQL only, and session settings.
	SearchText *string `gorm:"type:text"`
	CreatedAt  time.Time
}

func (IMMessage) TableName() string { return "im_messages" }

// IMConversation is one row per (viewer, channel), written on persist (fan-out on write).
// uidx_im_conv serves the upsert and mark-read; idx_im_conv_recent (uid, last_message_id)
// serves the keyset-paged conversation list.
type IMConversation struct {
	ID            uint64 `gorm:"primaryKey"`
	UID           uint   `gorm:"not null;uniqueIndex:uidx_im_conv,priority:1;index:idx_im_conv_recent,priority:1"`
	ChannelType   uint8  `gorm:"not null;uniqueIndex:uidx_im_conv,priority:2"`
	ChannelKey    string `gorm:"size:191;not null;uniqueIndex:uidx_im_conv,priority:3"`
	LastMessageID uint64 `gorm:"not null;index:idx_im_conv_recent,priority:2"`
	Unread        int    `gorm:"not null;default:0"`
	// HiddenAt is set when the viewer hides the row (Q3 §2); a message from anyone else
	// clears it. NULL = visible. Additive column, AutoMigrate adds it.
	HiddenAt  *time.Time
	UpdatedAt time.Time
}

func (IMConversation) TableName() string { return "im_conversations" }

type IMGroupAgentAllowlist struct {
	ID                uint   `gorm:"primaryKey"`
	GroupID           string `gorm:"size:191;not null;uniqueIndex:uidx_im_group_allow"`
	AgentUID          uint   `gorm:"not null;uniqueIndex:uidx_im_group_allow"`
	MemberUID         uint   `gorm:"not null;uniqueIndex:uidx_im_group_allow"`
	ManagedByOwnerUID uint   `gorm:"not null"`
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (IMGroupAgentAllowlist) TableName() string { return "im_group_agent_allowlist" }

type IMRateWindow struct {
	ID          uint      `gorm:"primaryKey"`
	Scope       string    `gorm:"size:16;not null;uniqueIndex:uidx_im_rate_window"`
	WindowKey   string    `gorm:"size:64;not null;uniqueIndex:uidx_im_rate_window"`
	WindowStart time.Time `gorm:"not null;uniqueIndex:uidx_im_rate_window"`
	Count       int       `gorm:"not null"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func (IMRateWindow) TableName() string { return "im_rate_windows" }
