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
	CreatedAt              time.Time
	UpdatedAt              time.Time
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
