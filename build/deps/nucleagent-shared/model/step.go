package model

import "time"

// Step 执行步骤（一次 Agent 动作单元，UUID 供 CallLog 关联）。
// 对应表 steps，字段对齐 docs/03-data-models.md。
// step_id 独立成列，便于 CallLog 高频关联查询。
type Step struct {
	ID             uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	ConversationID uint      `json:"conversationId" gorm:"column:conversation_id;index;comment:所属对话ID"`
	StepID         string    `json:"stepId" gorm:"column:step_id;size:64;index;comment:步骤UUID(CallLog关联)"`
	Status         string    `json:"status" gorm:"column:status;size:16;comment:状态(waiting/running/completed/failed)"`
	Config         JSON      `json:"config" gorm:"column:config;type:json;comment:步骤配置(agent_id/title/output)"`
	CreatedAt      time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
}
