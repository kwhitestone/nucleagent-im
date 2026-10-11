package model

import "time"

// CallLog 调用日志（LLM / 工具调用的输入输出与计费元数据）。
// 对应表 call_logs，字段对齐 docs/03-data-models.md。
// step_id 独立成列，便于按步骤关联查询；input/output 用 mediumtext。
type CallLog struct {
	ID             uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	ConversationID uint      `json:"conversationId" gorm:"column:conversation_id;index;comment:所属对话ID"`
	StepID         string    `json:"stepId" gorm:"column:step_id;size:64;index;comment:关联Step的UUID"`
	CallType       string    `json:"callType" gorm:"column:call_type;size:32;comment:调用类型(llm/tool)"`
	Model          string    `json:"model" gorm:"column:model;size:64;comment:模型或工具名"`
	Input          string    `json:"input" gorm:"column:input;type:mediumtext;comment:调用输入"`
	Output         string    `json:"output" gorm:"column:output;type:mediumtext;comment:调用输出"`
	Meta           JSON      `json:"meta" gorm:"column:meta;type:json;comment:调用元数据(tokens/latency_ms/error)"`
	CreatedAt      time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
}
