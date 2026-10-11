package model

import "time"

// 消息类型常量。msg_type 列取这些值。
// streaming 是流式占位消息（附录 §7.1）：先创建 streaming 行，边输出边就地 upsert content，
// 完成后提升为 text/result 或删除。
const (
	MsgTypeText      = "text"      // 普通文本消息（user/agent）
	MsgTypeStreaming = "streaming" // 流式占位（执行中，content 边写边更新）
	MsgTypePlan      = "plan"      // 计划
	MsgTypeResult    = "result"    // 最终结果
	MsgTypeError     = "error"     // 错误
	MsgTypeToolCall  = "tool_call" // 工具调用
	MsgTypeStatus    = "status"    // 系统状态消息（含上下文压缩 summary）
)

// 发送方类型常量。sender_type 列取这些值。
const (
	SenderTypeUser   = "user"
	SenderTypeAgent  = "agent"
	SenderTypeSystem = "system"
	SenderTypeTool   = "tool"
)

// Message 对话消息（用户/Agent/系统/工具产出，每条都要展示）。
// 对应表 messages，字段对齐 docs/03-data-models.md。
// 高频展示字段（sender_type / sender_name）独立成列，不放入 JSON。
//
// Metadata JSON 约定 key（附录 §7.1 / §7.7）：
//   - step_id            : 关联 Step UUID（流式 upsert 幂等定位用）
//   - delegation_id      : 本次委派 ID（流式 upsert + 最终结果幂等键用）
//   - stream.result_scope: "step" / "final"（区分步骤结果与最终结果）
//   - attachments        : 附件清单
//   - history            : 下发给 executor 的历史快照
//   - summaryUpToMsgID   : 上下文压缩 summary 消息的截断点（§7.7）
//   - summary            : 压缩摘要文本（仅 MsgTypeStatus 的 summary 消息）
type Message struct {
	ID             uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	ConversationID uint      `json:"conversationId" gorm:"column:conversation_id;index;comment:所属对话ID"`
	SenderType     string    `json:"senderType" gorm:"column:sender_type;size:16;comment:发送方类型(user/agent/system/tool)"`
	SenderName     string    `json:"senderName" gorm:"column:sender_name;size:64;comment:发送方展示名"`
	MsgType        string    `json:"msgType" gorm:"column:msg_type;size:32;comment:消息类型(text/streaming/plan/result/error/tool_call/status)"`
	Content        string    `json:"content" gorm:"column:content;type:text;comment:消息内容"`
	Metadata       JSON      `json:"metadata" gorm:"column:metadata;type:json;comment:元数据(step_id/delegation_id/stream.result_scope/attachments/history/summaryUpToMsgID)"`
	SourceKey      *string   `json:"sourceKey" gorm:"column:source_key;size:191;uniqueIndex;comment:外部来源幂等键"`
	CreatedAt      time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
}
