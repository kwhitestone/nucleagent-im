package model

import "time"

// Conversation 对话（一次 A2A 编排会话）。
// 对应表 conversations，字段对齐 docs/03-data-models.md。
// 高频更新字段（heartbeat_at / lease_until）独立成列，不放入 JSON。
type Conversation struct {
	ID               uint       `json:"id" gorm:"primaryKey;comment:主键ID"`
	UserID           uint       `json:"userId" gorm:"column:user_id;index;comment:所属用户ID"`
	AgentID          *uint      `json:"agentId" gorm:"column:agent_id;index;comment:关联Agent实例ID"`
	ProjectID        *uint      `json:"projectId" gorm:"column:project_id;index;comment:所属项目ID"`
	Title            string     `json:"title" gorm:"column:title;size:512;comment:对话标题"`
	Mode             string     `json:"mode" gorm:"column:mode;size:16;comment:编排模式(a2a/a2a_agent/a2a_employee)"`
	Status           string     `json:"status" gorm:"column:status;size:16;comment:状态(drafting/executing/blocked/completed/failed/cancelled)"`
	ProviderID       *uint      `json:"providerId" gorm:"column:provider_id;comment:会话级LLM提供商ID"`
	Model            string     `json:"model" gorm:"column:model;size:100;comment:会话级模型"`
	TargetDeviceID   string     `json:"targetDeviceId,omitempty" gorm:"column:target_device_id;size:64;not null;default:'';index"`
	ExecutionBackend string     `json:"executionBackend" gorm:"column:execution_backend;size:32;index;comment:会话级执行后端(codex/hermes/opencode/claude-code)"`
	State            JSON       `json:"state" gorm:"column:state;type:json;comment:编排状态(plan/context/pending_question)"`
	HeartbeatAt      *time.Time `json:"heartbeatAt" gorm:"column:heartbeat_at;comment:Executor心跳时间(高频更新)"`
	LeaseUntil       *time.Time `json:"leaseUntil" gorm:"column:lease_until;comment:Executor租约截止(高频更新)"`
	// ExecutionNonce 调度所有权 fencing（附录 §7.4）：= dispatch token UUID。
	// AcquireTaskLease 用它做乐观所有权判定，防止僵死 worker 覆盖新调度。
	ExecutionNonce  string     `json:"executionNonce" gorm:"column:execution_nonce;size:64;comment:调度所有权nonce(fencing)"`
	ExecutionStepID string     `json:"executionStepId" gorm:"column:execution_step_id;size:64;comment:当前执行step(fencing/rolling upgrade)"`
	CreatedAt       time.Time  `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt       time.Time  `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`
	CompletedAt     *time.Time `json:"completedAt" gorm:"column:completed_at;comment:完成时间"`
}
