package model

import "time"

// AgentInstance 用户雇佣的 Agent 实例（基于某个模板）。
// 对应表 agent_instances，字段对齐 docs/03-data-models.md。
type AgentInstance struct {
	ID               uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	UserID           uint      `json:"userId" gorm:"column:user_id;index;comment:所属用户ID"`
	TemplateID       uint      `json:"templateId" gorm:"column:template_id;index;comment:来源模板ID"`
	AuthAgentUserID  *uint     `json:"authAgentUserId" gorm:"column:auth_agent_user_id;uniqueIndex;comment:绑定的Auth Agent用户ID"`
	ExecutionBackend string    `json:"executionBackend" gorm:"column:execution_backend;size:32;comment:执行后端(codex/hermes/opencode/claude-code)"`
	ProviderID       *uint     `json:"providerId" gorm:"column:provider_id;comment:LLM提供商ID"`
	Model            string    `json:"model" gorm:"column:model;size:100;comment:模型"`
	IMEnabled        bool      `json:"imEnabled" gorm:"column:im_enabled;not null;default:false;comment:是否启用IM对话"`
	Config           JSON      `json:"config" gorm:"column:config;type:json;comment:实例配置(nickname/override)"`
	I18n             JSON      `json:"i18n" gorm:"column:i18n;type:json;comment:多语言覆盖"`
	CreatedAt        time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt        time.Time `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`

	// PersonalKey "p:<user_id>:<template_id>" marks the one personal instance per
	// user x definition. Legacy hired rows keep NULL, which a unique index allows
	// any number of times.
	PersonalKey *string `json:"personalKey,omitempty" gorm:"column:personal_key;size:64;uniqueIndex;comment:个人实例唯一键"`
	// Memory is the owner-visible cross-session summary (<=2000 characters).
	// MemoryVersion is its optimistic-concurrency counter; MemoryFoldedAt is the
	// completed_at of the newest conversation already folded into it.
	Memory         *string    `json:"memory,omitempty" gorm:"column:memory;type:text;comment:跨会话记忆摘要"`
	MemoryVersion  int        `json:"memoryVersion" gorm:"column:memory_version;not null;default:0;comment:记忆乐观锁版本"`
	MemoryFoldedAt *time.Time `json:"memoryFoldedAt,omitempty" gorm:"column:memory_folded_at;comment:记忆已折叠到的会话完成时间"`
}
