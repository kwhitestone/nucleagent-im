package model

import "time"

// AgentTemplate Agent 模板（对话/编排用的预设角色）。
// 对应表 agent_templates，字段对齐 docs/03-data-models.md。
type AgentTemplate struct {
	ID               uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	Revision         uint64    `json:"revision,string" gorm:"not null;default:1"`
	IdentityStatus   string    `json:"-" gorm:"size:16;not null;default:pending"`
	IdentityRevision uint64    `json:"-" gorm:"not null;default:0"`
	IdentityError    string    `json:"-" gorm:"size:64"`
	CreationKey      *string   `json:"-" gorm:"size:64;uniqueIndex"`
	CreationHash     string    `json:"-" gorm:"size:64"`
	Name             string    `json:"name" gorm:"column:name;size:128;comment:模板名称"`
	Slug             string    `json:"slug" gorm:"column:slug;size:128;uniqueIndex;comment:模板唯一标识"`
	Config           JSON      `json:"config" gorm:"column:config;type:json;comment:模板配置(category/role/personality/bio/prompt/avatar/color/sort_order)"`
	I18n             JSON      `json:"i18n" gorm:"column:i18n;type:json;comment:多语言文案"`
	IsActive         bool      `json:"isActive" gorm:"column:is_active;comment:是否启用"`
	CreatedAt        time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt        time.Time `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`

	// AuthAgentUserID is the definition-level IM identity: one Auth agent account
	// shared by every user's personal instance, routed to the sender's own
	// instance. NULL means the definition has no IM identity.
	AuthAgentUserID *uint `json:"authAgentUserId" gorm:"column:auth_agent_user_id;uniqueIndex;comment:定义级IM身份(Auth Agent用户ID)"`

	// DeletedAt/DeletedBy are an admin tombstone (AG1). Deliberately not
	// gorm.DeletedAt: history (conversations, instances, IM routing) must keep
	// resolving the row; readers that list or start work filter on it.
	// A deleted definition is always also is_active=false.
	DeletedAt *time.Time `json:"deletedAt" gorm:"column:deleted_at;comment:删除时间(墓碑,非空=已删除)"`
	DeletedBy *uint      `json:"deletedBy" gorm:"column:deleted_by;comment:删除操作者用户ID"`
}

// AgentDefinitionAudit is one admin change to an agent definition (AG1).
// Prompt text is never stored, only its SHA-256 before and after.
type AgentDefinitionAudit struct {
	ID                  uint      `json:"id" gorm:"primaryKey"`
	ResourceType        string    `json:"resourceType,omitempty" gorm:"size:16"`
	ResourceID          uint      `json:"resourceId,omitempty"`
	CancellationPending bool      `json:"-" gorm:"not null;default:false;index"`
	Revision            uint64    `json:"revision,string" gorm:"not null;default:1"`
	AgentTemplateID     uint      `json:"agentTemplateId" gorm:"column:agent_template_id;not null;index:idx_agent_def_audit_tpl,priority:1;comment:智能体定义ID"`
	ActorUserID         uint      `json:"actorUserId" gorm:"column:actor_user_id;not null;comment:操作者用户ID"`
	Action              string    `json:"action" gorm:"column:action;size:16;not null;comment:create/update/disable/enable/delete/restore"`
	Fields              JSON      `json:"fields" gorm:"column:fields;type:json;comment:改动字段名清单"`
	PromptSHA256Before  string    `json:"promptSha256Before" gorm:"column:prompt_sha256_before;size:64;comment:改前prompt哈希"`
	PromptSHA256After   string    `json:"promptSha256After" gorm:"column:prompt_sha256_after;size:64;comment:改后prompt哈希"`
	CreatedAt           time.Time `json:"createdAt" gorm:"column:created_at;index:idx_agent_def_audit_tpl,priority:2;comment:操作时间"`
}

func (AgentDefinitionAudit) TableName() string { return "agent_definition_audits" }

// Audit actions.
const (
	AgentAuditCreate  = "create"
	AgentAuditUpdate  = "update"
	AgentAuditDisable = "disable"
	AgentAuditEnable  = "enable"
	AgentAuditDelete  = "delete"
	AgentAuditRestore = "restore"
)
