package model

import "time"

// Tool 工具（元数据/配置持久化；运行时连接由 MCP addon 管理）。
// 对应表 tools，字段对齐 docs/03-data-models.md。
type Tool struct {
	Revision  uint64    `json:"revision,string" gorm:"not null;default:1"`
	ID        uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	Name      string    `json:"name" gorm:"column:name;size:128;comment:工具名称"`
	Slug      string    `json:"slug" gorm:"column:slug;size:128;uniqueIndex;comment:工具唯一标识"`
	Config    JSON      `json:"config" gorm:"column:config;type:json;comment:工具配置(type/description/mcp_config)"`
	I18n      JSON      `json:"i18n" gorm:"column:i18n;type:json;comment:多语言文案"`
	IsActive  bool      `json:"isActive" gorm:"column:is_active;comment:是否启用"`
	CreatedAt time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt time.Time `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`
}
