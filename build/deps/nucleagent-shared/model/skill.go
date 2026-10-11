package model

import "time"

// Skill 技能（可安装到 Agent/对话的能力包）。
// 对应表 skills，字段对齐 docs/03-data-models.md。
type Skill struct {
	Revision uint64 `json:"revision,string" gorm:"not null;default:1"`
	ID       uint   `json:"id" gorm:"primaryKey;comment:主键ID"`
	Name     string `json:"name" gorm:"column:name;size:128;comment:技能名称"`
	Slug     string `json:"slug" gorm:"column:slug;size:128;uniqueIndex;comment:技能唯一标识"`
	Config   JSON   `json:"config" gorm:"column:config;type:json;comment:技能配置(category/source_url/version/description/is_system)"`
	I18n     JSON   `json:"i18n" gorm:"column:i18n;type:json;comment:多语言文案"`
	IsActive bool   `json:"isActive" gorm:"column:is_active;comment:是否启用"`
	// 技能包投影校验用（executor 下载后比对，防中途篡改/过期缓存）。
	SourceSHA256 string    `json:"sourceSha256" gorm:"column:source_sha256;size:64;comment:技能包 sha256"`
	Version      string    `json:"version" gorm:"column:version;size:64;comment:技能包版本"`
	CreatedAt    time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt    time.Time `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`
}
