package model

import "time"

// Provider LLM 提供商（密钥加密存储，不回传给前端）。
// 对应表 providers，字段对齐 docs/03-data-models.md（无 i18n）。
type Provider struct {
	ID        uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	Name      string    `json:"name" gorm:"column:name;size:128;comment:提供商名称"`
	APIKey    string    `json:"-" gorm:"column:api_key;type:text;comment:API密钥(加密存储)"`
	Config    JSON      `json:"config" gorm:"column:config;type:json;comment:提供商配置(provider_name/base_url/api_format/models)"`
	IsActive  bool      `json:"isActive" gorm:"column:is_active;comment:是否启用"`
	CreatedAt time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt time.Time `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`
}
