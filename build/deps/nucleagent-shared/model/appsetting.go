package model

import "time"

// AppSetting 全局运行时配置（key-value，移植自 agentia app_settings）。
// 默认模型 v1/v2 配置等管理端设置存这里；coredata 统一迁移。
type AppSetting struct {
	ID          uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	Key         string    `json:"key" gorm:"column:key;size:128;not null;uniqueIndex;comment:配置键"`
	Value       string    `json:"value" gorm:"column:value;type:text;not null;comment:配置值(JSON文本)"`
	Description string    `json:"description" gorm:"column:description;size:255;not null;default:'';comment:配置说明"`
	CreatedAt   time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
	UpdatedAt   time.Time `json:"updatedAt" gorm:"column:updated_at;comment:更新时间"`
}

// AppSettingKeys nucleagent 用到的配置键。
const (
	AppSettingKeyDefaultLLMConfig     = "default_llm_config"     // v1：单一默认模型
	AppSettingKeyDefaultLLMModeConfig = "default_llm_mode_config" // v2：模式组/Profile 配置
)

// DefaultLLMConfig v1 默认模型：provider+model 一对（模型由目鱼网关目录提供）。
type DefaultLLMConfig struct {
	ProviderID uint   `json:"providerId"`
	Model      string `json:"model"`
}

// DefaultLLMModeKey 模式键（对齐 agentia 的 quick/thinking/expert）。
type DefaultLLMModeKey string

const (
	DefaultLLMModeQuick    DefaultLLMModeKey = "quick"
	DefaultLLMModeThinking DefaultLLMModeKey = "thinking"
	DefaultLLMModeExpert   DefaultLLMModeKey = "expert"
)

// ValidDefaultLLMModeKey 报告 mode 是否合法。
func ValidDefaultLLMModeKey(mode string) bool {
	switch mode {
	case string(DefaultLLMModeQuick), string(DefaultLLMModeThinking), string(DefaultLLMModeExpert):
		return true
	}
	return false
}

// DefaultLLMModeConfig v2 模式组/Profile 配置（移植自 agentia，去套餐锁）。
//
// 结构：modes 描述每个模式的默认组；groups 是对用户可见的模型组（显示名/排序/
// 归属模式）；profiles 是具体 provider×model 落点，组解析恒落 defaultProfileId。
type DefaultLLMModeConfig struct {
	Version  int                       `json:"version"`
	Modes    map[string]LLMModeSection `json:"modes"`
	Groups   []LLMModelGroup           `json:"groups"`
	Profiles []LLMModelProfile         `json:"profiles"`
}

// LLMModeSection 单个模式的默认组设置。
type LLMModeSection struct {
	DefaultGroupID string   `json:"defaultGroupId,omitempty"`
	GroupIDs       []string `json:"groupIds,omitempty"`
}

// LLMModelGroup 模型组：用户视角的一个可选模型（组内多 Profile 备选，默认 Profile 生效）。
type LLMModelGroup struct {
	ID               string `json:"id"`
	Slug             string `json:"slug"`
	Name             string `json:"name"`
	I18n             JSON   `json:"i18n,omitempty"`
	Enabled          bool   `json:"enabled"`
	ModeKeys         []string `json:"modeKeys"`
	ProfileIDs       []string `json:"profileIds"`
	DefaultProfileID string `json:"defaultProfileId"`
	SortOrder        int    `json:"sortOrder"`
}

// LLMModelProfile 组内落点：provider×model + 覆盖参数。
type LLMModelProfile struct {
	ID          string `json:"id"`
	ProviderID  uint   `json:"providerId"`
	Model       string `json:"model"`
	DisplayName string `json:"displayName"`
	Params      JSON   `json:"params,omitempty"`
	Enabled     bool   `json:"enabled"`
}

// UserVisibleModel 用户可见模型列表项（由 v2 组派生；无套餐锁字段）。
type UserVisibleModel struct {
	ConfigID uint   `json:"configId"`     // 默认 Profile 的 Provider ID
	Model    string `json:"model"`        // 组引用 "@group/<slug>"
	Alias    string `json:"alias"`        // 显示名（组名）
	IsQuick  bool   `json:"isQuick"`      // modeKeys 含 quick
	ModeKeys []string `json:"modeKeys,omitempty"`
	// TargetModel 默认 Profile 的具体模型名（前端把组引用解码为可持久化的
	// {configId, targetModel} 对；服务端解析，前端不需要组→profile 映射）。
	TargetModel string `json:"targetModel,omitempty"`
}

// UserVisibleModelGroupPrefix 组引用前缀。
const UserVisibleModelGroupPrefix = "@group/"

// DisplayName 显示名：alias 优先，退回 model。
func (m UserVisibleModel) DisplayName() string {
	if m.Alias != "" {
		return m.Alias
	}
	return m.Model
}
