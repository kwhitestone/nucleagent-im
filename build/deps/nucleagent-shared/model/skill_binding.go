package model

// SkillBinding 技能绑定（把某个 Skill 安装到 template/instance/conversation）。
// 对应表 skill_bindings，字段对齐 docs/03-data-models.md（无时间戳）。
type SkillBinding struct {
	ID          uint   `json:"id" gorm:"primaryKey;comment:主键ID"`
	OwnerType   string `json:"ownerType" gorm:"column:owner_type;size:16;comment:归属类型(template/instance/conversation)"`
	OwnerID     uint   `json:"ownerId" gorm:"column:owner_id;index:idx_skill_binding_owner;comment:归属ID"`
	SkillID     uint   `json:"skillId" gorm:"column:skill_id;index;comment:技能ID"`
	InstallPath string `json:"installPath" gorm:"column:install_path;size:512;comment:本地安装路径(Executor使用)"`
}
