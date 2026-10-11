package model

// SkillTool 技能↔工具关联（声明某个 Tool 归属某个 Skill）。
// 对应表 skill_tools，字段对齐 docs/03-data-models.md。
//
// 设计取舍：没有直接复用 SkillBinding（owner_type/owner_id/skill_id），因为
// SkillBinding 描述的是“Skill 安装到 template/instance/conversation”的归属关系，
// 语义上是 Skill 的挂载点；而这里要表达的是“Tool 归属于哪个 Skill”，主体和方向
// 都不同（owner 是 Tool 而不是 conversation/template/instance），硬套 owner_type
// 会引入一个恒为 "skill" 的冗余列。同时 Tool 表本身也不适合直接加 skill_id 外键：
// 工具可以是全局公共工具（不归属任何技能），也可能未来被多个技能共享，加单一外键
// 表达不了「公共工具」与「一工具多技能」的语义，因此新增一张最小关联表，风格上与
// 现有 SkillBinding 保持一致（同样只有必要字段、无时间戳）。
type SkillTool struct {
	ID      uint `json:"id" gorm:"primaryKey;comment:主键ID"`
	SkillID uint `json:"skillId" gorm:"column:skill_id;index:idx_skill_tool_skill;comment:技能ID"`
	ToolID  uint `json:"toolId" gorm:"column:tool_id;uniqueIndex:idx_skill_tool_tool;comment:工具ID(每个工具最多归属一个技能，暂不支持多技能共享同一工具)"`
}
