package model

import "time"

// Project 项目（用户组织对话的容器）。
// 对应表 projects，字段对齐 docs/03-data-models.md（仅 created_at）。
type Project struct {
	ID         uint      `json:"id" gorm:"primaryKey;comment:主键ID"`
	UserID     uint      `json:"userId" gorm:"column:user_id;index;comment:所属用户ID"`
	Name       string    `json:"name" gorm:"column:name;size:128;comment:项目名称"`
	IsArchived bool      `json:"isArchived" gorm:"column:is_archived;comment:是否归档"`
	CreatedAt  time.Time `json:"createdAt" gorm:"column:created_at;comment:创建时间"`
}
