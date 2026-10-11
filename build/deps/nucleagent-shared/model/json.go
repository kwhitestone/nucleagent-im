package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// JSON 基于 json.RawMessage 的 GORM 字段类型，用于存储任意 JSON 列。
// 零值为 nil（数据库 NULL）。用 NewJSON 从 Go 值构造，用 As 解码到目标结构。
// 只封装序列化/数据库读写，不含业务逻辑。
type JSON json.RawMessage

// Scan 实现 sql.Scanner，支持 []byte / string / nil。
func (j *JSON) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}
	switch v := value.(type) {
	case []byte:
		cp := make(JSON, len(v))
		copy(cp, v)
		*j = cp
	case string:
		*j = JSON(v)
	default:
		return fmt.Errorf("model.JSON: cannot scan %T", value)
	}
	return nil
}

// Value 实现 driver.Valuer，空值落库为 NULL。
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return []byte(j), nil
}

// MarshalJSON 直接输出原始 JSON 字节，空值输出 null。
func (j JSON) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("null"), nil
	}
	return []byte(j), nil
}

// UnmarshalJSON 保留原始字节，交由具体结构体按需解码。
func (j *JSON) UnmarshalJSON(data []byte) error {
	if j == nil {
		return fmt.Errorf("model.JSON: UnmarshalJSON on nil pointer")
	}
	cp := make(JSON, len(data))
	copy(cp, data)
	*j = cp
	return nil
}

// NewJSON 将任意 Go 值编码为 JSON 字段（辅助函数）。
func NewJSON(v interface{}) (JSON, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("model.NewJSON: %w", err)
	}
	return JSON(b), nil
}

// MustNewJSON 同 NewJSON，编码失败时返回 null（便于零值初始化，避免 panic）。
func MustNewJSON(v interface{}) JSON {
	j, err := NewJSON(v)
	if err != nil {
		return JSON("null")
	}
	return j
}

// As 将 JSON 字段解码到目标结构体（辅助函数），空值不修改目标。
func (j JSON) As(out interface{}) error {
	if len(j) == 0 {
		return nil
	}
	return json.Unmarshal([]byte(j), out)
}
