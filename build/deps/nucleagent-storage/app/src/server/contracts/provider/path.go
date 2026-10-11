package provider

import (
	"fmt"
	"path"
	"strings"
)

// SanitizeKey 清洗对象相对路径，防路径穿越。
//
// 规则：
//   - 统一 / 分隔，去掉首尾 /
//   - path.Clean 规整 . 与 ..
//   - Clean 后仍以 ../ 开头或等于 .. 的一律拒绝（穿越到 prefix 之外）
//   - 拒绝绝对路径与空路径
//
// 返回清洗后的相对路径（如 "2026/08/abc.png"）。
func SanitizeKey(key string) (string, error) {
	k := strings.TrimSpace(strings.ReplaceAll(key, "\\", "/"))
	if k == "" {
		return "", fmt.Errorf("对象路径不能为空")
	}
	if strings.ContainsRune(k, 0) {
		return "", fmt.Errorf("对象路径含非法字符")
	}
	k = strings.Trim(k, "/")
	k = path.Clean(k)
	if k == "." || k == ".." || k == "" {
		return "", fmt.Errorf("非法的对象路径: %s", key)
	}
	if strings.HasPrefix(k, "../") || strings.HasPrefix(k, "/") {
		return "", fmt.Errorf("对象路径不得越出命名空间: %s", key)
	}
	return k, nil
}

// JoinPrefix 把命名空间前缀与已清洗的 key 拼成后端完整路径（以 / 开头，无重复斜杠）。
func JoinPrefix(prefix, key string) string {
	p := "/" + strings.Trim(prefix, "/")
	if p == "/" {
		return "/" + key
	}
	return p + "/" + key
}
