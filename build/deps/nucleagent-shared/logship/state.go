package logship

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// stateVersion 是游标文件的结构版本。将来若改字段语义，靠它决定是否丢弃旧游标。
const stateVersion = 1

// fileState 记录单个日志文件的上送进度。
//
// 为什么不只记 offset：日志会轮转，轮转后原路径指向新文件、从 0 开始写。
// 只看 offset 的话，轮转后 offset 仍是个大数，会把新文件当成「已传到那里」
// 而跳过开头的内容；反过来若盲目归零，又会把没轮转的文件从头重传一遍。
// 所以必须同时记 inode（是否换了文件）与比对 size（是否被截断）。
type fileState struct {
	// Inode 是文件的 inode 号。变了说明这个路径现在指向另一个文件（已轮转）。
	Inode uint64 `json:"inode"`
	// Offset 是已成功上传到的字节位置。**只在上传成功后推进**。
	Offset int64 `json:"offset"`
	// Seq 是该文件已上传的分片序号，用于生成可排序的对象名。
	Seq int64 `json:"seq"`
	// UpdatedAt 便于人工排查「这个文件多久没动了」。
	UpdatedAt time.Time `json:"updatedAt"`
}

// state 是整个游标文件的内容。key 是相对日志根目录的斜杠路径
// （见包注释第 5 点：按天分子目录下用文件名做 key 会跨天串味）。
type state struct {
	Version int                   `json:"version"`
	Files   map[string]*fileState `json:"files"`
}

func newState() *state {
	return &state{Version: stateVersion, Files: map[string]*fileState{}}
}

// loadState 读取游标文件。文件不存在、损坏或版本不符时返回空游标而非报错——
// 游标只是「进度备忘」，丢了最坏是重传一轮，不该阻断服务启动。
func loadState(path string) *state {
	data, err := os.ReadFile(path)
	if err != nil {
		return newState()
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return newState()
	}
	if s.Version != stateVersion || s.Files == nil {
		return newState()
	}
	return &s
}

// saveState 原子写游标文件：先写临时文件再 rename。
//
// 直接覆盖写有风险：进程在写一半时被杀，游标文件就成了半截 JSON，
// 下次启动解析失败 → 游标归零 → 整个日志目录重传一遍。
func saveState(path string, s *state) error {
	if s == nil {
		return nil
	}
	s.Version = stateVersion
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("logship: 序列化游标失败: %w", err)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("logship: 创建游标目录失败: %w", err)
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("logship: 写游标临时文件失败: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("logship: 替换游标文件失败: %w", err)
	}
	return nil
}

// retain 只保留 names 里的游标，返回是否真的删掉了东西。
//
// 磁盘上文件没了，它的游标就再无意义：同名文件若日后重建，inode 与 size
// 判据也会把它当新文件从 0 开始读，留着旧游标反而可能跳过开头内容。
func (s *state) retain(names []string) bool {
	keep := make(map[string]struct{}, len(names))
	for _, n := range names {
		keep[n] = struct{}{}
	}
	removed := false
	for name := range s.Files {
		if _, ok := keep[name]; !ok {
			delete(s.Files, name)
			removed = true
		}
	}
	return removed
}

// get 取某文件的游标，不存在则新建零值。
func (s *state) get(name string) *fileState {
	if s.Files == nil {
		s.Files = map[string]*fileState{}
	}
	fst, ok := s.Files[name]
	if !ok || fst == nil {
		fst = &fileState{}
		s.Files[name] = fst
	}
	return fst
}
