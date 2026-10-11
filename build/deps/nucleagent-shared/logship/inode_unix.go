//go:build unix

package logship

import (
	"os"
	"syscall"
)

// inodeOf 从 FileInfo 取 inode 号，用于判定日志是否已轮转
// （同一个路径指向了不同的 inode）。
//
// 取不到时返回 0。调用方把 0 当作「无法判定轮转」——此时只靠
// size < offset 的截断判据兜底，不会误判成轮转而重传整个文件。
func inodeOf(fi os.FileInfo) uint64 {
	if fi == nil {
		return 0
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return 0
	}
	return uint64(st.Ino)
}
