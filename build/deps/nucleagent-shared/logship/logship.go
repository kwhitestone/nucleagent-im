// Package logship 周期性把服务日志文件的**新增部分**上送到对象存储。
//
// # 设计要点
//
//  1. 增量而非全量。单个日志文件能涨到上百 MB，每周期重传不可接受。
//     状态文件记 (inode, offset, seq)，每周期只读 offset 之后的新增字节。
//
//  2. 分段对象而非追加。每周期的新增内容作为独立对象上传，对象名以补零的 seq
//     开头，字典序等于时间序——下载时按名字排序拼接即还原原始日志。
//     附带好处是某一片失败不影响其它片。
//
//  3. 只在上传成功后推进 offset。失败保持原值，下个周期自然重试同一段，
//     不会因为一次网络抖动丢掉一段日志。
//
//  4. 默认关闭（见 Gate）。日志含 token 字样与完整 SQL，无声外传风险太大。
//
//  5. 递归扫描。NucleAgent 的日志按天分子目录（log/<YYYY-MM-DD>/<level>.log，
//     由 prism-fusion 的 rotatelogs 产生），不是扁平目录。因此游标的 key 是
//     **仓内相对路径**而非文件名——用文件名会让每天的 error.log 共用同一个
//     游标，次日凌晨 offset 不归零就会跳过新文件开头的内容。
//
//  6. 上传出口由接入方注入（Uploader）。本包不 import 任何存储客户端，
//     否则共享类型库就被拖上了一条网络依赖。
package logship

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultInterval         = 5 * time.Minute
	defaultMaxBytesPerCycle = int64(32) << 20 // 32 MiB
	defaultKeyPrefix        = "service-logs"
	stateFileName           = ".logship-state.json"
)

// Uploader 是上传出口。由接入方注入（Wave2 写指向 CS 的 adapter）。
//
// key 是完整对象路径，data 是这一片的原始字节。返回值 objectRef 只用于日志，
// 本包不解析它。
type Uploader interface {
	Upload(ctx context.Context, key string, data []byte) (objectRef string, err error)
}

// Config 描述一次接入。Dir/Service/Instance 必填。
type Config struct {
	// Dir 是日志根目录，递归扫描其下所有 *.log。
	// 必须由服务显式传入：各服务的目录来源不统一且多为相对进程 cwd 的相对路径，
	// 猜错的表现是静默上传空内容，没有任何报错。
	Dir string
	// Service 是服务名，作为对象 key 的一层。
	Service string
	// Instance 是实例 ID，作为对象 key 的一层，用于区分同服务的多副本。
	Instance string
	// Env 是部署实例隔离键，作为对象 key 的第一层。空则省略该层。
	Env string
	// Interval 是上送周期，默认 5m。
	Interval time.Duration
	// MaxBytesPerCycle 限制**每个文件**每周期最多上传多少字节，默认 32 MiB。
	// 这是限速而非丢弃：初次启用时已存在的大日志会分多个周期补齐。
	MaxBytesPerCycle int64
	// StateFile 是游标文件路径，默认 <Dir>/.logship-state.json。
	StateFile string
	// KeyPrefix 是对象 key 前缀，默认 "service-logs"。
	KeyPrefix string
}

func (c Config) interval() time.Duration {
	if c.Interval <= 0 {
		return defaultInterval
	}
	return c.Interval
}

func (c Config) maxBytesPerCycle() int64 {
	if c.MaxBytesPerCycle <= 0 {
		return defaultMaxBytesPerCycle
	}
	return c.MaxBytesPerCycle
}

func (c Config) stateFile() string {
	if strings.TrimSpace(c.StateFile) != "" {
		return c.StateFile
	}
	return filepath.Join(c.Dir, stateFileName)
}

func (c Config) keyPrefix() string {
	if p := strings.Trim(strings.TrimSpace(c.KeyPrefix), "/"); p != "" {
		return p
	}
	return defaultKeyPrefix
}

// Shipper 是增量上送器。非并发安全：只应由 Run 这一个 goroutine 驱动。
type Shipper struct {
	cfg    Config
	up     Uploader
	logger *slog.Logger
	state  *state
}

// NewShipper 构造上送器。必填项缺失时直接报错——这些都是接入时漏配，
// 静默禁用会让人以为在传，直到要查日志时才发现什么都没有。
func NewShipper(cfg Config, up Uploader, logger *slog.Logger) (*Shipper, error) {
	if up == nil {
		return nil, errors.New("logship: Uploader 为空")
	}
	if strings.TrimSpace(cfg.Dir) == "" {
		return nil, errors.New("logship: Config.Dir 为空（请传入服务的日志根目录）")
	}
	if strings.TrimSpace(cfg.Service) == "" || strings.TrimSpace(cfg.Instance) == "" {
		return nil, errors.New("logship: Config.Service / Config.Instance 不能为空（它们是对象 key 的组成部分）")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Shipper{cfg: cfg, up: up, logger: logger, state: loadState(cfg.stateFile())}, nil
}

// Run 周期上送，阻塞直到 ctx 结束，退出前落盘一次游标。
func (s *Shipper) Run(ctx context.Context) error {
	s.logger.Info("logship: 日志增量上送已启动",
		"dir", s.cfg.Dir,
		"interval", s.cfg.interval(),
		"maxBytesPerCycle", s.cfg.maxBytesPerCycle())

	ticker := time.NewTicker(s.cfg.interval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if err := saveState(s.cfg.stateFile(), s.state); err != nil {
				s.logger.Warn("logship: 退出时保存游标失败", "error", err)
			}
			return nil
		case <-ticker.C:
			s.ShipOnce(ctx)
		}
	}
}

// ShipOnce 执行一个周期的上送。导出以便测试与手工触发。
func (s *Shipper) ShipOnce(ctx context.Context) {
	names, err := s.scan()
	if err != nil {
		s.logger.Warn("logship: 扫描日志目录失败", "dir", s.cfg.Dir, "error", err)
		return
	}

	// 丢弃已不存在的文件的游标。日志按天分子目录，每天都会产生一批新 key，
	// 而旧日子的目录会被 rotatelogs 按保留期删掉——不清理的话游标文件
	// 只增不减，跑一年就成了几万条无用记录。
	changed := s.state.retain(names)

	for _, name := range names {
		if ctx.Err() != nil {
			break
		}
		shipped, err := s.shipFile(ctx, name)
		if err != nil {
			// 单个文件失败不影响其它文件：offset 未推进，下周期自然重试
			s.logger.Warn("logship: 上送失败", "logName", name, "error", err)
			continue
		}
		if shipped {
			changed = true
		}
	}

	if changed {
		if err := saveState(s.cfg.stateFile(), s.state); err != nil {
			// 游标没落盘不影响本轮已上传的数据，只是下轮可能重传一段
			s.logger.Warn("logship: 保存游标失败（下轮可能重传一段）", "error", err)
		}
	}
}

// scan 递归枚举 Dir 下所有 *.log，返回相对 Dir 的斜杠路径（排序保证行为可预期）。
//
// 递归而非只看一层：日志按天分子目录（log/2026-09-22/error.log），
// 平铺扫描会一个文件都找不到。
func (s *Shipper) scan() ([]string, error) {
	var out []string
	err := filepath.WalkDir(s.cfg.Dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// 根目录读不到就整轮报错让调用方打 Warn：Dir 配错的表现正是
			// 「一直传不上任何东西」，静默跳过会让人以为在传。
			if path == s.cfg.Dir {
				return err
			}
			// 某个子目录读不到则只跳过它——一个权限异常的日子
			// 不该让其它服务日志全部停传。
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".log") {
			return nil
		}
		rel, err := filepath.Rel(s.cfg.Dir, path)
		if err != nil {
			return nil
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// objectKey 拼出一片日志在对象存储上的完整 key：
//
//	<prefix>/<env>/<date>/<service>/<instance>/<logName>/<seq6>-<unixts>.log
//
// date 用 UTC，与对象名里的时间戳同一时区——一个本地一个 UTC 会出现
// 「对象名的日期和所在目录的日期差一天」这种没人想解释的现象。
// logName 是相对路径，含 "/" 时自然在对象存储上再分一层，与磁盘布局一致。
func (s *Shipper) objectKey(logName string, seq int64, now time.Time) string {
	parts := make([]string, 0, 7)
	parts = append(parts, s.cfg.keyPrefix())
	if env := strings.TrimSpace(s.cfg.Env); env != "" {
		parts = append(parts, env)
	}
	parts = append(parts,
		now.Format("2006-01-02"),
		s.cfg.Service,
		s.cfg.Instance,
		logName,
		fmt.Sprintf("%06d-%d.log", seq, now.Unix()),
	)
	return strings.Join(parts, "/")
}

// shipFile 处理单个日志文件的一次增量上送，返回是否真的上传了内容。
func (s *Shipper) shipFile(ctx context.Context, name string) (bool, error) {
	path := filepath.Join(s.cfg.Dir, filepath.FromSlash(name))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // 扫描到上送之间被轮转掉了，不算错误
		}
		return false, fmt.Errorf("打开失败: %w", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("读取文件信息失败: %w", err)
	}
	size := st.Size()
	inode := inodeOf(st)

	fst := s.state.get(name)

	// ── 轮转/截断判定（本包最核心的逻辑）──
	// inode 变了：这个路径现在指向另一个文件（旧的被改名或替换）。
	//   旧文件剩余部分放弃——继续追下去要么找不到、要么与新文件内容错乱。
	// size < offset：文件被截断（`: > file`，或被换成更小的文件）。
	//   继续用旧 offset 会 Seek 到文件尾之外，永远读到空。
	// 两种情况都从 0 重新开始。
	if fst.Inode != 0 && inode != 0 && fst.Inode != inode {
		s.logger.Info("logship: 检测到日志轮转，偏移归零",
			"logName", name, "oldInode", fst.Inode, "newInode", inode)
		fst.Offset = 0
	} else if size < fst.Offset {
		s.logger.Info("logship: 检测到日志被截断，偏移归零",
			"logName", name, "size", size, "offset", fst.Offset)
		fst.Offset = 0
	}
	fst.Inode = inode

	if size <= fst.Offset {
		return false, nil // 无新增
	}

	// 限速：每周期每个文件最多传 maxBytesPerCycle，超出部分留给后续周期
	want := size - fst.Offset
	if maxPerCycle := s.cfg.maxBytesPerCycle(); want > maxPerCycle {
		want = maxPerCycle
	}

	if _, err := f.Seek(fst.Offset, io.SeekStart); err != nil {
		return false, fmt.Errorf("定位到偏移 %d 失败: %w", fst.Offset, err)
	}
	buf := make([]byte, want)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, fmt.Errorf("读取增量失败: %w", err)
	}
	if n == 0 {
		return false, nil
	}

	seq := fst.Seq + 1
	now := time.Now().UTC()
	ref, err := s.up.Upload(ctx, s.objectKey(name, seq, now), buf[:n])
	if err != nil {
		// 关键：offset 不推进。下周期重传这一段，不丢日志。
		return false, fmt.Errorf("上传失败（偏移未推进，将重试）: %w", err)
	}

	// 只有到这里才推进进度
	fst.Offset += int64(n)
	fst.Seq = seq
	fst.UpdatedAt = now

	s.logger.Info("logship: 已上送日志增量",
		"logName", name, "seq", seq, "bytes", n, "offset", fst.Offset, "objectRef", ref)
	return true, nil
}
