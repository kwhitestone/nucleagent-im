package logship

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// 环境变量名。
//
// 刻意**没有 LOG_SHIP_DIR**：目录必须由服务在代码里显式传入（见包注释第 5 点）。
// 一旦给出环境变量，就会出现「运维配了 LOG_SHIP_DIR、服务实际写在别处」的错配，
// 表现是「传上去的日志一直是空的」——没有任何报错，直到要查日志时才发现。
const (
	EnvEnabled          = "LOG_SHIP_ENABLED"
	EnvInterval         = "LOG_SHIP_INTERVAL"
	EnvMaxBytesPerCycle = "LOG_SHIP_MAX_BYTES_PER_CYCLE"
	EnvKeyPrefix        = "LOG_SHIP_KEY_PREFIX"
	EnvEnv              = "LOG_SHIP_ENV"
)

// 日志专用对象存储的环境变量族。
//
// 与各服务业务侧的 CS_* 刻意**不同名**：同名就意味着「配了业务 CS 就等于配了
// 日志 CS」，隔离在部署时会悄悄退化成复用——正是这组变量要防的事。
const (
	EnvCSHost       = "LOG_SHIP_CS_HOST"
	EnvCSServerName = "LOG_SHIP_CS_SERVER_NAME"
	EnvCSAccessKey  = "LOG_SHIP_CS_ACCESS_KEY"
	EnvCSSecretKey  = "LOG_SHIP_CS_SECRET_KEY"
	EnvCSUserID     = "LOG_SHIP_CS_USER_ID"
	EnvCSCDNHost    = "LOG_SHIP_CS_CDN_HOST"
)

// CSCredentials 是日志专用对象存储的连接信息，由 Gate 解析、接入方拿去构造
// Uploader。本包只搬运不使用——logship 不 import 任何存储客户端。
type CSCredentials struct {
	Host       string
	ServerName string
	AccessKey  string
	SecretKey  string
	UserID     string
	CDNHost    string
}

// Gate 是启用前置检查的结果。
//
// Enabled=false 时 Reason 一定非空，调用方据此打 Warn 并跳过启动上送——
// 不要 panic，也不要静默当作成功：日志上送是辅助能力，不该拖垮服务；
// 但「以为在传其实没传」比「没传」更难查，所以必须有明确原因可打印。
type Gate struct {
	Enabled bool
	Reason  string
	CS      CSCredentials
	// Config 是从环境变量解析出的上送配置（Dir/Service/Instance 仍需接入方填）。
	Config Config
}

// NewGate 执行启用前置检查。
//
// 两道关：
//
//	LOG_SHIP_ENABLED   只认明确的肯定值（1/true/yes/on），其余（含空、false、
//	                   随便什么词）都是关闭。日志含 token 字样与完整 SQL，
//	                   「拿不准就当开启」是不可接受的默认。
//	LOG_SHIP_CS_*      HOST 非空即判定「运维意在隔离」，此时其余必填项缺任一
//	                   → 整条禁用，且**绝不回落业务配置**。
//
// # 为什么「配了一半」不回落业务 CS
//
// 回落看起来更友好，实际是把日志悄悄写进了本想隔离开的那个账号——运维以为
// 已经隔离，日志却仍在业务 CS 里躺着。宁可整条禁用并给出明确原因：
// 「传不上去」查起来容易，「传到了不该传的地方」往往过很久才被发现。
//
// # 为什么判定只看 HOST
//
// 单一变量作开关比「任意一项非空即算启用」更好预测：后者在只配了 USER_ID
// 这种半拉子状态下也会触发，反而更难解释。
func NewGate() Gate {
	if !envBool(EnvEnabled) {
		return Gate{Reason: EnvEnabled + " 未设为肯定值（1/true/yes/on），日志上送保持关闭（默认关闭）"}
	}

	host := envStr(EnvCSHost)
	if host == "" {
		return Gate{Reason: EnvCSHost + " 未配置，日志上送禁用（日志上送必须使用独立的 LOG_SHIP_CS_* 凭据，不复用业务配置）"}
	}

	creds := CSCredentials{
		Host:       host,
		ServerName: envStr(EnvCSServerName),
		AccessKey:  envStr(EnvCSAccessKey),
		SecretKey:  envStr(EnvCSSecretKey),
		UserID:     envStr(EnvCSUserID),
		CDNHost:    envStr(EnvCSCDNHost),
	}
	// CDNHost 不在必填里：它只影响下载域名，缺了回落 Host 即可，
	// 不至于让整条链路停掉。
	var missing []string
	for _, kv := range []struct {
		key string
		val string
	}{
		{EnvCSServerName, creds.ServerName},
		{EnvCSAccessKey, creds.AccessKey},
		{EnvCSSecretKey, creds.SecretKey},
		{EnvCSUserID, creds.UserID},
	} {
		if kv.val == "" {
			missing = append(missing, kv.key)
		}
	}
	if len(missing) > 0 {
		return Gate{Reason: EnvCSHost + " 已配置但 " + strings.Join(missing, ", ") +
			" 缺失，日志上送整条禁用（不回落业务配置，避免日志写进本想隔离的账号）"}
	}

	return Gate{
		Enabled: true,
		CS:      creds,
		Config: Config{
			Env:              envStr(EnvEnv),
			Interval:         envDuration(EnvInterval),
			MaxBytesPerCycle: envInt64(EnvMaxBytesPerCycle),
			KeyPrefix:        envStr(EnvKeyPrefix),
		},
	}
}

func envStr(key string) string { return strings.TrimSpace(os.Getenv(key)) }

// envBool 只认明确的肯定值（见 NewGate 的说明）。
func envBool(key string) bool {
	switch strings.ToLower(envStr(key)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// envDuration 先按 Go duration 解析，失败再按「纯秒数」解析。
// 兼容纯秒数是因为同类环境变量在别处常被写成数字，只支持 "5m" 会让
// 写 "300" 的人静默拿到默认值。非法值回落 0 = 用 Config 的默认值。
func envDuration(key string) time.Duration {
	raw := envStr(key)
	if raw == "" {
		return 0
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	if n, err := strconv.Atoi(raw); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 0
}

func envInt64(key string) int64 {
	n, err := strconv.ParseInt(envStr(key), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
