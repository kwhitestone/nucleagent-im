// Package ndcs CS（cs.101.com）存储后端插件。
//
// 独立 git 仓库（nucleagent-storage-ndcs），以 submodule 形式挂在主框架
// plugins/ 下；主框架 blank import 本包即完成自注册，provider 配置值 "cs"。
package ndcs

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"

	storage "github.com/kwhitestone/nucleagent-storage/contracts/provider"
)

// 默认值常量（与 agentia 生产配置对齐）。
const (
	// DefaultHost CS 上传主机。必须是 https：生产 CS 不收明文 http。
	DefaultHost = "https://cs.101.com/v0.1"
	// DefaultCDNHost 默认 CDN 下载地址（全球 CDN gcdncs）。
	// 它不是 cs.101.com→cdncs.101.com 推导规则的产物，必须单列。
	DefaultCDNHost = "https://gcdncs.101.com/v0.1"
	// DefaultUserID "0" 表示服务账号。
	DefaultUserID = "0"
	// DefaultExpires 签名有效期（秒）。
	DefaultExpires = 1800
	// DefaultTimeout HTTP 超时（秒）。
	DefaultTimeout = 60
)

// Config ndcs 后端配置（对应 config.yaml 的 storage.cs 段）。
type Config struct {
	Host       string // CS 上传主机（https://cs.101.com/v0.1）
	CDNHost    string // CDN 下载主机（https://gcdncs.101.com/v0.1）
	ServerName string // CS 服务名（签名 token 第一段，也是 CS 顶层目录名）
	AccessKey  string // CS AccessKey
	SecretKey  string // CS SecretKey（HMAC-SHA1 密钥，不出网）
	UserID     string // CS uid（policy 里的 uid 字段）
	Scope      int    // 0=私有（签名下载），1=公开；-1=未配置（默认公开）
	Expires    int    // 下载签名有效期（秒）
	// UploadValidity is the target CS deployment's verified upload-signature
	// lifetime in seconds. It is intentionally separate from download expiry.
	UploadValidity int
	Timeout        int // HTTP 客户端超时（秒）
	// SignedSizeEnforced confirms that the configured CS deployment rejects
	// multipart bodies whose actual length differs from the signed policy size.
	// Direct upload is unsafe without this provider-side guarantee.
	SignedSizeEnforced bool
	// UploadExpiryEnforced confirms that the target rejects an upload at request
	// completion (not only request start) after the credential lifetime.
	UploadExpiryEnforced bool
	// WriteOnceEnforced confirms that a signed path cannot be overwritten or
	// concurrently materialized more than once.
	WriteOnceEnforced bool
	// SignedPathDownloadEnabled confirms that private objects can be read by a
	// token bound to their stable path. Scope 0 cannot safely register without it.
	SignedPathDownloadEnabled bool
}

// GetHost 返回 CS 主机，空则用默认值。
func (c *Config) GetHost() string {
	if c.Host == "" {
		return DefaultHost
	}
	return c.Host
}

// GetCDNHost 返回 CDN 下载主机。
//
// 优先级：显式配置 > 从 CS Host 推导 > 默认 gcdncs。
// 推导只是兜底（自建/测试 CS 场景）；生产必须显式配 gcdncs.101.com ——
// 推导规则得到的是 cdncs.101.com，与生产 CDN 不是同一个域名。
func (c *Config) GetCDNHost() string {
	if c.CDNHost != "" {
		return c.CDNHost
	}
	host := c.Host
	if host == "" {
		return DefaultCDNHost
	}
	cdn := strings.Replace(host, "cs.101.com", "cdncs.101.com", 1)
	return strings.Replace(cdn, "http://", "https://", 1)
}

// GetScope 返回 scope。
//
// 注意：0 是合法值（私有），不能用 <=0 判空，否则永远拿不到私有 scope。
// 负数是 FromViper 用于识别“未显式配置”的哨兵，由 validate 拒绝。
func (c *Config) GetScope() int {
	return c.Scope
}

// GetExpires 返回签名有效期（秒）。
func (c *Config) GetExpires() int {
	if c.Expires <= 0 {
		return DefaultExpires
	}
	return c.Expires
}

func (c *Config) GetUploadValidity() int {
	return c.UploadValidity
}

// GetTimeout 返回 HTTP 超时（秒）。
func (c *Config) GetTimeout() int {
	if c.Timeout <= 0 {
		return DefaultTimeout
	}
	return c.Timeout
}

// GetUserID 返回 CS uid，空则用默认值。
func (c *Config) GetUserID() string {
	if c.UserID == "" {
		return DefaultUserID
	}
	return c.UserID
}

// FromViper 从 storage.cs 配置子树构造 Config（环境变量优先覆盖凭据）。
func FromViper(cfg *viper.Viper) (*Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("ndcs: 配置段缺失")
	}
	c := &Config{
		Host:                      strings.TrimSpace(cfg.GetString("host")),
		CDNHost:                   strings.TrimSpace(cfg.GetString("cdn-host")),
		ServerName:                strings.TrimSpace(cfg.GetString("server-name")),
		AccessKey:                 expandEnv(cfg.GetString("access-key")),
		SecretKey:                 expandEnv(cfg.GetString("secret-key")),
		UserID:                    expandEnv(cfg.GetString("user-id")),
		Scope:                     -1, // 哨兵：下面按 IsSet 区分「未配置」与「显式配 0」
		Expires:                   cfg.GetInt("expires"),
		UploadValidity:            cfg.GetInt("upload-validity"),
		Timeout:                   cfg.GetInt("timeout"),
		SignedSizeEnforced:        cfg.GetBool("signed-size-enforced"),
		UploadExpiryEnforced:      cfg.GetBool("upload-expiry-enforced"),
		WriteOnceEnforced:         cfg.GetBool("write-once-enforced"),
		SignedPathDownloadEnabled: cfg.GetBool("signed-path-download-enabled"),
	}
	if cfg.IsSet("scope") {
		c.Scope = cfg.GetInt("scope")
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// validate 必填项校验，缺失直接失败（fail fast，不静默降级）。
func (c *Config) validate() error {
	var missing []string
	if c.ServerName == "" {
		missing = append(missing, "server-name")
	}
	if c.AccessKey == "" {
		missing = append(missing, "access-key")
	}
	if c.SecretKey == "" {
		missing = append(missing, "secret-key")
	}
	if len(missing) > 0 {
		return fmt.Errorf("ndcs: 缺少必填配置: %s", strings.Join(missing, ", "))
	}
	if err := validateTransportEndpoint("host", c.GetHost()); err != nil {
		return err
	}
	if err := validateTransportEndpoint("cdn-host", c.GetCDNHost()); err != nil {
		return err
	}
	if c.Scope != 0 && c.Scope != 1 {
		return fmt.Errorf("ndcs: scope 必须显式配置为 0（私有）或 1（公开且不可撤销）")
	}
	if c.UploadValidity <= 0 || c.UploadValidity > 24*60*60 {
		return fmt.Errorf("ndcs: upload-validity 必须显式配置为经验证的上传签名有效秒数（1..86400）")
	}
	if _, err := c.userIDNumber(); err != nil {
		return err
	}
	if !c.SignedSizeEnforced {
		return fmt.Errorf("ndcs: signed-size-enforced 必须为 true；未确认服务端强制上传大小时禁止直传")
	}
	if !c.UploadExpiryEnforced {
		return fmt.Errorf("ndcs: upload-expiry-enforced 必须为 true；未确认上传在完成时强制凭证过期时禁止直传")
	}
	if !c.WriteOnceEnforced {
		return fmt.Errorf("ndcs: write-once-enforced 必须为 true；未确认对象不可覆盖时禁止直传")
	}
	if c.GetScope() == 0 && !c.SignedPathDownloadEnabled {
		return fmt.Errorf("ndcs: 私有 scope 必须启用 signed-path-download-enabled；未确认稳定路径签名下载时禁止使用")
	}
	return nil
}

func (c *Config) userIDNumber() (uint64, error) {
	raw := c.GetUserID()
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ndcs: user-id 必须是非负十进制整数")
	}
	return value, nil
}

func (c *Config) validateTransportEndpoints() error {
	if err := validateTransportEndpoint("host", c.GetHost()); err != nil {
		return err
	}
	return validateTransportEndpoint("cdn-host", c.GetCDNHost())
}

func validateTransportEndpoint(name, raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("ndcs: %s 必须是无凭据、查询串和片段的绝对 URL", name)
	}
	if parsed.Scheme == "https" {
		return nil
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if parsed.Scheme == "http" && (strings.EqualFold(host, "localhost") || (ip != nil && ip.IsLoopback())) {
		return nil
	}
	return fmt.Errorf("ndcs: %s 必须使用 HTTPS（仅 loopback 测试端点可用 HTTP）", name)
}

// expandEnv 展开 viper 未展开的 ${VAR} / ${VAR:-default} 占位符。
func expandEnv(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "${") {
		return s
	}
	return os.Expand(s, func(k string) string {
		if i := strings.Index(k, ":-"); i >= 0 {
			key, def := k[:i], k[i+2:]
			if v := os.Getenv(key); v != "" {
				return v
			}
			return def
		}
		return os.Getenv(k)
	})
}

// init 自注册到主框架 provider 注册表。
//
// 配置段约定：storage.cs（历史沿用）。
func init() {
	storage.RegisterFactory("cs", func(vp *viper.Viper) (storage.Provider, error) {
		cfg, err := FromViper(vp)
		if err != nil {
			return nil, err
		}
		return NewProvider(cfg), nil
	})
}
