package main

import (
	"context"
	"log/slog"
	"os"
	"strings"

	ndcs "github.com/kwhitestone/nucleagent-storage-ndcs"
	"github.com/nucleagent/nucleagent-shared/logship"
)

// 本服务名，作为日志对象 key 的一层。
const logShipService = "nucleagent-im"

// logShipCSPrefix 是日志对象在 CS 上的命名空间前缀（完整路径为
// /{serverName}/{prefix}/{logship key}）。与业务命名空间分开一层，
// 便于对日志单独做保留期与访问策略。
const logShipCSPrefix = "/service-logs/"

// csUploader 把 ndcs.Provider 适配成 logship.Uploader：前缀由接入方固定，
// logship 只负责生成 key。
type csUploader struct {
	provider *ndcs.Provider
	prefix   string
}

func (u csUploader) Upload(ctx context.Context, key string, data []byte) (string, error) {
	return u.provider.UploadObject(ctx, u.prefix, key, data)
}

// startLogShipping 按需启动日志增量上送，默认关闭（见 logship.NewGate）。
// 禁用路径除一条 Warn 外无任何副作用。
func startLogShipping(ctx context.Context) {
	g := logship.NewGate()
	if !g.Enabled {
		slog.Warn("logship disabled: " + g.Reason)
		return
	}

	cfg := g.Config
	cfg.Dir = "log" // 与 config.yaml 的 zap director 一致（进程 cwd = app/src/server）
	cfg.Service = logShipService
	cfg.Instance = logShipInstance()

	up := csUploader{provider: ndcs.NewProvider(logShipCSConfig(g.CS)), prefix: logShipCSPrefix}
	shipper, err := logship.NewShipper(cfg, up, slog.Default())
	if err != nil {
		slog.Warn("logship disabled: " + err.Error())
		return
	}
	go func() {
		if err := shipper.Run(ctx); err != nil {
			slog.Warn("logship stopped", "error", err)
		}
	}()
}

// logShipCSConfig 用日志专用凭据构造 ndcs 配置。
//
// scope=0：日志是私有对象，下载必须签名。三项 *Enforced 与
// signed-path-download-enabled 是 ndcs 要求接入方对目标 CS 部署做出的
// 契约声明；这里的声明依据是运维显式配置了 LOG_SHIP_CS_*，指向的是与业务
// 直传同一套已验证的 CS 部署。
func logShipCSConfig(cs logship.CSCredentials) *ndcs.Config {
	return &ndcs.Config{
		Host:                      cs.Host,
		CDNHost:                   cs.CDNHost,
		ServerName:                cs.ServerName,
		AccessKey:                 cs.AccessKey,
		SecretKey:                 cs.SecretKey,
		UserID:                    cs.UserID,
		Scope:                     0,
		Expires:                   ndcs.DefaultExpires,
		UploadValidity:            ndcs.DefaultExpires,
		SignedSizeEnforced:        true,
		UploadExpiryEnforced:      true,
		WriteOnceEnforced:         true,
		SignedPathDownloadEnabled: true,
	}
}

// logShipInstance 返回实例 ID：LOG_SHIP_INSTANCE 优先，否则用主机短名。
func logShipInstance() string {
	if v := strings.TrimSpace(os.Getenv("LOG_SHIP_INSTANCE")); v != "" {
		return v
	}
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "unknown"
	}
	return strings.SplitN(host, ".", 2)[0]
}
