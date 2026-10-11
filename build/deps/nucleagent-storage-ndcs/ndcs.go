package ndcs

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	storage "github.com/kwhitestone/nucleagent-storage/contracts/provider"
)

// CSDentryPrefix 私有 CS 文件在 DB 中的存储地址前缀（按 dentryId 索引）。
const CSDentryPrefix = "cs-dentry://"

// Provider CS（cs.101.com）存储后端。
//
// 签名逻辑移植自 agentia-engine/src/service/cs/cs_storage.go，
// 去掉了对 agentia config/global 的依赖，自包含。
//
// 上传和下载仍由客户端直连 CS；只有注册阶段的完整性校验由服务端读取一次对象。
type Provider struct {
	cfg        *Config
	httpClient *http.Client
}

// NewProvider 构造 CS Provider。
func NewProvider(cfg *Config) *Provider {
	return &Provider{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: time.Duration(cfg.GetTimeout()) * time.Second,
			CheckRedirect: func(request *http.Request, via []*http.Request) error {
				if len(via) == 0 || request.URL.Scheme != via[0].URL.Scheme || request.URL.Host != via[0].URL.Host {
					return http.ErrUseLastResponse
				}
				return nil
			},
			Transport: &http.Transport{
				Proxy:       nil,
				DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			},
		},
	}
}

// Name 返回后端名。
func (p *Provider) Name() string { return "cs" }

// csDateFormat CS 签名要求的 RFC1123 GMT 时间格式。
const csDateFormat = "Mon, 02 Jan 2006 15:04:05 GMT"

// GenerateUploadToken 生成 CS 上传 token 与 base64 policy。
//
// 签名串格式："{date}\n{reqPath}\n{method}\n{policyJSON}"，HMAC-SHA1(SecretKey)。
// token 格式："{serverName}:{accessKey}:{sign}"。
//
// 参数：
//   - csFilePath CS 上的完整文件路径（/server_name/dir/file.txt）
//   - reqURLPath 请求路径（/v0.1/upload）
//   - method     HTTP 方法（POST）
//   - dateGMT    RFC1123 GMT 格式时间
func (p *Provider) GenerateUploadToken(csFilePath, reqURLPath, method, dateGMT string, size int64) (string, string, error) {
	uid, err := p.cfg.userIDNumber()
	if err != nil {
		return "", "", err
	}
	policy := map[string]interface{}{
		"path":       csFilePath,
		"uid":        uid,
		"role":       "admin",
		"policyType": "upload",
		"scope":      p.cfg.GetScope(),
		"size":       size,
	}
	return p.sign(policy, dateGMT, reqURLPath, method)
}

// GenerateDownloadToken 生成 CS 私有文件下载 token 与 base64 policy。
//
// 与上传的差异：签名串首段是过期毫秒时间戳（而非日期），policyType=download，
// 请求路径带 dentryId 查询串。
func (p *Provider) GenerateDownloadToken(dentryID string, expireAtMs int64) (string, string, error) {
	uid, err := p.cfg.userIDNumber()
	if err != nil {
		return "", "", err
	}
	policy := map[string]interface{}{
		"dentryId":   dentryID,
		"uid":        uid,
		"role":       "admin",
		"policyType": "download",
	}
	reqURLPath := "/v0.1/download?dentryId=" + dentryID
	return p.sign(policy, fmt.Sprintf("%d", expireAtMs), reqURLPath, "GET")
}

// sign 计算 HMAC-SHA1 签名，返回 (token, policyBase64)。
//
// policy 的 JSON 必须与签名串里用的完全一致（同一份 marshal 结果），
// 否则服务端重算签名会不匹配。
func (p *Provider) sign(policy map[string]interface{}, first, reqURLPath, method string) (string, string, error) {
	policyBytes, err := json.Marshal(policy)
	if err != nil {
		return "", "", fmt.Errorf("marshal CS signing policy: %w", err)
	}
	policyStr := string(policyBytes)
	// URL-safe base64 且不带 padding（对齐 CS 服务端的 replace('=','')）。
	policyBase64 := base64.RawURLEncoding.EncodeToString(policyBytes)

	signSource := fmt.Sprintf("%s\n%s\n%s\n%s", first, reqURLPath, method, policyStr)

	mac := hmac.New(sha1.New, []byte(p.cfg.SecretKey))
	mac.Write([]byte(signSource))
	sign := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	token := fmt.Sprintf("%s:%s:%s", p.cfg.ServerName, p.cfg.AccessKey, sign)
	return token, policyBase64, nil
}

// CSFilePath 把命名空间前缀与对象 key 拼成 CS 上的完整路径。
//
// 格式：/{serverName}/{prefix}/{key}
// 例如：/nucleagent/nucleagent/core/2026/08/{fileId}/report.pdf
func (p *Provider) CSFilePath(prefix, key string) string {
	return normalizeSlashes("/" + p.cfg.ServerName + storage.JoinPrefix(prefix, key))
}

// buildUploadURL 构造带 token/date/policy 查询串的 CS 上传地址。
//
// 对齐 agentia 的 cs_storage.go:buildUploadURL —— 差别只在我们不发请求，
// 把 URL 交给客户端直传。
func (p *Provider) buildUploadURL(csFilePath, dateGMT string, size int64) (string, error) {
	token, policyBase64, err := p.GenerateUploadToken(csFilePath, csUploadReqPath, "POST", dateGMT, size)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/upload?token=%s&date=%s&policy=%s",
		p.cfg.GetHost(),
		url.QueryEscape(token),
		url.QueryEscape(dateGMT),
		policyBase64,
	), nil
}

// csUploadReqPath CS 上传接口的签名请求路径（固定值，与 host 无关）。
//
// 注意：它必须是 /v0.1/upload 这个**签名用**的路径，不能从 cfg.Host 推导 ——
// 服务端就是按这个常量重算签名的。
const csUploadReqPath = "/v0.1/upload"

// PresignUpload 生成 CS 上传凭证。客户端拿到后直接 POST multipart 到 URL。
func (p *Provider) PresignUpload(_ context.Context, prefix, key, _ string, size int64) (*storage.UploadCredential, error) {
	if err := p.cfg.validateTransportEndpoints(); err != nil {
		return nil, err
	}
	if _, err := p.cfg.userIDNumber(); err != nil {
		return nil, err
	}
	if !p.cfg.SignedSizeEnforced {
		return nil, fmt.Errorf("ndcs: signed upload size enforcement is not verified")
	}
	if !p.cfg.UploadExpiryEnforced {
		return nil, fmt.Errorf("ndcs: upload credential expiry enforcement is not verified")
	}
	if p.cfg.GetUploadValidity() <= 0 || p.cfg.GetUploadValidity() > 24*60*60 {
		return nil, fmt.Errorf("ndcs: verified upload credential lifetime is not configured")
	}
	if !p.cfg.WriteOnceEnforced {
		return nil, fmt.Errorf("ndcs: write-once object enforcement is not verified")
	}
	if p.cfg.GetScope() == 0 && !p.cfg.SignedPathDownloadEnabled {
		return nil, fmt.Errorf("ndcs: signed private path download is not verified")
	}
	if size <= 0 {
		return nil, fmt.Errorf("ndcs: upload size must be positive")
	}
	full := storage.JoinPrefix(prefix, key)
	csFilePath := p.CSFilePath(prefix, key)
	csDir := csFilePath[:strings.LastIndex(csFilePath, "/")]
	if csDir == "" {
		csDir = "/"
	}
	fileName := csFilePath[strings.LastIndex(csFilePath, "/")+1:]

	now := time.Now().UTC()
	dateGMT := now.Format(csDateFormat)
	uploadURL, err := p.buildUploadURL(csFilePath, dateGMT, size)
	if err != nil {
		return nil, err
	}

	cred := &storage.UploadCredential{
		Method:    "POST",
		URL:       uploadURL,
		FileField: "filename",
		FormFields: map[string]string{
			"path":  csDir,
			"name":  fileName,
			"scope": fmt.Sprintf("%d", p.cfg.GetScope()),
			"size":  fmt.Sprintf("%d", size),
		},
		ExpiresAt: now.Add(time.Duration(p.cfg.GetUploadValidity()) * time.Second).UnixMilli(),
	}

	// Public and private objects both receive the server-derived stable path.
	// Private downloads are signed later; registration never trusts a client
	// supplied dentry ID to choose a different object.
	cred.StoredURL = p.staticStoredURL(full)

	return cred, nil
}

// PresignDownload 把存储地址转成客户端可直接 GET 的 URL。
//
//   - cs-dentry://xxx → 生成带 token/policy/expireAt 的 CDN 签名 URL
//   - 私有稳定路径 → 生成绑定 path 的签名 URL
//   - 其它（已是 http(s) 公开地址）→ 原样返回
func (p *Provider) PresignDownload(_ context.Context, storedURL string) (string, error) {
	if err := p.cfg.validateTransportEndpoints(); err != nil {
		return "", err
	}
	if _, err := p.cfg.userIDNumber(); err != nil {
		return "", err
	}
	if storedURL == "" {
		return "", fmt.Errorf("存储地址为空，无法生成下载链接")
	}
	if full, ok := p.logicalPathFromStoredURL(storedURL); ok && p.cfg.GetScope() == 0 {
		return p.presignPathDownload(full)
	}
	dentryID, ok := ParseDentryURL(storedURL)
	if !ok {
		return storedURL, nil
	}

	expireAtMs := time.Now().Add(time.Duration(p.cfg.GetExpires()) * time.Second).UnixMilli()
	token, policyBase64, err := p.GenerateDownloadToken(dentryID, expireAtMs)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/download?dentryId=%s&token=%s&policy=%s&expireAt=%d",
		p.cfg.GetCDNHost(),
		url.QueryEscape(dentryID),
		url.QueryEscape(token),
		policyBase64,
		expireAtMs,
	), nil
}

// UploadObject 由服务端直传字节到 CS，返回可直接交给 PresignDownload 回读的存储地址。
//
// 与 PresignUpload 的关系：复用同一份凭证、同一套对象命名与校验 —— 差别只在
// 由本进程充当那个「客户端」，把 multipart 请求发出去。服务文件日志上送
// （logship）这类服务端已持有字节流的场景用它，不必把凭证下发到进程外。
//
// 返回值就是 PresignUpload 绑定的服务端推导路径（cred.StoredURL），
// 不采信 CS 响应里的 dentry_id —— 与注册链路保持同一条信任边界。
func (p *Provider) UploadObject(ctx context.Context, prefix, key string, data []byte) (string, error) {
	// 空对象在这里被 PresignUpload 的 size > 0 检查挡掉：CS 的签名 policy 以
	// size 为准，签一个 0 字节的 policy 无法与服务端的强制校验自洽。
	cred, err := p.PresignUpload(ctx, prefix, key, "", int64(len(data)))
	if err != nil {
		return "", err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for field, value := range cred.FormFields {
		if err := writer.WriteField(field, value); err != nil {
			return "", fmt.Errorf("write CS upload field %q: %w", field, err)
		}
	}
	part, err := writer.CreateFormFile(cred.FileField, cred.FormFields["name"])
	if err != nil {
		return "", fmt.Errorf("create CS upload file part: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return "", fmt.Errorf("write CS upload body: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("finalize CS upload body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, cred.Method, cred.URL, &body)
	if err != nil {
		return "", fmt.Errorf("create CS upload request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload CS object: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return "", fmt.Errorf("upload CS object returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	return cred.StoredURL, nil
}

// CanReleaseMissingObject implements provider.MissingObjectReleaseGuard.
// Configuration is fail-closed unless the target CS deployment has been
// verified to reject uploads that finish after their signed lifetime.
func (p *Provider) CanReleaseMissingObject(_ context.Context, _ string, expiresAt time.Time) (bool, error) {
	verified := p.cfg.UploadExpiryEnforced && p.cfg.GetUploadValidity() > 0 && p.cfg.GetUploadValidity() <= 24*60*60
	return verified && !time.Now().Before(expiresAt), nil
}

// Delete CS 侧删除。
//
// CS 未开放本服务可用的删除签名接口，故此处不支持 —— 上层只做 DB 软删除。
// 返回 storage.ErrNotSupported 让调用方显式处理，不静默假装成功。
func (p *Provider) Delete(_ context.Context, _ string) error {
	return storage.ErrNotSupported
}

func (p *Provider) staticStoredURL(full string) string {
	base := strings.TrimRight(p.cfg.GetCDNHost(), "/")
	pathValue := normalizeSlashes("/" + p.cfg.ServerName + full)
	return base + "/static" + escapeCSPath(pathValue)
}

func (p *Provider) logicalPathFromStoredURL(storedURL string) (string, bool) {
	stored, err := url.Parse(storedURL)
	if err != nil || stored.RawQuery != "" || stored.Fragment != "" {
		return "", false
	}
	base, err := url.Parse(strings.TrimRight(p.cfg.GetCDNHost(), "/"))
	if err != nil || stored.Scheme != base.Scheme || stored.Host != base.Host {
		return "", false
	}
	prefix := strings.TrimRight(base.Path, "/") + "/static/" + p.cfg.ServerName
	if !strings.HasPrefix(stored.Path, prefix+"/") {
		return "", false
	}
	return strings.TrimPrefix(stored.Path, prefix), true
}

func (p *Provider) presignPathDownload(full string) (string, error) {
	csFilePath := normalizeSlashes("/" + p.cfg.ServerName + full)
	reqURLPath := "/v0.1/static" + csFilePath
	expiresAt := time.Now().Add(time.Duration(p.cfg.GetExpires()) * time.Second).UnixMilli()
	uid, err := p.cfg.userIDNumber()
	if err != nil {
		return "", err
	}
	policy := map[string]interface{}{
		"path": csFilePath, "uid": uid, "role": "admin", "policyType": "download",
	}
	token, policyBase64, err := p.sign(policy, fmt.Sprintf("%d", expiresAt), reqURLPath, http.MethodGet)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/static%s?token=%s&expireAt=%d&policy=%s",
		strings.TrimRight(p.cfg.GetCDNHost(), "/"), escapeCSPath(csFilePath),
		url.QueryEscape(token), expiresAt, policyBase64,
	), nil
}

// Inspect downloads the exact server-derived object and computes authoritative
// metadata for storage registration. Redirects cannot leave the configured CS
// endpoint, preventing a compromised response from turning this into SSRF.
func (p *Provider) Inspect(ctx context.Context, storedURL string) (*storage.ObjectInfo, error) {
	downloadURL, err := p.PresignDownload(ctx, storedURL)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(downloadURL)
	if err != nil || !p.allowedDownloadEndpoint(parsed) {
		return nil, fmt.Errorf("CS inspect URL is outside the configured endpoint")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create CS inspect request: %w", err)
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("inspect CS object: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		return nil, storage.ErrObjectNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("inspect CS object returned HTTP %d", resp.StatusCode)
	}
	if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return nil, fmt.Errorf("inspect CS object returned unsupported Content-Encoding %q", encoding)
	}
	hasher := sha256.New()
	written, err := io.Copy(hasher, io.LimitReader(resp.Body, (1<<30)+1))
	if err != nil {
		return nil, fmt.Errorf("hash CS object: %w", err)
	}
	if written > 1<<30 {
		return nil, fmt.Errorf("CS object exceeds inspection limit")
	}
	return &storage.ObjectInfo{Size: written, SHA256: fmt.Sprintf("%x", hasher.Sum(nil))}, nil
}

func (p *Provider) allowedDownloadEndpoint(candidate *url.URL) bool {
	if candidate == nil || candidate.User != nil || (candidate.Scheme != "http" && candidate.Scheme != "https") {
		return false
	}
	for _, raw := range []string{p.cfg.GetCDNHost(), p.cfg.GetHost()} {
		configured, err := url.Parse(raw)
		if err == nil && candidate.Scheme == configured.Scheme && candidate.Host == configured.Host {
			return true
		}
	}
	return false
}

func escapeCSPath(rawPath string) string {
	segments := strings.Split(rawPath, "/")
	for index, segment := range segments {
		segments[index] = strings.ReplaceAll(url.PathEscape(segment), "+", "%2B")
	}
	return strings.Join(segments, "/")
}

// CSUploadResponse CS 上传接口的响应体。
//
// 字段名对齐 CS 服务端（snake_case），与 agentia 的 CSUploadResponse 一致。
// 客户端直传 CS 后拿到这个 JSON。DentryID 仅用于兼容旧记录；新记录在
// presign 阶段已绑定服务端推导的稳定路径，不接受客户端用它改写对象地址。
type CSUploadResponse struct {
	Path     string `json:"path"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	DentryID string `json:"dentry_id"`
}

// ExtractDentryID 从 CS 上传响应中提取 dentryId 并转成入库用的 cs-dentry:// 地址。
//
// 仅供历史客户端/记录迁移使用。兼容两种字段名：CS 正式响应用 dentry_id，
// 部分网关/旧版本回 dentryId。
func ExtractDentryID(raw []byte) (string, error) {
	var probe map[string]interface{}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return "", fmt.Errorf("解析 CS 上传响应失败: %w", err)
	}
	for _, k := range []string{"dentry_id", "dentryId"} {
		if v, ok := probe[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s, nil
			}
		}
	}
	return "", fmt.Errorf("CS 上传响应中缺少 dentry_id")
}

// MakeDentryURL 由 dentryId 构造入库用的 cs-dentry:// 地址。
func MakeDentryURL(dentryID string) string {
	return CSDentryPrefix + dentryID
}

// MakeRefURL 实现 storage.RefMaker：主框架注册接口把 refId 转成入库地址。
func (p *Provider) MakeRefURL(refID string) string { return MakeDentryURL(refID) }

// NormalizeStoredURL 实现 storage.StoredURLNormalizer：把签名的下载 URL 收敛回 durable 形态。
func (p *Provider) NormalizeStoredURL(stored string) string { return NormalizeStoredURL(stored) }

// NormalizeStoredURL 把客户端回传的存储地址收敛成可持久化的形态。
//
// 客户端可能回传三种东西，只有前两种能安全入库：
//  1. cs-dentry://xxx        —— 已是durable 形态，原样保留
//  2. 一个**签名的** CS 下载 URL —— 里面的 token 会过期，必须抽出 dentryId 收敛回
//     cs-dentry://，否则几小时后该文件永久 403
//  3. 其它 http(s) 公开地址   —— 原样保留
//
// 这是 agentia 踩过的坑（见 cs_storage.go:NormalizeStoredIcon）：直接存签名 URL
// 会导致文件先能看、后失效。任何可能写入 stored_url 的路径都要过这个函数。
func NormalizeStoredURL(stored string) string {
	if stored == "" {
		return stored
	}
	if _, ok := ParseDentryURL(stored); ok {
		return stored
	}
	if !strings.HasPrefix(stored, "http://") && !strings.HasPrefix(stored, "https://") {
		return stored
	}
	u, err := url.Parse(stored)
	if err != nil {
		return stored
	}
	// CS 下载 URL 把 durable 的 dentryId 放在查询串里。按 dentryId 检测比匹配
	// 路径更稳 —— 路径随 CS host 配置变化（/download 或 /v0.1/download）。
	if id := u.Query().Get("dentryId"); id != "" {
		return MakeDentryURL(id)
	}
	return stored
}

// ParseDentryURL 从 cs-dentry:// 地址提取 dentryId。非 dentry 地址返回 ("", false)。
func ParseDentryURL(storedURL string) (string, bool) {
	if strings.HasPrefix(storedURL, CSDentryPrefix) {
		return strings.TrimPrefix(storedURL, CSDentryPrefix), true
	}
	return "", false
}

// normalizeSlashes 压掉路径中的重复斜杠，但保留协议头的 "://"。
func normalizeSlashes(s string) string {
	scheme := ""
	if i := strings.Index(s, "://"); i >= 0 {
		scheme = s[:i+3]
		s = s[i+3:]
	}
	for strings.Contains(s, "//") {
		s = strings.ReplaceAll(s, "//", "/")
	}
	return scheme + s
}
