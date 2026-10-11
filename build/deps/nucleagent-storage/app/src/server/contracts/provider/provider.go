// Package provider 存储后端抽象。
//
// 核心原则：**storage 服务不代理文件字节流**。Provider 只负责生成凭证/签名 URL，
// 客户端拿到后直连存储后端（CS/CDN）上传下载，绝不走 io.Copy 中转。
//
// 内置实现：
//   - LocalProvider —— 本地磁盘（默认），HMAC-SHA256 签名，客户端 PUT 到本服务的 /blob 端点
//
// 其它后端（对象存储、私有网关等）通过 RegisterFactory 注册为插件，
// 配置住在 storage.{provider 名} 段下，见 registry.go。
package provider

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotSupported 表示该 Provider 不支持此操作。
	ErrNotSupported = errors.New("provider: 不支持的操作")
	// ErrObjectNotFound 表示 Provider 已权威确认目标对象当前不存在。
	ErrObjectNotFound = errors.New("provider: 对象不存在")
)

// UploadCredential 上传凭证：客户端凭此直传存储后端。
//
// Method + URL + Headers + FormFields 共同描述一次完整的上传请求：
//   - 表单型后端：Method=POST，URL 带签名查询串，FormFields 为 multipart 表单字段
//   - Local：Method=PUT，URL 带 sig/expires 查询串，无表单字段
type UploadCredential struct {
	// Method 上传使用的 HTTP 方法（POST / PUT）。
	Method string `json:"method" example:"POST"`
	// URL 上传目标地址（已含签名查询参数）。
	URL string `json:"url"`
	// Headers 上传请求需要携带的 header。
	Headers map[string]string `json:"headers,omitempty"`
	// FormFields multipart 表单字段（CS 上传必填；Local 为空）。
	FormFields map[string]string `json:"formFields,omitempty"`
	// FileField 文件内容在 multipart 中的字段名（表单型后端如 filename）。
	FileField string `json:"fileField,omitempty"`
	// StoredURL 上传成功后应回填的存储地址。
	//
	// 引用型后端上传前拿不到持久地址，此处为空，
	// 客户端需用后端返回的引用 ID 调 POST /api/v1/files 注册（见 RefMaker）。
	// Local 上传前即可确定路径，此处直接给出 file:// 地址。
	StoredURL string `json:"storedUrl,omitempty"`
	// ExpiresAt 凭证过期的 Unix 毫秒时间戳。
	ExpiresAt int64 `json:"expiresAt"`
}

// Provider 存储后端接口。
//
// 三个方法都只做「算签名 / 删元数据」，不搬运文件字节。
type Provider interface {
	// Name 返回后端名（local / cs），用于日志与健康检查。
	Name() string

	// PresignUpload 生成上传凭证，客户端凭此直传存储后端。
	//
	// 参数：
	//   - prefix      命名空间前缀（/nucleagent/core/），由调用方从配置解析
	//   - key         对象相对路径（不含 prefix），已做过路径安全清洗
	//   - contentType 文件 MIME 类型
	//   - size        文件字节数（-1 表示未知）
	PresignUpload(ctx context.Context, prefix, key, contentType string, size int64) (*UploadCredential, error)

	// PresignDownload 把存储地址转换成客户端可直接 GET 的签名 URL。
	//
	// storedURL 是入库的存储地址（后端自定义 scheme 或 file:///path）。
	PresignDownload(ctx context.Context, storedURL string) (string, error)

	// Delete 删除后端上的文件。后端不支持时返回 ErrNotSupported。
	Delete(ctx context.Context, storedURL string) error
}

// ObjectInfo 是存储后端直接读取并校验出的对象元数据，不能来自客户端声明。
type ObjectInfo struct {
	Size   int64
	SHA256 string
}

// ObjectInspector 是可选的服务端完整性校验能力。内置 local provider 必须实现；
// 其它 provider 应在能可靠读取对象长度与摘要时实现，Register 会优先使用它。
type ObjectInspector interface {
	Inspect(ctx context.Context, storedURL string) (*ObjectInfo, error)
}

// MissingObjectReleaseGuard is an optional capability for providers that do
// not support deletion. Returning true attests that an upload credential is
// enforced at completion time, so an object that is missing after expiresAt
// cannot materialize later from an already-open request.
type MissingObjectReleaseGuard interface {
	CanReleaseMissingObject(ctx context.Context, storedURL string, expiresAt time.Time) (bool, error)
}
