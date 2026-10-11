package provider

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"sync"

	"github.com/spf13/viper"
)

// Factory Provider 工厂：按名字构造存储后端。
//
// cfg 是该 provider 段的 viper 子树（如 storage.s3.*），
// 由插件自行读取自己的配置键 —— 主框架不感知任何插件的配置结构。
type Factory func(cfg *viper.Viper) (Provider, error)

var (
	providerName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?$`)
	registryMu   sync.RWMutex
	registry     = map[string]Factory{}
)

// RegisterFactory 注册一个存储后端工厂（由插件包 init() 调用）。
//
// 名字对应 config.yaml 的 storage.provider 值。
// 重复注册视为编程错误，直接 panic（fail fast，启动期暴露）。
func RegisterFactory(name string, f Factory) {
	if !providerName.MatchString(name) || f == nil {
		panic("provider: invalid factory contribution")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("provider: 工厂 %q 重复注册", name))
	}
	registry[name] = f
}

// Build 按名字构造 Provider。vp 是已加载的根配置。
//
// 插件配置约定住在 storage.{provider 名} 段下，Build 把该子树传给工厂 ——
// 约定优于配置，主框架无需感知任何插件的配置结构。
func Build(name string, vp *viper.Viper) (Provider, error) {
	registryMu.RLock()
	f, ok := registry[name]
	registryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("不支持的 provider: %s（可用: %v）", name, Names())
	}
	sec := vp
	if name != "" && vp != nil {
		sec = vp.Sub("storage." + name)
	}
	instance, err := f(sec)
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, fmt.Errorf("provider %s returned no implementation", name)
	}
	value := reflect.ValueOf(instance)
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		if value.IsNil() {
			return nil, fmt.Errorf("provider %s returned an empty implementation", name)
		}
	}
	return instance, nil
}

// Names 返回已注册的 provider 名列表（诊断用）。
func Names() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// RefMaker 是可选接口：支持「引用型存储地址」的 Provider 实现它。
//
// 典型场景：后端文件在上传完成前拿不到持久地址（引用型后端），
// 客户端把后端返回的引用 ID 回传，服务端调 MakeRefURL 转成入库用的
// scheme://xxx 地址。LocalProvider 不实现（地址上传前即确定）。
type RefMaker interface {
	// MakeRefURL 由后端引用 ID 构造入库用的存储地址。
	MakeRefURL(refID string) string
}

// StoredURLNormalizer 是可选接口：能识别并收敛自家签名 URL 的 Provider 实现它。
//
// 客户端可能回传一条**会过期的签名下载 URL**，直接入库会导致文件先能看、
// 后 403。实现方须把签名 URL 收敛回 durable 形态；不认识的地址原样返回。
type StoredURLNormalizer interface {
	NormalizeStoredURL(stored string) string
}
