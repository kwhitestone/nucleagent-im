package addons

import (
	// 框架 addon：auth（共享 JWT 校验中间件）+ rbac（授权解析器）。
	// im 是资源服务，不是认证控制面：config.yaml 里 auth.serve-routes=false
	// 关掉登录端点，rbac.provider=external 关掉 RBAC 管理面，只保留解析器。
	_ "github.com/kwhitestone/prism-fusion/addons"

	_ "nucleagent-im/addons/im"
)
