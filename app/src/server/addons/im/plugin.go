package im

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/gin-gonic/gin"
	authservice "github.com/kwhitestone/prism-fusion/addons/auth/service"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/kwhitestone/prism-fusion/plugin"
)

type Plugin struct {
	plugin.BasePlugin
	signer *tokenSigner
	wsAddr string
}

type contextKey int

const userIDKey contextKey = 1

type envelope[T any] struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

type HealthData struct {
	Service string `json:"service"`
	Status  string `json:"status"`
}

type ConnectData struct {
	UID    string `json:"uid"`
	Token  string `json:"token"`
	WSAddr string `json:"wsAddr"`
}

type HealthOutput struct {
	Body any
}

type ConnectOutput struct {
	Body any
}

var defaultPlugin = &Plugin{BasePlugin: plugin.BasePlugin{
	PluginName: "im", PluginDescription: "NucleAgent IM authentication bridge",
}}

func init() {
	plugin.Register(defaultPlugin)
}

func (p *Plugin) RoutePrefix() string { return "/api/v1/im" }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		APIVersion: plugin.APIVersionV2,
		ID:         p.Name(),
		Version:    "0.1.0",
		Kind:       plugin.KindBackendAddon,
		RouteScopes: []string{
			"/api/v1/im",
		},
	}
}

func (p *Plugin) Initialize(context.Context) error {
	if err := authservice.ValidateTokenConfiguration(); err != nil {
		return err
	}
	signer, err := newTokenSigner(configValue("IM_CONNECT_TOKEN_SECRET", "im.connect-token-secret", ""))
	if err != nil {
		return err
	}
	wsAddr := configValue("WUKONGIM_WS_ADDR", "wukongim.ws-addr", "ws://127.0.0.1:26652")
	parsed, err := url.Parse(wsAddr)
	if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" {
		return errors.New("wukongim.ws-addr must be a valid ws or wss URL")
	}
	p.signer = signer
	p.wsAddr = wsAddr
	return nil
}

func (p *Plugin) Middlewares() []gin.HandlerFunc {
	return []gin.HandlerFunc{JWTMiddleware()}
}

func (p *Plugin) RegisterRoutes(api huma.API) {
	huma.Register(api, huma.Operation{
		OperationID: "imHealth",
		Method:      http.MethodGet,
		Path:        "/api/v1/im/health",
		Summary:     "Check IM bridge health",
		Tags:        []string{"IM"},
	}, func(context.Context, *struct{}) (*HealthOutput, error) {
		return &HealthOutput{Body: envelope[HealthData]{
			Code: 0, Message: "success",
			Data: HealthData{Service: "nucleagent-im", Status: "ok"},
		}}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "imConnectToken",
		Method:      http.MethodPost,
		Path:        "/api/v1/im/connect-token",
		Summary:     "Mint a WuKongIM connect token",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, _ *struct{}) (*ConnectOutput, error) {
		uid := strconv.FormatUint(uint64(ctx.Value(userIDKey).(uint)), 10)
		token, err := p.signer.Mint(uid, time.Now())
		if err != nil {
			return nil, huma.NewError(http.StatusInternalServerError, "failed to mint connect token")
		}
		return &ConnectOutput{Body: envelope[ConnectData]{
			Code: 0, Message: "success",
			Data: ConnectData{UID: uid, Token: token, WSAddr: p.wsAddr},
		}}, nil
	})
}

func JWTMiddleware() gin.HandlerFunc {
	jwtService := &authservice.JwtService{}
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodGet && c.Request.URL.Path == "/api/v1/im/health" {
			c.Next()
			return
		}
		raw, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		if !ok || strings.TrimSpace(raw) == "" {
			abortUnauthorized(c, "authentication token required")
			return
		}
		claims, err := jwtService.ParseAccessToken(raw)
		if err != nil || claims.UserID == 0 || claims.IsServicePrincipal() || claims.SessionID == "" {
			abortUnauthorized(c, "invalid or expired authentication token")
			return
		}
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), userIDKey, claims.UserID))
		c.Next()
	}
}

func abortUnauthorized(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, envelope[any]{
		Code: http.StatusUnauthorized, Message: message, Data: nil,
	})
}

func configValue(envName, key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(envName)); value != "" {
		return value
	}
	value := strings.TrimSpace(global.PRISM_VP.GetString(key))
	if value == "" || strings.HasPrefix(value, "${") {
		return fallback
	}
	return value
}
