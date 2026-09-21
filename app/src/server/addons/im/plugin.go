package im

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/gin-gonic/gin"
	authservice "github.com/kwhitestone/prism-fusion/addons/auth/service"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/kwhitestone/prism-fusion/plugin"
)

type Plugin struct {
	plugin.BasePlugin
	signer              *tokenSigner
	apiAddr             string
	wsAddr              string
	coreURL             string
	webhookCapability   string
	serviceJWT          string
	wuKongAdminUser     string
	wuKongAdminPassword string
	webhookAdmission    webhookAdmission
	cancel              context.CancelFunc
	done                chan struct{}
	wg                  sync.WaitGroup
}

type contextKey int

const (
	userIDKey            contextKey = 1
	webhookCapabilityKey contextKey = 2
)

var (
	capabilityLogPattern = regexp.MustCompile(`([?&]token=)[^&\s"]+`)
	installLogRedaction  sync.Once
)

type capabilityRedactingWriter struct {
	io.Writer
}

func (w capabilityRedactingWriter) Write(data []byte) (int, error) {
	_, err := w.Writer.Write(capabilityLogPattern.ReplaceAll(data, []byte(`${1}<redacted>`)))
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

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

type ConversationListInput struct {
	Body struct {
		Cursor            string `json:"cursor,omitempty"`
		Limit             int    `json:"limit,omitempty"`
		CompletedCoverage int64  `json:"completed_coverage,omitempty"`
	}
}

type MessageSyncInput struct {
	Body struct {
		ChannelID       string `json:"channel_id" required:"true"`
		ChannelType     uint8  `json:"channel_type" minimum:"1"`
		StartMessageSeq uint64 `json:"start_message_seq,omitempty"`
		EndMessageSeq   uint64 `json:"end_message_seq,omitempty"`
		PullMode        int    `json:"pull_mode,omitempty"`
		Limit           int    `json:"limit,omitempty"`
		StreamV2        int    `json:"stream_v2,omitempty"`
	}
}

type ProxyOutput struct {
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
	installLogRedaction.Do(func() {
		gin.DefaultWriter = capabilityRedactingWriter{Writer: gin.DefaultWriter}
		gin.DefaultErrorWriter = capabilityRedactingWriter{Writer: gin.DefaultErrorWriter}
	})
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
	apiAddr := configValue("WUKONGIM_API_ADDR", "wukongim.api-addr", "http://127.0.0.1:26651")
	parsed, err = url.Parse(apiAddr)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("wukongim.api-addr must be a valid http or https URL")
	}
	p.signer = signer
	p.apiAddr = strings.TrimRight(apiAddr, "/")
	p.wsAddr = wsAddr
	p.webhookCapability = configValue("IM_WEBHOOK_CAPABILITY", "im.webhook-capability", "")
	if len(p.webhookCapability) < 32 || strings.Contains(p.webhookCapability, "${") {
		return errors.New("IM_WEBHOOK_CAPABILITY must be a non-placeholder secret of at least 32 bytes")
	}
	coreURL := configValue("CORE_URL", "im.core-url", "http://127.0.0.1:26653")
	parsed, err = url.Parse(coreURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("CORE_URL must be a valid http or https URL")
	}
	p.coreURL = strings.TrimRight(coreURL, "/")
	p.serviceJWT = configValue("IM_SERVICE_JWT", "im.service-jwt", "")
	if p.serviceJWT != "" {
		claims, parseErr := (&authservice.JwtService{}).ParseAccessToken(p.serviceJWT)
		if parseErr != nil || !claims.IsServicePrincipal() || claims.Username != "nucleagent-im" {
			return errors.New("IM_SERVICE_JWT must identify service nucleagent-im")
		}
	}
	p.wuKongAdminUser = configValue("IM_WUKONG_ADMIN_USER", "im.wukong-admin-user", "")
	p.wuKongAdminPassword = configValue("IM_WUKONG_ADMIN_PASS", "im.wukong-admin-pass", "")
	if (p.wuKongAdminUser == "") != (p.wuKongAdminPassword == "") {
		return errors.New("IM_WUKONG_ADMIN_USER and IM_WUKONG_ADMIN_PASS must be configured together")
	}
	return nil
}

func (p *Plugin) Middlewares() []gin.HandlerFunc {
	return []gin.HandlerFunc{JWTMiddleware()}
}

func (p *Plugin) Models() []interface{} {
	return []interface{}{
		&IMWebhookInbox{},
		&IMCoreConversationMap{},
		&IMGroup{},
		&IMGroupAgentAllowlist{},
		&IMRateWindow{},
	}
}

func (p *Plugin) Start(ctx context.Context) error {
	if global.PRISM_DB == nil {
		return errors.New("IM database is not initialized")
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.done = make(chan struct{})
	go p.runWorker(ctx)
	return nil
}

func (p *Plugin) Stop(ctx context.Context) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.done != nil {
		select {
		case <-p.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.wg.Wait()
	return nil
}

func (p *Plugin) RegisterRoutes(api huma.API) {
	p.registerWebhook(api)
	p.registerAgentStreams(api)

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
		if err := registerToken(ctx, p.apiAddr, uid, token); err != nil {
			return nil, huma.NewError(http.StatusBadGateway, "failed to register connect token")
		}
		return &ConnectOutput{Body: envelope[ConnectData]{
			Code: 0, Message: "success",
			Data: ConnectData{UID: uid, Token: token, WSAddr: p.wsAddr},
		}}, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "imConversationList",
		Method:      http.MethodPost,
		Path:        "/api/v1/im/conversation/list",
		Summary:     "List WuKongIM conversations",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, input *ConversationListInput) (*ProxyOutput, error) {
		body := map[string]any{
			"uid":                strconv.FormatUint(uint64(ctx.Value(userIDKey).(uint)), 10),
			"cursor":             input.Body.Cursor,
			"limit":              input.Body.Limit,
			"completed_coverage": input.Body.CompletedCoverage,
		}
		return p.proxy(ctx, "/conversation/list", body)
	})

	huma.Register(api, huma.Operation{
		OperationID: "imChannelMessageSync",
		Method:      http.MethodPost,
		Path:        "/api/v1/im/channel/messagesync",
		Summary:     "Sync WuKongIM channel messages",
		Tags:        []string{"IM"},
		Security:    []map[string][]string{{"AuthTokenAuth": {}}},
	}, func(ctx context.Context, input *MessageSyncInput) (*ProxyOutput, error) {
		body := map[string]any{
			"login_uid":         strconv.FormatUint(uint64(ctx.Value(userIDKey).(uint)), 10),
			"channel_id":        input.Body.ChannelID,
			"channel_type":      input.Body.ChannelType,
			"start_message_seq": input.Body.StartMessageSeq,
			"end_message_seq":   input.Body.EndMessageSeq,
			"pull_mode":         input.Body.PullMode,
			"limit":             input.Body.Limit,
			"stream_v2":         input.Body.StreamV2,
		}
		return p.proxy(ctx, "/channel/messagesync", body)
	})
}

func registerToken(ctx context.Context, apiAddr, uid, token string) error {
	return postWuKong(ctx, apiAddr, "/user/token", map[string]any{
		"uid": uid, "token": token, "device_flag": 1, "device_level": 1,
	}, nil)
}

func (p *Plugin) proxy(ctx context.Context, path string, body any) (*ProxyOutput, error) {
	var output any
	if err := postWuKong(ctx, p.apiAddr, path, body, &output); err != nil {
		return nil, huma.NewError(http.StatusBadGateway, "wukongim request failed")
	}
	return &ProxyOutput{Body: output}, nil
}

func postWuKong(ctx context.Context, apiAddr, path string, input, output any) error {
	return postWuKongWithAuth(ctx, apiAddr, path, "", "", input, output)
}

func postWuKongWithAuth(ctx context.Context, apiAddr, path, username, password string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, apiAddr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if username != "" || password != "" {
		request.SetBasicAuth(username, password)
	}
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		return &httpStatusError{
			status: response.StatusCode,
			code:   "wukong_http_" + strconv.Itoa(response.StatusCode),
		}
	}
	if output == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	return json.NewDecoder(response.Body).Decode(output)
}

func JWTMiddleware() gin.HandlerFunc {
	jwtService := &authservice.JwtService{}
	return func(c *gin.Context) {
		if c.Request.Method == http.MethodGet && c.Request.URL.Path == "/api/v1/im/health" {
			c.Next()
			return
		}
		if c.Request.Method == http.MethodPost && c.Request.URL.Path == "/api/v1/im/webhooks/wukong" {
			query := c.Request.URL.Query()
			capability := query.Get("token")
			query.Del("token")
			c.Request.URL.RawQuery = query.Encode()
			c.Request = c.Request.WithContext(context.WithValue(
				c.Request.Context(), webhookCapabilityKey, capability,
			))
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
