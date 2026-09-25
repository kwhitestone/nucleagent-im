package im

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	authmiddleware "github.com/kwhitestone/prism-fusion/addons/auth/middleware"
	authmodel "github.com/kwhitestone/prism-fusion/addons/auth/model"
	authservice "github.com/kwhitestone/prism-fusion/addons/auth/service"
	"github.com/kwhitestone/prism-fusion/config"
	"github.com/kwhitestone/prism-fusion/global"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// sharedAuthStack mounts the production request pipeline as initialize/router.go
// builds it: the shared auth plugin's global JWT middleware first, then im's
// scoped middlewares. Every auth assertion in this package goes through it, so
// the tests exercise the real stack rather than a hand-rolled stand-in.
func sharedAuthStack(t *testing.T, router *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	previousConfig, previousLog := global.PRISM_CONFIG, global.PRISM_LOG
	global.PRISM_CONFIG.JWT = config.JWT{
		SigningKey:               testSecret,
		ExpiresTime:              "15m",
		RefreshExpiresTime:       "24h",
		RefreshFamilyExpiresTime: "48h",
		Issuer:                   imTestIssuer,
	}
	// im consumes authorization state read-only; auth owns the RBAC control plane.
	global.PRISM_CONFIG.RBAC = config.RBAC{Provider: "disabled"}
	global.PRISM_LOG = zap.NewNop()
	t.Cleanup(func() {
		global.PRISM_CONFIG = previousConfig
		global.PRISM_LOG = previousLog
	})

	// Mirrors Initialize, reading the same list it registers. AddPublicPath is
	// append-only with no exported reset, but a repeated prefix matches
	// identically, so re-adding is harmless.
	for _, path := range publicPaths {
		authmiddleware.AddPublicPath(path)
	}

	router.Use(authmiddleware.JwtAuthMiddleware())
	router.Use(bridgeUserID(), webhookCapabilityMiddleware())
}

const imTestIssuer = "nucleagent-auth"

// issueSessionToken mints a token for userID and records the live refresh
// session the shared middleware requires, so revocation is testable.
func issueSessionToken(t *testing.T, db *gorm.DB, userID uint, sessionID string) string {
	t.Helper()
	token, err := (&authservice.JwtService{}).GenerateSessionToken(userID, "user", 0, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := db.Create(&authmodel.RefreshSession{
		UserID: userID, FamilyID: sessionID, TokenHash: sessionID,
		ExpiresAt: now.Add(24 * time.Hour), FamilyExpiresAt: now.Add(48 * time.Hour),
	}).Error; err != nil {
		t.Fatal(err)
	}
	return token
}

func authorizedRequest(t *testing.T, router http.Handler, method, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

// TestServicePrincipalCannotReachUserRoutes is a regression guard. The shared
// middleware only fences service tokens out of /auth and /rbac, so without the
// bridge's own check a service JWT (user_id 0) would reach im's user-scoped
// handlers and operate as user 0 — minting WuKong connect tokens for uid "0"
// and listing another principal's conversations. The retired JWTMiddleware
// rejected IsServicePrincipal() explicitly; the bridge must keep doing so.
func TestServicePrincipalCannotReachUserRoutes(t *testing.T) {
	db := m2DB(t)
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}

	router := gin.New()
	sharedAuthStack(t, router)
	router.POST("/api/v1/im/connect-token", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.GET(healthPath, func(c *gin.Context) { c.Status(http.StatusOK) })

	// roleID 0 makes GenerateToken stamp principal=service, matching the token
	// im itself presents to core.
	serviceToken, err := (&authservice.JwtService{}).GenerateToken(0, "nucleagent-im", 0)
	if err != nil {
		t.Fatal(err)
	}
	if code := authorizedRequest(t, router, http.MethodPost, "/api/v1/im/connect-token", serviceToken).Code; code != http.StatusUnauthorized {
		t.Fatalf("service principal on user route = %d, want 401", code)
	}
	// The guard must not break the genuinely public paths.
	if code := authorizedRequest(t, router, http.MethodGet, healthPath, "").Code; code != http.StatusOK {
		t.Fatalf("health = %d, want 200", code)
	}
}

// TestWebhookCapabilityMiddlewarePreservesAllThreeBehaviors pins the contract
// the retired JWTMiddleware used to provide for the webhook route: the
// capability is read from ?token=, stripped from RawQuery so it can never reach
// an access log or a downstream URL, and stashed for the handler.
func TestWebhookCapabilityMiddlewarePreservesAllThreeBehaviors(t *testing.T) {
	router := gin.New()
	sharedAuthStack(t, router)

	var seenCapability string
	var seenRawQuery string
	router.POST(webhookPath, func(c *gin.Context) {
		seenCapability, _ = c.Request.Context().Value(webhookCapabilityKey).(string)
		seenRawQuery = c.Request.URL.RawQuery
		c.Status(http.StatusOK)
	})

	response := authorizedRequest(t, router, http.MethodPost, webhookPath+"?token=super-secret&event=msg.notify", "")
	if response.Code != http.StatusOK {
		t.Fatalf("webhook status = %d, want 200 (route must be public)", response.Code)
	}
	if seenCapability != "super-secret" {
		t.Fatalf("capability = %q, want it stashed for the handler", seenCapability)
	}
	if strings.Contains(seenRawQuery, "super-secret") || strings.Contains(seenRawQuery, "token=") {
		t.Fatalf("RawQuery = %q, want the capability stripped", seenRawQuery)
	}
	if !strings.Contains(seenRawQuery, "event=msg.notify") {
		t.Fatalf("RawQuery = %q, want unrelated params preserved", seenRawQuery)
	}
}

// TestSharedAuthStackReplacesLocalMiddleware covers the PR-3 acceptance items
// that are assertable in-process: a revoked session is rejected immediately
// (previously honoured until expiry), a foreign issuer is rejected, and health
// stays public. Items 2 and 5 are live checks.
func TestSharedAuthStackReplacesLocalMiddleware(t *testing.T) {
	db := m2DB(t)
	if err := db.AutoMigrate(&authmodel.RefreshSession{}); err != nil {
		t.Fatal(err)
	}
	addUser(t, db, 7, authmodel.AccountTypeHuman)

	router := gin.New()
	sharedAuthStack(t, router)
	router.GET("/api/v1/im/conversation/list", func(c *gin.Context) {
		uid, _ := c.Request.Context().Value(userIDKey).(uint)
		c.JSON(http.StatusOK, gin.H{"user_id": uid})
	})
	router.GET(healthPath, func(c *gin.Context) { c.Status(http.StatusOK) })

	t.Run("health is public", func(t *testing.T) {
		if code := authorizedRequest(t, router, http.MethodGet, healthPath, "").Code; code != http.StatusOK {
			t.Fatalf("health status = %d, want 200", code)
		}
	})

	t.Run("bridge exposes user id to handlers", func(t *testing.T) {
		token := issueSessionToken(t, db, 7, "session-live")
		response := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/conversation/list", token)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d body = %s", response.Code, response.Body.String())
		}
		// The bridge is the whole point: without it the handler reads a nil uid.
		if body := response.Body.String(); body != `{"user_id":7}` {
			t.Fatalf("body = %s, want user_id 7 bridged into request context", body)
		}
	})

	t.Run("revoked session is rejected immediately", func(t *testing.T) {
		token := issueSessionToken(t, db, 7, "session-revoked")
		if err := db.Model(&authmodel.RefreshSession{}).
			Where("family_id = ?", "session-revoked").
			Update("revoked_at", time.Now()).Error; err != nil {
			t.Fatal(err)
		}
		if code := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/conversation/list", token).Code; code != http.StatusUnauthorized {
			t.Fatalf("revoked session status = %d, want 401", code)
		}
	})

	t.Run("foreign issuer is rejected", func(t *testing.T) {
		previous := global.PRISM_CONFIG.JWT.Issuer
		global.PRISM_CONFIG.JWT.Issuer = "someone-else"
		token := issueSessionToken(t, db, 7, "session-foreign-iss")
		global.PRISM_CONFIG.JWT.Issuer = previous
		if code := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/conversation/list", token).Code; code != http.StatusUnauthorized {
			t.Fatalf("foreign issuer status = %d, want 401", code)
		}
	})

	t.Run("missing token is rejected", func(t *testing.T) {
		if code := authorizedRequest(t, router, http.MethodGet, "/api/v1/im/conversation/list", "").Code; code != http.StatusUnauthorized {
			t.Fatalf("anonymous status = %d, want 401", code)
		}
	})
}
