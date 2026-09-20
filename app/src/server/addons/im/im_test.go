package im

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kwhitestone/prism-fusion/config"
	"github.com/kwhitestone/prism-fusion/global"
)

const testSecret = "0123456789abcdef0123456789abcdef"

func TestConnectTokenRoundTrip(t *testing.T) {
	signer, err := newTokenSigner(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	token, err := signer.Mint("42", now)
	if err != nil {
		t.Fatal(err)
	}
	uid, err := signer.Verify(token, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if uid != "42" {
		t.Fatalf("uid = %q, want 42", uid)
	}
}

func TestTamperedConnectTokenRejected(t *testing.T) {
	signer, _ := newTokenSigner(testSecret)
	token, err := signer.Mint("42", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signer.Verify(token+"x", time.Now()); err == nil {
		t.Fatal("tampered token was accepted")
	}
}

func TestExpiredConnectTokenRejected(t *testing.T) {
	signer, _ := newTokenSigner(testSecret)
	now := time.Unix(1_700_000_000, 0)
	token, err := signer.mint("42", now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := signer.Verify(token, now); err == nil {
		t.Fatal("expired token was accepted")
	}
}

func TestBadJWTRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previous := global.PRISM_CONFIG
	global.PRISM_CONFIG.JWT = config.JWT{SigningKey: testSecret}
	t.Cleanup(func() { global.PRISM_CONFIG = previous })

	router := gin.New()
	router.Use(JWTMiddleware())
	router.POST("/api/v1/im/connect-token", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/im/connect-token", nil)
	request.Header.Set("Authorization", "Bearer not-a-jwt")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
	for _, field := range []string{`"code":401`, `"message":`, `"data":null`} {
		if !strings.Contains(response.Body.String(), field) {
			t.Fatalf("response %q missing %s", response.Body.String(), field)
		}
	}
}
