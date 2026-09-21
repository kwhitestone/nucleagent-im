package im

import (
	"encoding/json"
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

func TestRegisterToken(t *testing.T) {
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/user/token" {
			t.Fatalf("path = %q, want /user/token", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := registerToken(t.Context(), server.URL, "42", "token-42"); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]any{
		"uid": "42", "token": "token-42", "device_flag": float64(1), "device_level": float64(1),
	} {
		if received[field] != want {
			t.Fatalf("%s = %#v, want %#v", field, received[field], want)
		}
	}
}

func TestPostWuKong(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/conversation/list" {
			t.Fatalf("path = %q, want /conversation/list", request.URL.Path)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"conversations":[]}`))
	}))
	defer server.Close()

	var output map[string]any
	if err := postWuKong(t.Context(), server.URL, "/conversation/list", map[string]any{"uid": "42"}, &output); err != nil {
		t.Fatal(err)
	}
	if conversations, ok := output["conversations"].([]any); !ok || len(conversations) != 0 {
		t.Fatalf("output = %#v, want empty conversations", output)
	}
}

func TestWuKongProxyUsesDomainError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := (&Plugin{apiAddr: server.URL}).proxy(t.Context(), "/conversation/list", map[string]any{"uid": "42"})
	problem, ok := err.(*imProblem)
	if !ok || problem.Status != http.StatusServiceUnavailable || problem.Code != "wukong_unavailable" {
		t.Fatalf("problem=%#v", err)
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
	for _, field := range []string{`"code":"authentication_required"`, `"detail":`} {
		if !strings.Contains(response.Body.String(), field) {
			t.Fatalf("response %q missing %s", response.Body.String(), field)
		}
	}
}
