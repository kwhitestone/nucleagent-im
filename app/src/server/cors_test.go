package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/kwhitestone/prism-fusion/core"
	"github.com/kwhitestone/prism-fusion/global"
	"github.com/kwhitestone/prism-fusion/middleware"
)

func TestSSEReconnectCORS(t *testing.T) {
	previous := global.PRISM_CONFIG
	t.Cleanup(func() { global.PRISM_CONFIG = previous })
	const origin = "https://im-web.example.test"
	t.Setenv("IM_WEB_FRONTEND_URL", origin)
	core.Viper("config.yaml")
	router := gin.New()
	router.Use(middleware.Cors())
	router.GET("/api/v1/im/stream", func(c *gin.Context) { c.Status(http.StatusOK) })
	for _, method := range []string{http.MethodOptions, http.MethodGet} {
		req := httptest.NewRequest(method, "/api/v1/im/stream", nil)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "GET")
		req.Header.Set("Access-Control-Request-Headers", "authorization,last-event-id")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		want := http.StatusOK
		if method == http.MethodOptions {
			want = http.StatusNoContent
		}
		if rec.Code != want || rec.Header().Get("Access-Control-Allow-Origin") != origin ||
			rec.Header().Get("Access-Control-Allow-Credentials") != "true" {
			t.Fatalf("%s: status=%d headers=%v", method, rec.Code, rec.Header())
		}
		allowed := "," + strings.ToLower(strings.ReplaceAll(rec.Header().Get("Access-Control-Allow-Headers"), " ", "")) + ","
		for _, header := range []string{"authorization", "content-type", "last-event-id"} {
			if !strings.Contains(allowed, ","+header+",") {
				t.Errorf("%s: CORS does not allow %s", method, header)
			}
		}
	}
}
