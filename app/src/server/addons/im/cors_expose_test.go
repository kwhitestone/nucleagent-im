package im

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/kwhitestone/prism-fusion/middleware"
)

// PROD serves im-web from an outer origin (.new.ndhy.com) that is not in the
// whitelist; prism-fusion then answers with its hardcoded expose list, which
// hides X-IM-Directory-Revision and breaks the agent directory.
func TestDirectoryRevisionExposedForAnyCORSOrigin(t *testing.T) {
	router := gin.New()
	router.Use(middleware.Cors())
	router.Use(exposeDirectoryRevision())
	router.GET("/api/v1/im/agents", func(c *gin.Context) { c.Status(http.StatusOK) })
	for _, origin := range []string{"https://outer.example.test", ""} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/im/agents", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		got := rec.Header().Get("Access-Control-Expose-Headers")
		if origin == "" {
			if got != "" {
				t.Errorf("same-origin request got expose header %q", got)
			}
			continue
		}
		if !strings.Contains(got, "X-IM-Directory-Revision") || !strings.Contains(got, "New-Token") {
			t.Errorf("origin %s: expose=%q", origin, got)
		}
		if strings.Count(got, "X-IM-Directory-Revision") != 1 {
			t.Errorf("origin %s: duplicated expose=%q", origin, got)
		}
	}
}
