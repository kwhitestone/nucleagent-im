package im

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestManagerAuthentication(t *testing.T) {
	for _, test := range []struct {
		name       string
		reject     int
		loginCode  int
		loginBody  string
		wantLogins int
		wantGets   int
		wantError  bool
	}{
		{name: "cached bearer", wantLogins: 1, wantGets: 2},
		{name: "refresh on 401", reject: 1, wantLogins: 2, wantGets: 3},
		{name: "retry only once", reject: 2, wantLogins: 2, wantGets: 2, wantError: true},
		{name: "login rejected", loginCode: 401, wantLogins: 1, wantError: true},
		{name: "invalid login body", loginBody: `{}`, wantLogins: 1, wantError: true},
		{name: "expired login", loginBody: `{"access_token":"expired","expires_at":"2000-01-01T00:00:00Z"}`,
			wantLogins: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			logins, gets := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/manager/login" {
					logins++
					var body map[string]string
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil ||
						r.Method != http.MethodPost || body["username"] != "admin" || body["password"] != "test-password" {
						t.Error("invalid manager login request")
					}
					if test.loginCode != 0 {
						w.WriteHeader(test.loginCode)
						return
					}
					if test.loginBody != "" {
						fmt.Fprint(w, test.loginBody)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"access_token": fmt.Sprintf("token-%d", logins), "expires_at": time.Now().Add(time.Hour),
					})
					return
				}
				gets++
				if r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", logins) || logins == 0 {
					t.Error("manager GET missing current bearer token")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if gets <= test.reject {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				fmt.Fprint(w, `{"items":[{"uid":"2"}],"has_more":false}`)
			}))
			defer server.Close()
			p := &Plugin{managerAddr: server.URL, wuKongAdminUser: "admin", wuKongAdminPassword: "test-password"}
			for i := 0; i < 2; i++ {
				members, err := p.wuKongGroupMembers(context.Background(), "group")
				if test.wantError {
					if err == nil || members != nil {
						t.Fatal("expected failure without partial membership")
					}
					break
				}
				if err != nil || !reflect.DeepEqual(members, []uint{2}) {
					t.Fatalf("members=%v err=%v", members, err)
				}
			}
			if logins != test.wantLogins || gets != test.wantGets {
				t.Fatalf("logins=%d gets=%d; want %d/%d", logins, gets, test.wantLogins, test.wantGets)
			}
		})
	}
}

func TestManagerPagination(t *testing.T) {
	for _, total := range []int{0, 499, 500, 501, 1001} {
		t.Run(strconv.Itoa(total), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("limit") != "500" {
					t.Error("manager limit must be capped at 500")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				offset := calls * 500
				cursor := ""
				if calls > 0 {
					cursor = fmt.Sprintf("opaque+/%d=", offset)
				}
				if r.URL.Query().Get("cursor") != cursor {
					t.Error("incorrect opaque cursor")
				}
				calls++
				end := min(offset+500, total)
				items := make([]map[string]string, 0)
				for i := offset; i < end; i++ {
					items = append(items, map[string]string{"uid": strconv.Itoa(i + 1)})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": items, "has_more": end < total, "next_cursor": fmt.Sprintf("opaque+/%d=", end),
				})
			}))
			defer server.Close()
			p := &Plugin{managerAddr: server.URL}
			members, err := p.wuKongGroupMembers(context.Background(), "group")
			if err != nil || len(members) != total || calls != max(1, (total+499)/500) {
				t.Fatalf("members=%d calls=%d err=%v", len(members), calls, err)
			}
			for i, uid := range members {
				if uid != uint(i+1) {
					t.Fatalf("member %d=%d", i, uid)
				}
			}
		})
	}
}

func TestManagerRejectsBrokenPagination(t *testing.T) {
	for _, cursor := range []string{"", "repeated"} {
		t.Run("cursor-"+cursor, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []map[string]string{{"uid": "2"}}, "has_more": true, "next_cursor": cursor,
				})
			}))
			defer server.Close()
			p := &Plugin{managerAddr: server.URL}
			if members, err := p.wuKongGroupMembers(context.Background(), "group"); err == nil || members != nil {
				t.Fatal("broken pagination must not return partial membership")
			}
		})
	}
}

func TestManagerRefreshesExpiredCache(t *testing.T) {
	logins := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logins++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "fresh", "expires_at": time.Now().Add(time.Hour),
		})
	}))
	defer server.Close()
	p := &Plugin{
		managerAddr: server.URL, wuKongAdminUser: "admin", wuKongAdminPassword: "test-password",
		managerToken: "expired", managerTokenExpiry: time.Now().Add(-time.Second),
	}
	if token, err := p.managerAccessToken(context.Background(), ""); err != nil || token != "fresh" || logins != 1 {
		t.Fatalf("expiry refresh failed: logins=%d err=%v", logins, err)
	}
}

func TestManagerPostReplaysBodyAfterUnauthorized(t *testing.T) {
	logins, posts := 0, 0
	const body = `{"uids":["5","11","33"]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/manager/login" {
			logins++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": fmt.Sprintf("token-%d", logins), "expires_at": time.Now().Add(time.Hour),
			})
			return
		}
		posts++
		got, err := io.ReadAll(r.Body)
		if err != nil || string(got) != body || r.Method != http.MethodPost ||
			r.Header.Get("Content-Type") != "application/json" ||
			r.Header.Get("Authorization") != fmt.Sprintf("Bearer token-%d", logins) {
			t.Error("invalid authenticated Manager POST")
		}
		if posts == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	p := &Plugin{managerAddr: server.URL, wuKongAdminUser: "admin", wuKongAdminPassword: "test-password"}
	response, err := p.managerRequest(context.Background(), http.MethodPost,
		server.URL+"/manager/channels/2/group/subscribers/remove", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || logins != 2 || posts != 2 {
		t.Fatalf("status=%d logins=%d posts=%d", response.StatusCode, logins, posts)
	}
}
