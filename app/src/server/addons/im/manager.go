package im

import (
	"context"
	"errors"
	"net/http"
	"time"
)

func (p *Plugin) managerAccessToken(ctx context.Context, rejected string) (string, error) {
	// Preserve the optional authentication contract for local auth-off managers.
	if p.wuKongAdminUser == "" && p.wuKongAdminPassword == "" {
		return "", nil
	}
	p.managerTokenMu.Lock()
	defer p.managerTokenMu.Unlock()
	if rejected != "" && p.managerToken == rejected {
		p.managerToken = ""
	}
	if p.managerToken != "" && time.Now().Before(p.managerTokenExpiry) {
		return p.managerToken, nil
	}
	var login struct {
		AccessToken string    `json:"access_token"`
		ExpiresAt   time.Time `json:"expires_at"`
	}
	if err := postWuKong(ctx, p.managerAddr, "/manager/login", map[string]string{
		"username": p.wuKongAdminUser, "password": p.wuKongAdminPassword,
	}, &login); err != nil {
		return "", err
	}
	if login.AccessToken == "" || !login.ExpiresAt.After(time.Now()) {
		return "", errors.New("wukong manager returned an invalid login response")
	}
	p.managerToken, p.managerTokenExpiry = login.AccessToken, login.ExpiresAt
	return p.managerToken, nil
}

func (p *Plugin) managerGet(ctx context.Context, endpoint string) (*http.Response, error) {
	token, err := p.managerAccessToken(ctx, "")
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusUnauthorized || token == "" || attempt == 1 {
			return response, nil
		}
		response.Body.Close()
		token, err = p.managerAccessToken(ctx, token)
		if err != nil {
			return nil, err
		}
	}
}
