package im

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const connectTokenTTL = 24 * time.Hour

var errInvalidConnectToken = errors.New("invalid connect token")

type tokenSigner struct {
	secret []byte
}

type tokenClaims struct {
	UID       string `json:"uid"`
	ExpiresAt int64  `json:"exp"`
	Nonce     string `json:"nonce"`
}

func newTokenSigner(secret string) (*tokenSigner, error) {
	if len(secret) < 32 || strings.Contains(secret, "${") {
		return nil, errors.New("im.connect-token-secret must be a non-placeholder secret of at least 32 bytes")
	}
	return &tokenSigner{secret: []byte(secret)}, nil
}

func (s *tokenSigner) Mint(uid string, now time.Time) (string, error) {
	return s.mint(uid, now.Add(connectTokenTTL))
}

func (s *tokenSigner) mint(uid string, expiresAt time.Time) (string, error) {
	if uid == "" {
		return "", errors.New("uid is required")
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	payload, err := json.Marshal(tokenClaims{
		UID:       uid,
		ExpiresAt: expiresAt.Unix(),
		Nonce:     base64.RawURLEncoding.EncodeToString(nonce),
	})
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return encoded + "." + s.signature(encoded), nil
}

func (s *tokenSigner) Verify(token string, now time.Time) (string, error) {
	encoded, signature, ok := strings.Cut(token, ".")
	if !ok || encoded == "" || signature == "" ||
		!hmac.Equal([]byte(signature), []byte(s.signature(encoded))) {
		return "", errInvalidConnectToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", errInvalidConnectToken
	}
	var claims tokenClaims
	if json.Unmarshal(payload, &claims) != nil || claims.UID == "" || claims.Nonce == "" ||
		claims.ExpiresAt <= now.Unix() {
		return "", errInvalidConnectToken
	}
	return claims.UID, nil
}

func (s *tokenSigner) signature(payload string) string {
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
