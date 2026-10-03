package authn

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

type accessKey struct{}

// AccessFrom returns the verified access token's claims, when the caller
// signed in with one (not with the development header).
func AccessFrom(ctx context.Context) (*AccessClaims, bool) {
	c, ok := ctx.Value(accessKey{}).(*AccessClaims)
	return c, ok
}

// WithAccess stores verified access claims, and their user, in ctx.
func WithAccess(ctx context.Context, c *AccessClaims) context.Context {
	return WithUser(context.WithValue(ctx, accessKey{}, c), c.Subject)
}

// RequireUser identifies a buyer by an auth-svc access token in
// "Authorization: Bearer". The user ID goes where handlers read it
// (UserFrom), and the claims, role included, are available through
// AccessFrom. With allowDevHeader, a request carrying no token may instead
// name its user in X-Dev-User-Id (development and load tests only; services
// refuse it in production). A token that is present but fails verification
// is refused: it never falls back to the header.
func RequireUser(v *AccessVerifier, allowDevHeader bool) httpx.Middleware {
	dev := RequireDevIdentity()
	return func(next http.Handler) http.Handler {
		devNext := dev(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, present := r.Header["Authorization"]; !present && allowDevHeader {
				devNext.ServeHTTP(w, r)
				return
			}
			token, ok := bearer(r)
			if !ok || v == nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="holdfast"`)
				httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "sign in: an access token is required"))
				return
			}
			claims, err := v.Verify(strings.TrimSpace(token))
			if err != nil {
				challenge(w, r, "the access token is invalid or expired; refresh it or sign in again")
				return
			}
			next.ServeHTTP(w, r.WithContext(WithAccess(r.Context(), claims)))
		})
	}
}

// NewAccessIdentity returns RequireUser for a service whose buyers sign in
// with auth-svc: it trusts the keys auth-svc publishes at jwksURL, through a
// JWKSClient the caller runs and adds to its readiness checks (nil when
// jwksURL is empty, which only a development header setup allows). The first
// fetch is attempted now; if auth-svc is not up yet, readiness stays false
// until a key is known.
func NewAccessIdentity(ctx context.Context, jwksURL string, allowDevHeader bool, leeway time.Duration, client *http.Client, reg prometheus.Registerer, log *slog.Logger) (httpx.Middleware, *JWKSClient) {
	if jwksURL == "" {
		return RequireUser(nil, allowDevHeader), nil
	}
	jwks := NewJWKSClient(JWKSOptions{URL: jwksURL, Client: client}, reg, log)
	if err := jwks.Refresh(ctx); err != nil {
		log.Warn("access keys: initial fetch failed; will retry", "url", jwksURL, "err", err)
	}
	return RequireUser(NewAccessVerifier(jwks.Lookup, leeway), allowDevHeader), jwks
}
