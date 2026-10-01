package authn

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

type claimsKey struct{}

// AdmissionFrom returns the verified admission claims stored by RequireAdmission.
func AdmissionFrom(ctx context.Context) (*AdmissionClaims, bool) {
	c, ok := ctx.Value(claimsKey{}).(*AdmissionClaims)
	return c, ok
}

// WithAdmission stores claims in ctx (used by RequireAdmission and by tests).
func WithAdmission(ctx context.Context, c *AdmissionClaims) context.Context {
	return context.WithValue(ctx, claimsKey{}, c)
}

// RequireAdmission authenticates "Authorization: Bearer <admission token>".
func RequireAdmission(v *Verifier) httpx.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearer(r)
			if !ok {
				challenge(w, r, "missing bearer token")
				return
			}
			claims, err := v.Verify(token)
			if err != nil {
				logging.FromContext(r.Context()).Warn("admission token rejected", "err", err)
				challenge(w, r, "invalid or expired admission token")
				return
			}
			next.ServeHTTP(w, r.WithContext(WithAdmission(r.Context(), claims)))
		})
	}
}

// RequireStaticToken protects internal operator endpoints with a shared
// secret compared in constant time. It is a stop-gap until auth-svc issues
// operator tokens; the endpoints it guards live on the internal admin port.
func RequireStaticToken(secret string) httpx.Middleware {
	want := []byte(secret)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, ok := bearer(r)
			if !ok || len(want) == 0 || subtle.ConstantTimeCompare([]byte(token), want) != 1 {
				challenge(w, r, "operator credentials required")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func bearer(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return "", false
	}
	return token, true
}

func challenge(w http.ResponseWriter, r *http.Request, detail string) {
	w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
	httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", detail))
}
