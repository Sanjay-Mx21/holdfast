package authn

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

// DevUserHeader carries the caller's user ID until auth-svc issues access
// tokens (Phase 4).
const DevUserHeader = "X-Dev-User-Id"

type userKey struct{}

// UserFrom returns the authenticated user ID stored by an identity middleware.
func UserFrom(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(userKey{}).(string)
	return id, ok && id != ""
}

// WithUser stores the authenticated user ID in ctx (used by identity
// middleware and by tests).
func WithUser(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userKey{}, userID)
}

// RequireDevIdentity trusts the X-Dev-User-Id header as the caller's identity.
// It exists only so the waiting room can be built before auth-svc: anyone can
// claim any user ID with it, so a service must never enable it in production
// (queue-svc refuses to start if asked to). The ID must be a canonical UUID;
// it is stored lower-cased.
func RequireDevIdentity() httpx.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := r.Header.Get(DevUserHeader)
			u, err := uuid.Parse(raw)
			if err != nil || len(raw) != 36 {
				httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED",
					"a user ID is required in the "+DevUserHeader+" header (development identity)"))
				return
			}
			next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), u.String())))
		})
	}
}
