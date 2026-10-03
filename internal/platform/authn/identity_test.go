package authn

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRequireUser(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	v := NewAccessVerifier(func(kid string) (ed25519.PublicKey, bool) { return pub, kid == KeyID(pub) }, time.Second)
	user := uuid.NewString()
	good, _, _ := NewAccessIssuer(priv, time.Minute).Issue(user, RoleAgent)
	forged, _, _ := NewAccessIssuer(otherKey, time.Minute).Issue(user, RoleBuyer)
	devUser := uuid.NewString()

	var seenUser, seenRole string
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenUser, _ = UserFrom(r.Context())
		seenRole = ""
		if c, has := AccessFrom(r.Context()); has {
			seenRole = c.Role
		}
		w.WriteHeader(http.StatusNoContent)
	})
	do := func(allowDev bool, header, value string) int {
		seenUser, seenRole = "", ""
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if header != "" {
			r.Header.Set(header, value)
		}
		w := httptest.NewRecorder()
		RequireUser(v, allowDev)(ok).ServeHTTP(w, r)
		return w.Code
	}

	if code := do(false, "Authorization", "Bearer "+good); code != http.StatusNoContent || seenUser != user || seenRole != RoleAgent {
		t.Fatalf("a valid token: %d, user %q, role %q", code, seenUser, seenRole)
	}
	for name, c := range map[string][2]string{
		"no credentials":      {"", ""},
		"a forged token":      {"Authorization", "Bearer " + forged},
		"garbage":             {"Authorization", "Bearer x.y.z"},
		"not Bearer":          {"Authorization", "Basic dXNlcjpwYXNz"},
		"the dev header, off": {DevUserHeader, devUser},
	} {
		if code := do(false, c[0], c[1]); code != http.StatusUnauthorized || seenUser != "" {
			t.Errorf("%s: %d (user %q), want 401", name, code, seenUser)
		}
	}
	// With the development header allowed: it identifies a request with no
	// token, never one with a bad token.
	if code := do(true, DevUserHeader, devUser); code != http.StatusNoContent || seenUser != devUser || seenRole != "" {
		t.Fatalf("the dev header, on: %d, user %q", code, seenUser)
	}
	if code := do(true, "Authorization", "Bearer "+forged); code != http.StatusUnauthorized {
		t.Fatalf("a forged token with the dev header allowed: %d", code)
	}
	if code := do(true, "Authorization", "Bearer "+good); code != http.StatusNoContent || seenUser != user {
		t.Fatalf("a valid token with the dev header allowed: %d", code)
	}
	if code := do(true, "", ""); code != http.StatusUnauthorized {
		t.Fatalf("nothing at all, dev header allowed: %d", code)
	}
}
