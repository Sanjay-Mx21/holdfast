package authn

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireDevIdentity(t *testing.T) {
	var seen string
	h := RequireDevIdentity()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := UserFrom(r.Context())
		if !ok {
			t.Error("handler reached without a user")
		}
		seen = id
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name   string
		header string
		status int
		want   string
	}{
		{"canonical UUID", "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77", http.StatusNoContent, "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"},
		{"upper case is canonicalised", "0196F0C1-7A3E-7C51-9B0E-5D2F8A1C4E77", http.StatusNoContent, "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"},
		{"missing", "", http.StatusUnauthorized, ""},
		{"not a UUID", "alice", http.StatusUnauthorized, ""},
		{"braced form", "{0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77}", http.StatusUnauthorized, ""},
		{"no hyphens", "0196f0c17a3e7c519b0e5d2f8a1c4e77", http.StatusUnauthorized, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen = ""
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.header != "" {
				req.Header.Set(DevUserHeader, tt.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.status || seen != tt.want {
				t.Fatalf("status %d user %q, want %d %q (body %s)", rec.Code, seen, tt.status, tt.want, rec.Body)
			}
		})
	}
}

func TestUserFromEmptyContext(t *testing.T) {
	if _, ok := UserFrom(httptest.NewRequest(http.MethodGet, "/", nil).Context()); ok {
		t.Fatal("UserFrom reported a user on an empty context")
	}
}
