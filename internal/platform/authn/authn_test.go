package authn

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func keyPair(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	privPEM, pubPEM, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	priv, err := ParsePrivateKeyPEM(privPEM)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := ParsePublicKeyPEM(pubPEM)
	if err != nil {
		t.Fatal(err)
	}
	return priv, pub
}

func TestIssueAndVerifyRoundTrip(t *testing.T) {
	priv, pub := keyPair(t)
	user, event := uuid.NewString(), uuid.NewString()
	tok, exp, err := NewIssuer(priv, time.Minute).Issue(user, event, "session-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := NewVerifier(map[string]ed25519.PublicKey{KeyID(pub): pub}, time.Second).Verify(tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.Subject != user || claims.EventID != event || claims.Rank != 7 || claims.SessionID != "session-1" {
		t.Fatalf("claims = %+v", claims)
	}
	if time.Until(exp) <= 0 || claims.ID == "" {
		t.Fatalf("expiry %v, jti %q", exp, claims.ID)
	}
}

func TestVerifyRejectsBadTokens(t *testing.T) {
	priv, pub := keyPair(t)
	otherPriv, otherPub := keyPair(t)
	v := NewVerifier(map[string]ed25519.PublicKey{KeyID(pub): pub}, 0)
	user, event, now := uuid.NewString(), uuid.NewString(), time.Now()

	base := func() AdmissionClaims {
		return AdmissionClaims{EventID: event, SessionID: "s", RegisteredClaims: jwt.RegisteredClaims{
			Issuer: IssuerQueue, Subject: user, Audience: jwt.ClaimStrings{AudienceInventory},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
		}}
	}
	sign := func(c AdmissionClaims, method jwt.SigningMethod, key any, kid string) string {
		tok := jwt.NewWithClaims(method, c)
		tok.Header["kid"] = kid
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	with := func(mutate func(*AdmissionClaims)) string {
		c := base()
		mutate(&c)
		return sign(c, jwt.SigningMethodEdDSA, priv, KeyID(pub))
	}
	valid := with(func(*AdmissionClaims) {})
	parts := strings.Split(valid, ".")
	forgedPayload := base()
	forgedPayload.Subject = uuid.NewString()
	forged := strings.Split(sign(forgedPayload, jwt.SigningMethodEdDSA, otherPriv, KeyID(pub)), ".")[1]

	cases := map[string]string{
		"expired": with(func(c *AdmissionClaims) {
			c.IssuedAt = jwt.NewNumericDate(now.Add(-2 * time.Minute))
			c.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Minute))
		}),
		"missing expiry":      with(func(c *AdmissionClaims) { c.ExpiresAt = nil }),
		"issued in future":    with(func(c *AdmissionClaims) { c.IssuedAt = jwt.NewNumericDate(now.Add(time.Hour)) }),
		"wrong audience":      with(func(c *AdmissionClaims) { c.Audience = jwt.ClaimStrings{"holdfast-booking"} }),
		"wrong issuer":        with(func(c *AdmissionClaims) { c.Issuer = "mallory" }),
		"subject not a uuid":  with(func(c *AdmissionClaims) { c.Subject = "alice" }),
		"event not a uuid":    with(func(c *AdmissionClaims) { c.EventID = "concert" }),
		"unknown key id":      sign(base(), jwt.SigningMethodEdDSA, otherPriv, KeyID(otherPub)),
		"kid of trusted key":  sign(base(), jwt.SigningMethodEdDSA, otherPriv, KeyID(pub)),
		"alg none":            sign(base(), jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, KeyID(pub)),
		"hmac alg confusion":  sign(base(), jwt.SigningMethodHS256, []byte(pub), KeyID(pub)),
		"tampered payload":    parts[0] + "." + forged + "." + parts[2],
		"garbage":             "not-a-jwt",
		"truncated signature": parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString([]byte("x")),
	}
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("Verify = %v, want ErrInvalidToken", err)
			}
		})
	}
}

func TestRequireAdmission(t *testing.T) {
	priv, pub := keyPair(t)
	v := NewVerifier(map[string]ed25519.PublicKey{KeyID(pub): pub}, time.Second)
	user := uuid.NewString()
	tok, _, _ := NewIssuer(priv, time.Minute).Issue(user, uuid.NewString(), "s", 1)

	var got *AdmissionClaims
	h := RequireAdmission(v)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = AdmissionFrom(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, tc := range []struct {
		name, header string
		want         int
	}{
		{"missing header", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic " + tok, http.StatusUnauthorized},
		{"invalid token", "Bearer nope", http.StatusUnauthorized},
		{"valid token", "Bearer " + tok, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got = nil
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("401 must carry a WWW-Authenticate challenge")
			}
			if tc.want == http.StatusNoContent && (got == nil || got.Subject != user) {
				t.Fatalf("claims not propagated: %+v", got)
			}
		})
	}
}

func TestRequireStaticToken(t *testing.T) {
	secret := strings.Repeat("s3cr3t", 6)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	call := func(h http.Handler, header string) int {
		req := httptest.NewRequest(http.MethodPut, "/", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	guarded := RequireStaticToken(secret)(ok)
	if c := call(guarded, "Bearer "+secret); c != http.StatusNoContent {
		t.Fatalf("correct token: %d", c)
	}
	for _, header := range []string{"", "Bearer wrong", "Bearer " + secret + "x", secret} {
		if c := call(guarded, header); c != http.StatusUnauthorized {
			t.Fatalf("header %q: %d, want 401", header, c)
		}
	}
	if c := call(RequireStaticToken("")(ok), "Bearer anything"); c != http.StatusUnauthorized {
		t.Fatalf("empty configured secret must reject everything, got %d", c)
	}
}

func TestLoadPublicKeys(t *testing.T) {
	dir := t.TempDir()
	var paths []string
	want := map[string]bool{}
	for i := 0; i < 2; i++ {
		privPEM, pubPEM, err := GenerateKeyPair()
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, uuid.NewString()+".pub")
		if err := os.WriteFile(p, pubPEM, 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
		priv, _ := ParsePrivateKeyPEM(privPEM)
		want[KeyID(priv.Public().(ed25519.PublicKey))] = true
	}
	keys, err := LoadPublicKeys(paths)
	if err != nil {
		t.Fatal(err)
	}
	for kid := range want {
		if _, ok := keys[kid]; !ok {
			t.Fatalf("key %s missing (rotation needs both keys trusted)", kid)
		}
	}
	if _, err := LoadPublicKeys(nil); err == nil {
		t.Fatal("no key files must be an error")
	}
	privPEM, _, _ := GenerateKeyPair()
	bad := filepath.Join(dir, "private-by-mistake.pub")
	_ = os.WriteFile(bad, privPEM, 0o600)
	if _, err := LoadPublicKeys([]string{bad}); err == nil {
		t.Fatal("a private key file must not load as a public key")
	}
}
