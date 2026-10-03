//go:build integration

package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

type fixture struct {
	pool  *pgxpool.Pool
	svc   *Service
	inbox *Inbox
	pub   ed25519.PublicKey
}

func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	pool := testenv.Postgres(t)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	if cfg.Pepper == nil {
		cfg.Pepper = []byte("test-pepper-test-pepper-test-pepper")
	}
	if cfg.CodeTTL == 0 {
		cfg.CodeTTL = 5 * time.Minute
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.ResendAfter == 0 {
		cfg.ResendAfter = 30 * time.Second
	}
	if cfg.MaxPerHour == 0 {
		cfg.MaxPerHour = 5
	}
	if cfg.RefreshTTL == 0 {
		cfg.RefreshTTL = 24 * time.Hour
	}
	inbox := NewInbox(100)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := NewService(pool, cfg, inbox, authn.NewAccessIssuer(priv, 15*time.Minute), NewMetrics(prometheus.NewRegistry()), quiet)
	return &fixture{pool: pool, svc: svc, inbox: inbox, pub: pub}
}

// phone is a fresh Indian mobile number, unique across runs.
func phone() string {
	n, _ := rand.Int(rand.Reader, big.NewInt(100_000_000))
	return fmt.Sprintf("+9199%08d", n.Int64())
}

var codeRE = regexp.MustCompile(`\b(\d{6})\b`)

func (f *fixture) code(t *testing.T, p string) string {
	t.Helper()
	m, ok := f.inbox.Last(p)
	if !ok {
		t.Fatalf("no message for %s", p)
	}
	c := codeRE.FindStringSubmatch(m.Text)
	if c == nil {
		t.Fatalf("no code in %q", m.Text)
	}
	return c[1]
}

func (f *fixture) signIn(t *testing.T, p string) Session {
	t.Helper()
	if err := f.svc.RequestOTP(ctx, p); err != nil {
		t.Fatal(err)
	}
	s, err := f.svc.VerifyOTP(ctx, p, f.code(t, p))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func wrongCode(right string) string {
	if right == "000000" {
		return "000001"
	}
	return "000000"
}

func TestSignInWithACode(t *testing.T) {
	f := newFixture(t, Config{})
	p := phone()
	s := f.signIn(t, p)
	if !s.NewUser || s.Role != authn.RoleBuyer || s.RefreshToken == "" || s.AccessToken == "" {
		t.Fatalf("first sign-in %+v", s)
	}
	claims, err := authn.NewAccessVerifier(func(kid string) (ed25519.PublicKey, bool) { return f.pub, kid == authn.KeyID(f.pub) }, time.Second).Verify(s.AccessToken)
	if err != nil || claims.Subject != s.UserID.String() {
		t.Fatalf("access token: %+v %v", claims, err)
	}

	// Nothing sensitive is stored in the clear.
	var hmacLen int
	var last4 string
	if err := f.pool.QueryRow(ctx, `SELECT length(phone_hmac), phone_last4 FROM auth.users WHERE id = $1`, s.UserID).Scan(&hmacLen, &last4); err != nil {
		t.Fatal(err)
	}
	if hmacLen != 32 || last4 != p[len(p)-4:] {
		t.Fatalf("stored phone: %d-byte HMAC, last4 %q", hmacLen, last4)
	}
	var stored int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM auth.refresh_tokens WHERE token_hash = $1`, []byte(s.RefreshToken)).Scan(&stored); err != nil || stored != 0 {
		t.Fatalf("a refresh token is stored in the clear (%d, %v)", stored, err)
	}

	// The same phone signs in to the same user (after the resend wait).
	if _, err := f.pool.Exec(ctx, `UPDATE auth.otp_challenges SET created_at = created_at - interval '1 minute' WHERE phone_hmac = $1`, f.svc.mac("phone", p)); err != nil {
		t.Fatal(err)
	}
	again := f.signIn(t, p)
	if again.UserID != s.UserID || again.NewUser {
		t.Fatalf("second sign-in %+v, want user %s again", again, s.UserID)
	}
}

func TestCodesAreLimitedAndUsedOnce(t *testing.T) {
	f := newFixture(t, Config{MaxAttempts: 3})
	p := phone()
	if err := f.svc.RequestOTP(ctx, p); err != nil {
		t.Fatal(err)
	}
	code := f.code(t, p)
	for i := range 3 {
		if _, err := f.svc.VerifyOTP(ctx, p, wrongCode(code)); !errors.Is(err, ErrInvalidCode) {
			t.Fatalf("wrong guess %d: %v", i+1, err)
		}
	}
	if _, err := f.svc.VerifyOTP(ctx, p, code); !errors.Is(err, ErrInvalidCode) {
		t.Fatalf("the right code after the attempts ran out: %v, want refused", err)
	}

	g := newFixture(t, Config{})
	q := phone()
	if err := g.svc.RequestOTP(ctx, q); err != nil {
		t.Fatal(err)
	}
	c := g.code(t, q)
	if _, err := g.svc.VerifyOTP(ctx, q, c); err != nil {
		t.Fatal(err)
	}
	if _, err := g.svc.VerifyOTP(ctx, q, c); !errors.Is(err, ErrInvalidCode) {
		t.Fatalf("a code used twice: %v", err)
	}
	for _, bad := range []string{"12345", "abcdef", ""} {
		if _, err := g.svc.VerifyOTP(ctx, q, bad); !errors.Is(err, ErrInvalidCode) {
			t.Fatalf("code %q: %v", bad, err)
		}
	}
	if _, err := g.svc.VerifyOTP(ctx, "123", c); !errors.Is(err, ErrInvalidPhone) {
		t.Fatalf("a bad phone: %v", err)
	}
}

func TestExpiredCodesAreRefused(t *testing.T) {
	f := newFixture(t, Config{})
	p := phone()
	if err := f.svc.RequestOTP(ctx, p); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE auth.otp_challenges SET expires_at = now() - interval '1 second' WHERE phone_hmac = $1`, f.svc.mac("phone", p)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.VerifyOTP(ctx, p, f.code(t, p)); !errors.Is(err, ErrInvalidCode) {
		t.Fatalf("an expired code: %v", err)
	}
}

func TestConcurrentGuessesSignInOnce(t *testing.T) {
	f := newFixture(t, Config{MaxAttempts: 10})
	p := phone()
	if err := f.svc.RequestOTP(ctx, p); err != nil {
		t.Fatal(err)
	}
	code := f.code(t, p)
	var mu sync.Mutex
	ok := 0
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.svc.VerifyOTP(ctx, p, code); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ok != 1 {
		t.Fatalf("%d concurrent sign-ins with one code, want 1", ok)
	}
}

func TestCodeRequestsAreRateLimited(t *testing.T) {
	f := newFixture(t, Config{ResendAfter: 30 * time.Second})
	p := phone()
	if err := f.svc.RequestOTP(ctx, p); err != nil {
		t.Fatal(err)
	}
	var limited *RateLimited
	if err := f.svc.RequestOTP(ctx, p); !errors.As(err, &limited) || limited.RetryAfter <= 0 || limited.RetryAfter > 30*time.Second {
		t.Fatalf("a second request at once: %v", err)
	}
	// Concurrent requests for one phone: the lock lets exactly one through.
	g := newFixture(t, Config{ResendAfter: time.Minute})
	q := phone()
	var wg sync.WaitGroup
	var mu sync.Mutex
	sent := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if g.svc.RequestOTP(ctx, q) == nil {
				mu.Lock()
				sent++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if sent != 1 {
		t.Fatalf("%d concurrent requests sent a code, want 1", sent)
	}
	// The hourly cap.
	h := newFixture(t, Config{ResendAfter: time.Millisecond, MaxPerHour: 3})
	r := phone()
	for i := range 3 {
		if err := h.svc.RequestOTP(ctx, r); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := h.svc.RequestOTP(ctx, r); !errors.As(err, &limited) || limited.RetryAfter < 59*time.Minute {
		t.Fatalf("a fourth request in the hour: %v", err)
	}
}

func TestRefreshRotatesAndDetectsReuse(t *testing.T) {
	f := newFixture(t, Config{})
	s := f.signIn(t, phone())
	next, err := f.svc.Refresh(ctx, s.RefreshToken)
	if err != nil || next.RefreshToken == s.RefreshToken || next.UserID != s.UserID || next.AccessToken == "" {
		t.Fatalf("refresh: %+v %v", next, err)
	}
	third, err := f.svc.Refresh(ctx, next.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	// The first token comes back (stolen, or replayed): the family is revoked.
	if _, err := f.svc.Refresh(ctx, s.RefreshToken); !errors.Is(err, ErrRefreshReused) {
		t.Fatalf("reusing a rotated token: %v, want ErrRefreshReused", err)
	}
	if _, err := f.svc.Refresh(ctx, third.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("the newest token after the reuse: %v, want the family revoked", err)
	}
	// Another login is another family, untouched.
	other := f.signInAgain(t, s)
	if _, err := f.svc.Refresh(ctx, other.RefreshToken); err != nil {
		t.Fatalf("another family: %v", err)
	}
	if _, err := f.svc.Refresh(ctx, "made-up"); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("an unknown token: %v", err)
	}
	if _, err := f.svc.Refresh(ctx, ""); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("no token: %v", err)
	}
}

// signInAgain signs the same user in once more (a second device).
func (f *fixture) signInAgain(t *testing.T, s Session) Session {
	t.Helper()
	p := phone()
	if _, err := f.pool.Exec(ctx, `UPDATE auth.users SET phone_hmac = $1 WHERE id = $2`, f.svc.mac("phone", p), s.UserID); err != nil {
		t.Fatal(err)
	}
	return f.signIn(t, p)
}

func TestConcurrentRefreshesRotateOnce(t *testing.T) {
	f := newFixture(t, Config{})
	s := f.signIn(t, phone())
	var mu sync.Mutex
	results := map[string]int{}
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.svc.Refresh(ctx, s.RefreshToken)
			key := "ok"
			switch {
			case errors.Is(err, ErrRefreshReused):
				key = "reused"
			case err != nil:
				key = err.Error()
			}
			mu.Lock()
			results[key]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if results["ok"] != 1 || results["ok"]+results["reused"] != 6 {
		t.Fatalf("6 concurrent refreshes of one token: %v; want 1 rotation and the rest refused as reuse", results)
	}
}

func TestLogoutEndsTheFamily(t *testing.T) {
	f := newFixture(t, Config{})
	s := f.signIn(t, phone())
	next, err := f.svc.Refresh(ctx, s.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.Logout(ctx, next.RefreshToken); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Refresh(ctx, next.RefreshToken); !errors.Is(err, ErrInvalidRefresh) {
		t.Fatalf("after logout: %v", err)
	}
	if err := f.svc.Logout(ctx, next.RefreshToken); err != nil {
		t.Fatalf("logging out twice: %v", err)
	}
	if err := f.svc.Logout(ctx, "unknown"); err != nil {
		t.Fatalf("an unknown token: %v", err)
	}
}

func TestHTTPAPI(t *testing.T) {
	f := newFixture(t, Config{})
	router := httpx.NewRouter()
	NewHandler(f.svc, authn.NewJWKSet(f.pub), HandlerConfig{DevInbox: f.inbox}).Register(router)
	srv := httptest.NewServer(router)
	defer srv.Close()
	post := func(path, body string, cookie *http.Cookie) *http.Response {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if cookie != nil {
			req.AddCookie(cookie)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}
	p := phone()
	if resp := post("/v1/auth/otp/request", `{"phone":"`+p+`"}`, nil); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("request: %d", resp.StatusCode)
	}
	if resp := post("/v1/auth/otp/request", `{"phone":"`+p+`"}`, nil); resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("a second request: %d %v", resp.StatusCode, resp.Header)
	}
	if resp := post("/v1/auth/otp/request", `{"phone":"12"}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a bad phone: %d", resp.StatusCode)
	}
	resp, err := http.Get(srv.URL + "/v1/auth/dev/inbox?phone=" + url.QueryEscape(p)) //nolint:noctx // test
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("inbox: %v %v", resp, err)
	}
	var msg Message
	_ = json.NewDecoder(resp.Body).Decode(&msg)
	_ = resp.Body.Close()
	code := codeRE.FindStringSubmatch(msg.Text)[1]

	if r := post("/v1/auth/otp/verify", `{"phone":"`+p+`","code":"`+wrongCode(code)+`"}`, nil); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a wrong code: %d", r.StatusCode)
	}
	r := post("/v1/auth/otp/verify", `{"phone":"`+p+`","code":"`+code+`"}`, nil)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("verify: %d", r.StatusCode)
	}
	body, _ := io.ReadAll(r.Body)
	var view sessionView
	if err := json.Unmarshal(body, &view); err != nil || view.TokenType != "Bearer" || view.ExpiresIn < 890 || view.AccessToken == "" {
		t.Fatalf("session %s %v", body, err)
	}
	var cookie *http.Cookie
	for _, c := range r.Cookies() {
		if c.Name == RefreshCookie {
			cookie = c
		}
	}
	if cookie == nil || !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/v1/auth" {
		t.Fatalf("refresh cookie %+v", cookie)
	}
	if bytes.Contains(body, []byte(cookie.Value)) {
		t.Fatal("the refresh token is in the response body")
	}

	rr := post("/v1/auth/refresh", "", cookie)
	if rr.StatusCode != http.StatusOK || len(rr.Cookies()) == 0 || rr.Cookies()[0].Value == cookie.Value {
		t.Fatalf("refresh: %d %v", rr.StatusCode, rr.Cookies())
	}
	reused := post("/v1/auth/refresh", "", cookie)
	var prob httpx.Problem
	_ = json.NewDecoder(reused.Body).Decode(&prob)
	if reused.StatusCode != http.StatusUnauthorized || prob.Code != "REFRESH_REUSED" || len(reused.Cookies()) == 0 || reused.Cookies()[0].MaxAge >= 0 {
		t.Fatalf("reuse: %d %s %v", reused.StatusCode, prob.Code, reused.Cookies())
	}
	if out := post("/v1/auth/logout", "", rr.Cookies()[0]); out.StatusCode != http.StatusNoContent {
		t.Fatalf("logout: %d", out.StatusCode)
	}

	keys, err := http.Get(srv.URL + "/.well-known/jwks.json") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	var set authn.JWKSet
	_ = json.NewDecoder(keys.Body).Decode(&set)
	_ = keys.Body.Close()
	if len(set.Keys) != 1 || set.Keys[0].Kid != authn.KeyID(f.pub) || keys.Header.Get("Cache-Control") != "public, max-age=300" {
		t.Fatalf("JWKS %+v %v", set, keys.Header)
	}

	// Without DEV_SMS_INBOX the inbox does not exist.
	plain := httpx.NewRouter()
	NewHandler(f.svc, authn.NewJWKSet(f.pub), HandlerConfig{}).Register(plain)
	ps := httptest.NewServer(plain)
	defer ps.Close()
	if r, err := http.Get(ps.URL + "/v1/auth/dev/inbox?phone=" + url.QueryEscape(p)); err != nil || r.StatusCode != http.StatusNotFound { //nolint:noctx // test
		t.Fatalf("inbox without DEV_SMS_INBOX: %v %v", r, err)
	}
}
