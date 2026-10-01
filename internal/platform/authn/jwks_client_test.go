package authn

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// keyServer serves a JWKS that tests can change, and counts fetches.
type keyServer struct {
	mu    sync.Mutex
	body  []byte
	code  int
	hits  atomic.Int64
	srv   *httptest.Server
	delay time.Duration
}

func newKeyServer(t *testing.T, keys ...ed25519.PublicKey) *keyServer {
	t.Helper()
	ks := &keyServer{code: http.StatusOK}
	ks.serve(keys...)
	ks.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ks.hits.Add(1)
		ks.mu.Lock()
		body, code, delay := ks.body, ks.code, ks.delay
		ks.mu.Unlock()
		time.Sleep(delay)
		w.WriteHeader(code)
		_, _ = w.Write(body)
	}))
	t.Cleanup(ks.srv.Close)
	return ks
}

func (ks *keyServer) serve(keys ...ed25519.PublicKey) {
	b, _ := json.Marshal(NewJWKSet(keys...))
	ks.mu.Lock()
	ks.body, ks.code = b, http.StatusOK
	ks.mu.Unlock()
}

func (ks *keyServer) fail(code int) {
	ks.mu.Lock()
	ks.code = code
	ks.mu.Unlock()
}

func newClient(t *testing.T, ks *keyServer, static map[string]ed25519.PublicKey) (*JWKSClient, *time.Time) {
	t.Helper()
	c := NewJWKSClient(JWKSOptions{URL: ks.srv.URL, Static: static, MinRefresh: 30 * time.Second, Timeout: time.Second},
		prometheus.NewRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	clock := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return clock }
	return c, &clock
}

func TestJWKSClientVerifiesTokensFromTheIssuersKeySet(t *testing.T) {
	priv, pub := keyPair(t)
	ks := newKeyServer(t, pub)
	c, _ := newClient(t, ks, nil)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Check(context.Background()); err != nil {
		t.Fatalf("ready check after a fetch: %v", err)
	}
	tok, _, err := NewIssuer(priv, time.Minute).Issue(uuid.NewString(), uuid.NewString(), "s", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewVerifierWith(c.Lookup, time.Second).Verify(tok); err != nil {
		t.Fatalf("token signed with a published key rejected: %v", err)
	}
}

func TestJWKSClientRefreshesForAnUnknownKidAtMostEveryMinRefresh(t *testing.T) {
	_, oldPub := keyPair(t)
	newPriv, newPub := keyPair(t)
	ks := newKeyServer(t, oldPub)
	c, clock := newClient(t, ks, nil)
	_ = c.Refresh(context.Background())
	*clock = clock.Add(time.Minute)

	// The issuer rotates: a token arrives signed by a key we have not seen.
	ks.serve(newPub, oldPub)
	if _, ok := c.Lookup(KeyID(newPub)); !ok {
		t.Fatal("unknown kid did not trigger a refresh")
	}
	tok, _, _ := NewIssuer(newPriv, time.Minute).Issue(uuid.NewString(), uuid.NewString(), "s", 1)
	if _, err := NewVerifierWith(c.Lookup, time.Second).Verify(tok); err != nil {
		t.Fatalf("token from the rotated key rejected: %v", err)
	}

	// Made-up kids within MinRefresh cause no further fetches.
	before := ks.hits.Load()
	for i := range 50 {
		if _, ok := c.Lookup("made-up-" + string(rune('a'+i%26))); ok {
			t.Fatal("made-up kid accepted")
		}
	}
	if got := ks.hits.Load() - before; got != 0 {
		t.Fatalf("%d fetches for made-up kids within MinRefresh, want 0", got)
	}
	*clock = clock.Add(31 * time.Second)
	c.Lookup("made-up-again")
	if got := ks.hits.Load() - before; got != 1 {
		t.Fatalf("%d fetches after MinRefresh passed, want 1", got)
	}
}

func TestJWKSClientDropsKeysTheIssuerNoLongerPublishes(t *testing.T) {
	_, oldPub := keyPair(t)
	_, newPub := keyPair(t)
	ks := newKeyServer(t, oldPub, newPub)
	c, _ := newClient(t, ks, nil)
	_ = c.Refresh(context.Background())
	ks.serve(newPub) // rotation finished
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.cached(KeyID(oldPub)); ok {
		t.Fatal("retired key still trusted after a refresh")
	}
	if _, ok := c.cached(KeyID(newPub)); !ok {
		t.Fatal("current key lost")
	}
}

func TestJWKSClientKeepsLastGoodKeysWhenTheIssuerIsDown(t *testing.T) {
	_, pub := keyPair(t)
	ks := newKeyServer(t, pub)
	c, _ := newClient(t, ks, nil)
	_ = c.Refresh(context.Background())
	ks.fail(http.StatusServiceUnavailable)
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("refresh against a failing issuer reported success")
	}
	if _, ok := c.cached(KeyID(pub)); !ok {
		t.Fatal("a failed refresh threw away the last good keys")
	}
}

func TestJWKSClientNotReadyWithoutKeys(t *testing.T) {
	_, pub := keyPair(t)
	ks := newKeyServer(t, pub)
	ks.fail(http.StatusInternalServerError)
	c, _ := newClient(t, ks, nil)
	_ = c.Refresh(context.Background())
	if err := c.Check(context.Background()); err == nil {
		t.Fatal("ready with no keys")
	}
	// Static keys (key files) make it ready on their own.
	s, _ := newClient(t, ks, map[string]ed25519.PublicKey{KeyID(pub): pub})
	if err := s.Check(context.Background()); err != nil {
		t.Fatalf("not ready with a static key: %v", err)
	}
	if _, ok := s.Lookup(KeyID(pub)); !ok {
		t.Fatal("static key not found")
	}
}

func TestJWKSClientRejectsBadKeySets(t *testing.T) {
	_, pub := keyPair(t)
	_, other := keyPair(t)
	good := PublicJWK(pub)
	posing := PublicJWK(other)
	posing.Kid = KeyID(pub) // another key claiming pub's ID
	wrongCurve := good
	wrongCurve.Crv = "X25519"
	encryption := good
	encryption.Use = "enc"
	shortX := good
	shortX.X = "AAAA"
	for name, body := range map[string]string{
		"not JSON":       "<html>",
		"no usable keys": mustJSON(JWKSet{Keys: []JWK{posing, wrongCurve, encryption, shortX}}),
		"oversized":      `{"keys":[` + strings.Repeat(" ", maxJWKSBytes) + `]}`,
		"empty set":      `{"keys":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			ks := newKeyServer(t)
			ks.mu.Lock()
			ks.body = []byte(body)
			ks.mu.Unlock()
			c, _ := newClient(t, ks, nil)
			if err := c.Refresh(context.Background()); err == nil {
				t.Fatal("bad key set accepted")
			}
			if c.Known() != 0 {
				t.Fatalf("%d keys trusted from a bad set", c.Known())
			}
		})
	}
	// Bad entries are skipped; good ones in the same set survive.
	ks := newKeyServer(t)
	ks.mu.Lock()
	ks.body = []byte(mustJSON(JWKSet{Keys: []JWK{posing, good, wrongCurve}}))
	ks.mu.Unlock()
	c, _ := newClient(t, ks, nil)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if k, ok := c.cached(KeyID(pub)); !ok || !k.Equal(pub) {
		t.Fatal("the posing key replaced the real one, or the real one was lost")
	}
}

func TestJWKSClientKnownKeysNeverWaitOnAFetch(t *testing.T) {
	_, pub := keyPair(t)
	ks := newKeyServer(t, pub)
	c, clock := newClient(t, ks, nil)
	_ = c.Refresh(context.Background())
	*clock = clock.Add(time.Minute)
	ks.mu.Lock()
	ks.delay = 500 * time.Millisecond // a slow issuer
	ks.mu.Unlock()
	go c.Lookup("unknown-kid") // starts a slow fetch
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if _, ok := c.Lookup(KeyID(pub)); !ok {
		t.Fatal("known key not found")
	}
	if waited := time.Since(start); waited > 100*time.Millisecond {
		t.Fatalf("a known key waited %s on a fetch for an unknown kid", waited)
	}
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
