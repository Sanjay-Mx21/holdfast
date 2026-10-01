package authn

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// maxJWKSBytes bounds a JWKS response: a handful of keys is a few hundred
// bytes, so anything near this is not a key set.
const maxJWKSBytes = 64 << 10

// JWKSOptions configures a JWKSClient.
type JWKSOptions struct {
	// URL of the issuer's key set, e.g. http://queue:8080/.well-known/jwks.json.
	URL string
	// Static keys are always trusted, whatever the URL serves (key files).
	Static map[string]ed25519.PublicKey
	// RefreshEvery is the periodic refresh (default 5m).
	RefreshEvery time.Duration
	// MinRefresh is the shortest gap between fetches, so tokens with
	// unknown key IDs cannot make this client hammer the issuer (default 30s).
	MinRefresh time.Duration
	// Timeout bounds one fetch (default 2s).
	Timeout time.Duration
	// Client defaults to a plain http.Client.
	Client *http.Client
}

// JWKSClient follows an issuer's published key set. It refreshes it
// periodically and when a token arrives signed by a key ID it does not know
// (at most every MinRefresh), so a key rotation at the issuer needs no
// restart here. A failed fetch keeps the last good keys.
type JWKSClient struct {
	opt     JWKSOptions
	log     *slog.Logger
	fetches *prometheus.CounterVec

	fetchMu   sync.Mutex   // serialises fetches; held across the network call
	mu        sync.RWMutex // guards keys; never held across the network call
	keys      map[string]ed25519.PublicKey
	lastFetch time.Time // guarded by fetchMu
	now       func() time.Time
}

// NewJWKSClient returns a client for opt.URL. Call Refresh once at start-up
// and run it as an app component for periodic refreshes.
func NewJWKSClient(opt JWKSOptions, reg prometheus.Registerer, log *slog.Logger) *JWKSClient {
	if opt.RefreshEvery <= 0 {
		opt.RefreshEvery = 5 * time.Minute
	}
	if opt.MinRefresh <= 0 {
		opt.MinRefresh = 30 * time.Second
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 2 * time.Second
	}
	if opt.Client == nil {
		opt.Client = &http.Client{}
	}
	c := &JWKSClient{
		opt: opt,
		log: log,
		fetches: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_authn_jwks_fetches_total",
			Help: "Fetches of the admission-token key set, by result.",
		}, []string{"result"}),
		keys: map[string]ed25519.PublicKey{},
		now:  time.Now,
	}
	for _, r := range []string{"ok", "error"} {
		c.fetches.WithLabelValues(r)
	}
	return c
}

// Lookup implements KeyLookup. Known keys are answered without waiting on
// any fetch. An unknown key ID triggers a refresh, unless one happened
// within MinRefresh, so tokens with made-up key IDs cannot make this client
// hammer the issuer or stall verification of good tokens.
func (c *JWKSClient) Lookup(kid string) (ed25519.PublicKey, bool) {
	if k, ok := c.cached(kid); ok {
		return k, true
	}
	if kid == "" {
		return nil, false
	}
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	if k, ok := c.cached(kid); ok { // fetched while we waited
		return k, true
	}
	if c.now().Sub(c.lastFetch) < c.opt.MinRefresh {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.opt.Timeout)
	defer cancel()
	if err := c.fetchLocked(ctx); err != nil {
		c.log.Warn("jwks: refresh for an unknown key id failed", "kid", kid, "err", err)
	}
	return c.cached(kid)
}

func (c *JWKSClient) cached(kid string) (ed25519.PublicKey, bool) {
	if k, ok := c.opt.Static[kid]; ok {
		return k, true
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	k, ok := c.keys[kid]
	return k, ok
}

// Refresh fetches the key set now.
func (c *JWKSClient) Refresh(ctx context.Context) error {
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, c.opt.Timeout)
	defer cancel()
	return c.fetchLocked(ctx)
}

// Known reports how many keys are trusted, static ones included.
func (c *JWKSClient) Known() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := len(c.opt.Static)
	for kid := range c.keys {
		if _, dup := c.opt.Static[kid]; !dup {
			n++
		}
	}
	return n
}

// Check is a readiness probe: a verifier with no keys can verify nothing.
func (c *JWKSClient) Check(context.Context) error {
	if c.Known() == 0 {
		return errors.New("no admission-token keys known yet")
	}
	return nil
}

// Name implements app.Component.
func (c *JWKSClient) Name() string { return "jwks-refresh" }

// Run implements app.Component: periodic refreshes until ctx ends.
func (c *JWKSClient) Run(ctx context.Context) error {
	t := time.NewTicker(c.opt.RefreshEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
				c.log.Warn("jwks: periodic refresh failed; keeping the last good keys", "err", err)
			}
		}
	}
}

// fetchLocked replaces the fetched keys with the issuer's current set.
// Callers hold c.fetchMu; c.mu is taken only to swap in the new map.
func (c *JWKSClient) fetchLocked(ctx context.Context) error {
	c.lastFetch = c.now()
	keys, err := c.get(ctx)
	if err != nil {
		c.fetches.WithLabelValues("error").Inc()
		return err
	}
	c.fetches.WithLabelValues("ok").Inc()
	c.mu.Lock()
	c.keys = keys
	c.mu.Unlock()
	return nil
}

func (c *JWKSClient) get(ctx context.Context) (map[string]ed25519.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.opt.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.opt.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jwks: fetch: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks: fetch: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return nil, fmt.Errorf("jwks: read: %w", err)
	}
	if len(body) > maxJWKSBytes {
		return nil, errors.New("jwks: response too large")
	}
	var set JWKSet
	if err := json.Unmarshal(body, &set); err != nil {
		return nil, fmt.Errorf("jwks: decode: %w", err)
	}
	keys := make(map[string]ed25519.PublicKey, len(set.Keys))
	for _, j := range set.Keys {
		pub, err := ParseJWK(j)
		if err != nil {
			c.log.Warn("jwks: skipping a key", "kid", j.Kid, "err", err)
			continue
		}
		keys[j.Kid] = pub
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks: no usable Ed25519 signing keys")
	}
	return keys, nil
}

// ParseJWK accepts only what HoldFast issues: an Ed25519 signing key (OKP)
// whose kid is the KeyID of the key itself, so a key cannot pose under
// another key's ID.
func ParseJWK(j JWK) (ed25519.PublicKey, error) {
	if j.Kty != "OKP" || j.Crv != "Ed25519" {
		return nil, fmt.Errorf("unsupported key type %q/%q", j.Kty, j.Crv)
	}
	if j.Alg != "" && j.Alg != "EdDSA" {
		return nil, fmt.Errorf("unsupported alg %q", j.Alg)
	}
	if j.Use != "" && j.Use != "sig" {
		return nil, fmt.Errorf("not a signing key (use %q)", j.Use)
	}
	raw, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("x is not a base64url Ed25519 public key")
	}
	pub := ed25519.PublicKey(raw)
	if KeyID(pub) != j.Kid {
		return nil, errors.New("kid does not match the key")
	}
	return pub, nil
}
