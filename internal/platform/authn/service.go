package authn

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Service-to-service authentication (decided in task 3.4): a calling service
// signs a short-lived Ed25519 token naming itself as issuer and subject and
// the callee as audience; the callee verifies it with that caller's public
// key and lets each caller use only the methods it needs. Service tokens can
// never pass for admission tokens or the reverse: their audiences, type
// header and trusted keys all differ.

const (
	serviceTokenType = "svc+jwt"
	serviceTokenTTL  = 5 * time.Minute
)

// ServiceAudience is the audience of tokens for calls to service, for
// example "service:inventory".
func ServiceAudience(service string) string { return "service:" + service }

// ServiceIssuer is the issuer (and subject) of tokens signed by service.
func ServiceIssuer(service string) string { return "service:" + service }

// ServiceTokenSource signs the caller's tokens and reuses each one until a
// third of its life is left, so a busy client signs a few per hour, not one
// per call.
type ServiceTokenSource struct {
	key      ed25519.PrivateKey
	kid      string
	caller   string
	audience string
	ttl      time.Duration
	now      func() time.Time

	mu    sync.Mutex
	token string
	exp   time.Time
}

// NewServiceTokenSource returns a source of tokens for calls from caller to
// callee (service names such as "booking" and "inventory").
func NewServiceTokenSource(key ed25519.PrivateKey, caller, callee string) *ServiceTokenSource {
	return &ServiceTokenSource{
		key: key, kid: KeyID(key.Public().(ed25519.PublicKey)),
		caller: ServiceIssuer(caller), audience: ServiceAudience(callee),
		ttl: serviceTokenTTL, now: time.Now,
	}
}

// Token returns a valid token.
func (s *ServiceTokenSource) Token() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.token != "" && s.exp.Sub(now) > s.ttl/3 {
		return s.token, nil
	}
	exp := now.Add(s.ttl)
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{
		Issuer:    s.caller,
		Subject:   s.caller,
		Audience:  jwt.ClaimStrings{s.audience},
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
		ID:        uuid.NewString(),
	})
	tok.Header["kid"] = s.kid
	tok.Header["typ"] = serviceTokenType
	signed, err := tok.SignedString(s.key)
	if err != nil {
		return "", fmt.Errorf("authn: sign service token: %w", err)
	}
	s.token, s.exp = signed, exp
	return signed, nil
}

// ServiceVerifier checks tokens presented to one service.
type ServiceVerifier struct {
	callers map[string]ed25519.PublicKey // issuer ("service:booking") -> key
	parser  *jwt.Parser
}

// NewServiceVerifier trusts the given callers (service name -> public key)
// for calls to service, tolerating leeway of clock skew.
func NewServiceVerifier(service string, callers map[string]ed25519.PublicKey, leeway time.Duration) *ServiceVerifier {
	byIssuer := make(map[string]ed25519.PublicKey, len(callers))
	for name, key := range callers {
		byIssuer[ServiceIssuer(name)] = key
	}
	return &ServiceVerifier{
		callers: byIssuer,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
			jwt.WithAudience(ServiceAudience(service)),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(leeway),
		),
	}
}

// Verify checks token and returns the calling service's name.
func (v *ServiceVerifier) Verify(token string) (string, error) {
	claims := &jwt.RegisteredClaims{}
	_, err := v.parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if typ, _ := t.Header["typ"].(string); typ != serviceTokenType {
			return nil, fmt.Errorf("not a service token (typ %q)", typ)
		}
		iss, _ := t.Claims.GetIssuer()
		key, ok := v.callers[iss]
		if !ok {
			return nil, fmt.Errorf("unknown caller %q", iss)
		}
		if kid, _ := t.Header["kid"].(string); kid != KeyID(key) {
			return nil, fmt.Errorf("key id %q is not the caller's key", kid)
		}
		return key, nil
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if claims.Subject != claims.Issuer {
		return "", fmt.Errorf("%w: subject %q is not the issuer", ErrInvalidToken, claims.Subject)
	}
	return strings.TrimPrefix(claims.Issuer, "service:"), nil
}

// LoadServiceKeys reads trusted callers' public keys from specs of the form
// "booking=/keys/booking.pub".
func LoadServiceKeys(specs []string) (map[string]ed25519.PublicKey, error) {
	out := make(map[string]ed25519.PublicKey, len(specs))
	for _, spec := range specs {
		name, path, ok := strings.Cut(spec, "=")
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("authn: %q is not service=path", spec)
		}
		raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied key path
		if err != nil {
			return nil, fmt.Errorf("authn: read %s's key: %w", name, err)
		}
		key, err := ParsePublicKeyPEM(raw)
		if err != nil {
			return nil, fmt.Errorf("authn: %s's key: %w", name, err)
		}
		out[name] = key
	}
	if len(out) == 0 {
		return nil, errors.New("authn: no trusted callers")
	}
	return out, nil
}
