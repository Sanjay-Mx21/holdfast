package authn

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	// IssuerQueue is the "iss" of admission tokens (queue-svc issues them).
	IssuerQueue = "holdfast-queue"
	// AudienceInventory is the "aud" of admission tokens (inventory-svc consumes them).
	AudienceInventory = "holdfast-inventory"
)

// ErrInvalidToken wraps every verification failure. The wrapped detail is for
// logs; clients only ever see a generic 401.
var ErrInvalidToken = errors.New("authn: invalid token")

// AdmissionClaims is the capability token a user receives when the waiting
// room admits them. inventory-svc accepts holds only with a valid one, scoped
// to exactly one event and user.
type AdmissionClaims struct {
	EventID   string `json:"evt"`
	SessionID string `json:"sid"`
	Rank      int64  `json:"rank,omitempty"`
	jwt.RegisteredClaims
}

// Issuer signs admission tokens.
type Issuer struct {
	key ed25519.PrivateKey
	kid string
	ttl time.Duration
	now func() time.Time
}

// NewIssuer returns an Issuer whose tokens live for ttl.
func NewIssuer(key ed25519.PrivateKey, ttl time.Duration) *Issuer {
	return &Issuer{key: key, kid: KeyID(key.Public().(ed25519.PublicKey)), ttl: ttl, now: time.Now}
}

// Issue signs a token for userID on eventID and returns it with its expiry.
func (i *Issuer) Issue(userID, eventID, sessionID string, rank int64) (string, time.Time, error) {
	now := i.now()
	exp := now.Add(i.ttl)
	claims := AdmissionClaims{
		EventID:   eventID,
		SessionID: sessionID,
		Rank:      rank,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    IssuerQueue,
			Subject:   userID,
			Audience:  jwt.ClaimStrings{AudienceInventory},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        uuid.NewString(),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = i.kid
	signed, err := tok.SignedString(i.key)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("authn: sign: %w", err)
	}
	return signed, exp, nil
}

// Verifier checks admission tokens against a set of trusted public keys.
type Verifier struct {
	keys   map[string]ed25519.PublicKey
	parser *jwt.Parser
}

// NewVerifier trusts keys (indexed by KeyID) and tolerates leeway of clock skew.
func NewVerifier(keys map[string]ed25519.PublicKey, leeway time.Duration) *Verifier {
	return &Verifier{
		keys: keys,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}), // rejects "none" and HMAC confusion
			jwt.WithIssuer(IssuerQueue),
			jwt.WithAudience(AudienceInventory),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(leeway),
		),
	}
}

// Verify parses and validates token, returning its claims.
func (v *Verifier) Verify(token string) (*AdmissionClaims, error) {
	claims := &AdmissionClaims{}
	_, err := v.parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		key, ok := v.keys[kid]
		if !ok {
			return nil, fmt.Errorf("unknown key id %q", kid)
		}
		return key, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if _, err := uuid.Parse(claims.Subject); err != nil {
		return nil, fmt.Errorf("%w: subject is not a UUID", ErrInvalidToken)
	}
	if _, err := uuid.Parse(claims.EventID); err != nil {
		return nil, fmt.Errorf("%w: event is not a UUID", ErrInvalidToken)
	}
	return claims, nil
}
