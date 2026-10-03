package authn

import (
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	// IssuerAuth is the "iss" of access tokens (auth-svc issues them).
	IssuerAuth = "holdfast-auth"
	// AudienceAPI is the "aud" of access tokens: HoldFast's buyer-facing APIs.
	AudienceAPI = "holdfast-api"
)

// Roles a user can have (design doc 8.1). The policy engine (Phase 4) keeps
// agents out of the opening minutes.
const (
	RoleBuyer = "BUYER"
	RoleAgent = "AGENT"
	RoleAdmin = "ADMIN"
)

// AccessClaims identify a signed-in user: the subject is the user's ID.
// Verified says the user's identity is verified (a phone code, in this
// build); the policy engine opens a sale's first window to verified buyers
// only.
type AccessClaims struct {
	Role     string `json:"role"`
	Verified bool   `json:"vrf"`
	jwt.RegisteredClaims
}

// AccessIssuer signs access tokens.
type AccessIssuer struct {
	key ed25519.PrivateKey
	kid string
	ttl time.Duration
	now func() time.Time
}

// NewAccessIssuer returns an issuer whose tokens live for ttl (15 minutes in
// the design: short, because they cannot be revoked).
func NewAccessIssuer(key ed25519.PrivateKey, ttl time.Duration) *AccessIssuer {
	return &AccessIssuer{key: key, kid: KeyID(key.Public().(ed25519.PublicKey)), ttl: ttl, now: time.Now}
}

// Issue signs a token for userID with role, saying whether the user's
// identity is verified, and returns it with its expiry.
func (i *AccessIssuer) Issue(userID, role string, verified bool) (string, time.Time, error) {
	now := i.now()
	exp := now.Add(i.ttl)
	claims := AccessClaims{
		Role:     role,
		Verified: verified,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    IssuerAuth,
			Subject:   userID,
			Audience:  jwt.ClaimStrings{AudienceAPI},
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

// AccessVerifier checks access tokens against trusted public keys, usually a
// JWKSClient following auth-svc's key set.
type AccessVerifier struct {
	lookup KeyLookup
	parser *jwt.Parser
}

// NewAccessVerifier trusts whatever lookup returns and tolerates leeway of
// clock skew.
func NewAccessVerifier(lookup KeyLookup, leeway time.Duration) *AccessVerifier {
	return &AccessVerifier{
		lookup: lookup,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodEdDSA.Alg()}),
			jwt.WithIssuer(IssuerAuth),
			jwt.WithAudience(AudienceAPI),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(leeway),
		),
	}
}

// Verify parses and validates token, returning its claims.
func (v *AccessVerifier) Verify(token string) (*AccessClaims, error) {
	claims := &AccessClaims{}
	_, err := v.parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		key, ok := v.lookup(kid)
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
	switch claims.Role {
	case RoleBuyer, RoleAgent, RoleAdmin:
	default:
		return nil, fmt.Errorf("%w: unknown role %q", ErrInvalidToken, claims.Role)
	}
	return claims, nil
}
