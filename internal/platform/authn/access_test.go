package authn

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestAccessTokenRoundTrip(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	issuer := NewAccessIssuer(priv, 15*time.Minute)
	user := uuid.NewString()
	tok, exp, err := issuer.Issue(user, RoleBuyer)
	if err != nil || time.Until(exp) < 14*time.Minute {
		t.Fatalf("Issue = %v, expires %s", err, exp)
	}
	set := NewJWKSet(pub)
	lookup := func(kid string) (ed25519.PublicKey, bool) {
		for _, k := range set.Keys {
			if k.Kid == kid {
				key, err := ParseJWK(k)
				return key, err == nil
			}
		}
		return nil, false
	}
	claims, err := NewAccessVerifier(lookup, time.Second).Verify(tok)
	if err != nil || claims.Subject != user || claims.Role != RoleBuyer || claims.Issuer != IssuerAuth {
		t.Fatalf("Verify = %+v, %v", claims, err)
	}
}

func TestAccessVerifierRefuses(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	trusted := func(kid string) (ed25519.PublicKey, bool) { return pub, kid == KeyID(pub) }
	v := NewAccessVerifier(trusted, time.Second)
	sign := func(key ed25519.PrivateKey, kid string, claims AccessClaims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		tok.Header["kid"] = kid
		s, err := tok.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	now := time.Now()
	good := func() AccessClaims {
		return AccessClaims{Role: RoleBuyer, RegisteredClaims: jwt.RegisteredClaims{
			Issuer: IssuerAuth, Subject: uuid.NewString(), Audience: jwt.ClaimStrings{AudienceAPI},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
		}}
	}
	expired, wrongIss, wrongAud, badRole, badSub := good(), good(), good(), good(), good()
	expired.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Minute))
	wrongIss.Issuer = IssuerQueue
	wrongAud.Audience = jwt.ClaimStrings{AudienceInventory}
	badRole.Role = "ROOT"
	badSub.Subject = "alice"
	hmacTok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, good()).SignedString([]byte("secret"))
	cases := map[string]string{
		"another key":        sign(other, KeyID(pub), good()),
		"an unknown kid":     sign(priv, "nope", good()),
		"expired":            sign(priv, KeyID(pub), expired),
		"an admission token": sign(priv, KeyID(pub), wrongIss),
		"another audience":   sign(priv, KeyID(pub), wrongAud),
		"an unknown role":    sign(priv, KeyID(pub), badRole),
		"a non-UUID subject": sign(priv, KeyID(pub), badSub),
		"HMAC, not EdDSA":    hmacTok,
		"not a token":        "not.a.token",
	}
	for name, tok := range cases {
		if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: %v, want ErrInvalidToken", name, err)
		}
	}
	if _, err := v.Verify(sign(priv, KeyID(pub), good())); err != nil {
		t.Fatalf("a good token: %v", err)
	}
}
