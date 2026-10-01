package authn

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIssueUntilCapsTheExpiry(t *testing.T) {
	priv, pub := keyPair(t)
	iss := NewIssuer(priv, 10*time.Minute)
	ver := NewVerifier(map[string]ed25519.PublicKey{KeyID(pub): pub}, time.Second)

	notAfter := time.Now().Add(90 * time.Second)
	tok, exp, err := iss.IssueUntil(uuid.NewString(), uuid.NewString(), "s", 3, notAfter)
	if err != nil {
		t.Fatal(err)
	}
	if !exp.Equal(notAfter) {
		t.Fatalf("expiry %s, want the cap %s", exp, notAfter)
	}
	claims, err := ver.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	if got := claims.ExpiresAt.Unix(); got != notAfter.Unix() {
		t.Fatalf("exp claim %d, want %d", got, notAfter.Unix())
	}

	// A cap later than the TTL leaves the TTL in charge.
	_, exp, err = iss.IssueUntil(uuid.NewString(), uuid.NewString(), "s", 3, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(exp); d > 10*time.Minute || d < 9*time.Minute {
		t.Fatalf("expiry in %s, want the 10m TTL", d)
	}

	// A cap in the past refuses to sign an already-expired token.
	if _, _, err := iss.IssueUntil(uuid.NewString(), uuid.NewString(), "s", 3, time.Now().Add(-time.Second)); err == nil {
		t.Fatal("signed a token that is already expired")
	}
}

func TestJWKSet(t *testing.T) {
	_, a := keyPair(t)
	_, b := keyPair(t)
	set := NewJWKSet(a, b, a)
	if len(set.Keys) != 2 {
		t.Fatalf("%d keys, want 2 (duplicates removed)", len(set.Keys))
	}
	j := set.Keys[0]
	if j.Kty != "OKP" || j.Crv != "Ed25519" || j.Alg != "EdDSA" || j.Use != "sig" || j.Kid != KeyID(a) {
		t.Fatalf("jwk %+v", j)
	}
	raw, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil || !ed25519.PublicKey(raw).Equal(a) {
		t.Fatalf("x does not decode to the public key: %v", err)
	}
	if set.Keys[1].Kid != KeyID(b) {
		t.Fatal("keys out of order")
	}
}
