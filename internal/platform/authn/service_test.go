package authn

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, k, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func pub(k ed25519.PrivateKey) ed25519.PublicKey { return k.Public().(ed25519.PublicKey) }

func TestServiceTokensIdentifyTheCaller(t *testing.T) {
	booking := newKey(t)
	src := NewServiceTokenSource(booking, "booking", "inventory")
	v := NewServiceVerifier("inventory", map[string]ed25519.PublicKey{"booking": pub(booking)}, time.Second)
	tok, err := src.Token()
	if err != nil {
		t.Fatal(err)
	}
	caller, err := v.Verify(tok)
	if err != nil || caller != "booking" {
		t.Fatalf("Verify = %q, %v; want booking", caller, err)
	}
}

func TestServiceTokensAreReusedUntilAThirdOfTheirLifeIsLeft(t *testing.T) {
	src := NewServiceTokenSource(newKey(t), "booking", "inventory")
	now := time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	src.now = func() time.Time { return now }
	first, _ := src.Token()
	now = now.Add(3 * time.Minute) // 2 of 5 minutes left: still more than a third
	if again, _ := src.Token(); again != first {
		t.Fatal("token re-signed while still fresh")
	}
	now = now.Add(90 * time.Second) // 30 s left
	if fresh, _ := src.Token(); fresh == first {
		t.Fatal("token not re-signed near its expiry")
	}
}

func TestServiceVerifierRefuses(t *testing.T) {
	booking, stranger, queue := newKey(t), newKey(t), newKey(t)
	trusted := map[string]ed25519.PublicKey{"booking": pub(booking)}
	v := NewServiceVerifier("inventory", trusted, time.Second)

	tok := func(k ed25519.PrivateKey, caller, callee string) string {
		s, err := NewServiceTokenSource(k, caller, callee).Token()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	admission, _, err := NewIssuer(queue, time.Minute).Issue("0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77", "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e78", "s", 1)
	if err != nil {
		t.Fatal(err)
	}
	expired := NewServiceTokenSource(booking, "booking", "inventory")
	expired.now = func() time.Time { return time.Now().Add(-time.Hour) }
	old, _ := expired.Token()

	cases := map[string]string{
		"another callee's token":                tok(booking, "booking", "payment"),
		"an unknown caller":                     tok(stranger, "auditor", "inventory"),
		"a trusted name with a stranger's key":  tok(stranger, "booking", "inventory"),
		"an admission token":                    admission,
		"an expired token":                      old,
		"garbage":                               "not.a.token",
		"a token for inventory signed by queue": tok(queue, "queue", "inventory"),
	}
	for name, token := range cases {
		if caller, err := v.Verify(token); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: Verify = %q, %v; want ErrInvalidToken", name, caller, err)
		}
	}
	// And a service token never passes as an admission token.
	if _, err := NewVerifier(map[string]ed25519.PublicKey{KeyID(pub(booking)): pub(booking)}, time.Second).Verify(tok(booking, "booking", "inventory")); err == nil {
		t.Error("a service token passed as an admission token")
	}
}

func TestLoadServiceKeys(t *testing.T) {
	dir := t.TempDir()
	_, pubPEM, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "booking.pub")
	if err := os.WriteFile(path, pubPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	keys, err := LoadServiceKeys([]string{"booking=" + path})
	if err != nil || len(keys["booking"]) != ed25519.PublicKeySize {
		t.Fatalf("LoadServiceKeys = %v, %v", keys, err)
	}
	for _, bad := range [][]string{nil, {"booking"}, {"=" + path}, {"booking=" + filepath.Join(dir, "missing.pub")}} {
		if _, err := LoadServiceKeys(bad); err == nil {
			t.Errorf("LoadServiceKeys(%v) accepted", bad)
		}
	}
}
