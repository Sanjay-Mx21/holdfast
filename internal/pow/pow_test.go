package pow

import (
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

var secret = []byte("test-only-pow-secret-0123456789abcdef")

const (
	event = "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e77"
	user  = "0196f0c1-7a3e-7c51-9b0e-5d2f8a1c4e78"
)

func TestIssueSolveVerify(t *testing.T) {
	iss, err := NewIssuer(secret, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c, err := iss.Issue(event, user, 12)
	if err != nil || c.Difficulty != 12 || time.Until(c.ExpiresAt) < 55*time.Second {
		t.Fatalf("Issue = %+v, %v", c, err)
	}
	if d, err := DifficultyOf(c.Token); err != nil || d != 12 {
		t.Fatalf("DifficultyOf = %d, %v", d, err)
	}
	nonce := Solve(c.Token, 12)
	if err := iss.Verify(event, user, c.Token, nonce); err != nil {
		t.Fatalf("a solution was refused: %v", err)
	}

	cases := []struct {
		name                      string
		event, user, token, nonce string
	}{
		{"another user", event, "0196f0c1-0000-7c51-9b0e-5d2f8a1c4e78", c.Token, nonce},
		{"another event", "0196f0c1-0000-7c51-9b0e-5d2f8a1c4e77", user, c.Token, nonce},
		{"a forged difficulty", event, user, strings.Replace(c.Token, ".12.", ".1.", 1), nonce},
		{"a token signed with another secret", event, user, other(t, c), nonce},
		{"not a token", event, user, "nope", nonce},
		{"an empty nonce", event, user, c.Token, ""},
		{"a non-numeric nonce", event, user, c.Token, "12a"},
		{"a huge nonce", event, user, c.Token, strings.Repeat("9", 21)},
	}
	for _, tc := range cases {
		if err := iss.Verify(tc.event, tc.user, tc.token, tc.nonce); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", tc.name, err)
		}
	}
	// A nonce that does not solve it: most do not, at 12 bits.
	for n := 0; ; n++ {
		if bad := strconv.Itoa(n); LeadingZeroBits(Hash(c.Token, bad)) < 12 {
			if err := iss.Verify(event, user, c.Token, bad); !errors.Is(err, ErrInvalid) {
				t.Fatalf("a wrong nonce: %v, want ErrInvalid", err)
			}
			break
		}
	}

	iss.now = func() time.Time { return time.Now().Add(time.Minute) }
	if err := iss.Verify(event, user, c.Token, nonce); !errors.Is(err, ErrExpired) {
		t.Fatalf("an expired challenge: %v, want ErrExpired", err)
	}
}

func other(t *testing.T, c Challenge) string {
	t.Helper()
	iss, _ := NewIssuer([]byte("another-secret-0123456789abcdef-xyz"), time.Minute)
	o, _ := iss.Issue(event, user, c.Difficulty)
	parts := strings.Split(c.Token, ".")
	return strings.Join(parts[:3], ".") + "." + strings.Split(o.Token, ".")[3]
}

func TestIssuerRefusesBadSettings(t *testing.T) {
	if _, err := NewIssuer([]byte("short"), time.Minute); err == nil {
		t.Fatal("a short secret was accepted")
	}
	iss, _ := NewIssuer(secret, time.Minute)
	for _, d := range []int{0, MaxDifficulty + 1} {
		if _, err := iss.Issue(event, user, d); err == nil {
			t.Fatalf("difficulty %d was accepted", d)
		}
	}
}

func TestLeadingZeroBits(t *testing.T) {
	var h [32]byte
	if LeadingZeroBits(h) != 256 {
		t.Fatal("all zero")
	}
	h[0] = 0x80
	if LeadingZeroBits(h) != 0 {
		t.Fatal("first bit set")
	}
	h[0], h[1] = 0, 0x10
	if got := LeadingZeroBits(h); got != 11 {
		t.Fatalf("got %d, want 11", got)
	}
}

// TestHashVector pins the work function, so the web app's solver can be
// checked against the same values (web/src/lib/pow/solve.test.ts).
func TestHashVector(t *testing.T) {
	// Computed independently: Python's hashlib.sha256 of "<token>:0", and the
	// first nonce from 0 up with 8 leading zero bits.
	const token = "1700000000000.8.AAAAAAAAAAAAAAAAAAAAAA.mac"
	h := Hash(token, "0")
	if got := hex.EncodeToString(h[:4]); got != "dce31f38" {
		t.Fatalf("hash prefix %s, want dce31f38", got)
	}
	if n := Solve(token, 8); n != "458" {
		t.Fatalf("first solution at 8 bits: %s, want 458", n)
	}
}

func TestDifficultyRisesWithTheRate(t *testing.T) {
	d, err := NewDifficulty(18, 22, 100)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return now }
	for range 100 {
		if got := d.Next(); got != 18 {
			t.Fatalf("below the surge rate: %d, want 18", got)
		}
	}
	if got := d.Next(); got != 19 { // 101 per second: above the surge
		t.Fatalf("just above the surge: %d, want 19", got)
	}
	for range 300 { // 401 per second: two doublings above
		d.Next()
	}
	if got := d.Current(); got != 21 {
		t.Fatalf("401 per second: %d, want 21", got)
	}
	for range 10_000 {
		d.Next()
	}
	if got := d.Current(); got != 22 {
		t.Fatalf("capped: %d, want 22", got)
	}
	now = now.Add(1500 * time.Millisecond) // the next second remembers the surge
	if got := d.Current(); got != 22 {
		t.Fatalf("the second after a surge: %d, want 22", got)
	}
	now = now.Add(3 * time.Second) // a quiet gap: back to the base
	if got := d.Next(); got != 18 {
		t.Fatalf("after a quiet gap: %d, want 18", got)
	}
	if _, err := NewDifficulty(20, 18, 100); err == nil {
		t.Fatal("base above max was accepted")
	}
	if _, err := NewDifficulty(18, 22, 0); err == nil {
		t.Fatal("a zero surge rate was accepted")
	}
}
