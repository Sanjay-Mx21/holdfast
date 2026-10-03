// Package pow is the waiting room's proof of work (design doc section 11):
// before joining, a client must find a nonce whose hash with a fresh,
// server-signed challenge starts with enough zero bits. A browser spends
// about a second on it in a Web Worker; a bot that wants a thousand
// identities spends a thousand seconds. It raises the cost of automation; it
// does not stop a determined, well-funded attacker, and it is one layer among
// several (rate limits, verified identities, the policy windows).
//
// Challenges are stateless: one is an HMAC over the event, the user, its
// expiry, its difficulty and a random value, so any replica can verify it
// with one HMAC and one SHA-256, and nothing is stored. A solved challenge
// stays valid until it expires, but it is bound to one user and one event,
// and joining is idempotent, so reusing it gains nothing.
//
// The work: find a nonce (a decimal string) such that
//
//	SHA-256(challenge + ":" + nonce)
//
// has at least the challenge's difficulty in leading zero bits. Expected
// work is 2^difficulty hashes. The web app's solver
// (web/src/lib/pow/solve.ts) implements the same rule.
package pow

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

// Errors returned by Verify.
var (
	ErrInvalid = errors.New("pow: invalid proof of work")
	ErrExpired = errors.New("pow: challenge expired")
)

// MaxDifficulty bounds a difficulty: 2^30 hashes is minutes even for a
// laptop, far beyond any sensible setting.
const MaxDifficulty = 30

// Challenge is an issued challenge.
type Challenge struct {
	// Token is what the client hashes and sends back: expiry, difficulty, a
	// random value and the MAC, dot-separated.
	Token      string
	Difficulty int
	ExpiresAt  time.Time
}

// Issuer issues and verifies challenges with a secret every replica shares.
type Issuer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

// NewIssuer returns an Issuer. The secret must be at least 32 bytes.
func NewIssuer(secret []byte, ttl time.Duration) (*Issuer, error) {
	if len(secret) < 32 {
		return nil, errors.New("pow: the secret must be at least 32 bytes")
	}
	if ttl <= 0 {
		return nil, errors.New("pow: the challenge lifetime must be positive")
	}
	return &Issuer{secret: secret, ttl: ttl, now: time.Now}, nil
}

// Issue returns a challenge of the given difficulty for userID on eventID.
func (i *Issuer) Issue(eventID, userID string, difficulty int) (Challenge, error) {
	if difficulty < 1 || difficulty > MaxDifficulty {
		return Challenge{}, fmt.Errorf("pow: difficulty must be between 1 and %d", MaxDifficulty)
	}
	var r [16]byte
	if _, err := rand.Read(r[:]); err != nil {
		return Challenge{}, fmt.Errorf("pow: random: %w", err)
	}
	exp := i.now().Add(i.ttl).UnixMilli()
	head := strconv.FormatInt(exp, 10) + "." + strconv.Itoa(difficulty) + "." + base64.RawURLEncoding.EncodeToString(r[:])
	return Challenge{
		Token:      head + "." + i.mac(eventID, userID, head),
		Difficulty: difficulty,
		ExpiresAt:  time.UnixMilli(exp).UTC(),
	}, nil
}

// Verify checks that nonce solves token for userID on eventID: the MAC is
// ours, the challenge has not expired, and the hash has enough zero bits.
func (i *Issuer) Verify(eventID, userID, token, nonce string) error {
	parts := strings.Split(token, ".")
	if len(parts) != 4 || len(nonce) == 0 || len(nonce) > 20 || strings.Trim(nonce, "0123456789") != "" {
		return ErrInvalid
	}
	head := parts[0] + "." + parts[1] + "." + parts[2]
	if !hmac.Equal([]byte(parts[3]), []byte(i.mac(eventID, userID, head))) {
		return ErrInvalid
	}
	exp, err1 := strconv.ParseInt(parts[0], 10, 64)
	difficulty, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return ErrInvalid // cannot happen for a token we signed
	}
	if !i.now().Before(time.UnixMilli(exp)) {
		return ErrExpired
	}
	if LeadingZeroBits(Hash(token, nonce)) < difficulty {
		return ErrInvalid
	}
	return nil
}

func (i *Issuer) mac(eventID, userID, head string) string {
	m := hmac.New(sha256.New, i.secret)
	m.Write([]byte("holdfast-pow-v1|" + eventID + "|" + userID + "|" + head))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// Hash is the work function: SHA-256 of token, a colon and nonce.
func Hash(token, nonce string) [32]byte {
	return sha256.Sum256([]byte(token + ":" + nonce))
}

// LeadingZeroBits counts the zero bits at the start of h.
func LeadingZeroBits(h [32]byte) int {
	n := 0
	for _, b := range h {
		if b != 0 {
			return n + bits.LeadingZeros8(b)
		}
		n += 8
	}
	return n
}

// Solve finds a nonce for token at difficulty, trying 0, 1, 2, ... It is
// for tools and tests (holdfastctl pow solve); browsers use the web app's
// solver in a Web Worker.
func Solve(token string, difficulty int) string {
	for n := uint64(0); ; n++ {
		nonce := strconv.FormatUint(n, 10)
		if LeadingZeroBits(Hash(token, nonce)) >= difficulty {
			return nonce
		}
	}
}

// DifficultyOf reads the difficulty a token was issued with, without
// verifying it.
func DifficultyOf(token string) (int, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 4 {
		return 0, ErrInvalid
	}
	d, err := strconv.Atoi(parts[1])
	if err != nil || d < 1 || d > MaxDifficulty {
		return 0, ErrInvalid
	}
	return d, nil
}
