// Package auth is auth-svc (design doc sections 8.1, 10.1 and 11): sign-in
// with a one-time code sent to a phone, 15-minute EdDSA access tokens, and
// refresh tokens that rotate on every use, where presenting a token that was
// already rotated revokes its whole family (reuse detection). Phones, codes
// and refresh tokens are never stored in the clear.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/auth/authdb"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
)

// Errors.
var (
	// ErrInvalidCode covers a wrong, expired, used or exhausted code alike, so
	// the answer reveals nothing about the challenge.
	ErrInvalidCode = errors.New("auth: invalid or expired code")
	// ErrInvalidRefresh covers an unknown, expired or revoked refresh token.
	ErrInvalidRefresh = errors.New("auth: invalid refresh token")
	// ErrRefreshReused means a token that was already rotated came back: it
	// was stolen or replayed, and its whole family is now revoked.
	ErrRefreshReused = errors.New("auth: refresh token reused; the session is revoked")
)

// RateLimited says when the phone may ask for a code again.
type RateLimited struct{ RetryAfter time.Duration }

func (e *RateLimited) Error() string {
	return fmt.Sprintf("auth: too many codes requested; retry after %s", e.RetryAfter.Round(time.Second))
}

// Config tunes the service.
type Config struct {
	// Pepper keys the HMACs of phone numbers and codes (at least 32 bytes).
	Pepper []byte
	// CodeTTL is how long a code is valid (5 minutes).
	CodeTTL time.Duration
	// MaxAttempts is how many guesses one code allows (5).
	MaxAttempts int
	// ResendAfter is the minimum time between two codes for a phone (30 s),
	// and MaxPerHour the most codes per phone per hour (5).
	ResendAfter time.Duration
	MaxPerHour  int
	// RefreshTTL is the life of a refresh token; every rotation starts a new
	// one (30 days).
	RefreshTTL time.Duration
}

// Session is what a sign-in or a refresh returns.
type Session struct {
	UserID         uuid.UUID
	Role           string
	AccessToken    string
	AccessExpires  time.Time
	RefreshToken   string // opaque; goes in an httpOnly cookie, never in a body
	RefreshExpires time.Time
	NewUser        bool
}

// Service signs users in.
type Service struct {
	pool   *pgxpool.Pool
	q      *authdb.Queries
	cfg    Config
	sms    Sender
	access *authn.AccessIssuer
	m      *Metrics
	log    *slog.Logger
	now    func() time.Time
}

// NewService returns the service.
func NewService(pool *pgxpool.Pool, cfg Config, sms Sender, access *authn.AccessIssuer, m *Metrics, log *slog.Logger) *Service {
	return &Service{pool: pool, q: authdb.New(pool), cfg: cfg, sms: sms, access: access, m: m, log: log, now: time.Now}
}

// RequestOTP sends a fresh code to phone. Within the limits (one per
// ResendAfter, MaxPerHour per hour) it always succeeds the same way, whether
// or not the phone belongs to a user, so the endpoint reveals nothing.
func (s *Service) RequestOTP(ctx context.Context, rawPhone string) error {
	phone, err := NormalizePhone(rawPhone)
	if err != nil {
		s.m.otp("invalid_phone")
		return err
	}
	ph := s.mac("phone", phone)
	code, err := sixDigits()
	if err != nil {
		return err
	}
	id := uuid.Must(uuid.NewV7())
	now := s.now()
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		if err := q.LockPhone(ctx, int64(binary.BigEndian.Uint64(ph))); err != nil { //nolint:gosec // a lock key: the bit pattern is what matters
			return err
		}
		recent, err := q.RecentChallenges(ctx, authdb.RecentChallengesParams{PhoneHmac: ph, Since: now.Add(-time.Hour)})
		if err != nil {
			return err
		}
		if wait := recent.Latest.Add(s.cfg.ResendAfter).Sub(now); recent.Sent > 0 && wait > 0 {
			return &RateLimited{RetryAfter: wait}
		}
		if int(recent.Sent) >= s.cfg.MaxPerHour {
			return &RateLimited{RetryAfter: max(recent.Oldest.Add(time.Hour).Sub(now), time.Second)}
		}
		return q.InsertChallenge(ctx, authdb.InsertChallengeParams{
			ID: id, PhoneHmac: ph, CodeHash: s.codeHash(id, code), ExpiresAt: now.Add(s.cfg.CodeTTL),
		})
	})
	var limited *RateLimited
	if errors.As(err, &limited) {
		s.m.otp("rate_limited")
		return err
	}
	if err != nil {
		s.m.otp("error")
		return fmt.Errorf("auth: record code: %w", err)
	}
	text := fmt.Sprintf("Your HoldFast code is %s. It expires in %d minutes. Never share it.", code, int(s.cfg.CodeTTL.Minutes()))
	if err := s.sms.Send(ctx, phone, text); err != nil {
		s.m.otp("error")
		return fmt.Errorf("auth: send code: %w", err)
	}
	s.m.otp("sent")
	return nil
}

// VerifyOTP checks code against the phone's latest open code. A correct
// code signs the user in (creating the user the first time) and starts a new
// refresh-token family. Every guess counts, right or wrong.
func (s *Service) VerifyOTP(ctx context.Context, rawPhone, code string) (Session, error) {
	phone, err := NormalizePhone(rawPhone)
	if err != nil {
		return Session{}, err
	}
	if !validCode(code) {
		s.m.verify("invalid")
		return Session{}, ErrInvalidCode
	}
	ph := s.mac("phone", phone)
	// The attempt is taken in its own statement, outside any transaction, so
	// a wrong guess is counted even though the sign-in fails.
	ch, err := s.q.TakeAttempt(ctx, authdb.TakeAttemptParams{PhoneHmac: ph, MaxAttempts: int16(s.cfg.MaxAttempts)}) //nolint:gosec // validated in config
	if errors.Is(err, pgx.ErrNoRows) {
		s.m.verify("invalid")
		return Session{}, ErrInvalidCode
	}
	if err != nil {
		return Session{}, fmt.Errorf("auth: check code: %w", err)
	}
	if !hmac.Equal(ch.CodeHash, s.codeHash(ch.ID, code)) {
		s.m.verify("invalid")
		return Session{}, ErrInvalidCode
	}
	var sess Session
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		n, err := q.ConsumeChallenge(ctx, ch.ID)
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrInvalidCode // used a moment ago by a concurrent request
		}
		u, err := q.UpsertVerifiedUser(ctx, authdb.UpsertVerifiedUserParams{ID: uuid.Must(uuid.NewV7()), PhoneHmac: ph, PhoneLast4: last4(phone)})
		if err != nil {
			return err
		}
		sess.UserID, sess.Role, sess.NewUser = u.ID, u.Role, !u.PhoneVerifiedAt.Valid || u.CreatedAt.Equal(u.PhoneVerifiedAt.Time)
		return s.startRefresh(ctx, q, &sess, uuid.Must(uuid.NewV7()))
	})
	if errors.Is(err, ErrInvalidCode) {
		s.m.verify("invalid")
		return Session{}, err
	}
	if err != nil {
		return Session{}, fmt.Errorf("auth: sign in: %w", err)
	}
	if err := s.issueAccess(&sess); err != nil {
		return Session{}, err
	}
	s.m.verify("ok")
	if sess.NewUser {
		s.m.usersCreated.Inc()
	}
	return sess, nil
}

// Refresh rotates a refresh token: the presented one is replaced, once, and
// a new one in the same family is returned with a fresh access token.
// Presenting a token that was already replaced revokes the whole family.
func (s *Service) Refresh(ctx context.Context, raw string) (Session, error) {
	if raw == "" {
		s.m.refresh("invalid")
		return Session{}, ErrInvalidRefresh
	}
	old := tokenHash(raw)
	next, err := randomToken()
	if err != nil {
		return Session{}, err
	}
	var sess Session
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		row, err := q.RotateRefreshToken(ctx, authdb.RotateRefreshTokenParams{ReplacedBy: tokenHash(next), TokenHash: old})
		if err != nil {
			return err
		}
		u, err := q.GetUser(ctx, row.UserID)
		if err != nil {
			return err
		}
		sess.UserID, sess.Role = u.ID, u.Role
		return s.storeRefresh(ctx, q, &sess, row.FamilyID, next)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, s.refused(ctx, old)
	}
	if err != nil {
		return Session{}, fmt.Errorf("auth: refresh: %w", err)
	}
	if err := s.issueAccess(&sess); err != nil {
		return Session{}, err
	}
	s.m.refresh("rotated")
	return sess, nil
}

// refused explains a token that could not be rotated, revoking its family
// if it had already been rotated (reuse).
func (s *Service) refused(ctx context.Context, h []byte) error {
	t, err := s.q.GetRefreshToken(ctx, h)
	if errors.Is(err, pgx.ErrNoRows) {
		s.m.refresh("invalid")
		return ErrInvalidRefresh
	}
	if err != nil {
		return fmt.Errorf("auth: refresh: %w", err)
	}
	if t.ReplacedBy == nil {
		s.m.refresh("invalid") // expired or revoked
		return ErrInvalidRefresh
	}
	n, err := s.q.RevokeFamily(ctx, t.FamilyID)
	if err != nil {
		return fmt.Errorf("auth: revoke family: %w", err)
	}
	s.m.refresh("reused")
	s.log.WarnContext(ctx, "auth: a rotated refresh token was presented again; its family is revoked",
		"user_id", t.UserID, "family_id", t.FamilyID, "tokens_revoked", n)
	return ErrRefreshReused
}

// Logout revokes the refresh token's family. An unknown token is not an
// error: logging out twice is fine.
func (s *Service) Logout(ctx context.Context, raw string) error {
	if raw == "" {
		return nil
	}
	t, err := s.q.GetRefreshToken(ctx, tokenHash(raw))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = s.q.RevokeFamily(ctx, t.FamilyID)
	return err
}

func (s *Service) startRefresh(ctx context.Context, q *authdb.Queries, sess *Session, family uuid.UUID) error {
	raw, err := randomToken()
	if err != nil {
		return err
	}
	return s.storeRefresh(ctx, q, sess, family, raw)
}

func (s *Service) storeRefresh(ctx context.Context, q *authdb.Queries, sess *Session, family uuid.UUID, raw string) error {
	exp := s.now().Add(s.cfg.RefreshTTL)
	if err := q.InsertRefreshToken(ctx, authdb.InsertRefreshTokenParams{
		TokenHash: tokenHash(raw), UserID: sess.UserID, FamilyID: family, ExpiresAt: exp,
	}); err != nil {
		return err
	}
	sess.RefreshToken, sess.RefreshExpires = raw, exp
	return nil
}

func (s *Service) issueAccess(sess *Session) error {
	tok, exp, err := s.access.Issue(sess.UserID.String(), sess.Role)
	if err != nil {
		return err
	}
	sess.AccessToken, sess.AccessExpires = tok, exp
	return nil
}

// mac is HMAC-SHA256 under the pepper, with a purpose prefix so the same
// input never yields the same value for two uses.
func (s *Service) mac(purpose, value string) []byte {
	h := hmac.New(sha256.New, s.cfg.Pepper)
	h.Write([]byte(purpose + ":" + value))
	return h.Sum(nil)
}

// codeHash binds a code to its challenge: a six-digit code alone is easy to
// brute-force from a stolen table; with the pepper and the challenge's ID it
// is not.
func (s *Service) codeHash(challenge uuid.UUID, code string) []byte {
	return s.mac("otp", challenge.String()+":"+code)
}

// tokenHash is how refresh tokens are stored. They are 256 random bits, so a
// plain SHA-256 is enough.
func tokenHash(raw string) []byte {
	sum := sha256.Sum256([]byte(raw))
	return sum[:]
}

func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func sixDigits() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

func validCode(code string) bool {
	if len(code) != 6 {
		return false
	}
	for _, r := range code {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
