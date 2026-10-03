package inventory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Config tunes the hold lifecycle.
type Config struct {
	// HoldTTL is how long a HELD hold lives before the sweeper frees it.
	HoldTTL time.Duration
	// PaymentWindow protects a PAYING hold: payment deadline plus grace.
	PaymentWindow time.Duration
}

func (c Config) validate() error {
	if c.HoldTTL < time.Second || c.HoldTTL > time.Hour {
		return errors.New("inventory: hold TTL must be between 1s and 1h")
	}
	if c.PaymentWindow < c.HoldTTL {
		return errors.New("inventory: payment window must not be shorter than the hold TTL")
	}
	return nil
}

// Service implements the inventory use cases: it validates and canonicalises
// input, derives deterministic IDs, delegates atomic state changes to Store
// and records metrics.
type Service struct {
	store *Store
	cfg   Config
	m     *Metrics
}

// NewService returns a Service.
func NewService(store *Store, cfg Config, m *Metrics) (*Service, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Service{store: store, cfg: cfg, m: m}, nil
}

// CreateHold holds req.Quantity units of an event for a user. Retrying with
// the same idempotency key returns the original hold instead of a second one.
func (s *Service) CreateHold(ctx context.Context, req CreateHoldRequest) (HoldResult, error) {
	eventID, err := canonicalUUID("eventId", req.EventID)
	if err != nil {
		s.m.holdResult("invalid")
		return HoldResult{}, err
	}
	userID, err := canonicalUUID("userId", req.UserID)
	if err != nil {
		s.m.holdResult("invalid")
		return HoldResult{}, err
	}
	if !validIdempotencyKey(req.IdempotencyKey) {
		s.m.holdResult("invalid")
		return HoldResult{}, fmt.Errorf("%w: Idempotency-Key must be 8-255 characters from [A-Za-z0-9_.:-]", ErrInvalidRequest)
	}
	if req.Quantity < 1 || req.Quantity > 10 {
		s.m.holdResult("invalid")
		return HoldResult{}, fmt.Errorf("%w: quantity must be between 1 and 10", ErrInvalidQuantity)
	}

	holdID := HoldIDFor(userID, req.IdempotencyKey)
	start := time.Now()
	r, err := s.store.hold(ctx, eventID, userID, holdID, req.Quantity, s.cfg.HoldTTL)
	s.m.holdLatency.Observe(time.Since(start).Seconds())
	if err != nil {
		s.m.holdResult("error")
		return HoldResult{}, err
	}

	switch r.code {
	case 1:
		s.m.holdResult("held")
		s.m.setAvailable(eventID, r.n)
		return HoldResult{
			Hold: Hold{
				ID: holdID, EventID: eventID, UserID: userID, Quantity: req.Quantity,
				State: StateHeld, ExpiresAt: time.UnixMilli(r.expiresAtMs).UTC(),
			},
			Remaining: int(r.n),
		}, nil
	case 2:
		s.m.holdResult("replay")
		existing, err := s.store.GetHold(ctx, eventID, holdID)
		if err != nil {
			return HoldResult{}, err
		}
		if existing.Quantity != req.Quantity {
			return HoldResult{}, ErrIdempotencyKeyReused
		}
		if existing.State == StateReleased {
			return HoldResult{}, ErrHoldExpired
		}
		return HoldResult{Hold: existing, Remaining: int(r.n), Replayed: true}, nil
	case 0:
		s.m.holdResult("sold_out")
		s.m.setAvailable(eventID, r.n)
		return HoldResult{}, ErrSoldOut
	case -1:
		s.m.holdResult("user_limit")
		return HoldResult{}, ErrUserLimit
	case -2:
		s.m.holdResult("invalid")
		return HoldResult{}, fmt.Errorf("%w: quantity must be between 1 and %d for this event", ErrInvalidQuantity, r.n)
	case -3:
		s.m.holdResult("not_provisioned")
		return HoldResult{}, ErrEventNotProvisioned
	case -4:
		s.m.holdResult("paused")
		return HoldResult{}, ErrSalePaused
	default:
		s.m.holdResult("error")
		return HoldResult{}, fmt.Errorf("inventory: unexpected hold reply code %d", r.code)
	}
}

// GetHold returns a hold owned by userID. Other users' holds are reported as
// not found so hold IDs can't be probed.
func (s *Service) GetHold(ctx context.Context, eventID, userID, holdID string) (Hold, error) {
	ids, err := canonicalIDs(eventID, userID, holdID)
	if err != nil {
		return Hold{}, err
	}
	h, err := s.store.GetHold(ctx, ids[0], ids[2])
	if err != nil {
		return Hold{}, err
	}
	if h.UserID != ids[1] {
		return Hold{}, ErrHoldNotFound
	}
	return h, nil
}

// CancelHold releases a user's own hold. Cancelling an already released hold
// succeeds (DELETE is idempotent); a hold in checkout or sold cannot be cancelled.
func (s *Service) CancelHold(ctx context.Context, eventID, userID, holdID string) error {
	h, err := s.GetHold(ctx, eventID, userID, holdID)
	if err != nil {
		return err
	}
	switch h.State {
	case StateReleased:
		return nil
	case StateSold, StatePaying:
		return ErrHoldNotCancellable
	}
	released, err := s.store.Release(ctx, h.EventID, h.UserID, h.ID, ReleaseUserCancel)
	if err != nil {
		return err
	}
	if released {
		s.m.releases.WithLabelValues(string(ReleaseUserCancel)).Inc()
	}
	return nil
}

// MarkPaying protects a hold for the payment window when checkout starts.
func (s *Service) MarkPaying(ctx context.Context, eventID, userID, holdID string) (time.Time, error) {
	ids, err := canonicalIDs(eventID, userID, holdID)
	if err != nil {
		return time.Time{}, err
	}
	return s.store.MarkPaying(ctx, ids[0], ids[1], ids[2], s.cfg.PaymentWindow)
}

// Confirm marks a hold SOLD. Call it only after PostgreSQL has committed the
// booking; it is idempotent and tolerates holds that expired meanwhile.
func (s *Service) Confirm(ctx context.Context, eventID, userID, holdID string, qty int) (ConfirmOutcome, error) {
	ids, err := canonicalIDs(eventID, userID, holdID)
	if err != nil {
		return "", err
	}
	if qty < 1 || qty > 10 {
		return "", fmt.Errorf("%w: quantity must be between 1 and 10", ErrInvalidQuantity)
	}
	outcome, err := s.store.Confirm(ctx, ids[0], ids[1], ids[2], qty)
	if err != nil {
		return "", err
	}
	s.m.confirms.WithLabelValues(string(outcome)).Inc()
	return outcome, nil
}

// ReleaseForFailedPayment returns a hold's units after a definitive payment failure.
func (s *Service) ReleaseForFailedPayment(ctx context.Context, eventID, userID, holdID string) (bool, error) {
	ids, err := canonicalIDs(eventID, userID, holdID)
	if err != nil {
		return false, err
	}
	released, err := s.store.Release(ctx, ids[0], ids[1], ids[2], ReleasePaymentFailed)
	if err != nil {
		return false, err
	}
	if released {
		s.m.releases.WithLabelValues(string(ReleasePaymentFailed)).Inc()
	}
	return released, nil
}

// Provision initialises an event's inventory. It is idempotent for identical
// settings and refuses to silently change the capacity of an existing event.
func (s *Service) Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error) {
	id, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return false, err
	}
	if err := cfg.Validate(); err != nil {
		return false, err
	}
	created, err := s.store.Provision(ctx, id, cfg)
	if err != nil {
		return false, err
	}
	if created {
		s.m.setAvailable(id, int64(cfg.initialAvailable()))
	}
	return created, nil
}

// SetFrozen freezes or unfreezes an event's sale (runbook RB-1): while
// frozen, no new holds are taken. It reports whether this call changed the
// flag; setting it again is harmless.
func (s *Service) SetFrozen(ctx context.Context, eventID string, frozen bool) (bool, error) {
	id, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return false, err
	}
	return s.store.SetFrozen(ctx, id, frozen)
}

// Availability returns an event's remaining units.
func (s *Service) Availability(ctx context.Context, eventID string) (Availability, error) {
	id, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return Availability{}, err
	}
	a, err := s.store.Availability(ctx, id)
	if err != nil {
		return Availability{}, err
	}
	s.m.setAvailable(id, int64(a.Available))
	return a, nil
}

// canonicalUUID accepts only the 36-character hyphenated form and returns it
// lower-cased. Keys are built from IDs, so one UUID must map to one key, and
// characters such as '{' or '|' must never reach the keyspace.
func canonicalUUID(field, v string) (string, error) {
	u, err := uuid.Parse(v)
	if err != nil || len(v) != 36 {
		return "", fmt.Errorf("%w: %s must be a UUID", ErrInvalidRequest, field)
	}
	return u.String(), nil
}

func canonicalIDs(eventID, userID, holdID string) ([3]string, error) {
	var out [3]string
	var err error
	if out[0], err = canonicalUUID("eventId", eventID); err != nil {
		return out, err
	}
	if out[1], err = canonicalUUID("userId", userID); err != nil {
		return out, err
	}
	if out[2], err = canonicalUUID("holdId", holdID); err != nil {
		return out, err
	}
	return out, nil
}

func validIdempotencyKey(k string) bool {
	if len(k) < 8 || len(k) > 255 {
		return false
	}
	for i := 0; i < len(k); i++ {
		if !isKeyChar(k[i]) {
			return false
		}
	}
	return true
}

func isKeyChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '-' || c == '_' || c == '.' || c == ':'
}
