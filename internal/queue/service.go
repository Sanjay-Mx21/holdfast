package queue

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// Service implements the queue use cases: it validates and canonicalises
// input, then delegates every state change to one atomic Store operation.
type Service struct {
	store *Store
}

// NewService returns a Service backed by store.
func NewService(store *Store) *Service { return &Service{store: store} }

// Provision stores an event's queue settings and opens its waiting room in
// state PRE. It is idempotent for identical settings and refuses to silently
// change the settings of an existing queue.
func (s *Service) Provision(ctx context.Context, eventID string, cfg EventConfig) (bool, error) {
	id, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return false, err
	}
	if err := cfg.Validate(); err != nil {
		return false, err
	}
	return s.store.Provision(ctx, id, cfg)
}

// Join puts userID in the event's waiting room: before T0 with a random
// lottery position, after T0 in arrival order. Joining again is harmless and
// keeps the original position.
func (s *Service) Join(ctx context.Context, eventID, userID string) (JoinResult, error) {
	ev, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return JoinResult{}, err
	}
	user, err := canonicalUUID("userId", userID)
	if err != nil {
		return JoinResult{}, err
	}
	score, err := lotteryScore()
	if err != nil {
		return JoinResult{}, fmt.Errorf("queue: lottery score: %w", err)
	}
	out, err := s.store.Join(ctx, ev, user, score)
	if err != nil {
		return JoinResult{}, err
	}
	return JoinResult{EventID: ev, Joined: out.joined, Ordering: orderingOf(out.score), openedQueue: out.opened}, nil
}

// Position tells userID where they stand in the event's queue: when the
// lottery closes, before T0, or their rank from T0 on.
func (s *Service) Position(ctx context.Context, eventID, userID string) (Position, error) {
	ev, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return Position{}, err
	}
	user, err := canonicalUUID("userId", userID)
	if err != nil {
		return Position{}, err
	}
	return s.store.Position(ctx, ev, user)
}

// Status returns the event's status document.
func (s *Service) Status(ctx context.Context, eventID string) (Status, error) {
	ev, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return Status{}, err
	}
	return s.store.Status(ctx, ev)
}

// Overview reads everything an operator needs about an event's queue.
func (s *Service) Overview(ctx context.Context, eventID string) (Overview, error) {
	ev, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return Overview{}, err
	}
	st, err := s.store.Status(ctx, ev)
	if err != nil {
		return Overview{}, err
	}
	o, err := s.store.Overview(ctx, ev)
	if err != nil {
		return Overview{}, err
	}
	o.Status = st
	return o, nil
}

// Admit lets userID claim their turn once their rank is within
// admittedUpTo; the caller then issues an admission token bounded by the
// session slot.
func (s *Service) Admit(ctx context.Context, eventID, userID string) (Turn, error) {
	ev, err := canonicalUUID("eventId", eventID)
	if err != nil {
		return Turn{}, err
	}
	user, err := canonicalUUID("userId", userID)
	if err != nil {
		return Turn{}, err
	}
	return s.store.Admit(ctx, ev, user)
}

// sessionNamespace scopes deterministic admission session IDs (UUIDv5).
var sessionNamespace = uuid.MustParse("8b0e7c2a-3f5d-4e91-a6c7-1d2e3f4a5b6c")

// SessionID is the admission session of (event, user): the same on every
// claim, so retries refer to one session; each token still has its own jti.
func SessionID(eventID, userID string) string {
	return uuid.NewSHA1(sessionNamespace, []byte(eventID+"\x00"+userID)).String()
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
