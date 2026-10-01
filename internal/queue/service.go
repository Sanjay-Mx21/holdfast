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
