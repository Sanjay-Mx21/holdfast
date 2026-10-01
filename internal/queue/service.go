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
