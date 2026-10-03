// Package catalog owns the event catalog (booking.events). It creates each
// event together with its final-guard inventory row, atomically.
package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound means no event has the requested ID.
var ErrNotFound = errors.New("catalog: event not found")

// ErrInvalid wraps validation failures.
var ErrInvalid = errors.New("catalog: invalid event")

// NewEvent describes an event to create.
type NewEvent struct {
	Name                 string
	SaleOpensAt          time.Time
	VerifiedWindowEndsAt *time.Time
	AgentLockoutEndsAt   *time.Time
	PerUserLimit         int
	UnitPricePaise       int64
	Capacity             int
}

// Event is a stored event with its inventory counters.
type Event struct {
	ID          uuid.UUID
	Name        string
	SaleOpensAt time.Time
	// The policy windows: verified buyers only, and no agents, until these
	// instants; nil means no window.
	VerifiedWindowEndsAt *time.Time
	AgentLockoutEndsAt   *time.Time
	PerUserLimit         int
	UnitPricePaise       int64
	Status               string
	Capacity             int
	Sold                 int
}

func (e NewEvent) validate() error {
	switch {
	case strings.TrimSpace(e.Name) == "" || len(e.Name) > 200:
		return fmt.Errorf("%w: name must be 1-200 characters", ErrInvalid)
	case e.SaleOpensAt.IsZero():
		return fmt.Errorf("%w: sale opening time is required", ErrInvalid)
	case e.PerUserLimit < 1 || e.PerUserLimit > 10:
		return fmt.Errorf("%w: per-user limit must be between 1 and 10", ErrInvalid)
	case e.UnitPricePaise < 1:
		return fmt.Errorf("%w: unit price must be positive", ErrInvalid)
	case e.Capacity < 1 || e.Capacity > 10_000_000:
		return fmt.Errorf("%w: capacity must be between 1 and 10000000", ErrInvalid)
	case e.VerifiedWindowEndsAt != nil && !e.VerifiedWindowEndsAt.After(e.SaleOpensAt):
		return fmt.Errorf("%w: the verified-only window must end after the sale opens", ErrInvalid)
	case e.AgentLockoutEndsAt != nil && !e.AgentLockoutEndsAt.After(e.SaleOpensAt):
		return fmt.Errorf("%w: the agent lockout must end after the sale opens", ErrInvalid)
	}
	return nil
}

// Create inserts the event and its inventory row in one transaction.
func Create(ctx context.Context, db *pgxpool.Pool, e NewEvent) (uuid.UUID, error) {
	if err := e.validate(); err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	err = pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO booking.events
			  (id, name, sale_opens_at, verified_window_ends_at, agent_lockout_ends_at, per_user_limit, unit_price_paise)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id, e.Name, e.SaleOpensAt, e.VerifiedWindowEndsAt, e.AgentLockoutEndsAt, e.PerUserLimit, e.UnitPricePaise,
		); err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO booking.event_inventory (event_id, capacity) VALUES ($1, $2)`, id, e.Capacity,
		); err != nil {
			return fmt.Errorf("insert inventory: %w", err)
		}
		return nil
	})
	if err != nil {
		return uuid.Nil, fmt.Errorf("catalog: create: %w", err)
	}
	return id, nil
}

// Get returns an event with its current inventory counters.
func Get(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (Event, error) {
	var e Event
	err := db.QueryRow(ctx, `
		SELECT e.id, e.name, e.sale_opens_at, e.verified_window_ends_at, e.agent_lockout_ends_at,
		       e.per_user_limit, e.unit_price_paise, e.status, i.capacity, i.sold
		FROM booking.events e
		JOIN booking.event_inventory i ON i.event_id = e.id
		WHERE e.id = $1`, id,
	).Scan(&e.ID, &e.Name, &e.SaleOpensAt, &e.VerifiedWindowEndsAt, &e.AgentLockoutEndsAt,
		&e.PerUserLimit, &e.UnitPricePaise, &e.Status, &e.Capacity, &e.Sold)
	if errors.Is(err, pgx.ErrNoRows) {
		return Event{}, ErrNotFound
	}
	if err != nil {
		return Event{}, fmt.Errorf("catalog: get: %w", err)
	}
	return e, nil
}

// Delete removes an event and (by cascade) its inventory and purchase rows.
// Intended for tests and tooling.
func Delete(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) error {
	_, err := db.Exec(ctx, `DELETE FROM booking.events WHERE id = $1`, id)
	return err
}
