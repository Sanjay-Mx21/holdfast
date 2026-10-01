// Package guard is the PostgreSQL final guard for quantity-mode sales: the
// last line of defence against overselling (invariant I1) and against
// per-user cap violations (invariant I4), whatever the Valkey fast path
// believes. booking-svc calls Reserve inside the same transaction that
// confirms a booking, so the sale and the guard commit together or not at all.
package guard

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Guard errors.
var (
	ErrSoldOut         = errors.New("guard: capacity exhausted")
	ErrUserCapExceeded = errors.New("guard: per-user cap exceeded")
	ErrUnknownEvent    = errors.New("guard: unknown event")
	ErrInvalid         = errors.New("guard: invalid reservation")
)

// Reservation is a request to commit Qty units of an event to a user.
type Reservation struct {
	EventID      uuid.UUID
	UserID       uuid.UUID
	Qty          int
	PerUserLimit int
}

// Reserve commits the reservation inside tx. It runs in a savepoint, so a
// rejection leaves tx exactly as it was and the caller can record the outcome
// (for example, a refund) in the same transaction.
//
// Both statements are single conditional writes: PostgreSQL row locks
// serialise concurrent confirmations for the same event, and the WHERE
// clauses make the checks and the increments atomic. No SELECT ... FOR UPDATE,
// no application-side read-modify-write.
func Reserve(ctx context.Context, tx pgx.Tx, r Reservation) (err error) {
	if r.Qty < 1 || r.PerUserLimit < 1 || r.Qty > r.PerUserLimit {
		return fmt.Errorf("%w: need 1 <= qty <= per-user limit", ErrInvalid)
	}
	sp, err := tx.Begin(ctx) // SAVEPOINT
	if err != nil {
		return fmt.Errorf("guard: savepoint: %w", err)
	}
	defer func() {
		if err != nil {
			_ = sp.Rollback(ctx) // ROLLBACK TO SAVEPOINT
		}
	}()

	var sold int
	err = sp.QueryRow(ctx, `
		UPDATE booking.event_inventory
		SET sold = sold + $2, updated_at = now()
		WHERE event_id = $1 AND sold + $2 <= capacity
		RETURNING sold`, r.EventID, r.Qty,
	).Scan(&sold)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if qerr := sp.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM booking.event_inventory WHERE event_id = $1)`, r.EventID,
		).Scan(&exists); qerr != nil {
			return fmt.Errorf("guard: check event: %w", qerr)
		}
		if !exists {
			return ErrUnknownEvent
		}
		return ErrSoldOut
	}
	if err != nil {
		return fmt.Errorf("guard: reserve units: %w", err)
	}

	var total int
	err = sp.QueryRow(ctx, `
		INSERT INTO booking.user_event_purchases AS p (event_id, user_id, qty)
		VALUES ($1, $2, $3)
		ON CONFLICT (event_id, user_id)
		DO UPDATE SET qty = p.qty + EXCLUDED.qty, updated_at = now()
		WHERE p.qty + EXCLUDED.qty <= $4
		RETURNING qty`, r.EventID, r.UserID, r.Qty, r.PerUserLimit,
	).Scan(&total)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUserCapExceeded
	}
	if err != nil {
		return fmt.Errorf("guard: record purchase: %w", err)
	}
	if err = sp.Commit(ctx); err != nil { // RELEASE SAVEPOINT
		return fmt.Errorf("guard: release savepoint: %w", err)
	}
	return nil
}

// ReserveTx runs Reserve in its own transaction (tooling and tests).
func ReserveTx(ctx context.Context, db *pgxpool.Pool, r Reservation) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error { return Reserve(ctx, tx, r) })
}
