package booking

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
)

// InventorySnapshot is what an event's inventory must be by booking-svc's
// records, the source of truth: inventory is rebuilt from it after Valkey
// loses data (runbook RB-2, `holdfastctl inventory rebuild`).
type InventorySnapshot struct {
	Capacity, PerUserLimit, Sold int
	// Purchases maps each user to the units they bought (the final guard's
	// counter).
	Purchases map[uuid.UUID]int
	// Pending are the bookings waiting for payment: each still holds its
	// units through a PAYING hold.
	Pending []PendingBooking
}

// PendingBooking is one PENDING_PAYMENT booking's claim on units.
type PendingBooking struct {
	HoldID, UserID  uuid.UUID
	Qty             int
	PaymentDeadline time.Time
}

// ReadInventorySnapshot reads an event's snapshot in one read-only,
// repeatable-read transaction, so its parts agree with each other.
// catalog.ErrNotFound if the event does not exist.
func ReadInventorySnapshot(ctx context.Context, db *pgxpool.Pool, eventID uuid.UUID) (InventorySnapshot, error) {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return InventorySnapshot{}, fmt.Errorf("booking: snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := bookingdb.New(tx)

	inv, err := q.SnapshotEventInventory(ctx, eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return InventorySnapshot{}, catalog.ErrNotFound
	}
	if err != nil {
		return InventorySnapshot{}, fmt.Errorf("booking: snapshot: %w", err)
	}
	s := InventorySnapshot{
		Capacity: int(inv.Capacity), PerUserLimit: int(inv.PerUserLimit), Sold: int(inv.Sold),
		Purchases: map[uuid.UUID]int{},
	}
	purchases, err := q.SnapshotPurchases(ctx, eventID)
	if err != nil {
		return InventorySnapshot{}, fmt.Errorf("booking: snapshot purchases: %w", err)
	}
	for _, p := range purchases {
		s.Purchases[p.UserID] = int(p.Qty)
	}
	pending, err := q.SnapshotPendingBookings(ctx, eventID)
	if err != nil {
		return InventorySnapshot{}, fmt.Errorf("booking: snapshot pending bookings: %w", err)
	}
	for _, b := range pending {
		s.Pending = append(s.Pending, PendingBooking{
			HoldID: b.HoldID, UserID: b.UserID, Qty: int(b.Qty), PaymentDeadline: b.PaymentDeadline,
		})
	}
	return s, nil
}
