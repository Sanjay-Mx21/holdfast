//go:build integration

package guard_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/guard"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

func newEvent(t *testing.T, pool *pgxpool.Pool, capacity, perUser int) uuid.UUID {
	t.Helper()
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "guard test", SaleOpensAt: time.Now(), PerUserLimit: perUser, UnitPricePaise: 250000, Capacity: capacity,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = catalog.Delete(context.Background(), pool, id) })
	return id
}

func sold(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) int {
	t.Helper()
	e, err := catalog.Get(ctx, pool, id)
	if err != nil {
		t.Fatal(err)
	}
	return e.Sold
}

func reserve(pool *pgxpool.Pool, event, user uuid.UUID, qty, limit int) error {
	return guard.ReserveTx(ctx, pool, guard.Reservation{EventID: event, UserID: user, Qty: qty, PerUserLimit: limit})
}

func TestReserveUntilSoldOut(t *testing.T) {
	pool := testenv.Postgres(t)
	ev := newEvent(t, pool, 5, 4)
	for i := 0; i < 5; i++ {
		if err := reserve(pool, ev, uuid.New(), 1, 4); err != nil {
			t.Fatalf("reservation %d: %v", i, err)
		}
	}
	if err := reserve(pool, ev, uuid.New(), 1, 4); !errors.Is(err, guard.ErrSoldOut) {
		t.Fatalf("sixth reservation: got %v, want ErrSoldOut", err)
	}
	if s := sold(t, pool, ev); s != 5 {
		t.Fatalf("sold = %d, want 5", s)
	}
}

func TestPerUserCapRejectionUndoesTheUnitIncrement(t *testing.T) {
	pool := testenv.Postgres(t)
	ev := newEvent(t, pool, 10, 2)
	user := uuid.New()
	for i := 0; i < 2; i++ {
		if err := reserve(pool, ev, user, 1, 2); err != nil {
			t.Fatal(err)
		}
	}
	if err := reserve(pool, ev, user, 1, 2); !errors.Is(err, guard.ErrUserCapExceeded) {
		t.Fatalf("third unit: got %v, want ErrUserCapExceeded", err)
	}
	if s := sold(t, pool, ev); s != 2 {
		t.Fatalf("sold = %d, want 2 (the rejected unit must not stay counted)", s)
	}
}

func TestUnknownEvent(t *testing.T) {
	pool := testenv.Postgres(t)
	if err := reserve(pool, uuid.New(), uuid.New(), 1, 4); !errors.Is(err, guard.ErrUnknownEvent) {
		t.Fatalf("got %v, want ErrUnknownEvent", err)
	}
}

func TestRejectionLeavesTheCallersTransactionUsable(t *testing.T) {
	pool := testenv.Postgres(t)
	ev := newEvent(t, pool, 10, 2)
	a, b := uuid.New(), uuid.New()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := guard.Reserve(ctx, tx, guard.Reservation{EventID: ev, UserID: a, Qty: 2, PerUserLimit: 2}); err != nil {
			return err
		}
		if err := guard.Reserve(ctx, tx, guard.Reservation{EventID: ev, UserID: a, Qty: 1, PerUserLimit: 2}); !errors.Is(err, guard.ErrUserCapExceeded) {
			t.Errorf("over-cap reservation: got %v", err)
		}
		return guard.Reserve(ctx, tx, guard.Reservation{EventID: ev, UserID: b, Qty: 1, PerUserLimit: 2})
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	if s := sold(t, pool, ev); s != 3 {
		t.Fatalf("sold = %d, want 3", s)
	}
}

func TestConcurrentConfirmationsNeverOversell(t *testing.T) {
	pool := testenv.Postgres(t)
	const capacity, attempts = 50, 300
	ev := newEvent(t, pool, capacity, 4)
	var ok, soldOut, other atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := reserve(pool, ev, uuid.New(), 1, 4); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, guard.ErrSoldOut):
				soldOut.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != capacity || soldOut.Load() != attempts-capacity || other.Load() != 0 {
		t.Fatalf("ok=%d soldOut=%d other=%d", ok.Load(), soldOut.Load(), other.Load())
	}
	var purchased int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(qty),0) FROM booking.user_event_purchases WHERE event_id=$1`, ev).Scan(&purchased); err != nil {
		t.Fatal(err)
	}
	if s := sold(t, pool, ev); s != capacity || purchased != capacity {
		t.Fatalf("sold=%d purchases=%d, want %d", s, purchased, capacity)
	}
}

func TestConcurrentRequestsFromOneUserRespectTheCap(t *testing.T) {
	pool := testenv.Postgres(t)
	ev := newEvent(t, pool, 100, 3)
	user := uuid.New()
	var ok, capped atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := reserve(pool, ev, user, 1, 3); {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, guard.ErrUserCapExceeded):
				capped.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 3 || capped.Load() != 27 || sold(t, pool, ev) != 3 {
		t.Fatalf("ok=%d capped=%d sold=%d", ok.Load(), capped.Load(), sold(t, pool, ev))
	}
}
