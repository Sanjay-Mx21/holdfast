//go:build integration

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// inventory rebuild end to end: PostgreSQL's records against a Valkey that
// lost a pending booking's hold and kept a hold nobody booked.
func TestInventoryRebuild(t *testing.T) {
	rdb := testenv.Valkey(t)
	pool := testenv.Postgres(t)
	ctx := context.Background()
	valkeyAddr := os.Getenv(testenv.EnvValkeyAddr)
	dsn := testenv.PostgresDSN(t)

	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "holdfastctl rebuild test", SaleOpensAt: time.Now().Add(-time.Minute), PerUserLimit: 4, UnitPricePaise: 100, Capacity: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	ev := id.String()
	store := inventory.NewStore(rdb)
	t.Cleanup(func() {
		ctx := context.Background()
		_ = store.Purge(ctx, ev)
		_, _ = pool.Exec(ctx, `DELETE FROM booking.bookings WHERE event_id = $1`, id)
		_ = catalog.Delete(ctx, pool, id)
	})
	if out, err := run(t, "inventory", "provision", "--valkey", valkeyAddr, "--dsn", dsn, "--event", ev); err != nil {
		t.Fatalf("provision: %v\n%s", err, out)
	}
	svc, err := inventoryService(rdb)
	if err != nil {
		t.Fatal(err)
	}

	// Valkey: a buyer in checkout, and a hold nobody booked.
	payer, browser, buyer := uuid.New(), uuid.New(), uuid.New()
	paying, err := svc.CreateHold(ctx, inventory.CreateHoldRequest{EventID: ev, UserID: payer.String(), IdempotencyKey: "rebuild-1", Quantity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkPaying(ctx, ev, payer.String(), paying.Hold.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CreateHold(ctx, inventory.CreateHoldRequest{EventID: ev, UserID: browser.String(), IdempotencyKey: "rebuild-2", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	// PostgreSQL: the payer's pending booking, and 3 units another buyer
	// bought (their hold long gone from Valkey).
	if _, err := pool.Exec(ctx, `
		INSERT INTO booking.bookings (id, event_id, user_id, hold_id, qty, amount_paise, payment_deadline)
		VALUES ($1, $2, $3, $4, 2, 200, now() + interval '10 minutes')`,
		uuid.Must(uuid.NewV7()), id, payer, uuid.MustParse(paying.Hold.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO booking.user_event_purchases (event_id, user_id, qty) VALUES ($1, $2, 3)`, id, buyer); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE booking.event_inventory SET sold = 3 WHERE event_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	// The failover lost the payer's hold.
	if err := rdb.Del(ctx, "inv:{"+ev+"}:hold:"+paying.Hold.ID).Err(); err != nil {
		t.Fatal(err)
	}

	out, err := run(t, "inventory", "rebuild", "--valkey", valkeyAddr, "--dsn", dsn, "--event", ev, "--dry-run")
	if err != nil {
		t.Fatalf("dry run: %v\n%s", err, out)
	}
	for _, want := range []string{
		"dry run: nothing was changed",
		"postgres    capacity 20, sold 3, 1 pending bookings (2 units), 2 buyers with units",
		"available   17 -> 15",
		"pending bookings: 1 recreated, 0 kept, 0 confirmed meanwhile; 1 without a booking released",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("dry run lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "frozen") {
		t.Errorf("a dry run must not freeze:\n%s", out)
	}

	out, err = run(t, "inventory", "rebuild", "--valkey", valkeyAddr, "--dsn", dsn, "--event", ev)
	if err != nil {
		t.Fatalf("rebuild: %v\n%s", err, out)
	}
	for _, want := range []string{
		"inventory: holds frozen for the rebuild",
		"available   17 -> 15",
		"per-user    1 counters set, 1 deleted", // the buyer's set; the browser's deleted
		"holds stay frozen: check 'holdfastctl inventory status --event " + ev,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rebuild lacks %q:\n%s", want, out)
		}
	}
	a, err := svc.Availability(ctx, ev)
	if err != nil || a.Available != 15 {
		t.Fatalf("available %+v, %v; want 15", a, err)
	}
	h, err := svc.GetHold(ctx, ev, payer.String(), paying.Hold.ID)
	if err != nil || h.State != inventory.StatePaying {
		t.Fatalf("payer's hold %+v, %v; want PAYING", h, err)
	}
}
