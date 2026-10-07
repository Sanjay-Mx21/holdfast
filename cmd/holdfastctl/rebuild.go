package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/booking"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
)

// cmdInventoryRebuild sets an event's Valkey inventory to what PostgreSQL's
// records say (runbook RB-2, task 5.3): available = capacity − sold − units
// in pending bookings; per-user counters = bought + pending; a PAYING hold
// for every pending booking; every other open hold released. It freezes the
// event's holds first and leaves them frozen for the operator to check.
func cmdInventoryRebuild(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("inventory rebuild", flag.ContinueOnError)
	dsn := dsnFlag(fs)
	vk := valkeyFlag(fs)
	event := fs.String("event", "", "event ID (required)")
	dryRun := fs.Bool("dry-run", false, "report what would change, and change nothing (not even the freeze)")
	grace := fs.Duration("grace", 3*time.Minute, "booking-svc's PAYMENT_GRACE: a pending booking's hold lasts until its deadline plus this")
	if err := fs.Parse(args); err != nil {
		return err
	}
	id, err := uuid.Parse(*event)
	if err != nil {
		return errors.New("--event must be a UUID")
	}
	if *grace < 0 {
		return errors.New("--grace must not be negative")
	}
	pool, err := openPool(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	rdb, err := openValkey(ctx, *vk)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	store := inventory.NewStore(rdb)

	// Freeze before reading PostgreSQL: a hold taken after the read would
	// have no booking in the snapshot and be dropped.
	if !*dryRun {
		changed, err := store.SetFrozen(ctx, id.String(), true)
		switch {
		case errors.Is(err, inventory.ErrEventNotProvisioned):
			fmt.Fprintln(stdout, "inventory: no pool in Valkey (nothing can be held); rebuilding it from scratch")
		case err != nil:
			return err
		default:
			fmt.Fprintf(stdout, "inventory: holds frozen for the rebuild%s\n", unchangedNote(changed))
		}
	}

	snap, err := booking.ReadInventorySnapshot(ctx, pool, id)
	if err != nil {
		return err
	}
	plan := planFromSnapshot(snap, *grace)
	start := time.Now()
	res, err := store.Rebuild(ctx, id.String(), plan, *dryRun)
	if err != nil {
		return err
	}
	printRebuild(snap, plan, res, time.Since(start), *dryRun, id)
	return nil
}

// planFromSnapshot turns PostgreSQL's records into inventory's plan.
func planFromSnapshot(s booking.InventorySnapshot, grace time.Duration) inventory.RebuildPlan {
	purchases := make(map[string]int, len(s.Purchases))
	for u, q := range s.Purchases {
		purchases[u.String()] = q
	}
	holds := make([]inventory.RebuildHold, 0, len(s.Pending))
	for _, b := range s.Pending {
		holds = append(holds, inventory.RebuildHold{
			HoldID: b.HoldID.String(), UserID: b.UserID.String(), Qty: b.Qty,
			ExpiresAt: b.PaymentDeadline.Add(grace),
		})
	}
	return inventory.NewRebuildPlan(s.Capacity, s.PerUserLimit, s.Sold, purchases, holds)
}

func printRebuild(s booking.InventorySnapshot, p inventory.RebuildPlan, r inventory.RebuildResult, took time.Duration, dryRun bool, id uuid.UUID) {
	if dryRun {
		fmt.Fprintln(stdout, "dry run: nothing was changed; a rebuild would do this:")
	}
	pendingUnits := 0
	for _, h := range p.Holds {
		pendingUnits += h.Qty
	}
	fmt.Fprintf(stdout, "postgres    capacity %d, sold %d, %d pending bookings (%d units), %d buyers with units\n",
		s.Capacity, s.Sold, len(p.Holds), pendingUnits, len(p.Users))
	before := "no pool"
	if r.Provisioned {
		before = fmt.Sprint(r.AvailableBefore)
	}
	fmt.Fprintf(stdout, "available   %s -> %d\n", before, p.Available)
	fmt.Fprintf(stdout, "per-user    %d counters set, %d deleted\n", r.UsersChanged, r.UsersDeleted)
	fmt.Fprintf(stdout, "holds       pending bookings: %d recreated, %d kept, %d confirmed meanwhile; %d without a booking released\n",
		r.HoldsRecreated, r.HoldsKept, r.HoldsSold, r.HoldsDropped)
	fmt.Fprintf(stdout, "took        %s\n", took.Round(100*time.Microsecond))
	if p.Available < 0 {
		fmt.Fprintln(stdout, "WARNING: sold plus pending exceeds capacity in PostgreSQL; see runbook RB-INV-6")
	}
	if !dryRun {
		fmt.Fprintf(stdout, "holds stay frozen: check 'holdfastctl inventory status --event %s' and the auditor, then 'holdfastctl unfreeze --event %s'\n", id, id)
	}
}
