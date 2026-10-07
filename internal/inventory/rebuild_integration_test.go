//go:build integration

package inventory

import (
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// hold takes qty units for user and returns the hold's ID.
func (f *fixture) hold(t *testing.T, user string, qty int) string {
	t.Helper()
	res, err := f.create(user, uuid.NewString(), qty)
	mustErr(t, "hold", err, nil)
	return res.Hold.ID
}

func (f *fixture) paying(t *testing.T, user string, qty int) string {
	t.Helper()
	id := f.hold(t, user, qty)
	_, err := f.svc.MarkPaying(ctx, f.eventID, user, id)
	mustErr(t, "mark paying", err, nil)
	return id
}

func (f *fixture) state(t *testing.T, user, holdID string) Hold {
	t.Helper()
	h, err := f.svc.GetHold(ctx, f.eventID, user, holdID)
	mustErr(t, "get hold", err, nil)
	return h
}

// A Valkey that lost writes and drifted is set back to PostgreSQL's records,
// and the rebuilt holds behave like the originals afterwards.
func TestRebuildMatchesTheRecords(t *testing.T) {
	f := newFixture(t, 100, 4)
	k := keysFor(f.eventID)
	payer, browser, buyer, unlucky, stale := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()

	pendingKept := f.paying(t, payer, 2) // a pending booking whose hold survived
	unbooked := f.hold(t, browser, 1)    // a hold nobody booked: its units go back
	bought := f.paying(t, buyer, 3)      // a purchase, counted as sold
	out, err := f.svc.Confirm(ctx, f.eventID, buyer, bought, 3)
	mustErr(t, "confirm", err, nil)
	mustEqual(t, "confirm", out, ConfirmApplied)
	// A pending booking whose hold and counter the failover lost.
	pendingLost := f.paying(t, unlucky, 1)
	f.rdb.Del(ctx, k.hold(pendingLost), k.user(unlucky))
	f.rdb.ZRem(ctx, k.expiry(), expiryMember(pendingLost, unlucky))
	// And counters that drifted.
	f.rdb.Set(ctx, k.avail(), 999, 0)
	f.rdb.Set(ctx, k.user(stale), 2, 0)

	deadline := time.Now().Add(5 * time.Minute).Truncate(time.Millisecond)
	plan := NewRebuildPlan(100, 4, 3, map[string]int{buyer: 3}, []RebuildHold{
		{HoldID: pendingKept, UserID: payer, Qty: 2, ExpiresAt: deadline},
		{HoldID: pendingLost, UserID: unlucky, Qty: 1, ExpiresAt: deadline},
	})
	mustEqual(t, "planned available", plan.Available, 94)
	want := RebuildResult{
		AvailableBefore: 999, Provisioned: true,
		UsersChanged: 1, UsersDeleted: 2, // unlucky set; browser's and the stale counter deleted
		HoldsRecreated: 1, HoldsKept: 1, HoldsDropped: 1,
	}

	// A dry run reports the same and changes nothing.
	got, err := f.store.Rebuild(ctx, f.eventID, plan, true)
	mustErr(t, "dry run", err, nil)
	mustEqual(t, "dry run", got, want)
	mustEqual(t, "available after the dry run", f.avail(t), 999)
	mustEqual(t, "unbooked hold after the dry run", f.state(t, browser, unbooked).State, StateHeld)

	got, err = f.store.Rebuild(ctx, f.eventID, plan, false)
	mustErr(t, "rebuild", err, nil)
	mustEqual(t, "rebuild", got, want)
	mustEqual(t, "available", f.avail(t), 94)
	for user, units := range map[string]int{payer: 2, browser: 0, buyer: 3, unlucky: 1, stale: 0} {
		mustEqual(t, "units of "+user, f.userUnits(t, user), units)
	}
	lost := f.state(t, unlucky, pendingLost)
	mustEqual(t, "recreated hold", lost.State, StatePaying)
	mustEqual(t, "recreated hold's units", lost.Quantity, 1)
	mustEqual(t, "recreated hold's expiry", lost.ExpiresAt.UnixMilli(), deadline.UnixMilli())
	mustEqual(t, "unbooked hold", f.state(t, browser, unbooked).State, StateReleased)
	mustEqual(t, "sold hold", f.state(t, buyer, bought).State, StateSold)
	members, _ := f.rdb.ZRange(ctx, k.expiry(), 0, -1).Result()
	slices.Sort(members)
	wantMembers := []string{expiryMember(pendingKept, payer), expiryMember(pendingLost, unlucky)}
	slices.Sort(wantMembers)
	if !slices.Equal(members, wantMembers) {
		t.Fatalf("expiry index %v, want %v", members, wantMembers)
	}

	// The sale stays frozen until an operator checks and unfreezes it.
	_, err = f.create(uuid.NewString(), uuid.NewString(), 1)
	mustErr(t, "hold while frozen", err, ErrSalePaused)

	// A second rebuild finds nothing to change.
	got, err = f.store.Rebuild(ctx, f.eventID, plan, false)
	mustErr(t, "second rebuild", err, nil)
	mustEqual(t, "second rebuild", got, RebuildResult{AvailableBefore: 94, Provisioned: true, WasFrozen: true, HoldsKept: 2})

	// The recreated hold is confirmed normally, not as a late confirmation
	// that would take its units a second time; a failed payment returns the
	// other's units once.
	out, err = f.svc.Confirm(ctx, f.eventID, unlucky, pendingLost, 1)
	mustErr(t, "confirm the recreated hold", err, nil)
	mustEqual(t, "outcome", out, ConfirmApplied)
	mustEqual(t, "available after confirming", f.avail(t), 94)
	released, err := f.svc.ReleaseForFailedPayment(ctx, f.eventID, payer, pendingKept)
	mustErr(t, "failed payment", err, nil)
	mustEqual(t, "released", released, true)
	mustEqual(t, "available after the failed payment", f.avail(t), 96)
	mustEqual(t, "payer's units", f.userUnits(t, payer), 0)
}

// A booking confirmed between reading PostgreSQL and the rebuild keeps its
// SOLD hold: the rebuild must not turn a sale back into a payment.
func TestRebuildLeavesAHoldConfirmedMeanwhile(t *testing.T) {
	f := newFixture(t, 10, 4)
	user := uuid.NewString()
	id := f.paying(t, user, 2)
	plan := NewRebuildPlan(10, 4, 0, nil, []RebuildHold{{HoldID: id, UserID: user, Qty: 2, ExpiresAt: time.Now().Add(time.Minute)}})

	_, err := f.svc.Confirm(ctx, f.eventID, user, id, 2) // lands after the read
	mustErr(t, "confirm", err, nil)

	got, err := f.store.Rebuild(ctx, f.eventID, plan, false)
	mustErr(t, "rebuild", err, nil)
	mustEqual(t, "confirmed meanwhile", got.HoldsSold, 1)
	mustEqual(t, "kept or recreated", got.HoldsKept+got.HoldsRecreated, 0)
	mustEqual(t, "state", f.state(t, user, id).State, StateSold)
	mustEqual(t, "available", f.avail(t), 8) // pending then, sold now: counted once either way
}

// How long a rebuild of a large sale takes, and so how long it blocks
// Valkey: 40,000 buyers, 10,000 pending bookings, 5,000 abandoned holds.
// Opt in with HOLDFAST_TEST_REBUILD_SCALE=1 (it takes a few seconds to set up).
func TestRebuildAtScale(t *testing.T) {
	if os.Getenv("HOLDFAST_TEST_REBUILD_SCALE") != "1" {
		t.Skip("set HOLDFAST_TEST_REBUILD_SCALE=1 to measure a large rebuild")
	}
	const buyers, pending, abandoned = 40_000, 10_000, 5_000
	f := newFixture(t, 200_000, 4)
	k := keysFor(f.eventID)
	purchases := make(map[string]int, buyers)
	holds := make([]RebuildHold, 0, pending)
	exp := time.Now().Add(10 * time.Minute)
	pipe := f.rdb.Pipeline()
	for range buyers {
		u := uuid.NewString()
		purchases[u] = 2
		pipe.Set(ctx, k.user(u), 1, 0) // drifted
	}
	for range pending {
		h, u := uuid.NewString(), uuid.NewString()
		holds = append(holds, RebuildHold{HoldID: h, UserID: u, Qty: 2, ExpiresAt: exp})
	}
	for range abandoned {
		h, u := uuid.NewString(), uuid.NewString()
		pipe.HSet(ctx, k.hold(h), "user", u, "qty", 1, "state", "HELD", "expires_at", exp.UnixMilli())
		pipe.ZAdd(ctx, k.expiry(), redis.Z{Score: float64(exp.UnixMilli()), Member: expiryMember(h, u)})
		pipe.Set(ctx, k.user(u), 1, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	plan := NewRebuildPlan(200_000, 4, 2*buyers, purchases, holds)

	for _, dry := range []bool{true, false} {
		start := time.Now()
		got, err := f.store.Rebuild(ctx, f.eventID, plan, dry)
		took := time.Since(start)
		mustErr(t, "rebuild", err, nil)
		mustEqual(t, "counters set", got.UsersChanged, buyers+pending)
		mustEqual(t, "counters deleted", got.UsersDeleted, abandoned)
		mustEqual(t, "holds recreated", got.HoldsRecreated, pending)
		mustEqual(t, "holds dropped", got.HoldsDropped, abandoned)
		t.Logf("dry run %t: %d counters, %d pending holds, %d abandoned holds in %s", dry, buyers+pending, pending, abandoned, took)
	}
	mustEqual(t, "available", f.avail(t), 200_000-2*buyers-2*pending)
}

// After Valkey lost everything, the rebuild provisions the event again and
// registers it with the sweeper.
func TestRebuildFromNothing(t *testing.T) {
	f := newFixture(t, 10, 4)
	mustErr(t, "purge", f.store.Purge(ctx, f.eventID), nil)
	bought, waiting, holdID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	plan := NewRebuildPlan(10, 4, 2, map[string]int{bought: 2}, []RebuildHold{
		{HoldID: holdID, UserID: waiting, Qty: 1, ExpiresAt: time.Now().Add(time.Minute)},
	})

	got, err := f.store.Rebuild(ctx, f.eventID, plan, false)
	mustErr(t, "rebuild", err, nil)
	mustEqual(t, "rebuild", got, RebuildResult{UsersChanged: 2, HoldsRecreated: 1})
	mustEqual(t, "available", f.avail(t), 7)
	mustEqual(t, "units bought", f.userUnits(t, bought), 2)
	mustEqual(t, "units pending", f.userUnits(t, waiting), 1)
	events, err := f.store.Events(ctx)
	mustErr(t, "events", err, nil)
	if !slices.Contains(events, f.eventID) {
		t.Fatal("the rebuilt event is not on the sweeper's list")
	}
	cfg, _ := f.rdb.HGetAll(ctx, keysFor(f.eventID).config()).Result()
	if cfg["capacity"] != "10" || cfg["per_user_limit"] != "4" || cfg["frozen"] != "1" {
		t.Fatalf("config %v", cfg)
	}
}
