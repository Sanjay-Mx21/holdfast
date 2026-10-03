//go:build integration

package inventory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

type fixture struct {
	svc     *Service
	store   *Store
	rdb     redis.UniversalClient
	m       *Metrics
	eventID string
}

func newFixture(t *testing.T, capacity, perUser int) *fixture {
	t.Helper()
	rdb := testenv.Valkey(t)
	store := NewStore(rdb)
	m := NewMetrics(prometheus.NewRegistry())
	svc, err := NewService(store, Config{HoldTTL: time.Minute, PaymentWindow: 2 * time.Minute}, m)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.Must(uuid.NewV7()).String()
	if _, err := svc.Provision(ctx, eventID, EventConfig{Capacity: capacity, PerUserLimit: perUser}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Purge(context.Background(), eventID) })
	return &fixture{svc: svc, store: store, rdb: rdb, m: m, eventID: eventID}
}

func (f *fixture) create(user, key string, qty int) (HoldResult, error) {
	return f.svc.CreateHold(ctx, CreateHoldRequest{EventID: f.eventID, UserID: user, IdempotencyKey: key, Quantity: qty})
}

func (f *fixture) avail(t *testing.T) int {
	t.Helper()
	a, err := f.svc.Availability(ctx, f.eventID)
	if err != nil {
		t.Fatal(err)
	}
	return a.Available
}

func (f *fixture) userUnits(t *testing.T, user string) int {
	t.Helper()
	n, err := f.rdb.Get(ctx, keysFor(f.eventID).user(user)).Int()
	if errors.Is(err, redis.Nil) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustEqual[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func mustErr(t *testing.T, what string, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("%s: got error %v, want %v", what, got, want)
	}
}

func TestHoldReplayLimitsAndSoldOut(t *testing.T) {
	f := newFixture(t, 10, 4)
	user := uuid.NewString()

	res, err := f.create(user, "order-0001", 2)
	mustErr(t, "first hold", err, nil)
	mustEqual(t, "remaining", res.Remaining, 8)
	mustEqual(t, "state", res.Hold.State, StateHeld)
	if d := time.Until(res.Hold.ExpiresAt); d < 50*time.Second || d > 70*time.Second {
		t.Fatalf("expiry %v should be about one hold TTL away", d)
	}

	again, err := f.create(user, "order-0001", 2)
	mustErr(t, "replay", err, nil)
	mustEqual(t, "replayed", again.Replayed, true)
	mustEqual(t, "replayed hold ID", again.Hold.ID, res.Hold.ID)
	mustEqual(t, "units after replay", f.avail(t), 8)

	_, err = f.create(user, "order-0001", 3)
	mustErr(t, "same key, different quantity", err, ErrIdempotencyKeyReused)
	_, err = f.create(user, "order-0002", 3)
	mustErr(t, "per-user cap", err, ErrUserLimit)
	_, err = f.create(user, "order-0003", 2)
	mustErr(t, "up to the cap", err, nil)
	mustEqual(t, "user units", f.userUnits(t, user), 4)
	_, err = f.create(uuid.NewString(), "order-0004", 5)
	mustErr(t, "quantity above the event limit", err, ErrInvalidQuantity)

	_, err = f.create(uuid.NewString(), "order-0005", 4)
	mustErr(t, "take four", err, nil)
	u := uuid.NewString()
	_, err = f.create(u, "order-0006", 3)
	mustErr(t, "three of two left", err, ErrSoldOut)
	_, err = f.create(u, "order-0007", 2)
	mustErr(t, "last two", err, nil)
	_, err = f.create(uuid.NewString(), "order-0008", 1)
	mustErr(t, "nothing left", err, ErrSoldOut)
	mustEqual(t, "available", f.avail(t), 0)
}

func TestUnknownEventIsNotProvisioned(t *testing.T) {
	f := newFixture(t, 1, 1)
	other := uuid.NewString()
	_, err := f.svc.CreateHold(ctx, CreateHoldRequest{EventID: other, UserID: uuid.NewString(), IdempotencyKey: "order-0001", Quantity: 1})
	mustErr(t, "hold on unknown event", err, ErrEventNotProvisioned)
	_, err = f.svc.Availability(ctx, other)
	mustErr(t, "availability of unknown event", err, ErrEventNotProvisioned)
}

func TestCancelReturnsUnitsAndIsIdempotent(t *testing.T) {
	f := newFixture(t, 10, 4)
	user, stranger := uuid.NewString(), uuid.NewString()
	res, err := f.create(user, "cancel-0001", 3)
	mustErr(t, "hold", err, nil)
	mustEqual(t, "available", f.avail(t), 7)

	mustErr(t, "stranger cancels", f.svc.CancelHold(ctx, f.eventID, stranger, res.Hold.ID), ErrHoldNotFound)
	_, err = f.svc.GetHold(ctx, f.eventID, stranger, res.Hold.ID)
	mustErr(t, "stranger reads", err, ErrHoldNotFound)

	mustErr(t, "cancel", f.svc.CancelHold(ctx, f.eventID, user, res.Hold.ID), nil)
	mustEqual(t, "available after cancel", f.avail(t), 10)
	mustEqual(t, "user units after cancel", f.userUnits(t, user), 0)
	mustErr(t, "cancel again", f.svc.CancelHold(ctx, f.eventID, user, res.Hold.ID), nil)
	mustEqual(t, "available after second cancel", f.avail(t), 10)

	h, err := f.svc.GetHold(ctx, f.eventID, user, res.Hold.ID)
	mustErr(t, "read tombstone", err, nil)
	mustEqual(t, "tombstone state", h.State, StateReleased)
	_, err = f.create(user, "cancel-0001", 3)
	mustErr(t, "reuse key of released hold", err, ErrHoldExpired)
}

func TestCheckoutConfirmAndReplays(t *testing.T) {
	f := newFixture(t, 10, 4)
	user := uuid.NewString()
	res, err := f.create(user, "pay-0001", 2)
	mustErr(t, "hold", err, nil)

	until, err := f.svc.MarkPaying(ctx, f.eventID, user, res.Hold.ID)
	mustErr(t, "mark paying", err, nil)
	if !until.After(res.Hold.ExpiresAt) {
		t.Fatalf("payment window %v must extend past the hold expiry %v", until, res.Hold.ExpiresAt)
	}
	_, err = f.svc.MarkPaying(ctx, f.eventID, user, res.Hold.ID)
	mustErr(t, "mark paying again", err, nil)
	_, err = f.svc.MarkPaying(ctx, f.eventID, uuid.NewString(), res.Hold.ID)
	mustErr(t, "stranger marks paying", err, ErrHoldNotFound)
	mustErr(t, "cancel during checkout", f.svc.CancelHold(ctx, f.eventID, user, res.Hold.ID), ErrHoldNotCancellable)

	out, err := f.svc.Confirm(ctx, f.eventID, user, res.Hold.ID, 2)
	mustErr(t, "confirm", err, nil)
	mustEqual(t, "outcome", out, ConfirmApplied)
	out, err = f.svc.Confirm(ctx, f.eventID, user, res.Hold.ID, 2)
	mustErr(t, "confirm again", err, nil)
	mustEqual(t, "outcome of retry", out, ConfirmReplay)
	_, err = f.svc.Confirm(ctx, f.eventID, uuid.NewString(), res.Hold.ID, 2)
	mustErr(t, "stranger confirms", err, ErrHoldNotFound)

	released, err := f.svc.ReleaseForFailedPayment(ctx, f.eventID, user, res.Hold.ID)
	mustErr(t, "release sold hold", err, nil)
	mustEqual(t, "released", released, false)
	mustEqual(t, "available", f.avail(t), 8)
	h, _ := f.svc.GetHold(ctx, f.eventID, user, res.Hold.ID)
	mustEqual(t, "final state", h.State, StateSold)
}

func TestFailedPaymentReleasesAndLateConfirmRetakesUnits(t *testing.T) {
	f := newFixture(t, 10, 4)
	user := uuid.NewString()
	res, err := f.create(user, "late-0001", 3)
	mustErr(t, "hold", err, nil)
	_, err = f.svc.MarkPaying(ctx, f.eventID, user, res.Hold.ID)
	mustErr(t, "mark paying", err, nil)

	released, err := f.svc.ReleaseForFailedPayment(ctx, f.eventID, user, res.Hold.ID)
	mustErr(t, "release", err, nil)
	mustEqual(t, "released", released, true)
	mustEqual(t, "available after release", f.avail(t), 10)
	mustEqual(t, "user units after release", f.userUnits(t, user), 0)

	// The payment succeeded after all (the provider was slow): PostgreSQL
	// committed, so Valkey must re-take the units rather than drop the sale.
	out, err := f.svc.Confirm(ctx, f.eventID, user, res.Hold.ID, 3)
	mustErr(t, "late confirm", err, nil)
	mustEqual(t, "outcome", out, ConfirmLate)
	mustEqual(t, "available after late confirm", f.avail(t), 7)
	mustEqual(t, "user units after late confirm", f.userUnits(t, user), 3)
}

func TestSweeperReleasesOnlyExpiredHeldHolds(t *testing.T) {
	f := newFixture(t, 10, 4)
	sw := NewSweeper(f.store, time.Second, 100, f.m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	u1, u2, u3 := uuid.NewString(), uuid.NewString(), uuid.NewString()

	// Short-lived holds go through the store directly (the service insists
	// on a TTL of at least one second).
	h1 := HoldIDFor(u1, "sweep-0001")
	if r, err := f.store.hold(ctx, f.eventID, u1, h1, 2, 50*time.Millisecond); err != nil || r.code != 1 {
		t.Fatalf("short hold: %+v %v", r, err)
	}
	h2 := HoldIDFor(u2, "sweep-0002")
	if r, err := f.store.hold(ctx, f.eventID, u2, h2, 1, 50*time.Millisecond); err != nil || r.code != 1 {
		t.Fatalf("short hold: %+v %v", r, err)
	}
	if _, err := f.store.MarkPaying(ctx, f.eventID, u2, h2, time.Minute); err != nil {
		t.Fatal(err)
	}
	long, err := f.create(u3, "sweep-0003", 3)
	mustErr(t, "long hold", err, nil)
	mustEqual(t, "available", f.avail(t), 4)

	time.Sleep(150 * time.Millisecond)
	n, err := sw.sweepEvent(ctx, f.eventID)
	mustErr(t, "sweep", err, nil)
	mustEqual(t, "released by sweep", n, 1)

	for id, want := range map[string]HoldState{h1: StateReleased, h2: StatePaying, long.Hold.ID: StateHeld} {
		h, err := f.store.GetHold(ctx, f.eventID, id)
		mustErr(t, "read "+id, err, nil)
		mustEqual(t, "state of "+id, h.State, want)
	}
	mustEqual(t, "available after sweep", f.avail(t), 6)
	mustEqual(t, "expired user's units", f.userUnits(t, u1), 0)
	if _, err := sw.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce across all events: %v", err)
	}
}

func TestSweeperDrainsInBatches(t *testing.T) {
	f := newFixture(t, 100, 1)
	sw := NewSweeper(f.store, time.Second, 10, f.m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i := 0; i < 25; i++ {
		u := uuid.NewString()
		if r, err := f.store.hold(ctx, f.eventID, u, HoldIDFor(u, "batch-0001"), 1, 20*time.Millisecond); err != nil || r.code != 1 {
			t.Fatalf("hold %d: %+v %v", i, r, err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	n, err := sw.sweepEvent(ctx, f.eventID)
	mustErr(t, "sweep", err, nil)
	mustEqual(t, "released", n, 25)
	mustEqual(t, "available", f.avail(t), 100)
	left, _ := f.rdb.ZCard(ctx, keysFor(f.eventID).expiry()).Result()
	mustEqual(t, "expiry index size", left, int64(0))
}

func TestConcurrentBuyersNeverOversell(t *testing.T) {
	const capacity, buyers = 200, 2000
	f := newFixture(t, capacity, 4)
	var held, soldOut, other atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 256)
	for i := 0; i < buyers; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			_, err := f.create(uuid.NewString(), fmt.Sprintf("race-%06d", i), 1)
			switch {
			case err == nil:
				held.Add(1)
			case errors.Is(err, ErrSoldOut):
				soldOut.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	mustEqual(t, "held", held.Load(), int64(capacity))
	mustEqual(t, "sold out", soldOut.Load(), int64(buyers-capacity))
	mustEqual(t, "errors", other.Load(), int64(0))
	mustEqual(t, "available", f.avail(t), 0)
}

func TestOneUserRacingManyRequestsStaysWithinTheCap(t *testing.T) {
	f := newFixture(t, 100, 3)
	user := uuid.NewString()
	var ok, limited atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.create(user, fmt.Sprintf("tab-%06d", i), 1)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrUserLimit):
				limited.Add(1)
			}
		}()
	}
	wg.Wait()
	mustEqual(t, "accepted", ok.Load(), int64(3))
	mustEqual(t, "rejected by cap", limited.Load(), int64(47))
	mustEqual(t, "user units", f.userUnits(t, user), 3)
	mustEqual(t, "available", f.avail(t), 97)
}

func TestProvisionIsIdempotentAndRefusesSilentChanges(t *testing.T) {
	f := newFixture(t, 100, 4)
	created, err := f.svc.Provision(ctx, f.eventID, EventConfig{Capacity: 100, PerUserLimit: 4})
	mustErr(t, "same settings", err, nil)
	mustEqual(t, "created", created, false)
	_, err = f.svc.Provision(ctx, f.eventID, EventConfig{Capacity: 200, PerUserLimit: 4})
	mustErr(t, "different capacity", err, ErrProvisionConflict)
	_, err = f.svc.Provision(ctx, f.eventID, EventConfig{Capacity: 100, PerUserLimit: 5})
	mustErr(t, "different per-user limit", err, ErrProvisionConflict)
	member, _ := f.rdb.SIsMember(ctx, eventsKey, f.eventID).Result()
	mustEqual(t, "registered for sweeping", member, true)

	rebuilt := uuid.Must(uuid.NewV7()).String()
	t.Cleanup(func() { _ = f.store.Purge(context.Background(), rebuilt) })
	_, err = f.svc.Provision(ctx, rebuilt, EventConfig{Capacity: 100, PerUserLimit: 4, InitialAvailable: 40})
	mustErr(t, "rebuild provision", err, nil)
	a, err := f.svc.Availability(ctx, rebuilt)
	mustErr(t, "availability", err, nil)
	mustEqual(t, "rebuilt pool", a.Available, 40)
	mustEqual(t, "rebuilt capacity", a.Capacity, 100)
}

func TestScriptsPreload(t *testing.T) {
	f := newFixture(t, 1, 1)
	if err := f.store.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	for _, s := range allScripts {
		exists, err := f.rdb.ScriptExists(ctx, s.Hash()).Result()
		if err != nil || len(exists) != 1 || !exists[0] {
			t.Fatalf("script %s not cached: %v %v", s.Hash(), exists, err)
		}
	}
}

// TestRandomizedAgainstModel runs a random mix of operations against Valkey
// and against an in-memory model of the rules, and requires them to agree
// after every step. Rerun a failure with HOLDFAST_TEST_SEED=<seed>.
func TestRandomizedAgainstModel(t *testing.T) {
	seed := time.Now().UnixNano()
	if s := os.Getenv("HOLDFAST_TEST_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(uint64(seed), 0))

	const capacity, limit, steps = 30, 4, 1500
	f := newFixture(t, capacity, limit)
	m := newModel(capacity, limit)
	users := make([]string, 6)
	for i := range users {
		users[i] = uuid.NewString()
	}

	for step := 0; step < steps; step++ {
		var id, desc string
		var got, want error
		if op := rng.IntN(10); op < 5 || len(m.ids) == 0 {
			u, key, qty := users[rng.IntN(len(users))], fmt.Sprintf("model-%04d", rng.IntN(40)), 1+rng.IntN(limit+1)
			desc = fmt.Sprintf("create(user %d, %s, qty %d)", indexOf(users, u), key, qty)
			want = m.create(u, HoldIDFor(u, key), qty)
			_, got = f.create(u, key, qty)
		} else {
			id = m.ids[rng.IntN(len(m.ids))]
			h := m.holds[id]
			switch {
			case op < 6:
				desc, want = "cancel", m.cancel(id)
				got = f.svc.CancelHold(ctx, f.eventID, h.user, id)
			case op < 7:
				desc, want = "markPaying", m.markPaying(id)
				_, got = f.svc.MarkPaying(ctx, f.eventID, h.user, id)
			case op < 9:
				var wantOutcome, gotOutcome ConfirmOutcome
				desc = "confirm"
				wantOutcome = m.confirm(id)
				gotOutcome, got = f.svc.Confirm(ctx, f.eventID, h.user, id, h.qty)
				if got == nil && gotOutcome != wantOutcome {
					t.Fatalf("seed %d step %d confirm %s: outcome %s, want %s", seed, step, id, gotOutcome, wantOutcome)
				}
			default:
				desc = "releaseForFailedPayment"
				wantReleased := m.releaseFailed(id)
				var released bool
				released, got = f.svc.ReleaseForFailedPayment(ctx, f.eventID, h.user, id)
				if got == nil && released != wantReleased {
					t.Fatalf("seed %d step %d release %s: released %v, want %v", seed, step, id, released, wantReleased)
				}
			}
		}
		if !sameError(got, want) {
			t.Fatalf("seed %d step %d %s %s: got %v, want %v", seed, step, desc, id, got, want)
		}
		if a := f.avail(t); a != m.avail {
			t.Fatalf("seed %d step %d after %s: available %d, model %d", seed, step, desc, a, m.avail)
		}
		for i, u := range users {
			if got := f.userUnits(t, u); got != m.used[u] {
				t.Fatalf("seed %d step %d after %s: user %d units %d, model %d", seed, step, desc, i, got, m.used[u])
			}
		}
		if active := m.activeUnits(); m.avail+active != capacity {
			t.Fatalf("seed %d step %d: conservation broken in the model itself", seed, step)
		}
		if step%100 == 0 || step == steps-1 {
			for hid, h := range m.holds {
				got, err := f.store.GetHold(ctx, f.eventID, hid)
				if err != nil || got.State != h.state || got.Quantity != h.qty || got.UserID != h.user {
					t.Fatalf("seed %d step %d: hold %s is %+v (%v), model %+v", seed, step, hid, got, err, *h)
				}
			}
		}
	}
	t.Logf("%d steps, %d holds, final available %d", steps, len(m.holds), m.avail)
}

func sameError(got, want error) bool {
	if want == nil {
		return got == nil
	}
	return errors.Is(got, want)
}

type modelHold struct {
	user  string
	qty   int
	state HoldState
}

// model is an executable specification of the hold rules.
type model struct {
	limit int
	avail int
	used  map[string]int
	holds map[string]*modelHold
	ids   []string
}

func newModel(capacity, limit int) *model {
	return &model{limit: limit, avail: capacity, used: map[string]int{}, holds: map[string]*modelHold{}}
}

func (m *model) create(user, id string, qty int) error {
	if qty < 1 || qty > m.limit {
		return ErrInvalidQuantity
	}
	if h, ok := m.holds[id]; ok {
		if h.qty != qty {
			return ErrIdempotencyKeyReused
		}
		if h.state == StateReleased {
			return ErrHoldExpired
		}
		return nil
	}
	if m.used[user]+qty > m.limit {
		return ErrUserLimit
	}
	if m.avail < qty {
		return ErrSoldOut
	}
	m.avail -= qty
	m.used[user] += qty
	m.holds[id] = &modelHold{user: user, qty: qty, state: StateHeld}
	m.ids = append(m.ids, id)
	return nil
}

func (m *model) free(h *modelHold) {
	m.avail += h.qty
	m.used[h.user] -= h.qty
	h.state = StateReleased
}

func (m *model) cancel(id string) error {
	switch h := m.holds[id]; h.state {
	case StateHeld:
		m.free(h)
		return nil
	case StatePaying, StateSold:
		return ErrHoldNotCancellable
	default:
		return nil
	}
}

func (m *model) markPaying(id string) error {
	switch h := m.holds[id]; h.state {
	case StateHeld:
		h.state = StatePaying
		return nil
	case StatePaying:
		return nil
	default:
		return ErrHoldExpired
	}
}

func (m *model) confirm(id string) ConfirmOutcome {
	switch h := m.holds[id]; h.state {
	case StateHeld, StatePaying:
		h.state = StateSold
		return ConfirmApplied
	case StateSold:
		return ConfirmReplay
	default:
		m.avail -= h.qty
		m.used[h.user] += h.qty
		h.state = StateSold
		return ConfirmLate
	}
}

func (m *model) releaseFailed(id string) bool {
	h := m.holds[id]
	if h.state == StateHeld || h.state == StatePaying {
		m.free(h)
		return true
	}
	return false
}

func (m *model) activeUnits() int {
	n := 0
	for _, h := range m.holds {
		if h.state != StateReleased {
			n += h.qty
		}
	}
	return n
}

func indexOf(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

// TestFreezeStopsNewHoldsOnly: a frozen sale refuses new holds, but a retry
// of a hold made before the freeze still gets its hold, and existing holds
// go through checkout and release as usual. Unfreezing resumes sales.
func TestFreezeStopsNewHoldsOnly(t *testing.T) {
	f := newFixture(t, 10, 4)
	user := uuid.NewString()
	before, err := f.create(user, "order-0001", 2)
	mustErr(t, "hold before the freeze", err, nil)

	changed, err := f.svc.SetFrozen(ctx, f.eventID, true)
	mustErr(t, "freeze", err, nil)
	mustEqual(t, "freeze changed the flag", changed, true)
	changed, err = f.svc.SetFrozen(ctx, f.eventID, true)
	mustErr(t, "freeze again", err, nil)
	mustEqual(t, "freezing twice changes nothing", changed, false)

	_, err = f.create(uuid.NewString(), "order-0002", 1)
	mustErr(t, "a new hold while frozen", err, ErrSalePaused)
	again, err := f.create(user, "order-0001", 2)
	mustErr(t, "a retry of the earlier hold", err, nil)
	mustEqual(t, "replayed", again.Replayed, true)
	_, err = f.svc.MarkPaying(ctx, f.eventID, user, before.Hold.ID)
	mustErr(t, "checkout of an existing hold", err, nil)

	a, err := f.svc.Availability(ctx, f.eventID)
	mustErr(t, "availability", err, nil)
	mustEqual(t, "frozen", a.Frozen, true)
	mustEqual(t, "available", a.Available, 8)
	mustEqual(t, "active holds", a.ActiveHolds, 1)

	_, err = f.svc.SetFrozen(ctx, f.eventID, false)
	mustErr(t, "unfreeze", err, nil)
	_, err = f.create(uuid.NewString(), "order-0003", 1)
	mustErr(t, "a new hold after unfreezing", err, nil)
	a, _ = f.svc.Availability(ctx, f.eventID)
	mustEqual(t, "frozen after unfreezing", a.Frozen, false)
	mustEqual(t, "active holds", a.ActiveHolds, 2)

	_, err = f.svc.SetFrozen(ctx, uuid.NewString(), true)
	mustErr(t, "freeze an unknown event", err, ErrEventNotProvisioned)
	_, err = f.svc.SetFrozen(ctx, "not-a-uuid", true)
	mustErr(t, "freeze a bad ID", err, ErrInvalidRequest)
}
