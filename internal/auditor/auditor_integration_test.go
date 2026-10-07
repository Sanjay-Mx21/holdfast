//go:build integration

package auditor

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

// auditValkeyDB is this package's own Valkey logical database.
const auditValkeyDB = 3

type fixture struct {
	db  *pgxpool.Pool // read-write, to arrange data
	rdb redis.UniversalClient
	a   *Auditor
	m   *Metrics
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	iso := testenv.NewIsolated(t, "_audit", auditValkeyDB)
	ro, err := postgres.NewReadOnlyPool(ctx, config.Postgres{
		DSN: iso.DSN, MaxConns: 4, MaxConnLifetime: time.Hour, MaxConnIdleTime: time.Minute, StatementTimeout: 10 * time.Second,
	}, "holdfast-auditor-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ro.Close)
	m := NewMetrics(prometheus.NewRegistry())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := New(ro, iso.Valkey, Config{Interval: time.Hour, MoneyDeadline: 15 * time.Minute, HoldGrace: 5 * time.Minute}, m, quiet)
	return &fixture{db: iso.Pool, rdb: iso.Valkey, a: a, m: m}
}

func (f *fixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.db.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", strings.Fields(sql)[0], err)
	}
}

// event creates an event with its inventory row.
func (f *fixture) event(t *testing.T, capacity, sold, perUser int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	f.exec(t, `INSERT INTO booking.events (id, name, sale_opens_at, per_user_limit, unit_price_paise) VALUES ($1, 'audit', now(), $2, 100)`, id, perUser)
	f.exec(t, `INSERT INTO booking.event_inventory (event_id, capacity, sold) VALUES ($1, $2, $3)`, id, capacity, sold)
	return id
}

func (f *fixture) booking(t *testing.T, event, user uuid.UUID, qty int, status string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	f.exec(t, `INSERT INTO booking.bookings (id, event_id, user_id, hold_id, qty, amount_paise, status, payment_deadline)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now() + interval '10 minutes')`, id, event, user, uuid.New(), qty, qty*100, status)
	return id
}

// payment creates an intent for bookingID with status, and books captures
// capture ledger transactions at capturedAgo.
func (f *fixture) payment(t *testing.T, bookingID uuid.UUID, status string, captures int, capturedAgo time.Duration) {
	t.Helper()
	intent, event := uuid.New(), uuid.New()
	f.exec(t, `INSERT INTO payment.payment_intents (id, booking_id, event_id, amount_paise, status, psp_payment_id, expires_at)
		VALUES ($1, $2, $3, 500, $4, $5, now() + interval '10 minutes')`, intent, bookingID, event, status, "pay_"+intent.String())
	for range captures {
		tx, err := f.db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		txn := uuid.New()
		at := time.Now().Add(-capturedAgo)
		for _, e := range []struct{ account, dir string }{{"psp_receivable", "D"}, {"unearned_revenue:" + event.String(), "C"}} {
			if _, err := tx.Exec(ctx, `INSERT INTO payment.ledger_entries (txn_id, account, direction, amount_paise, intent_id, created_at)
				VALUES ($1, $2, $3, 500, $4, $5)`, txn, e.account, e.dir, intent, at); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *fixture) check(t *testing.T) Report {
	t.Helper()
	r, err := f.a.Check(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestACleanStoreHasNoViolations(t *testing.T) {
	f := newFixture(t)
	e := f.event(t, 10, 3, 4)
	b := f.booking(t, e, uuid.New(), 3, "CONFIRMED")
	f.payment(t, b, "CAPTURED", 1, time.Hour) // captured long ago, and confirmed: resolved
	r := f.check(t)
	for _, inv := range Invariants {
		if r[inv] != 0 {
			t.Errorf("%s: %d violations in a clean store", inv, r[inv])
		}
		if got := testutil.ToFloat64(f.m.violations.WithLabelValues(inv)); got != 0 {
			t.Errorf("%s gauge %v", inv, got)
		}
	}
	if testutil.ToFloat64(f.m.lastSuccess) == 0 {
		t.Error("the check did not record its success")
	}
}

func TestEachInvariantIsCounted(t *testing.T) {
	f := newFixture(t)

	// I1: 3 units confirmed on a 2-unit event (the guard's counter says 2).
	over := f.event(t, 2, 2, 4)
	f.booking(t, over, uuid.New(), 2, "CONFIRMED")
	f.booking(t, over, uuid.New(), 1, "CONFIRMED")

	// I2: one intent whose capture is booked twice.
	ok := f.event(t, 100, 0, 4)
	f.payment(t, f.booking(t, ok, uuid.New(), 1, "CONFIRMED"), "CAPTURED", 2, time.Minute)

	// I3: captured 20 minutes ago, booking cancelled and no refund; and a
	// refund pending for 20 minutes. Not counted: captured 20 minutes ago
	// and confirmed, captured a minute ago and not yet confirmed, refunded.
	f.payment(t, f.booking(t, ok, uuid.New(), 1, "CANCELLED"), "CAPTURED", 1, 20*time.Minute)
	f.payment(t, f.booking(t, ok, uuid.New(), 1, "REFUND_REQUIRED"), "REFUND_PENDING", 1, 20*time.Minute)
	f.payment(t, f.booking(t, ok, uuid.New(), 1, "CONFIRMED"), "CAPTURED", 1, 20*time.Minute)
	f.payment(t, f.booking(t, ok, uuid.New(), 1, "PENDING_PAYMENT"), "CAPTURED", 1, time.Minute)
	f.payment(t, f.booking(t, ok, uuid.New(), 1, "REFUNDED"), "REFUNDED", 1, time.Hour)

	// I4: one user holding 3 and bought 2 against a limit of 4; another at
	// exactly the limit; the guard's counter over the limit for a third.
	greedy, fair, counted := uuid.New(), uuid.New(), uuid.New()
	f.booking(t, ok, greedy, 3, "PENDING_PAYMENT")
	f.booking(t, ok, greedy, 2, "CONFIRMED")
	f.booking(t, ok, fair, 4, "CONFIRMED")
	f.booking(t, ok, fair, 3, "CANCELLED") // cancelled units do not count
	f.exec(t, `INSERT INTO booking.user_event_purchases (event_id, user_id, qty) VALUES ($1, $2, 5)`, ok, counted)

	// I5: one hold 10 minutes past its expiry; one just past it (within the
	// grace); one not expired.
	nowMs := time.Now().UnixMilli()
	ev := uuid.NewString()
	f.rdb.SAdd(ctx, "inv:events", ev)
	f.rdb.ZAdd(ctx, "inv:{"+ev+"}:expiry",
		redis.Z{Score: float64(nowMs - 10*60_000), Member: "h1|u1"},
		redis.Z{Score: float64(nowMs - 30_000), Member: "h2|u2"},
		redis.Z{Score: float64(nowMs + 60_000), Member: "h3|u3"})

	r := f.check(t)
	want := Report{I1: 1, I2: 1, I3: 2, I4: 2, I5: 1}
	for _, inv := range Invariants {
		if r[inv] != want[inv] {
			t.Errorf("%s: %d violations, want %d", inv, r[inv], want[inv])
		}
		if got := testutil.ToFloat64(f.m.violations.WithLabelValues(inv)); got != float64(want[inv]) {
			t.Errorf("%s gauge %v, want %d", inv, got, want[inv])
		}
	}
}

// TestTheAuditorCannotWrite: its connections are read-only whatever the
// role may do.
func TestTheAuditorCannotWrite(t *testing.T) {
	f := newFixture(t)
	_, err := f.a.db.Exec(ctx, `INSERT INTO booking.events (id, name, sale_opens_at, per_user_limit, unit_price_paise) VALUES ($1, 'x', now(), 1, 1)`, uuid.New())
	if err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("write through the auditor's pool: %v, want a read-only error", err)
	}
}

// TestAFailedCheckKeepsTheOthers: Valkey down, I5 cannot be checked; the
// SQL invariants are still published, and the run counts as an error.
func TestAFailedCheckKeepsTheOthers(t *testing.T) {
	f := newFixture(t)
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = dead.Close() })
	f.a.rdb = dead
	r, err := f.a.Check(ctx)
	if err == nil || !strings.Contains(err.Error(), I5) {
		t.Fatalf("Check with Valkey down: %v", err)
	}
	if _, ok := r[I5]; ok || r[I1] != 0 {
		t.Fatalf("report %v: I5 must be missing, the others present", r)
	}
	if testutil.ToFloat64(f.m.runs.WithLabelValues("error")) != 1 || testutil.ToFloat64(f.m.lastSuccess) != 0 {
		t.Fatal("a partial check must count as an error and not as a success")
	}
}

// TestInventoryDriftIsReported: inventory offering more units than
// PostgreSQL has free is reported per event; offering fewer (a hold not yet
// booked) is healthy, and an event missing from the catalog is skipped.
func TestInventoryDriftIsReported(t *testing.T) {
	f := newFixture(t)
	healthy := f.event(t, 10, 3, 4) // free: 10 - 3 sold - 2 pending = 5
	f.booking(t, healthy, uuid.New(), 2, "PENDING_PAYMENT")
	drifted := f.event(t, 10, 3, 4) // free: 7
	unknown := uuid.New()
	for e, avail := range map[uuid.UUID]int{healthy: 4, drifted: 9, unknown: 50} {
		k := "inv:{" + e.String() + "}:avail"
		if err := f.rdb.Set(ctx, k, avail, 0).Err(); err != nil {
			t.Fatal(err)
		}
		f.rdb.SAdd(ctx, "inv:events", e.String())
	}
	f.check(t)
	drift := func(e uuid.UUID) float64 { return testutil.ToFloat64(f.m.drift.WithLabelValues(e.String())) }
	if got := drift(drifted); got != 2 {
		t.Errorf("drifted event: %v units, want 2", got)
	}
	if got := drift(healthy); got != 0 {
		t.Errorf("healthy event (one unit in an unbooked hold): %v units, want 0", got)
	}
	if n := testutil.CollectAndCount(f.m.drift); n != 2 {
		t.Errorf("%d series, want 2: the event missing from the catalog must not be published", n)
	}

	// Rebuilt: back to 0.
	f.rdb.Set(ctx, "inv:{"+drifted.String()+"}:avail", 7, 0)
	f.check(t)
	if got := drift(drifted); got != 0 {
		t.Errorf("after the fix: %v units, want 0", got)
	}
}
