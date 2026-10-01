//go:build integration

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// openQueueWith provisions the fixture's queue (not on any work list),
// fills it with n members and opens it.
func (f *fixture) openQueueWith(t *testing.T, n int, maxSessions int) {
	t.Helper()
	cfg := validConfig()
	cfg.OpensAt = time.Now().Add(-time.Minute)
	cfg.MaxSessions = maxSessions
	k := keysFor(f.eventID)
	code, err := provisionScript.Run(ctx, f.rdb, []string{k.config(), k.state()},
		cfg.OpensAt.UnixMilli(), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL.Milliseconds()).Int64()
	if err != nil || code != 1 {
		t.Fatalf("provision: %d %v", code, err)
	}
	members := make([]redis.Z, n)
	for i := range members {
		members[i] = redis.Z{Score: float64(2 + i), Member: uuid.NewString()}
	}
	if n > 0 {
		if err := f.rdb.ZAdd(ctx, k.members(), members...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	f.setState(t, StateOpen)
	t.Cleanup(func() {
		_ = f.rdb.Del(context.Background(), k.admitted(), k.epoch(), k.sessions(), k.status()).Err()
	})
}

func (f *fixture) admittedUpTo(t *testing.T) int64 {
	t.Helper()
	n, err := f.rdb.Get(ctx, keysFor(f.eventID).admitted()).Int64()
	if errors.Is(err, redis.Nil) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAdvanceRefusesAStaleEpoch(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 100, 10_000)
	old, err := f.store.NewTerm(ctx, f.eventID)
	mustErr(t, "first term", err, nil)
	adv, err := f.store.Advance(ctx, f.eventID, old, 5)
	mustErr(t, "advance by the first leader", err, nil)
	mustEqual(t, "admitted", adv.Admitted, int64(5))

	newer, err := f.store.NewTerm(ctx, f.eventID)
	mustErr(t, "second term", err, nil)
	if newer <= old {
		t.Fatalf("new epoch %d not above old %d", newer, old)
	}
	// The old leader wakes up (a GC pause, a frozen VM) and tries to admit.
	_, err = f.store.Advance(ctx, f.eventID, old, 50)
	mustErr(t, "advance by the stale leader", err, ErrFenced)
	mustEqual(t, "admittedUpTo after the fenced write", f.admittedUpTo(t), int64(5))

	adv, err = f.store.Advance(ctx, f.eventID, newer, 5)
	mustErr(t, "advance by the new leader", err, nil)
	mustEqual(t, "admittedUpTo", adv.AdmittedUpTo, int64(10))
}

func TestAdvanceOnlyWhileOpen(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 50, 10_000)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	for _, s := range []State{StatePre, StateFrozen, StateSoldOut, StateClosed} {
		f.setState(t, s)
		adv, err := f.store.Advance(ctx, f.eventID, epoch, 10)
		mustErr(t, "advance in "+string(s), err, nil)
		mustEqual(t, "admitted in "+string(s), adv.Admitted, int64(0))
	}
	f.setState(t, StateOpen)
	adv, err := f.store.Advance(ctx, f.eventID, epoch, 10)
	mustErr(t, "advance when OPEN", err, nil)
	mustEqual(t, "admitted when OPEN", adv.Admitted, int64(10))
}

func TestAdvanceCapsAtTheSessionBudgetAndQueueLength(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 20, 5)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	adv, err := f.store.Advance(ctx, f.eventID, epoch, 100)
	mustErr(t, "advance", err, nil)
	mustEqual(t, "admitted with 5 sessions max", adv.Admitted, int64(5))
	mustEqual(t, "active sessions", adv.ActiveSessions, int64(5))
	adv, _ = f.store.Advance(ctx, f.eventID, epoch, 100)
	mustEqual(t, "admitted with the budget full", adv.Admitted, int64(0))

	// Sessions whose TTL passed free their slots on the next tick.
	sessions := keysFor(f.eventID).sessions()
	past := float64(time.Now().Add(-time.Second).UnixMilli())
	for _, rank := range []string{"1", "2", "3"} {
		if err := f.rdb.ZAddXX(ctx, sessions, redis.Z{Score: past, Member: rank}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	adv, _ = f.store.Advance(ctx, f.eventID, epoch, 100)
	mustEqual(t, "admitted after 3 sessions expired", adv.Admitted, int64(3))
	mustEqual(t, "admittedUpTo", adv.AdmittedUpTo, int64(8))
	mustEqual(t, "active sessions", adv.ActiveSessions, int64(5))

	// Never beyond the last member in line.
	g := newFixture(t)
	g.openQueueWith(t, 3, 10_000)
	e2, _ := g.store.NewTerm(ctx, g.eventID)
	adv, _ = g.store.Advance(ctx, g.eventID, e2, 100)
	mustEqual(t, "admitted from a queue of 3", adv.Admitted, int64(3))
	adv, _ = g.store.Advance(ctx, g.eventID, e2, 100)
	mustEqual(t, "admitted when everyone is in", adv.Admitted, int64(0))
}

func TestAdmittedRanksGetSessionSlotsForTheTTL(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 10, 10_000)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	before := time.Now()
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 4); err != nil {
		t.Fatal(err)
	}
	slots, err := f.rdb.ZRangeWithScores(ctx, keysFor(f.eventID).sessions(), 0, -1).Result()
	mustErr(t, "read sessions", err, nil)
	mustEqual(t, "session slots", len(slots), 4)
	for i, z := range slots {
		mustEqual(t, "slot member (rank)", z.Member.(string), strconv.Itoa(i+1))
		expires := time.UnixMilli(int64(z.Score))
		want := before.Add(validConfig().SessionTTL)
		if d := expires.Sub(want); d < -time.Second || d > 5*time.Second {
			t.Fatalf("slot for rank %d expires at %s, want about %s", i+1, expires, want)
		}
	}
}

func gauge(t *testing.T, m *Metrics, eventID string) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.leader.WithLabelValues(eventID).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetGauge().GetValue()
}

func counter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var out dto.Metric
	if err := c.Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

// TestOneLeaderAndFailover runs two real controllers for one event against
// PostgreSQL and Valkey: exactly one leads; when it dies, the standby takes
// over with a newer epoch, admissions continue, and the old leader's epoch
// can no longer admit anyone.
func TestOneLeaderAndFailover(t *testing.T) {
	f := newFixture(t)
	pool := testenv.Postgres(t)
	f.openQueueWith(t, 10_000, 10_000)
	cfg := AdmissionConfig{Tick: 20 * time.Millisecond, RetryLeadership: 100 * time.Millisecond, Rescan: time.Second}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	type runner struct {
		m      *Metrics
		cancel context.CancelFunc
		done   chan struct{}
	}
	start := func() *runner {
		m := NewMetrics(prometheus.NewRegistry())
		c := NewController(f.store, pool, f.eventID, cfg, m, quiet)
		runCtx, cancel := context.WithCancel(ctx)
		r := &runner{m: m, cancel: cancel, done: make(chan struct{})}
		go func() { defer close(r.done); c.Run(runCtx) }()
		return r
	}
	a, b := start(), start()
	defer func() { a.cancel(); b.cancel(); <-a.done; <-b.done }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor("a leader to admit people", func() bool { return f.admittedUpTo(t) >= 5 })

	leaders := gauge(t, a.m, f.eventID) + gauge(t, b.m, f.eventID)
	mustEqual(t, "leaders", leaders, float64(1))
	leader, standby := a, b
	if gauge(t, b.m, f.eventID) == 1 {
		leader, standby = b, a
	}
	firstEpoch, _ := f.rdb.Get(ctx, keysFor(f.eventID).epoch()).Int64()

	// Kill the leader: its connection closes and PostgreSQL releases the lock.
	leader.cancel()
	<-leader.done
	before := f.admittedUpTo(t)
	waitFor("the standby to take over", func() bool { return gauge(t, standby.m, f.eventID) == 1 })
	waitFor("admissions to continue", func() bool { return f.admittedUpTo(t) > before })
	secondEpoch, _ := f.rdb.Get(ctx, keysFor(f.eventID).epoch()).Int64()
	if secondEpoch <= firstEpoch {
		t.Fatalf("epoch after failover %d, want above %d", secondEpoch, firstEpoch)
	}
	mustEqual(t, "terms won by the standby", counter(t, standby.m.terms), float64(1))

	// The dead leader's epoch is fenced off.
	_, err := f.store.Advance(ctx, f.eventID, firstEpoch, 100)
	mustErr(t, "advance with the old leader's epoch", err, ErrFenced)
}

// --- status document ---

func TestAdvanceWritesTheStatusDocumentEveryTick(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 30, 10_000)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	before := time.Now()
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 7); err != nil {
		t.Fatal(err)
	}
	st, err := f.svc.Status(ctx, f.eventID)
	mustErr(t, "status", err, nil)
	mustEqual(t, "state", st.State, StateOpen)
	mustEqual(t, "admittedUpTo", st.AdmittedUpTo, int64(7))
	mustEqual(t, "queueSize", st.QueueSize, int64(30))
	if st.UpdatedAt.Before(before.Add(-time.Second)) || st.UpdatedAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("updatedAt %s not around now", st.UpdatedAt)
	}

	// Ticks that admit nobody still refresh it, in every state.
	f.setState(t, StateFrozen)
	time.Sleep(5 * time.Millisecond)
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 7); err != nil {
		t.Fatal(err)
	}
	st2, _ := f.svc.Status(ctx, f.eventID)
	mustEqual(t, "state while frozen", st2.State, StateFrozen)
	mustEqual(t, "admittedUpTo while frozen", st2.AdmittedUpTo, int64(7))
	if !st2.UpdatedAt.After(st.UpdatedAt) {
		t.Fatalf("updatedAt not refreshed: %s then %s", st.UpdatedAt, st2.UpdatedAt)
	}
}

func TestStaleLeaderCannotOverwriteTheStatusDocument(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 30, 10_000)
	old, _ := f.store.NewTerm(ctx, f.eventID)
	if _, err := f.store.Advance(ctx, f.eventID, old, 3); err != nil {
		t.Fatal(err)
	}
	newer, _ := f.store.NewTerm(ctx, f.eventID)
	if _, err := f.store.Advance(ctx, f.eventID, newer, 2); err != nil {
		t.Fatal(err)
	}
	doc, _ := f.rdb.Get(ctx, keysFor(f.eventID).status()).Result()
	_, err := f.store.Advance(ctx, f.eventID, old, 10)
	mustErr(t, "stale advance", err, ErrFenced)
	after, _ := f.rdb.Get(ctx, keysFor(f.eventID).status()).Result()
	mustEqual(t, "status document after a fenced tick", after, doc)
}

func TestStatusReportsPrePastT0AsOpen(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 5, 10_000) // opens a minute ago
	f.setState(t, StatePre)       // nobody has flipped it
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 5); err != nil {
		t.Fatal(err)
	}
	st, _ := f.svc.Status(ctx, f.eventID)
	mustEqual(t, "state shown", st.State, StateOpen)
	mustEqual(t, "admitted (advance only admits when the stored state is OPEN)", st.AdmittedUpTo, int64(0))
}

func TestStatusFallbackBeforeAnyLeader(t *testing.T) {
	f := newFixture(t)
	opensAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	f.provisionAt(t, opensAt)
	for range 3 {
		if _, err := f.svc.Join(ctx, f.eventID, uuid.NewString()); err != nil {
			t.Fatal(err)
		}
	}
	st, err := f.svc.Status(ctx, f.eventID)
	mustErr(t, "status", err, nil)
	mustEqual(t, "state", st.State, StatePre)
	if !st.OpensAt.Equal(opensAt) {
		t.Fatalf("opensAt %s, want %s", st.OpensAt, opensAt)
	}
	mustEqual(t, "queueSize", st.QueueSize, int64(3))
	mustEqual(t, "admittedUpTo", st.AdmittedUpTo, int64(0))
	mustEqual(t, "updatedAt is zero without a leader", st.UpdatedAt.IsZero(), true)
}

func TestStatusNotProvisioned(t *testing.T) {
	f := newFixture(t)
	_, err := f.svc.Status(ctx, f.eventID)
	mustErr(t, "status", err, ErrEventNotFound)
}
