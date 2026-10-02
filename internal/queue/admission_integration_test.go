//go:build integration

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
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
		reg    *prometheus.Registry
		cancel context.CancelFunc
		done   chan struct{}
	}
	start := func() *runner {
		reg := prometheus.NewRegistry()
		m := NewMetrics(reg)
		c := NewController(f.store, pool, f.eventID, cfg, m, quiet)
		runCtx, cancel := context.WithCancel(ctx)
		r := &runner{m: m, reg: reg, cancel: cancel, done: make(chan struct{})}
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

	// Only the current leader reports the per-event gauges.
	waitFor("the new leader's gauges", func() bool { return eventGauge(t, standby.reg, "holdfast_queue_size", f.eventID) == 10_000 })
	mustEqual(t, "leader epoch gauge", eventGauge(t, standby.reg, "holdfast_queue_leader_epoch", f.eventID), float64(secondEpoch))
	mustEqual(t, "max sessions gauge", eventGauge(t, standby.reg, "holdfast_queue_max_sessions", f.eventID), float64(10_000))
	if v := eventGauge(t, standby.reg, "holdfast_queue_admitted_up_to", f.eventID); v < 1 {
		t.Fatalf("admittedUpTo gauge %v, want >= 1", v)
	}
	for _, name := range []string{"holdfast_queue_size", "holdfast_queue_admitted_up_to", "holdfast_queue_leader_epoch"} {
		if v := eventGauge(t, leader.reg, name, f.eventID); v != -1 {
			t.Fatalf("the former leader still exports %s = %v", name, v)
		}
	}
	mustEqual(t, "former leader's leader gauge", gauge(t, leader.m, f.eventID), float64(0))
}

// TestControllerStopsWhenTheEventIsGone: a controller whose event has been
// removed stops, instead of campaigning forever and recreating the event's
// epoch key on every attempt (issue P24); and a controller for an event that
// was never provisioned stops at once, without creating any key.
func TestControllerStopsWhenTheEventIsGone(t *testing.T) {
	f := newFixture(t)
	pool := testenv.Postgres(t)
	f.openQueueWith(t, 1_000, 10_000)
	cfg := AdmissionConfig{Tick: 20 * time.Millisecond, RetryLeadership: 50 * time.Millisecond, Rescan: time.Second}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	k := keysFor(f.eventID)
	run := func(eventID string) chan struct{} {
		done := make(chan struct{})
		c := NewController(f.store, pool, eventID, cfg, NewMetrics(prometheus.NewRegistry()), quiet)
		go func() { defer close(done); c.Run(ctx) }()
		return done
	}
	waitStopped := func(what string, done chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s kept running", what)
		}
	}

	done := run(f.eventID)
	deadline := time.Now().Add(5 * time.Second)
	for f.admittedUpTo(t) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the controller never admitted anyone")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Remove the event, as a test's cleanup (or an operator) would.
	if err := f.rdb.Del(ctx, k.config(), k.state(), k.members(), k.seq(), k.admitted(), k.epoch(), k.sessions(), k.status()).Err(); err != nil {
		t.Fatal(err)
	}
	waitStopped("the controller of a removed event", done)
	if n, _ := f.rdb.Exists(ctx, k.epoch()).Result(); n != 0 {
		t.Fatal("the stopped controller recreated the epoch key")
	}

	never := uuid.Must(uuid.NewV7()).String()
	waitStopped("a controller for an unknown event", run(never))
	if n, _ := f.rdb.Exists(ctx, keysFor(never).epoch()).Result(); n != 0 {
		t.Fatal("a controller for an unknown event created its epoch key")
	}
}

// eventGauge returns the value of name{event=eventID} in reg, or -1 if the
// series does not exist.
func eventGauge(t *testing.T, reg *prometheus.Registry, name, eventID string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "event" && l.GetValue() == eventID {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return -1
}

func TestOpenerExportsStatusAge(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 5, 10_000)
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	op := NewOpener(f.store, time.Second, m, slog.New(slog.NewTextHandler(io.Discard, nil)))

	op.observeStatusAge(ctx, f.eventID)
	mustEqual(t, "age before any leader wrote the document", eventGauge(t, reg, "holdfast_queue_status_age_seconds", f.eventID), float64(-1))

	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	start := time.Now() // with a monotonic reading, to detect a wall-clock step (E7)
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	op.observeStatusAge(ctx, f.eventID)
	age := eventGauge(t, reg, "holdfast_queue_status_age_seconds", f.eventID)
	if age < 0 || age > 5 {
		t.Fatalf("status age %v s, want about 0.3", age)
	}
	// The age is measured by Valkey's wall clock, which WSL2 steps to resync it
	// with Windows (E7); a run in which the measured age was 0.15 s after a
	// 0.3 s sleep showed it. Judge the lower bound only without a step.
	if step := time.Now().Round(0).Sub(start.Round(0)) - time.Since(start); step > 50*time.Millisecond || step < -50*time.Millisecond {
		t.Logf("wall clock stepped by %s during the test; skipping the lower bound", step)
	} else if age < 0.25 {
		t.Fatalf("status age %v s, want about 0.3", age)
	}
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

// --- admission tokens (claiming a turn) ---

func TestAdmitOnlyWithinAdmittedUpTo(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 0, 10_000)
	users := make([]string, 6)
	for i := range users {
		users[i] = uuid.NewString()
		if err := f.rdb.ZAdd(ctx, keysFor(f.eventID).members(), redis.Z{Score: float64(2 + i), Member: users[i]}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 3); err != nil {
		t.Fatal(err)
	}
	turn, err := f.svc.Admit(ctx, f.eventID, strings.ToUpper(users[2]))
	mustErr(t, "admit rank 3 of 3 admitted", err, nil)
	mustEqual(t, "rank", turn.Rank, int64(3))
	mustEqual(t, "user (canonical)", turn.UserID, users[2])
	if d := time.Until(turn.SessionExpires); d < validConfig().SessionTTL-5*time.Second || d > validConfig().SessionTTL {
		t.Fatalf("session expires in %s, want about the session TTL", d)
	}
	_, err = f.svc.Admit(ctx, f.eventID, users[4])
	var nt *NotYourTurnError
	if !errors.As(err, &nt) || nt.Rank != 5 || nt.AdmittedUpTo != 3 {
		t.Fatalf("rank 5 with 3 admitted: %v", err)
	}
	mustErr(t, "not your turn", err, ErrNotYourTurn)
}

func TestAdmitAfterTheSessionSlotExpired(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 2, 10_000)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 2); err != nil {
		t.Fatal(err)
	}
	first, _ := f.rdb.ZRange(ctx, keysFor(f.eventID).members(), 0, 0).Result()
	if err := f.rdb.ZRem(ctx, keysFor(f.eventID).sessions(), "1").Err(); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.Admit(ctx, f.eventID, first[0])
	mustErr(t, "claim after the slot expired", err, ErrTurnExpired)
}

// TestAdmitAfterTheSlotExpiredBeforeTheSweep: a slot past its expiry is
// expired even while it is still in adm:{E}:sessions (the leader removes it
// on its next tick, or never while no leader runs). Before the fix, the claim
// passed and signing then failed, a 500 instead of TURN_EXPIRED.
func TestAdmitAfterTheSlotExpiredBeforeTheSweep(t *testing.T) {
	f := newFixture(t)
	f.openQueueWith(t, 2, 10_000)
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	if _, err := f.store.Advance(ctx, f.eventID, epoch, 2); err != nil {
		t.Fatal(err)
	}
	members, _ := f.rdb.ZRange(ctx, keysFor(f.eventID).members(), 0, 1).Result()
	past := float64(time.Now().Add(-time.Second).UnixMilli())
	if err := f.rdb.ZAddXX(ctx, keysFor(f.eventID).sessions(), redis.Z{Score: past, Member: "1"}).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.Admit(ctx, f.eventID, members[0])
	mustErr(t, "claim with an expired slot not yet swept", err, ErrTurnExpired)
	turn, err := f.svc.Admit(ctx, f.eventID, members[1])
	mustErr(t, "claim with a live slot", err, nil)
	if !turn.SessionExpires.After(time.Now()) {
		t.Fatalf("live slot expires at %s, in the past", turn.SessionExpires)
	}
}

func TestAdmitRefusedWhenClosedOrUnknown(t *testing.T) {
	f := newFixture(t)
	_, err := f.svc.Admit(ctx, f.eventID, uuid.NewString())
	mustErr(t, "not provisioned", err, ErrEventNotFound)
	f.openQueueWith(t, 1, 10_000)
	_, err = f.svc.Admit(ctx, f.eventID, uuid.NewString())
	mustErr(t, "never joined", err, ErrNotInQueue)
	for _, s := range []State{StateSoldOut, StateClosed} {
		f.setState(t, s)
		_, err = f.svc.Admit(ctx, f.eventID, uuid.NewString())
		mustErr(t, "admit when "+string(s), err, ErrQueueClosed)
	}
}
