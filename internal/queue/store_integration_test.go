//go:build integration

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
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
	eventID string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	rdb := testenv.Valkey(t)
	store := NewStore(rdb)
	if err := store.LoadScripts(ctx); err != nil {
		t.Fatal(err)
	}
	eventID := uuid.Must(uuid.NewV7()).String()
	k := keysFor(eventID)
	t.Cleanup(func() {
		c := context.Background()
		_ = rdb.Del(c, k.config(), k.state(), k.members(), k.seq(), k.admitted(), k.epoch(), k.sessions(), k.status()).Err()
		_ = rdb.SRem(c, eventsKey, eventID).Err()
	})
	return &fixture{svc: NewService(store), store: store, rdb: rdb, eventID: eventID}
}

func (f *fixture) state(t *testing.T) string {
	t.Helper()
	s, err := f.rdb.Get(ctx, keysFor(f.eventID).state()).Result()
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	return s
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

func TestProvisionStoresConfigAndStartsInPre(t *testing.T) {
	f := newFixture(t)
	cfg := validConfig()
	created, err := f.svc.Provision(ctx, f.eventID, cfg)
	mustErr(t, "provision", err, nil)
	mustEqual(t, "created", created, true)
	mustEqual(t, "state", f.state(t), string(StatePre))

	got, err := f.rdb.HGetAll(ctx, keysFor(f.eventID).config()).Result()
	mustErr(t, "read config", err, nil)
	mustEqual(t, "opens_at_ms", got["opens_at_ms"], strconv.FormatInt(cfg.OpensAt.UnixMilli(), 10))
	mustEqual(t, "admission_rate", got["admission_rate"], "83")
	mustEqual(t, "max_sessions", got["max_sessions"], "10000")
	mustEqual(t, "session_ttl_ms", got["session_ttl_ms"], "600000")
	mustEqual(t, "config fields", len(got), 4)
}

func TestProvisionIsIdempotentAndRefusesChanges(t *testing.T) {
	f := newFixture(t)
	cfg := validConfig()
	if _, err := f.svc.Provision(ctx, f.eventID, cfg); err != nil {
		t.Fatal(err)
	}

	// Identical settings: a safe retry, also via a differently-cased event ID
	// and with sub-millisecond noise that Valkey's millisecond storage drops.
	retry := cfg
	retry.OpensAt = cfg.OpensAt.Add(300 * time.Microsecond)
	created, err := f.svc.Provision(ctx, strings.ToUpper(f.eventID), retry)
	mustErr(t, "identical retry", err, nil)
	mustEqual(t, "created on retry", created, false)

	changes := map[string]func(*EventConfig){
		"opening time": func(c *EventConfig) { c.OpensAt = c.OpensAt.Add(time.Minute) },
		"rate":         func(c *EventConfig) { c.AdmissionRate++ },
		"sessions":     func(c *EventConfig) { c.MaxSessions++ },
		"session TTL":  func(c *EventConfig) { c.SessionTTL += time.Second },
	}
	for name, change := range changes {
		c := cfg
		change(&c)
		_, err := f.svc.Provision(ctx, f.eventID, c)
		mustErr(t, "different "+name, err, ErrProvisionConflict)
	}
	// Refused changes leave the stored settings untouched.
	rate, _ := f.rdb.HGet(ctx, keysFor(f.eventID).config(), "admission_rate").Result()
	mustEqual(t, "admission_rate after conflicts", rate, "83")
}

func TestProvisionNeverMovesStateBackToPre(t *testing.T) {
	f := newFixture(t)
	// A queue that is already OPEN (for example, config lost but state kept):
	// provisioning must not rewind it to PRE.
	if err := f.rdb.Set(ctx, keysFor(f.eventID).state(), string(StateOpen), 0).Err(); err != nil {
		t.Fatal(err)
	}
	created, err := f.svc.Provision(ctx, f.eventID, validConfig())
	mustErr(t, "provision", err, nil)
	mustEqual(t, "created", created, true)
	mustEqual(t, "state", f.state(t), string(StateOpen))
}

func TestConcurrentProvisionCreatesExactlyOnce(t *testing.T) {
	f := newFixture(t)
	const callers = 50
	var createdCount, errCount atomic.Int64
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := f.svc.Provision(ctx, f.eventID, validConfig())
			switch {
			case err != nil:
				errCount.Add(1)
			case created:
				createdCount.Add(1)
			}
		}()
	}
	wg.Wait()
	mustEqual(t, "errors", errCount.Load(), int64(0))
	mustEqual(t, "callers that created", createdCount.Load(), int64(1))
	mustEqual(t, "state", f.state(t), string(StatePre))
}

func TestProvisionRejectsInvalidInputBeforeTouchingValkey(t *testing.T) {
	f := newFixture(t)
	bad := validConfig()
	bad.MaxSessions = 0
	_, err := f.svc.Provision(ctx, f.eventID, bad)
	mustErr(t, "invalid config", err, ErrInvalidRequest)
	_, err = f.svc.Provision(ctx, "not-a-uuid", validConfig())
	mustErr(t, "invalid event ID", err, ErrInvalidRequest)
	n, err := f.rdb.Exists(ctx, keysFor(f.eventID).config(), keysFor(f.eventID).state()).Result()
	mustErr(t, "exists", err, nil)
	mustEqual(t, "keys written", n, int64(0))
}

// --- joining ---

// provision provisions the fixture's queue to open an hour from now, so
// joins land before T0.
func (f *fixture) provision(t *testing.T) { f.provisionAt(t, time.Now().Add(time.Hour)) }

// provisionAt provisions the fixture's queue to open at opensAt by running
// provision.lua directly, without registering the event on the opener's work
// list. A queue-svc running against the same Valkey (make up) would otherwise
// open it behind the test's back: registering and then removing the event
// leaves a gap its opener can hit, which made TestJoinAtT0OpensTheQueueItself
// fail about once in 40 runs. TestOpenerOpensAtT0WithoutJoins registers its
// event through Service.Provision on purpose.
func (f *fixture) provisionAt(t *testing.T, opensAt time.Time) {
	t.Helper()
	cfg := validConfig()
	cfg.OpensAt = opensAt
	k := keysFor(f.eventID)
	code, err := provisionScript.Run(ctx, f.rdb, []string{k.config(), k.state()},
		cfg.OpensAt.UnixMilli(), cfg.AdmissionRate, cfg.MaxSessions, cfg.SessionTTL.Milliseconds()).Int64()
	if err != nil || code != 1 {
		t.Fatalf("provision: code %d, err %v", code, err)
	}
}

func (f *fixture) setState(t *testing.T, s State) {
	t.Helper()
	if err := f.rdb.Set(ctx, keysFor(f.eventID).state(), string(s), 0).Err(); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) score(t *testing.T, user string) float64 {
	t.Helper()
	s, err := f.rdb.ZScore(ctx, keysFor(f.eventID).members(), user).Result()
	if err != nil {
		t.Fatalf("score of %s: %v", user, err)
	}
	return s
}

func TestJoinBeforeT0GetsLotteryPositionOnce(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	user := uuid.NewString()
	res, err := f.svc.Join(ctx, f.eventID, user)
	mustErr(t, "join", err, nil)
	mustEqual(t, "joined", res.Joined, true)
	mustEqual(t, "ordering", res.Ordering, OrderingLottery)
	first := f.score(t, user)
	if first < 0 || first >= 1 {
		t.Fatalf("lottery score %v outside [0, 1)", first)
	}
	// Joining again, even many times, never re-rolls the lottery.
	for range 20 {
		res, err := f.svc.Join(ctx, f.eventID, strings.ToUpper(user))
		mustErr(t, "rejoin", err, nil)
		mustEqual(t, "joined on rejoin", res.Joined, false)
		mustEqual(t, "ordering on rejoin", res.Ordering, OrderingLottery)
	}
	mustEqual(t, "score after rejoins", f.score(t, user), first)
	n, _ := f.rdb.ZCard(ctx, keysFor(f.eventID).members()).Result()
	mustEqual(t, "members", n, int64(1))
}

func TestJoinAfterT0IsFIFOBehindEveryLotteryJoiner(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	early := make([]string, 50)
	for i := range early {
		early[i] = uuid.NewString()
		if _, err := f.svc.Join(ctx, f.eventID, early[i]); err != nil {
			t.Fatal(err)
		}
	}
	f.setState(t, StateOpen)
	late := make([]string, 5)
	for i := range late {
		late[i] = uuid.NewString()
		res, err := f.svc.Join(ctx, f.eventID, late[i])
		mustErr(t, "join after T0", err, nil)
		mustEqual(t, "ordering after T0", res.Ordering, OrderingFIFO)
	}
	for i, u := range late {
		mustEqual(t, "FIFO score", f.score(t, u), float64(2+i)) // 1 + arrival number
	}
	// Every post-T0 joiner ranks behind every pre-T0 joiner, in arrival order.
	ranked, err := f.rdb.ZRange(ctx, keysFor(f.eventID).members(), 0, -1).Result()
	mustErr(t, "range", err, nil)
	mustEqual(t, "total members", len(ranked), 55)
	for i, u := range late {
		mustEqual(t, "rank of late joiner", ranked[50+i], u)
	}
}

// Regression test for the plan's original join.lua (design doc 9.1), which
// gave lottery scores to every state but OPEN: a join while FROZEN (only
// reachable after T0) would have jumped ahead of every post-T0 joiner.
func TestJoinWhileFrozenStaysBehindEarlierJoiners(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	f.setState(t, StateOpen)
	before := uuid.NewString()
	if _, err := f.svc.Join(ctx, f.eventID, before); err != nil {
		t.Fatal(err)
	}
	f.setState(t, StateFrozen)
	during := uuid.NewString()
	res, err := f.svc.Join(ctx, f.eventID, during)
	mustErr(t, "join while frozen", err, nil)
	mustEqual(t, "joined while frozen", res.Joined, true)
	mustEqual(t, "ordering while frozen", res.Ordering, OrderingFIFO)
	if f.score(t, during) <= f.score(t, before) {
		t.Fatalf("frozen-time joiner (score %v) ranks ahead of an earlier joiner (score %v)", f.score(t, during), f.score(t, before))
	}
}

func TestJoinRefusedWhenClosedOrNotProvisioned(t *testing.T) {
	f := newFixture(t)
	_, err := f.svc.Join(ctx, f.eventID, uuid.NewString())
	mustErr(t, "join before provisioning", err, ErrEventNotFound)

	f.provision(t)
	for _, s := range []State{StateSoldOut, StateClosed} {
		f.setState(t, s)
		_, err := f.svc.Join(ctx, f.eventID, uuid.NewString())
		mustErr(t, "join when "+string(s), err, ErrQueueClosed)
	}
	n, _ := f.rdb.ZCard(ctx, keysFor(f.eventID).members()).Result()
	mustEqual(t, "members after refused joins", n, int64(0))
}

func TestJoinRejectsBadIDs(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	_, err := f.svc.Join(ctx, f.eventID, "{not-a-uuid}")
	mustErr(t, "bad user", err, ErrInvalidRequest)
	_, err = f.svc.Join(ctx, "not-a-uuid", uuid.NewString())
	mustErr(t, "bad event", err, ErrInvalidRequest)
}

func TestConcurrentJoinsOneSlotPerUser(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	f.setState(t, StateOpen)
	const users, attemptsEach = 100, 10
	ids := make([]string, users)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	var joined atomic.Int64
	var wg sync.WaitGroup
	for _, u := range ids {
		for range attemptsEach {
			wg.Add(1)
			go func() {
				defer wg.Done()
				res, err := f.svc.Join(ctx, f.eventID, u)
				if err != nil {
					t.Error(err)
					return
				}
				if res.Joined {
					joined.Add(1)
				}
			}()
		}
	}
	wg.Wait()
	mustEqual(t, "joins reported as new", joined.Load(), int64(users))
	n, _ := f.rdb.ZCard(ctx, keysFor(f.eventID).members()).Result()
	mustEqual(t, "members", n, int64(users))
	seq, _ := f.rdb.Get(ctx, keysFor(f.eventID).seq()).Int64()
	mustEqual(t, "arrival counter (one number per new member)", seq, int64(users))
}

// --- the T0 transition ---

func TestJoinAtT0OpensTheQueueItself(t *testing.T) {
	f := newFixture(t)
	f.provisionAt(t, time.Now().Add(-time.Second)) // T0 has passed; nobody has opened the queue
	mustEqual(t, "state before the join", f.state(t), string(StatePre))

	first := uuid.NewString()
	res, err := f.svc.Join(ctx, f.eventID, first)
	mustErr(t, "join", err, nil)
	mustEqual(t, "ordering of a join after T0", res.Ordering, OrderingFIFO)
	mustEqual(t, "join opened the queue", res.openedQueue, true)
	mustEqual(t, "state after the join", f.state(t), string(StateOpen))
	mustEqual(t, "first FIFO score", f.score(t, first), float64(2))

	res, err = f.svc.Join(ctx, f.eventID, uuid.NewString())
	mustErr(t, "second join", err, nil)
	mustEqual(t, "second join opened the queue", res.openedQueue, false)
	mustEqual(t, "second ordering", res.Ordering, OrderingFIFO)
}

func TestJoinBeforeT0LeavesTheQueueInPre(t *testing.T) {
	f := newFixture(t)
	f.provisionAt(t, time.Now().Add(time.Minute))
	res, err := f.svc.Join(ctx, f.eventID, uuid.NewString())
	mustErr(t, "join", err, nil)
	mustEqual(t, "ordering", res.Ordering, OrderingLottery)
	mustEqual(t, "opened", res.openedQueue, false)
	mustEqual(t, "state", f.state(t), string(StatePre))
}

// TestJoinsAcrossT0 joins continuously while T0 passes. Whatever the timing,
// exactly one join opens the queue, the lottery and FIFO counts match what
// Valkey stored, and every FIFO member got exactly one arrival number.
func TestJoinsAcrossT0(t *testing.T) {
	f := newFixture(t)
	start := time.Now() // with a monotonic reading, to detect a wall-clock step (D11)
	f.provisionAt(t, start.Add(150*time.Millisecond).Round(0))
	var lottery, fifo, opened atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(450 * time.Millisecond)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				res, err := f.svc.Join(ctx, f.eventID, uuid.NewString())
				if err != nil {
					t.Error(err)
					return
				}
				if res.Ordering == OrderingLottery {
					lottery.Add(1)
				} else {
					fifo.Add(1)
				}
				if res.openedQueue {
					opened.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if lottery.Load() == 0 || fifo.Load() == 0 {
		if step := time.Now().Round(0).Sub(start.Round(0)) - time.Since(start); step > 50*time.Millisecond || step < -50*time.Millisecond {
			t.Skipf("wall clock stepped by %s around T0, so the joins could not straddle it", step)
		}
		t.Fatalf("joins did not straddle T0: %d lottery, %d FIFO", lottery.Load(), fifo.Load())
	}
	mustEqual(t, "joins that opened the queue", opened.Load(), int64(1))
	mustEqual(t, "state", f.state(t), string(StateOpen))
	seq, _ := f.rdb.Get(ctx, keysFor(f.eventID).seq()).Int64()
	mustEqual(t, "arrival numbers issued", seq, fifo.Load())
	below, _ := f.rdb.ZCount(ctx, keysFor(f.eventID).members(), "-inf", "(1").Result()
	mustEqual(t, "members with lottery scores", below, lottery.Load())
}

func TestOpenFlipsPreToOpenOnceDue(t *testing.T) {
	due, notDue := newFixture(t), newFixture(t)
	due.provisionAt(t, time.Now().Add(-200*time.Millisecond))
	notDue.provisionAt(t, time.Now().Add(time.Hour))

	opened, _, err := notDue.store.Open(ctx, notDue.eventID)
	mustErr(t, "open before T0", err, nil)
	mustEqual(t, "opened before T0", opened, false)
	mustEqual(t, "state before T0", notDue.state(t), string(StatePre))

	opened, late, err := due.store.Open(ctx, due.eventID)
	mustErr(t, "open after T0", err, nil)
	mustEqual(t, "opened after T0", opened, true)
	if late < 200*time.Millisecond || late > 5*time.Second {
		t.Fatalf("reported lateness %s, want about 200ms", late)
	}
	mustEqual(t, "state after T0", due.state(t), string(StateOpen))

	opened, _, err = due.store.Open(ctx, due.eventID)
	mustErr(t, "second open", err, nil)
	mustEqual(t, "second open opened", opened, false)
}

func TestOpenNeverTouchesStatesPastPre(t *testing.T) {
	f := newFixture(t)
	f.provisionAt(t, time.Now().Add(-time.Second))
	for _, s := range []State{StateFrozen, StateSoldOut, StateClosed, StateOpen} {
		f.setState(t, s)
		opened, _, err := f.store.Open(ctx, f.eventID)
		mustErr(t, "open in "+string(s), err, nil)
		mustEqual(t, "opened in "+string(s), opened, false)
		mustEqual(t, "state", f.state(t), string(s))
	}
}

func TestOpenUnprovisioned(t *testing.T) {
	f := newFixture(t)
	_, _, err := f.store.Open(ctx, f.eventID)
	mustErr(t, "open unprovisioned", err, ErrEventNotFound)
}

func TestPurgeRemovesEveryKeyAndTheWorkListEntry(t *testing.T) {
	f := newFixture(t)
	f.provisionAt(t, time.Now().Add(-time.Second))
	if _, err := f.svc.Join(ctx, f.eventID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	epoch, _ := f.store.NewTerm(ctx, f.eventID)
	_, _ = f.store.Advance(ctx, f.eventID, epoch, 1)
	// On the work list only for the moment it takes to purge, so a running
	// queue-svc has no time to start a controller for it.
	if err := f.rdb.SAdd(ctx, eventsKey, f.eventID).Err(); err != nil {
		t.Fatal(err)
	}
	mustErr(t, "purge", f.store.Purge(ctx, f.eventID), nil)
	k := keysFor(f.eventID)
	n, err := f.rdb.Exists(ctx, k.config(), k.state(), k.members(), k.seq(), k.admitted(), k.status(), k.epoch(), k.sessions()).Result()
	mustErr(t, "exists", err, nil)
	mustEqual(t, "keys left after purge", n, int64(0))
	listed, _ := f.rdb.SIsMember(ctx, eventsKey, f.eventID).Result()
	mustEqual(t, "still on the work list", listed, false)
	mustErr(t, "purging again", f.store.Purge(ctx, f.eventID), nil)
}

func TestProvisionRegistersEventForTheOpener(t *testing.T) {
	f := newFixture(t)
	if _, err := f.svc.Provision(ctx, f.eventID, validConfig()); err != nil {
		t.Fatal(err)
	}
	ok, err := f.rdb.SIsMember(ctx, eventsKey, f.eventID).Result()
	mustErr(t, "sismember", err, nil)
	mustEqual(t, "event on the opener's work list", ok, true)
}

// TestOpenerOpensAtT0WithoutJoins runs a real opener. Another queue-svc on
// the same Valkey may get there first, so the test checks when the queue
// opened, not which opener did it.
//
// T0 is a wall-clock time and Valkey judges it by its wall clock, which WSL2
// steps by about half a second when it resyncs with Windows. So "too early"
// is judged by Valkey's own clock, and the promptness check is skipped (with
// a log line) for a run in which the wall clock stepped (issue D11).
func TestOpenerOpensAtT0WithoutJoins(t *testing.T) {
	f := newFixture(t)
	start := time.Now() // with a monotonic reading
	opensAt := start.Add(300 * time.Millisecond).Round(0)
	cfg := validConfig()
	cfg.OpensAt = opensAt
	if _, err := f.svc.Provision(ctx, f.eventID, cfg); err != nil {
		t.Fatal(err)
	}
	m := NewMetrics(prometheus.NewRegistry())
	op := NewOpener(f.store, 25*time.Millisecond, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- op.Run(runCtx) }()
	defer func() { cancel(); <-done }()

	valkeyNow := func() time.Time {
		t.Helper()
		vt, err := f.rdb.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		return vt
	}
	// Never open before T0 by Valkey's clock: read the state, then the clock.
	for {
		s := f.state(t)
		now := valkeyNow()
		if !now.Before(opensAt) {
			break
		}
		if s != string(StatePre) {
			t.Fatalf("queue %s while Valkey's clock is %s before T0", s, opensAt.Sub(now))
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.state(t) == string(StatePre) {
		if time.Now().After(deadline) {
			t.Fatal("queue still PRE well after T0")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mustEqual(t, "state", f.state(t), string(StateOpen))

	// Promptness, unless the wall clock stepped during the test.
	step := time.Now().Round(0).Sub(start.Round(0)) - time.Since(start)
	if step > 50*time.Millisecond || step < -50*time.Millisecond {
		t.Logf("wall clock stepped by %s during the test; skipping the promptness check", step)
		return
	}
	if late := time.Now().Round(0).Sub(opensAt); late > 500*time.Millisecond {
		t.Fatalf("queue opened %s after T0, want within a few opener intervals", late)
	}
}

// --- position ---

func TestPositionBeforeT0IsRandomizing(t *testing.T) {
	f := newFixture(t)
	opensAt := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	f.provisionAt(t, opensAt)
	user := uuid.NewString()
	if _, err := f.svc.Join(ctx, f.eventID, user); err != nil {
		t.Fatal(err)
	}
	pos, err := f.svc.Position(ctx, f.eventID, strings.ToUpper(user))
	mustErr(t, "position", err, nil)
	mustEqual(t, "rank before T0", pos.Rank, int64(0))
	mustEqual(t, "state", pos.State, StatePre)
	if !pos.RandomizingAt.Equal(opensAt) {
		t.Fatalf("randomizingAt %s, want %s", pos.RandomizingAt, opensAt)
	}
}

func TestPositionAfterT0IsRankInLine(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	lottery := make([]string, 30)
	for i := range lottery {
		lottery[i] = uuid.NewString()
		if _, err := f.svc.Join(ctx, f.eventID, lottery[i]); err != nil {
			t.Fatal(err)
		}
	}
	f.setState(t, StateOpen)
	late := uuid.NewString()
	if _, err := f.svc.Join(ctx, f.eventID, late); err != nil {
		t.Fatal(err)
	}
	// Every rank 1..30 goes to exactly one lottery joiner, in score order.
	seen := make(map[int64]bool)
	for _, u := range lottery {
		pos, err := f.svc.Position(ctx, f.eventID, u)
		mustErr(t, "position", err, nil)
		mustEqual(t, "state", pos.State, StateOpen)
		if pos.Rank < 1 || pos.Rank > 30 || seen[pos.Rank] {
			t.Fatalf("lottery joiner got rank %d (duplicate or out of 1..30)", pos.Rank)
		}
		seen[pos.Rank] = true
		if !pos.RandomizingAt.IsZero() {
			t.Fatal("randomizingAt set after T0")
		}
	}
	pos, err := f.svc.Position(ctx, f.eventID, late)
	mustErr(t, "late position", err, nil)
	mustEqual(t, "rank of the first post-T0 joiner", pos.Rank, int64(31))
}

// A queue still marked PRE after T0 (no join or opener has flipped it yet)
// already has final lottery ranks: T0 is the clock's call.
func TestPositionPreButPastT0ReadsAsOpen(t *testing.T) {
	f := newFixture(t)
	f.provision(t) // T0 an hour away: the join below is certainly before it
	user := uuid.NewString()
	if _, err := f.svc.Join(ctx, f.eventID, user); err != nil {
		t.Fatal(err)
	}
	// Now move T0 into the past without anyone flipping the state. (Waiting
	// for a T0 150 ms away was flaky: a WSL2 clock step during the join made
	// the join itself open the queue; E7.)
	past := strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
	if err := f.rdb.HSet(ctx, keysFor(f.eventID).config(), "opens_at_ms", past).Err(); err != nil {
		t.Fatal(err)
	}
	mustEqual(t, "stored state", f.state(t), string(StatePre))
	pos, err := f.svc.Position(ctx, f.eventID, user)
	mustErr(t, "position", err, nil)
	mustEqual(t, "reported state", pos.State, StateOpen)
	mustEqual(t, "rank", pos.Rank, int64(1))
	mustEqual(t, "stored state untouched (lookups are read-only)", f.state(t), string(StatePre))
}

func TestPositionKeepsRankInLaterStates(t *testing.T) {
	f := newFixture(t)
	f.provision(t)
	user := uuid.NewString()
	if _, err := f.svc.Join(ctx, f.eventID, user); err != nil {
		t.Fatal(err)
	}
	for _, s := range []State{StateFrozen, StateSoldOut, StateClosed} {
		f.setState(t, s)
		pos, err := f.svc.Position(ctx, f.eventID, user)
		mustErr(t, "position in "+string(s), err, nil)
		mustEqual(t, "state", pos.State, s)
		mustEqual(t, "rank", pos.Rank, int64(1))
	}
}

func TestPositionNotInQueueOrNotProvisioned(t *testing.T) {
	f := newFixture(t)
	_, err := f.svc.Position(ctx, f.eventID, uuid.NewString())
	mustErr(t, "unprovisioned", err, ErrEventNotFound)
	f.provision(t)
	_, err = f.svc.Position(ctx, f.eventID, uuid.NewString())
	mustErr(t, "never joined", err, ErrNotInQueue)
	_, err = f.svc.Position(ctx, f.eventID, "nope")
	mustErr(t, "bad user", err, ErrInvalidRequest)
}
