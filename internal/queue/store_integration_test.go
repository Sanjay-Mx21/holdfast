//go:build integration

package queue

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

type fixture struct {
	svc     *Service
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
	t.Cleanup(func() { _ = rdb.Del(context.Background(), k.config(), k.state()).Err() })
	return &fixture{svc: NewService(store), rdb: rdb, eventID: eventID}
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

func (f *fixture) provision(t *testing.T) {
	t.Helper()
	if _, err := f.svc.Provision(ctx, f.eventID, validConfig()); err != nil {
		t.Fatal(err)
	}
	k := keysFor(f.eventID)
	t.Cleanup(func() { _ = f.rdb.Del(context.Background(), k.members(), k.seq()).Err() })
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
