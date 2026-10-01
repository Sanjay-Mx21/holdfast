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
