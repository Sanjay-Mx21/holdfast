package queue

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AdmissionConfig tunes the admission controllers.
type AdmissionConfig struct {
	// Tick is how often the leader admits (design: 250 ms).
	Tick time.Duration
	// RetryLeadership is how long a standby waits between attempts to
	// become leader (design: 2 s).
	RetryLeadership time.Duration
	// Rescan is how often the manager looks for newly provisioned events.
	Rescan time.Duration
}

// Admission runs one admission controller per provisioned event. Every
// queue-svc replica runs one; per event, exactly one controller across all
// replicas leads at a time, elected with a PostgreSQL advisory lock.
type Admission struct {
	store *Store
	pool  *pgxpool.Pool
	cfg   AdmissionConfig
	m     *Metrics
	log   *slog.Logger
}

// NewAdmission returns the manager of the admission controllers.
func NewAdmission(store *Store, pool *pgxpool.Pool, cfg AdmissionConfig, m *Metrics, log *slog.Logger) *Admission {
	return &Admission{store: store, pool: pool, cfg: cfg, m: m, log: log}
}

// Name implements app.Component.
func (a *Admission) Name() string { return "queue-admission" }

// Run implements app.Component: it starts a controller for every event on
// the work list, and for new ones as they are provisioned.
func (a *Admission) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	running := make(map[string]bool)
	t := time.NewTicker(a.cfg.Rescan)
	defer t.Stop()
	for {
		events, err := a.store.Events(ctx)
		if err != nil && ctx.Err() == nil {
			a.log.Warn("admission: list events failed", "err", err)
		}
		for _, ev := range events {
			if running[ev] {
				continue
			}
			running[ev] = true
			c := NewController(a.store, a.pool, ev, a.cfg, a.m, a.log)
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.Run(ctx)
			}()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Controller is the admission controller of one event. It campaigns for
// leadership and, while leading, admits people at the event's rate within
// the session budget.
type Controller struct {
	store   *Store
	pool    *pgxpool.Pool
	eventID string
	lockKey int64
	cfg     AdmissionConfig
	m       *Metrics
	log     *slog.Logger
}

// NewController returns the controller for one event.
func NewController(store *Store, pool *pgxpool.Pool, eventID string, cfg AdmissionConfig, m *Metrics, log *slog.Logger) *Controller {
	return &Controller{
		store: store, pool: pool, eventID: eventID, lockKey: leaderLockKey(eventID),
		cfg: cfg, m: m, log: log.With("event_id", eventID),
	}
}

// errNotLeader means another controller holds the leadership lock.
var errNotLeader = errors.New("queue: another controller is admission leader")

// Run campaigns for leadership until ctx ends. A lost or refused term is
// followed by another attempt after RetryLeadership.
func (c *Controller) Run(ctx context.Context) {
	for {
		err := c.term(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, errNotLeader) {
			c.log.Warn("admission: leadership term ended", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.cfg.RetryLeadership):
		}
	}
}

// term tries to become leader and, if it does, leads until ctx ends, the
// lock's connection is lost, or a newer leader fences this one off.
func (c *Controller) term(ctx context.Context) error {
	// A dedicated connection: the advisory lock belongs to the session, so
	// it must not go back to the pool while held. If this process dies, the
	// connection drops and PostgreSQL releases the lock for a standby.
	pc, err := c.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	conn := pc.Hijack()
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var leader bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, c.lockKey).Scan(&leader); err != nil {
		return fmt.Errorf("try lock: %w", err)
	}
	if !leader {
		return errNotLeader
	}
	epoch, err := c.store.NewTerm(ctx, c.eventID)
	if err != nil {
		return err
	}
	rate, err := c.store.admissionRate(ctx, c.eventID)
	if err != nil {
		return err
	}
	c.m.terms.Inc()
	c.m.setLeader(c.eventID, true)
	defer c.m.setLeader(c.eventID, false)
	c.log.Info("admission: became leader", "epoch", epoch, "rate_per_second", rate)

	bucket := newAllowance(float64(rate), time.Now())
	t := time.NewTicker(c.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-t.C:
			if err := conn.Ping(ctx); err != nil {
				return fmt.Errorf("lost the lock's connection: %w", err) // step down; a standby takes over
			}
			adv, err := c.store.Advance(ctx, c.eventID, epoch, bucket.available(now))
			switch {
			case errors.Is(err, ErrFenced):
				c.m.tick(tickFenced)
				c.log.Warn("admission: fenced off by a newer leader; stepping down", "epoch", epoch)
				return err
			case err != nil:
				c.m.tick(tickError)
				c.log.Warn("admission: tick failed", "err", err)
				continue
			}
			bucket.take(adv.Admitted)
			if adv.Admitted > 0 {
				c.m.tick(tickAdvanced)
				c.m.admitted.Add(float64(adv.Admitted))
			} else {
				c.m.tick(tickIdle)
			}
		}
	}
}

// leaderLockKey maps an event to its advisory-lock key. FNV-1a over a
// HoldFast-specific prefix keeps it stable across processes and unlikely to
// collide with other users of advisory locks in the same database.
func leaderLockKey(eventID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("holdfast/queue/admission/" + eventID))
	return int64(h.Sum64()) //nolint:gosec // the bit pattern is the key; wrapping is intended
}

// allowance is the leader's rate limiter: a token bucket that refills at the
// event's admission rate and holds at most one second's worth, so a pause
// (a slow tick, an empty queue) never turns into a burst.
type allowance struct {
	rate   float64
	tokens float64
	last   time.Time
}

func newAllowance(rate float64, now time.Time) *allowance {
	return &allowance{rate: rate, last: now}
}

// available refills the bucket up to now and returns the whole tokens in it.
func (a *allowance) available(now time.Time) int {
	if dt := now.Sub(a.last).Seconds(); dt > 0 {
		a.tokens = math.Min(a.rate, a.tokens+dt*a.rate)
		a.last = now
	}
	return int(a.tokens)
}

// take spends n tokens: only the people actually admitted, so a tick capped
// by the session budget keeps its unused allowance (up to one second's worth).
func (a *allowance) take(n int64) { a.tokens = math.Max(0, a.tokens-float64(n)) }

// admissionRate reads the event's admission rate from q:{E}:config.
func (s *Store) admissionRate(ctx context.Context, eventID string) (int, error) {
	raw, err := s.rdb.HGet(ctx, keysFor(eventID).config(), "admission_rate").Result()
	if err != nil {
		return 0, fmt.Errorf("queue: read admission rate: %w", err)
	}
	rate, err := strconv.Atoi(raw)
	if err != nil || rate < 1 {
		return 0, fmt.Errorf("queue: bad admission rate %q", raw)
	}
	return rate, nil
}
