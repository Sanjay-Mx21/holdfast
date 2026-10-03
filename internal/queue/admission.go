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

	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
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
	// Inventory, if set, is read on every tick (P17): the leader keeps open
	// sessions within the units left times Oversubscription, and marks the
	// queue SOLD_OUT once no units are left and no open hold can return
	// any. Without it, neither happens; inventory still refuses every hold
	// beyond capacity.
	Inventory Inventory
	// Oversubscription is the factor (design: 1.3 to start): how many
	// sessions to open per unit left, since not everyone admitted buys.
	Oversubscription float64
}

// Inventory is what the admission leader needs from inventory-svc
// (*inventory.Client, over gRPC).
type Inventory interface {
	GetAvailability(ctx context.Context, eventID string) (inventory.Availability, error)
}

// Admission runs one admission controller per provisioned event. Every
// queue-svc replica runs one; per event, exactly one controller across all
// replicas leads at a time, elected with a PostgreSQL advisory lock. A
// replica's controllers hold their locks on one shared connection
// (LockSession).
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
// the work list, and for new ones as they are provisioned. A controller stops
// when its event no longer exists; it is started again if the event is
// provisioned again.
func (a *Admission) Run(ctx context.Context) error {
	locks := NewLockSession(a.pool, a.cfg.Tick)
	defer locks.Close() // after every controller has stopped
	var wg sync.WaitGroup
	defer wg.Wait()
	var mu sync.Mutex
	running := make(map[string]bool)
	t := time.NewTicker(a.cfg.Rescan)
	defer t.Stop()
	for {
		events, err := a.store.Events(ctx)
		if err != nil && ctx.Err() == nil {
			a.log.Warn("admission: list events failed", "err", err)
		}
		mu.Lock()
		for _, ev := range events {
			if running[ev] {
				continue
			}
			running[ev] = true
			c := NewController(a.store, locks, ev, a.cfg, a.m, a.log)
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.Run(ctx)
				mu.Lock()
				delete(running, ev)
				mu.Unlock()
			}()
		}
		mu.Unlock()
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
	locks   *LockSession
	eventID string
	lockKey int64
	cfg     AdmissionConfig
	m       *Metrics
	log     *slog.Logger

	// invFailing is true while inventory cannot be read, so the failure is
	// logged once rather than on every tick. Only the leading goroutine
	// touches it.
	invFailing bool
}

// NewController returns the controller for one event. Its leadership lock is
// taken on locks, which the replica's other controllers share.
func NewController(store *Store, locks *LockSession, eventID string, cfg AdmissionConfig, m *Metrics, log *slog.Logger) *Controller {
	return &Controller{
		store: store, locks: locks, eventID: eventID, lockKey: leaderLockKey(eventID),
		cfg: cfg, m: m, log: log.With("event_id", eventID),
	}
}

// errNotLeader means another controller holds the leadership lock.
var errNotLeader = errors.New("queue: another controller is admission leader")

// Run campaigns for leadership until ctx ends or the event no longer exists.
// A lost or refused term is followed by another attempt after
// RetryLeadership.
func (c *Controller) Run(ctx context.Context) {
	for {
		err := c.term(ctx)
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, ErrEventNotFound) {
			c.log.Info("admission: event no longer provisioned; controller stopped")
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
// lock's session is lost, or a newer leader fences this one off. The lock is
// released when the term ends; if this process dies, its session drops and
// PostgreSQL releases every lock it held for the standbys.
func (c *Controller) term(ctx context.Context) error {
	gen, leader, err := c.locks.TryLock(ctx, c.lockKey)
	if err != nil {
		return err
	}
	if !leader {
		return errNotLeader
	}
	defer c.locks.Unlock(ctx, c.lockKey, gen)
	// Settings first: a term for an event that no longer exists must not
	// recreate its epoch key.
	rate, maxSessions, err := c.store.termConfig(ctx, c.eventID)
	if err != nil {
		return err
	}
	epoch, err := c.store.NewTerm(ctx, c.eventID)
	if err != nil {
		return err
	}
	c.m.terms.Inc()
	c.m.leaderTerm(c.eventID, epoch, maxSessions, true)
	defer c.m.leaderTerm(c.eventID, epoch, maxSessions, false)
	c.log.Info("admission: became leader", "epoch", epoch, "rate_per_second", rate, "max_sessions", maxSessions)
	return c.lead(ctx, epoch, rate, func(ctx context.Context) error { return c.locks.Alive(ctx, gen) }, c.store)
}

// leaderOps are the leader's fenced writes (*Store).
type leaderOps interface {
	AdvanceWithin(ctx context.Context, eventID string, epoch int64, n, unitsCap int) (Advance, error)
	MarkSoldOut(ctx context.Context, eventID string, epoch int64) (bool, error)
}

// lead is the leader's tick loop: every Tick it checks that the lock is
// still held (ping), reads the units left, then admits what the rate, the
// session budget and the units left allow. It returns nil when ctx ends, and
// an error when the lock's session is lost, a newer leader has fenced this
// one off, or the event no longer exists.
func (c *Controller) lead(ctx context.Context, epoch int64, rate int, ping func(context.Context) error, ops leaderOps) error {
	bucket := newAllowance(float64(rate), time.Now())
	t := time.NewTicker(c.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-t.C:
			if err := ping(ctx); err != nil {
				return fmt.Errorf("lost the lock: %w", err) // step down; a standby takes over
			}
			unitsCap, soldOut := c.unitsLeft(ctx)
			if soldOut {
				marked, err := ops.MarkSoldOut(ctx, c.eventID, epoch)
				switch {
				case errors.Is(err, ErrFenced):
					c.m.tick(tickFenced)
					c.log.Warn("admission: fenced off by a newer leader; stepping down", "epoch", epoch)
					return err
				case err != nil:
					c.log.Warn("admission: marking the queue sold out failed", "err", err)
				case marked:
					c.log.Info("admission: inventory is sold out; queue marked SOLD_OUT")
				}
			}
			adv, err := ops.AdvanceWithin(ctx, c.eventID, epoch, bucket.available(now), unitsCap)
			switch {
			case errors.Is(err, ErrFenced):
				c.m.tick(tickFenced)
				c.log.Warn("admission: fenced off by a newer leader; stepping down", "epoch", epoch)
				return err
			case errors.Is(err, ErrEventNotFound):
				c.m.tick(tickError)
				return err
			case err != nil:
				c.m.tick(tickError)
				c.log.Warn("admission: tick failed", "err", err)
				continue
			}
			bucket.take(adv.Admitted)
			c.m.leaderTick(c.eventID, adv)
			if adv.Admitted > 0 {
				c.m.tick(tickAdvanced)
				c.m.admitted.WithLabelValues(c.eventID).Add(float64(adv.Admitted))
			} else {
				c.m.tick(tickIdle)
			}
		}
	}
}

// unitsLeft asks inventory how many open sessions the units left justify:
// the units left times the oversubscription factor (P17), or -1 for no cap.
// It also reports a sold-out sale: no units left and no open hold that could
// return any. If inventory cannot be read in time, the cap is lifted rather
// than admissions stopped: inventory still refuses every hold beyond
// capacity, so failing open costs buyers a SOLD_OUT answer, never a unit.
func (c *Controller) unitsLeft(ctx context.Context) (unitsCap int, soldOut bool) {
	if c.cfg.Inventory == nil {
		return -1, false
	}
	readCtx, cancel := context.WithTimeout(ctx, c.cfg.Tick)
	defer cancel()
	a, err := c.cfg.Inventory.GetAvailability(readCtx, c.eventID)
	if errors.Is(err, inventory.ErrEventNotProvisioned) {
		// A waiting room without a sale behind it (a load test's, or one
		// provisioned before its inventory): nothing to cap by.
		c.m.inventoryRead(inventoryReadNotProvisioned)
		return -1, false
	}
	if err != nil {
		c.m.inventoryRead(inventoryReadError)
		if !c.invFailing && ctx.Err() == nil {
			c.log.Warn("admission: cannot read inventory; admitting without the units cap until it can", "err", err)
		}
		c.invFailing = true
		return -1, false
	}
	c.m.inventoryRead(inventoryReadOK)
	if c.invFailing {
		c.log.Info("admission: inventory readable again; units cap restored")
		c.invFailing = false
	}
	if a.Available <= 0 {
		return 0, a.ActiveHolds == 0
	}
	return int(math.Floor(float64(a.Available) * c.cfg.Oversubscription)), false
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

// termConfig reads the event's admission rate and session budget from
// q:{E}:config; they are fixed once provisioned. ErrEventNotFound means the
// event is not (or no longer) provisioned.
func (s *Store) termConfig(ctx context.Context, eventID string) (rate, maxSessions int, err error) {
	vals, err := s.rdb.HMGet(ctx, keysFor(eventID).config(), "admission_rate", "max_sessions").Result()
	if err != nil {
		return 0, 0, fmt.Errorf("queue: read admission settings: %w", err)
	}
	if vals[0] == nil && vals[1] == nil {
		return 0, 0, ErrEventNotFound
	}
	parse := func(v any) int {
		s, _ := v.(string)
		n, _ := strconv.Atoi(s)
		return n
	}
	rate, maxSessions = parse(vals[0]), parse(vals[1])
	if rate < 1 || maxSessions < 1 {
		return 0, 0, fmt.Errorf("queue: bad admission settings %v", vals)
	}
	return rate, maxSessions, nil
}
