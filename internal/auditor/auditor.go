// Package auditor continuously checks HoldFast's invariants I1 to I5 from
// the stores themselves and publishes the number of violations of each as
// holdfast_invariant_violations{invariant}. Every value must stay 0; an alert
// pages when one does not (deploy/alerting).
//
// It is the one component allowed to read across service schemas (design
// doc 8.1): a read-only observer, never a participant. Its PostgreSQL
// connections are read-only (postgres.NewReadOnlyPool), and it only reads
// inventory's Valkey keys.
//
// The checks do not re-read what a database constraint already forces
// (booking.event_inventory's sold <= capacity, for example); each one
// recomputes the invariant from independent records, so a bug in the code
// that maintains a counter shows up here.
package auditor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// Invariant IDs, the values of the invariant label (design doc 2.3).
const (
	I1 = "I1" // no oversell
	I2 = "I2" // no double charge
	I3 = "I3" // money safety
	I4 = "I4" // per-user cap
	I5 = "I5" // no lost units
)

// Invariants lists them in order.
var Invariants = []string{I1, I2, I3, I4, I5}

// Config tunes the auditor.
type Config struct {
	// Interval between checks.
	Interval time.Duration
	// MoneyDeadline is how long a captured payment may stay unresolved
	// (I3: CONFIRMED or REFUNDED within 15 minutes).
	MoneyDeadline time.Duration
	// HoldGrace is how long past its expiry a hold may stay unreleased before
	// it counts as lost (I5): the sweeper releases HELD holds within a
	// second, and booking-svc's deadline job settles PAYING ones.
	HoldGrace time.Duration
}

// Auditor runs the checks.
type Auditor struct {
	db  *pgxpool.Pool
	rdb redis.UniversalClient
	cfg Config
	m   *Metrics
	log *slog.Logger
}

// New returns an auditor reading db (read-only) and inventory's Valkey.
func New(db *pgxpool.Pool, rdb redis.UniversalClient, cfg Config, m *Metrics, log *slog.Logger) *Auditor {
	return &Auditor{db: db, rdb: rdb, cfg: cfg, m: m, log: log}
}

// Name implements app.Component.
func (a *Auditor) Name() string { return "invariant-auditor" }

// Run implements app.Component: a check at start and every Interval.
func (a *Auditor) Run(ctx context.Context) error {
	t := time.NewTicker(a.cfg.Interval)
	defer t.Stop()
	for {
		if _, err := a.Check(ctx); err != nil && ctx.Err() == nil {
			a.log.Warn("auditor: check failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Report is one check's result: violations per invariant.
type Report map[string]int64

// Check evaluates every invariant once and publishes the results. An
// invariant whose check fails keeps its last published value, and the error
// is returned; the others are still published.
func (a *Auditor) Check(ctx context.Context) (Report, error) {
	checks := map[string]func(context.Context) (int64, error){
		I1: a.oversold, I2: a.doubleCharged, I3: a.moneyUnresolved, I4: a.overCap, I5: a.lostHolds,
	}
	report := Report{}
	var errs []error
	for _, inv := range Invariants {
		n, err := checks[inv](ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", inv, err))
			continue
		}
		report[inv] = n
		a.m.violations.WithLabelValues(inv).Set(float64(n))
		if n > 0 {
			a.log.ErrorContext(ctx, "auditor: invariant violated", "invariant", inv, "violations", n)
		}
	}
	if len(errs) > 0 {
		a.m.runs.WithLabelValues("error").Inc()
		return report, errors.Join(errs...)
	}
	a.m.runs.WithLabelValues("ok").Inc()
	a.m.lastSuccess.SetToCurrentTime()
	return report, nil
}

func (a *Auditor) count(ctx context.Context, sql string, args ...any) (int64, error) {
	var n int64
	err := a.db.QueryRow(ctx, sql, args...).Scan(&n)
	return n, err
}

// oversold (I1): events whose confirmed bookings hold more units than the
// event's capacity. It sums the bookings themselves rather than trusting the
// guard's sold counter, and also counts a counter that has drifted above
// capacity.
func (a *Auditor) oversold(ctx context.Context) (int64, error) {
	return a.count(ctx, `
		SELECT count(*) FROM booking.event_inventory i
		LEFT JOIN (
		  SELECT event_id, sum(qty) AS confirmed FROM booking.bookings
		  WHERE status = 'CONFIRMED' GROUP BY event_id
		) b ON b.event_id = i.event_id
		WHERE coalesce(b.confirmed, 0) > i.capacity OR i.sold > i.capacity`)
}

// doubleCharged (I2): payment intents whose capture is booked in the ledger
// more than once (each capture debits psp_receivable). The schema already
// allows one intent per booking.
func (a *Auditor) doubleCharged(ctx context.Context) (int64, error) {
	return a.count(ctx, `
		SELECT count(*) FROM (
		  SELECT intent_id FROM payment.ledger_entries
		  WHERE account = 'psp_receivable' AND direction = 'D'
		  GROUP BY intent_id HAVING count(*) > 1
		) d`)
}

// moneyUnresolved (I3): money captured more than MoneyDeadline ago that has
// not ended as a CONFIRMED booking or a REFUNDED intent: a refund still
// pending, or a captured payment whose booking is anything but confirmed
// (or missing).
func (a *Auditor) moneyUnresolved(ctx context.Context) (int64, error) {
	return a.count(ctx, `
		SELECT count(*) FROM payment.payment_intents i
		JOIN LATERAL (
		  SELECT min(l.created_at) AS captured_at FROM payment.ledger_entries l
		  WHERE l.intent_id = i.id AND l.account = 'psp_receivable' AND l.direction = 'D'
		) c ON c.captured_at IS NOT NULL
		LEFT JOIN booking.bookings b ON b.id = i.booking_id
		WHERE i.status IN ('CAPTURED', 'REFUND_PENDING')
		  AND c.captured_at < now() - make_interval(secs => $1)
		  AND (i.status = 'REFUND_PENDING' OR b.status IS DISTINCT FROM 'CONFIRMED')`,
		a.cfg.MoneyDeadline.Seconds())
}

// overCap (I4): (event, user) pairs whose units held for payment or bought
// exceed the event's per-user limit, from the bookings themselves and from
// the guard's per-user counter.
func (a *Auditor) overCap(ctx context.Context) (int64, error) {
	return a.count(ctx, `
		SELECT count(*) FROM (
		  SELECT b.event_id, b.user_id FROM booking.bookings b
		  JOIN booking.events e ON e.id = b.event_id
		  WHERE b.status IN ('PENDING_PAYMENT', 'CONFIRMED')
		  GROUP BY b.event_id, b.user_id, e.per_user_limit
		  HAVING sum(b.qty) > e.per_user_limit
		  UNION
		  SELECT p.event_id, p.user_id FROM booking.user_event_purchases p
		  JOIN booking.events e ON e.id = p.event_id
		  WHERE p.qty > e.per_user_limit
		) v`)
}

// lostHolds (I5): holds still in inventory's expiry index more than
// HoldGrace past their expiry, on every provisioned event: units that are
// neither sold nor released. Valkey's clock decides "past".
func (a *Auditor) lostHolds(ctx context.Context) (int64, error) {
	events, err := a.rdb.SMembers(ctx, "inv:events").Result()
	if err != nil {
		return 0, err
	}
	now, err := a.rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	cutoff := strconv.FormatInt(now.Add(-a.cfg.HoldGrace).UnixMilli(), 10)
	pipe := a.rdb.Pipeline()
	counts := make([]*redis.IntCmd, len(events))
	for i, e := range events {
		counts[i] = pipe.ZCount(ctx, "inv:{"+e+"}:expiry", "-inf", "("+cutoff)
	}
	if len(events) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}
	var n int64
	for _, c := range counts {
		n += c.Val()
	}
	return n, nil
}
