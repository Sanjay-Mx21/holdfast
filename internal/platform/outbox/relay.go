// Package outbox relays a service's transactional outbox to Kafka (design doc
// 9.6). A service writes each event into <schema>.outbox in the same
// transaction as the change it describes; the relay publishes unpublished
// rows in commit order and marks them published.
//
// One relay leads per schema (a PostgreSQL advisory lock, as for queue-svc's
// admission controllers), because two relays could publish one booking's
// events out of order. A crash between publishing and marking republishes
// the batch: delivery is at least once, and consumers drop duplicates by the
// event's ce_id.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

// Publisher sends events to Kafka (*kafka.Producer).
type Publisher interface {
	Publish(ctx context.Context, events ...kafka.Event) error
}

// Config tunes a relay.
type Config struct {
	// Schema owns the outbox table (<schema>.outbox).
	Schema string
	// Source is the ce_source of every event, for example "booking-svc".
	Source string
	// Batch is the most rows published per transaction (default 500).
	Batch int
	// Interval is the pause after a pass that found less than a full batch
	// (default 200 ms).
	Interval time.Duration
	// RetryLeadership is how long a standby waits between attempts to lead
	// (default 2 s).
	RetryLeadership time.Duration
	// Retain is how long published rows are kept (default 7 days).
	Retain time.Duration
}

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

func (c *Config) defaults() error {
	if !identifier.MatchString(c.Schema) || c.Source == "" {
		return fmt.Errorf("outbox: invalid schema %q or empty source", c.Schema)
	}
	if c.Batch < 1 {
		c.Batch = 500
	}
	if c.Interval <= 0 {
		c.Interval = 200 * time.Millisecond
	}
	if c.RetryLeadership <= 0 {
		c.RetryLeadership = 2 * time.Second
	}
	if c.Retain <= 0 {
		c.Retain = 7 * 24 * time.Hour
	}
	return nil
}

// Relay publishes one schema's outbox.
type Relay struct {
	pool    *pgxpool.Pool
	pub     Publisher
	cfg     Config
	m       *Metrics
	log     *slog.Logger
	lockKey int64
	claim   string
	mark    string
	pending string
	purge   string
}

// NewRelay returns the relay of cfg.Schema's outbox.
func NewRelay(pool *pgxpool.Pool, pub Publisher, cfg Config, m *Metrics, log *slog.Logger) (*Relay, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte("holdfast/outbox/" + cfg.Schema))
	table := cfg.Schema + ".outbox" // the schema name is validated above
	return &Relay{
		pool: pool, pub: pub, cfg: cfg, m: m, log: log.With("schema", cfg.Schema),
		lockKey: int64(h.Sum64()), //nolint:gosec // the bit pattern is the key
		claim: `SELECT id, event_id, topic, aggregate_id, event_type, payload, headers, created_at
		        FROM ` + table + ` WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`,
		mark:    `UPDATE ` + table + ` SET published_at = now() WHERE id = ANY($1)`,
		pending: `SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0) FROM ` + table + ` WHERE published_at IS NULL`,
		purge:   `DELETE FROM ` + table + ` WHERE published_at < $1`,
	}, nil
}

// Name implements app.Component.
func (r *Relay) Name() string { return "outbox-relay-" + r.cfg.Schema }

var errNotLeader = errors.New("outbox: another relay leads")

// Run campaigns for leadership until ctx ends, and relays while leading.
func (r *Relay) Run(ctx context.Context) error {
	for {
		err := r.term(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !errors.Is(err, errNotLeader) {
			r.log.Warn("outbox relay: term ended", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.RetryLeadership):
		}
	}
}

// term leads until ctx ends or the lock's connection is lost.
func (r *Relay) term(ctx context.Context) error {
	pc, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	conn := pc.Hijack() // the advisory lock belongs to this session: keep it out of the pool
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var leader bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, r.lockKey).Scan(&leader); err != nil {
		return fmt.Errorf("try lock: %w", err)
	}
	if !leader {
		return errNotLeader
	}
	r.m.leader.WithLabelValues(r.cfg.Schema).Set(1)
	defer r.m.leader.WithLabelValues(r.cfg.Schema).Set(0)
	r.log.Info("outbox relay: leading")

	var lastPurge time.Time
	for {
		if err := conn.Ping(ctx); err != nil {
			return fmt.Errorf("lost the lock's connection: %w", err)
		}
		n, err := r.Pass(ctx)
		if err != nil && ctx.Err() == nil {
			r.m.passes.WithLabelValues(r.cfg.Schema, "error").Inc()
			r.log.Warn("outbox relay: pass failed; retrying", "err", err)
		} else {
			r.m.passes.WithLabelValues(r.cfg.Schema, "ok").Inc()
		}
		r.observe(ctx)
		if time.Since(lastPurge) >= time.Hour {
			r.purgeOld(ctx)
			lastPurge = time.Now()
		}
		if n == r.cfg.Batch && err == nil {
			continue // a full batch: there may be more right away
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.cfg.Interval):
		}
	}
}

// Pass publishes one batch in commit order and marks it published, in one
// transaction. It returns how many rows it published.
func (r *Relay) Pass(ctx context.Context) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, r.claim, r.cfg.Batch)
		if err != nil {
			return err
		}
		var ids []int64
		var events []kafka.Event
		for rows.Next() {
			var (
				id                 int64
				eventID, aggregate uuid.UUID
				topic, eventType   string
				payload, headers   []byte
				created            time.Time
			)
			if err := rows.Scan(&id, &eventID, &topic, &aggregate, &eventType, &payload, &headers, &created); err != nil {
				rows.Close()
				return err
			}
			var h map[string]string
			if err := json.Unmarshal(headers, &h); err != nil {
				h = nil // a malformed header blob only loses the trace context
			}
			ids = append(ids, id)
			events = append(events, kafka.Event{
				Topic: topic, Key: aggregate.String(), Value: payload, ID: eventID.String(),
				Type: eventType, Source: r.cfg.Source, Time: created, Headers: h,
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		if err := r.pub.Publish(ctx, events...); err != nil {
			return err // nothing is marked: the batch is published again later
		}
		if _, err := tx.Exec(ctx, r.mark, ids); err != nil {
			return err
		}
		n = len(events)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("outbox: relay %s: %w", r.cfg.Schema, err)
	}
	r.m.published.WithLabelValues(r.cfg.Schema).Add(float64(n))
	return n, nil
}

// observe exports how far behind the relay is.
func (r *Relay) observe(ctx context.Context) {
	var pending int64
	var age float64
	if err := r.pool.QueryRow(ctx, r.pending).Scan(&pending, &age); err != nil {
		return
	}
	r.m.pending.WithLabelValues(r.cfg.Schema).Set(float64(pending))
	r.m.lag.WithLabelValues(r.cfg.Schema).Set(age)
}

func (r *Relay) purgeOld(ctx context.Context) {
	tag, err := r.pool.Exec(ctx, r.purge, time.Now().Add(-r.cfg.Retain))
	if err != nil {
		r.log.Warn("outbox relay: purge failed", "err", err)
		return
	}
	if tag.RowsAffected() > 0 {
		r.log.Info("outbox relay: purged published rows", "count", tag.RowsAffected())
	}
}

// Metrics describe the relays of a process.
type Metrics struct {
	published *prometheus.CounterVec
	passes    *prometheus.CounterVec
	pending   *prometheus.GaugeVec
	lag       *prometheus.GaugeVec
	leader    *prometheus.GaugeVec
}

// NewMetrics registers the relay metrics; the schema label is bounded.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		published: f.NewCounterVec(prometheus.CounterOpts{Name: "holdfast_outbox_published_total", Help: "Outbox events published to Kafka."}, []string{"schema"}),
		passes:    f.NewCounterVec(prometheus.CounterOpts{Name: "holdfast_outbox_relay_passes_total", Help: "Relay passes by result: ok, error."}, []string{"schema", "result"}),
		pending:   f.NewGaugeVec(prometheus.GaugeOpts{Name: "holdfast_outbox_pending", Help: "Outbox events not yet published (as seen by the leading relay)."}, []string{"schema"}),
		lag:       f.NewGaugeVec(prometheus.GaugeOpts{Name: "holdfast_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event; 0 when none."}, []string{"schema"}),
		leader:    f.NewGaugeVec(prometheus.GaugeOpts{Name: "holdfast_outbox_relay_leader", Help: "1 while this process leads the schema's relay."}, []string{"schema"}),
	}
}
