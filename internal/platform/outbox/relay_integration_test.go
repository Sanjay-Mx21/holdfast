//go:build integration

package outbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// testSchema creates a schema with an outbox shaped like booking.outbox, so
// the tests never touch a running service's outbox.
func testSchema(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	schema := "outbox_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA %[1]s;
		CREATE TABLE %[1]s.outbox (
		  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		  event_id uuid NOT NULL UNIQUE, topic text NOT NULL, aggregate_id uuid NOT NULL,
		  event_type text NOT NULL, payload bytea NOT NULL, headers jsonb NOT NULL DEFAULT '{}',
		  created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);`, schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	return schema
}

func insert(t *testing.T, pool *pgxpool.Pool, schema, topic string, aggregate uuid.UUID, headers string) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if _, err := pool.Exec(ctx, `INSERT INTO `+schema+`.outbox (event_id, topic, aggregate_id, event_type, payload, headers)
		VALUES ($1, $2, $3, 'test.happened.v1', $4, $5)`, id, topic, aggregate, []byte(id.String()), headers); err != nil {
		t.Fatal(err)
	}
	return id
}

func unpublished(t *testing.T, pool *pgxpool.Pool, schema string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func gauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == name {
			var sum float64
			for _, m := range mf.GetMetric() {
				sum += m.GetGauge().GetValue() + m.GetCounter().GetValue()
			}
			return sum
		}
	}
	return 0
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func run(t *testing.T, r *Relay) (stop func()) {
	t.Helper()
	c, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(c) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func newTopic(t *testing.T, cfg config.Kafka) string {
	t.Helper()
	topic := "test.outbox." + uuid.NewString()[:8] + ".v1"
	if _, err := kafka.EnsureTopics(ctx, cfg, kafka.TopicSpec{Partitions: 3, ReplicationFactor: 1}, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if kc, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...)); err == nil {
			_, _ = kadm.NewClient(kc).DeleteTopics(context.Background(), topic)
			kc.Close()
		}
	})
	return topic
}

func TestRelayPublishesInCommitOrderWithEveryHeader(t *testing.T) {
	pool := testenv.Postgres(t)
	kcfg := testenv.Kafka(t)
	schema := testSchema(t, pool)
	topic := newTopic(t, kcfg)
	const stored = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	a, b := uuid.New(), uuid.New()
	var want []uuid.UUID
	for i := range 10 {
		agg := a
		if i%2 == 1 {
			agg = b
		}
		want = append(want, insert(t, pool, schema, topic, agg, fmt.Sprintf(`{"traceparent":%q}`, stored)))
	}

	p, err := kafka.NewProducer(ctx, kcfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	reg := prometheus.NewRegistry()
	r, err := NewRelay(pool, p, Config{Schema: schema, Source: "test-svc", Batch: 3, Interval: 20 * time.Millisecond}, NewMetrics(reg), quiet)
	if err != nil {
		t.Fatal(err)
	}
	run(t, r)
	waitFor(t, "every row published", func() bool { return unpublished(t, pool, schema) == 0 })

	// Consume the topic and compare, key by key.
	var mu sync.Mutex
	got := map[string][]string{}
	headers := map[string]map[string]string{}
	c, err := kafka.NewConsumer(kcfg, "test", kafka.ConsumerConfig{Group: "outbox-test-" + uuid.NewString()[:8], Topics: []string{topic}},
		func(_ context.Context, m kafka.Message) error {
			mu.Lock()
			defer mu.Unlock()
			got[m.Key] = append(got[m.Key], m.ID())
			headers[m.ID()] = m.Headers
			return nil
		}, nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(cctx) }()
	waitFor(t, "10 messages", func() bool { mu.Lock(); defer mu.Unlock(); return len(headers) >= 10 })
	cancel()
	<-done

	for _, agg := range []uuid.UUID{a, b} {
		var expect []string
		for i, id := range want {
			if (i%2 == 0) == (agg == a) {
				expect = append(expect, id.String())
			}
		}
		if !slices.Equal(got[agg.String()], expect) {
			t.Fatalf("aggregate %s: got %v, want %v (commit order)", agg, got[agg.String()], expect)
		}
	}
	h := headers[want[0].String()]
	if h[kafka.HeaderType] != "test.happened.v1" || h[kafka.HeaderSource] != "test-svc" || h[kafka.HeaderID] != want[0].String() {
		t.Fatalf("headers %v", h)
	}
	if !strings.Contains(h[kafka.HeaderTraceParent], "0af7651916cd43dd8448eb211c80319c") {
		t.Fatalf("traceparent %q does not continue the stored trace", h[kafka.HeaderTraceParent])
	}
	if v := gauge(t, reg, "holdfast_outbox_published_total"); v != 10 {
		t.Fatalf("published counter %v, want 10", v)
	}
	waitFor(t, "the lag gauge to drop to 0", func() bool { return gauge(t, reg, "holdfast_outbox_lag_seconds") == 0 })
}

func TestOnlyOneRelayLeads(t *testing.T) {
	pool := testenv.Postgres(t)
	schema := testSchema(t, pool)
	pubA, pubB := &recorder{}, &recorder{}
	regA, regB := prometheus.NewRegistry(), prometheus.NewRegistry()
	cfg := Config{Schema: schema, Source: "test-svc", Interval: 20 * time.Millisecond, RetryLeadership: 50 * time.Millisecond}
	ra, _ := NewRelay(pool, pubA, cfg, NewMetrics(regA), quiet)
	rb, _ := NewRelay(pool, pubB, cfg, NewMetrics(regB), quiet)
	stopA := run(t, ra)
	waitFor(t, "a to lead", func() bool { return gauge(t, regA, "holdfast_outbox_relay_leader") == 1 })
	run(t, rb)
	time.Sleep(200 * time.Millisecond)
	if gauge(t, regB, "holdfast_outbox_relay_leader") != 0 {
		t.Fatal("two relays lead at once")
	}
	insert(t, pool, schema, "t", uuid.New(), "{}")
	// Wait for a's pass to commit, not just to publish: a relay stopped
	// between publishing and committing leaves the row for the next leader,
	// which publishes it again (at least once, by design).
	waitFor(t, "a to publish and commit", func() bool { return pubA.count() == 1 && unpublished(t, pool, schema) == 0 })

	stopA()
	waitFor(t, "b to take over", func() bool { return gauge(t, regB, "holdfast_outbox_relay_leader") == 1 })
	second := insert(t, pool, schema, "t", uuid.New(), "{}")
	waitFor(t, "b to publish", func() bool { return pubB.count() == 1 })
	pubB.mu.Lock()
	got := pubB.events[0].ID
	pubB.mu.Unlock()
	if got != second.String() {
		t.Fatalf("b published %s, want the new event %s", got, second)
	}
	if pubA.count() != 1 {
		t.Fatal("the stopped relay kept publishing")
	}
}

func TestAFailedPublishLeavesTheBatchForLater(t *testing.T) {
	pool := testenv.Postgres(t)
	schema := testSchema(t, pool)
	insert(t, pool, schema, "t", uuid.New(), "{}")
	insert(t, pool, schema, "t", uuid.New(), "[1, 2]") // valid JSON, but not a header map
	pub := &recorder{fail: errors.New("kafka is down")}
	r, _ := NewRelay(pool, pub, Config{Schema: schema, Source: "test-svc"}, NewMetrics(prometheus.NewRegistry()), quiet)
	if _, err := r.Pass(ctx); err == nil {
		t.Fatal("a failed publish reported success")
	}
	if n := unpublished(t, pool, schema); n != 2 {
		t.Fatalf("%d unpublished after a failed publish, want 2", n)
	}
	pub.fail = nil
	if n, err := r.Pass(ctx); err != nil || n != 2 {
		t.Fatalf("retry: %d %v", n, err)
	}
	if n := unpublished(t, pool, schema); n != 0 {
		t.Fatalf("%d left unpublished", n)
	}
}

func TestPurgeKeepsRecentAndUnpublishedRows(t *testing.T) {
	pool := testenv.Postgres(t)
	schema := testSchema(t, pool)
	old := insert(t, pool, schema, "t", uuid.New(), "{}")
	insert(t, pool, schema, "t", uuid.New(), "{}") // unpublished
	recent := insert(t, pool, schema, "t", uuid.New(), "{}")
	_, _ = pool.Exec(ctx, `UPDATE `+schema+`.outbox SET published_at = now() - interval '8 days' WHERE event_id = $1`, old)
	_, _ = pool.Exec(ctx, `UPDATE `+schema+`.outbox SET published_at = now() WHERE event_id = $1`, recent)
	r, _ := NewRelay(pool, &recorder{}, Config{Schema: schema, Source: "s"}, NewMetrics(prometheus.NewRegistry()), quiet)
	r.purgeOld(ctx)
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.outbox`).Scan(&n)
	if n != 2 {
		t.Fatalf("%d rows after the purge, want 2 (the recent and the unpublished)", n)
	}
}

func TestConfigRefusesUnsafeSchemas(t *testing.T) {
	for _, bad := range []string{"", "Booking", "booking; DROP TABLE x", "a.b"} {
		if _, err := NewRelay(nil, &recorder{}, Config{Schema: bad, Source: "s"}, NewMetrics(prometheus.NewRegistry()), quiet); err == nil {
			t.Errorf("schema %q accepted", bad)
		}
	}
}

// recorder is a Publisher that records events, or fails.
type recorder struct {
	mu     sync.Mutex
	events []kafka.Event
	fail   error
}

func (r *recorder) Publish(_ context.Context, events ...kafka.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return r.fail
	}
	r.events = append(r.events, events...)
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// crashAfterPublish delivers the events, then fails as if the relay died
// before its transaction committed: the broker has them, the outbox does not
// know.
type crashAfterPublish struct {
	recorder
	crashes int
}

func (c *crashAfterPublish) Publish(ctx context.Context, events ...kafka.Event) error {
	_ = c.recorder.Publish(ctx, events...)
	if c.crashes > 0 {
		c.crashes--
		return errors.New("relay crashed after publishing")
	}
	return nil
}

// TestACrashAfterPublishingRepublishesTheSameEvents pins the relay's half of
// at-least-once delivery: events published by a relay that crashed before
// marking them are published again, under the same IDs, so consumers can
// drop the copies.
func TestACrashAfterPublishingRepublishesTheSameEvents(t *testing.T) {
	pool := testenv.Postgres(t)
	schema := testSchema(t, pool)
	a := insert(t, pool, schema, "t", uuid.New(), "{}")
	b := insert(t, pool, schema, "t", uuid.New(), "{}")
	pub := &crashAfterPublish{crashes: 1}
	r, _ := NewRelay(pool, pub, Config{Schema: schema, Source: "test-svc"}, NewMetrics(prometheus.NewRegistry()), quiet)
	if _, err := r.Pass(ctx); err == nil {
		t.Fatal("the crashed pass reported success")
	}
	if n := unpublished(t, pool, schema); n != 2 {
		t.Fatalf("%d unpublished after the crash, want 2", n)
	}
	if n, err := r.Pass(ctx); err != nil || n != 2 {
		t.Fatalf("after the restart: %d %v", n, err)
	}
	if n := unpublished(t, pool, schema); n != 0 {
		t.Fatalf("%d left unpublished", n)
	}
	var ids []string
	for _, e := range pub.events {
		ids = append(ids, e.ID)
	}
	want := []string{a.String(), b.String(), a.String(), b.String()}
	if !slices.Equal(ids, want) {
		t.Fatalf("published IDs %v, want each event twice under its own ID %v", ids, want)
	}
	if string(pub.events[0].Value) != string(pub.events[2].Value) || pub.events[0].Type != pub.events[2].Type {
		t.Fatal("the republished event differs from the first copy")
	}
	// A third pass finds nothing: the copies stop once marked.
	if n, err := r.Pass(ctx); err != nil || n != 0 {
		t.Fatalf("a pass after marking: %d %v", n, err)
	}
}
