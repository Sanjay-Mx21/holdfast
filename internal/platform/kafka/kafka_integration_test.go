//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// testTopic creates a fresh topic and its dead-letter topic for one test, and
// deletes both afterwards. Brokers run with auto-creation off, as in
// production, so every topic must exist first.
func testTopic(t *testing.T, cfg config.Kafka, partitions int32) string {
	t.Helper()
	topic := "test." + uuid.NewString()[:8] + ".v1"
	if _, err := EnsureTopics(context.Background(), cfg, TopicSpec{Partitions: partitions, ReplicationFactor: 1}, topic, DLQ(topic)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		kc, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if err != nil {
			return
		}
		defer kc.Close()
		_, _ = kadm.NewClient(kc).DeleteTopics(context.Background(), topic, DLQ(topic))
	})
	return topic
}

func groupName() string { return "test-group-" + uuid.NewString()[:8] }

// collector is a handler that records what it was given.
type collector struct {
	mu   sync.Mutex
	msgs []Message
	fail func(m Message) error
}

func (c *collector) handle(_ context.Context, m Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, m)
	if c.fail != nil {
		return c.fail(m)
	}
	return nil
}

func (c *collector) snapshot() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.msgs)
}

// runConsumer runs a consumer until stop is called; stop returns Run's error.
func runConsumer(t *testing.T, cfg config.Kafka, cc ConsumerConfig, h Handler, m *Metrics) (stop func() error) {
	t.Helper()
	c, err := NewConsumer(cfg, "test", cc, h, m, discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() { cancel(); runErr = <-done })
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
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

func publish(t *testing.T, p *Producer, topic string, n int, keyOf func(i int) string) []string {
	t.Helper()
	ids := make([]string, n)
	events := make([]Event, n)
	for i := range n {
		ids[i] = uuid.Must(uuid.NewV7()).String()
		events[i] = Event{Topic: topic, Key: keyOf(i), Value: fmt.Appendf(nil, "%d", i), ID: ids[i], Type: "test.v1", Source: "test"}
	}
	if err := p.Publish(context.Background(), events...); err != nil {
		t.Fatal(err)
	}
	return ids
}

func counter(t *testing.T, m *Metrics, topic, result string) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.consumed.WithLabelValues(topic, result).Write(&out); err != nil {
		t.Fatal(err)
	}
	return out.GetCounter().GetValue()
}

func TestEnsureTopicsIsIdempotent(t *testing.T) {
	cfg := testenv.Kafka(t)
	topic := testTopic(t, cfg, 3)
	res, err := EnsureTopics(context.Background(), cfg, TopicSpec{Partitions: 6, ReplicationFactor: 1}, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Created || res[0].Partitions != 3 {
		t.Fatalf("second EnsureTopics = %+v, want existing with its 3 partitions untouched", res)
	}
	if _, err := EnsureTopics(context.Background(), cfg, TopicSpec{Partitions: 0, ReplicationFactor: 1}, topic); err == nil {
		t.Fatal("zero partitions accepted")
	}
}

func TestPublishAndConsumeInOrderPerKey(t *testing.T) {
	cfg := testenv.Kafka(t)
	topic := testTopic(t, cfg, 6)
	p, err := NewProducer(context.Background(), cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	// 300 events over 3 keys: each key's events must arrive in publish order.
	ids := publish(t, p, topic, 300, func(i int) string { return fmt.Sprintf("booking-%d", i%3) })

	col := &collector{}
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	stop := runConsumer(t, cfg, ConsumerConfig{Group: groupName(), Topics: []string{topic}}, col.handle, m)
	waitFor(t, "300 messages", func() bool { return len(col.snapshot()) >= 300 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	got := col.snapshot()
	byKey := map[string][]string{}
	for _, msg := range got {
		byKey[msg.Key] = append(byKey[msg.Key], msg.ID())
		if msg.Type() != "test.v1" || msg.Headers[HeaderSource] != "test" || msg.Attempt != 1 {
			t.Fatalf("message %+v", msg)
		}
	}
	for k := range 3 {
		var want []string
		for i := k; i < 300; i += 3 {
			want = append(want, ids[i])
		}
		if !slices.Equal(byKey[fmt.Sprintf("booking-%d", k)], want) {
			t.Fatalf("key booking-%d arrived out of order or incomplete", k)
		}
	}
	if v := counter(t, m, topic, resultOK); v != 300 {
		t.Fatalf("ok counter %v, want 300", v)
	}
}

// TestCommittedOnlyAfterHandling: a restarted group member resumes after the
// last handled message, and nothing handled is lost or skipped.
func TestCommittedOnlyAfterHandling(t *testing.T) {
	cfg := testenv.Kafka(t)
	topic := testTopic(t, cfg, 1)
	p, err := NewProducer(context.Background(), cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	group := groupName()

	ids := publish(t, p, topic, 10, func(int) string { return "k" })
	first := &collector{}
	stop := runConsumer(t, cfg, ConsumerConfig{Group: group, Topics: []string{topic}}, first.handle, nil)
	waitFor(t, "the first 10", func() bool { return len(first.snapshot()) >= 10 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	more := publish(t, p, topic, 5, func(int) string { return "k" })
	second := &collector{}
	stop = runConsumer(t, cfg, ConsumerConfig{Group: group, Topics: []string{topic}}, second.handle, nil)
	waitFor(t, "the next 5", func() bool { return len(second.snapshot()) >= 5 })
	time.Sleep(500 * time.Millisecond) // anything redelivered would arrive by now
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	var gotIDs []string
	for _, m := range second.snapshot() {
		gotIDs = append(gotIDs, m.ID())
	}
	if !slices.Equal(gotIDs, more) {
		t.Fatalf("after a restart the member got %d messages, want exactly the 5 new ones (first batch: %d)", len(gotIDs), len(ids))
	}
}

// TestUncommittedWorkIsRedelivered: if the member stops while a message is
// still failing, the message is redelivered to the next member: at least once.
func TestUncommittedWorkIsRedelivered(t *testing.T) {
	cfg := testenv.Kafka(t)
	topic := testTopic(t, cfg, 1)
	p, err := NewProducer(context.Background(), cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	group := groupName()
	ids := publish(t, p, topic, 1, func(int) string { return "k" })

	failing := &collector{fail: func(Message) error { return errors.New("database down") }}
	stop := runConsumer(t, cfg, ConsumerConfig{Group: group, Topics: []string{topic}, MaxAttempts: 1000,
		Backoff: 50 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}, failing.handle, nil)
	waitFor(t, "a few failed attempts", func() bool { return len(failing.snapshot()) >= 3 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	healthy := &collector{}
	stop = runConsumer(t, cfg, ConsumerConfig{Group: group, Topics: []string{topic}}, healthy.handle, nil)
	waitFor(t, "the redelivery", func() bool { return len(healthy.snapshot()) >= 1 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := healthy.snapshot()[0]; got.ID() != ids[0] {
		t.Fatalf("redelivered %s, want %s", got.ID(), ids[0])
	}
}

// TestPoisonMessagesGoToTheDeadLetterTopic: retried, then dead-lettered with
// their origin, while the messages behind them keep flowing.
func TestPoisonMessagesGoToTheDeadLetterTopic(t *testing.T) {
	cfg := testenv.Kafka(t)
	topic := testTopic(t, cfg, 1)
	p, err := NewProducer(context.Background(), cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ids := publish(t, p, topic, 4, func(int) string { return "k" })
	transient, permanentID := ids[1], ids[2]

	col := &collector{fail: func(m Message) error {
		switch m.ID() {
		case transient:
			return errors.New("still failing")
		case permanentID:
			return Permanent(errors.New("cannot decode"))
		}
		return nil
	}}
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	stop := runConsumer(t, cfg, ConsumerConfig{Group: groupName(), Topics: []string{topic}, MaxAttempts: 3,
		Backoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}, col.handle, m)
	waitFor(t, "the last message", func() bool {
		for _, msg := range col.snapshot() {
			if msg.ID() == ids[3] {
				return true
			}
		}
		return false
	})
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	attempts := map[string]int{}
	for _, msg := range col.snapshot() {
		attempts[msg.ID()] = max(attempts[msg.ID()], msg.Attempt)
	}
	if attempts[transient] != 3 || attempts[permanentID] != 1 || attempts[ids[0]] != 1 || attempts[ids[3]] != 1 {
		t.Fatalf("attempts %v: want 3 for the transient failure, 1 for the permanent one and the healthy ones", attempts)
	}

	dead := &collector{}
	stopDLQ := runConsumer(t, cfg, ConsumerConfig{Group: groupName(), Topics: []string{DLQ(topic)}}, dead.handle, nil)
	waitFor(t, "two dead letters", func() bool { return len(dead.snapshot()) >= 2 })
	if err := stopDLQ(); err != nil {
		t.Fatal(err)
	}
	for _, d := range dead.snapshot() {
		if d.ID() != transient && d.ID() != permanentID {
			t.Fatalf("unexpected dead letter %s", d.ID())
		}
		if d.Headers["dlq_topic"] != topic || d.Headers["dlq_reason"] == "" || d.Headers["dlq_offset"] == "" || d.Key != "k" {
			t.Fatalf("dead letter lacks its origin: %+v", d.Headers)
		}
	}
	if v := counter(t, m, topic, resultDeadLettered); v != 2 {
		t.Fatalf("dead_lettered counter %v, want 2", v)
	}
	if v := counter(t, m, topic, resultRetried); v != 2 {
		t.Fatalf("retried counter %v, want 2 (attempts 1 and 2 of the transient failure)", v)
	}
}

func TestProducerNeedsABroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := NewProducer(ctx, testConfig(), "test"); err == nil {
		t.Fatal("a producer connected to nothing")
	}
}

// TestTraceFlowsThroughKafka: the handler runs in the publisher's trace, so a
// request and everything it causes downstream are one trace.
func TestTraceFlowsThroughKafka(t *testing.T) {
	cfg := testenv.Kafka(t)
	rec := recordSpans(t)
	topic := testTopic(t, cfg, 1)
	p, err := NewProducer(context.Background(), cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	reqCtx, req := otel.Tracer("test").Start(context.Background(), "POST /v1/bookings")
	if err := p.Publish(reqCtx, Event{Topic: topic, Key: "b1", Type: "booking.created.v1", Source: "test"}); err != nil {
		t.Fatal(err)
	}
	req.End()

	var mu sync.Mutex
	var handlerTrace trace.TraceID
	h := func(ctx context.Context, _ Message) error {
		mu.Lock()
		defer mu.Unlock()
		handlerTrace = trace.SpanContextFromContext(ctx).TraceID()
		return nil
	}
	stop := runConsumer(t, cfg, ConsumerConfig{Group: groupName(), Topics: []string{topic}}, h, nil)
	waitFor(t, "the message", func() bool { mu.Lock(); defer mu.Unlock(); return handlerTrace.IsValid() })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if handlerTrace != req.SpanContext().TraceID() {
		t.Fatal("the consumer's handler is not in the request's trace")
	}
	kinds := map[trace.SpanKind]bool{}
	for _, s := range rec.Ended() {
		if s.SpanContext().TraceID() == req.SpanContext().TraceID() {
			kinds[s.SpanKind()] = true
		}
	}
	if !kinds[trace.SpanKindProducer] || !kinds[trace.SpanKindConsumer] {
		t.Fatalf("span kinds in the trace: %v, want producer and consumer", kinds)
	}
}

// TestConsumerLagIsReported: the lag gauge counts what the group has not
// committed yet.
func TestConsumerLagIsReported(t *testing.T) {
	cfg := testenv.Kafka(t)
	topic := testTopic(t, cfg, 1)
	p, err := NewProducer(context.Background(), cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	group := groupName()
	publish(t, p, topic, 3, func(int) string { return "k" })
	col := &collector{}
	stop := runConsumer(t, cfg, ConsumerConfig{Group: group, Topics: []string{topic}}, col.handle, nil)
	waitFor(t, "the first 3", func() bool { return len(col.snapshot()) >= 3 })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	publish(t, p, topic, 5, func(int) string { return "k" }) // nobody consumes these yet

	m := NewMetrics(prometheus.NewRegistry())
	c, err := NewConsumer(cfg, "test", ConsumerConfig{Group: group, Topics: []string{topic}}, col.handle, m, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer c.kc.Close()
	c.reportLag(context.Background())
	var out dto.Metric
	if err := m.lag.WithLabelValues(group, topic).Write(&out); err != nil {
		t.Fatal(err)
	}
	if v := out.GetGauge().GetValue(); v != 5 {
		t.Fatalf("lag %v, want 5", v)
	}
}
