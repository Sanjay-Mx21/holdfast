package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// Handler processes one message. It must be idempotent: a message can be
// delivered more than once (after a crash before the commit, or a
// rebalance). Returning an error retries the message; wrap it with Permanent
// to send the message straight to the dead-letter topic.
type Handler func(ctx context.Context, m Message) error

// ConsumerConfig configures a consumer group member.
type ConsumerConfig struct {
	Group  string
	Topics []string
	// MaxAttempts is how often the handler is tried before the message goes
	// to the dead-letter topic (default 5).
	MaxAttempts int
	// Backoff is the first retry delay; it doubles per attempt, with jitter,
	// up to MaxBackoff (defaults 100 ms and 5 s).
	Backoff, MaxBackoff time.Duration
	// MaxPollRecords bounds one batch (default 500).
	MaxPollRecords int
}

func (c *ConsumerConfig) defaults() error {
	if c.Group == "" || len(c.Topics) == 0 {
		return errors.New("kafka: a consumer needs a group and at least one topic")
	}
	if c.MaxAttempts < 1 {
		c.MaxAttempts = 5
	}
	if c.Backoff <= 0 {
		c.Backoff = 100 * time.Millisecond
	}
	if c.MaxBackoff < c.Backoff {
		c.MaxBackoff = max(5*time.Second, c.Backoff)
	}
	if c.MaxPollRecords < 1 {
		c.MaxPollRecords = 500
	}
	return nil
}

// Consumer is one member of a consumer group.
type Consumer struct {
	kc      *kgo.Client
	cfg     ConsumerConfig
	handle  Handler
	m       *Metrics
	log     *slog.Logger
	backoff func(attempt int) time.Duration
}

// NewConsumer joins the consumer group. A new group starts from the
// earliest offset, so no event published before it first ran is missed.
func NewConsumer(cfg config.Kafka, clientID string, cc ConsumerConfig, h Handler, m *Metrics, log *slog.Logger) (*Consumer, error) {
	if err := cc.defaults(); err != nil {
		return nil, err
	}
	opts := append(clientOpts(cfg, clientID),
		kgo.ConsumerGroup(cc.Group),
		kgo.ConsumeTopics(cc.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// Hold rebalances while a polled batch is being processed and
		// committed, so a partition is never handed to another member with
		// processed but uncommitted records.
		kgo.BlockRebalanceOnPoll(),
	)
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: consumer: %w", err)
	}
	c := &Consumer{kc: kc, cfg: cc, handle: h, m: m, log: log.With("group", cc.Group)}
	c.backoff = func(attempt int) time.Duration {
		d := cc.Backoff << min(attempt-1, 16)
		d = min(d, cc.MaxBackoff)
		return d/2 + rand.N(d/2+1) //nolint:gosec // jitter, not security
	}
	return c, nil
}

// Name implements app.Component.
func (c *Consumer) Name() string { return "kafka-consumer-" + c.cfg.Group }

// Run consumes until ctx ends. It returns an error only when a message could
// neither be handled nor dead-lettered (Kafka unreachable for the DLQ); the
// message stays uncommitted and is redelivered.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.kc.Close()
	for {
		fetches := c.kc.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			c.kc.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("kafka: fetch failed", "topic", topic, "partition", partition, "err", err)
		})

		var done []*kgo.Record
		var stop error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				if stop != nil {
					return
				}
				if err := c.process(ctx, r); err != nil {
					stop = err
					return
				}
				done = append(done, r)
			}
		})
		if len(done) > 0 {
			// Commit even if ctx is ending: the work is done.
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			err := c.kc.CommitRecords(commitCtx, done...)
			cancel()
			if err != nil {
				c.log.Warn("kafka: commit failed; the batch will be redelivered", "err", err)
			}
		}
		c.kc.AllowRebalance()
		switch {
		case ctx.Err() != nil:
			return nil
		case stop != nil:
			return stop
		}
	}
}

// process hands one record to the handler until it succeeds, retrying with
// backoff, and dead-letters it after MaxAttempts or a permanent error. A nil
// return means the record may be committed.
func (c *Consumer) process(ctx context.Context, r *kgo.Record) error {
	var err error
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		err = c.handle(ctx, messageOf(r, attempt))
		if err == nil {
			c.m.inc(r.Topic, resultOK)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err() // shutting down: leave it uncommitted
		}
		if IsPermanent(err) || attempt == c.cfg.MaxAttempts {
			break
		}
		c.m.inc(r.Topic, resultRetried)
		c.log.Warn("kafka: handler failed; retrying", "topic", r.Topic, "partition", r.Partition,
			"offset", r.Offset, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.backoff(attempt)):
		}
	}
	return c.deadLetter(ctx, r, err)
}

// deadLetter copies the record to its topic's dead-letter topic, with the
// reason and origin in headers.
func (c *Consumer) deadLetter(ctx context.Context, r *kgo.Record, cause error) error {
	headers := append([]kgo.RecordHeader{}, r.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: "dlq_reason", Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: "dlq_topic", Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: "dlq_partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: "dlq_offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: "dlq_group", Value: []byte(c.cfg.Group)},
	)
	dlq := &kgo.Record{Topic: DLQ(r.Topic), Key: r.Key, Value: r.Value, Headers: headers}
	if err := c.kc.ProduceSync(ctx, dlq).FirstErr(); err != nil {
		return fmt.Errorf("kafka: dead-letter %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	c.m.inc(r.Topic, resultDeadLettered)
	c.log.Error("kafka: message dead-lettered", "topic", r.Topic, "partition", r.Partition,
		"offset", r.Offset, "dlq", dlq.Topic, "err", cause)
	return nil
}

// Results of handling one message.
const (
	resultOK           = "ok"
	resultRetried      = "retried"
	resultDeadLettered = "dead_lettered"
)

// Metrics counts consumed messages by topic and result.
type Metrics struct {
	consumed *prometheus.CounterVec
}

// NewMetrics registers holdfast_kafka_consumed_total. The topic label is
// bounded: topics are HoldFast's own.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	return &Metrics{consumed: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_kafka_consumed_total",
		Help: "Messages handled by consumers, by topic and result: ok, retried (one failed attempt), dead_lettered.",
	}, []string{"topic", "result"})}
}

func (m *Metrics) inc(topic, result string) {
	if m != nil {
		m.consumed.WithLabelValues(topic, result).Inc()
	}
}
