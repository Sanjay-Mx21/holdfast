package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// HoldFast's topics (design doc 8.3). A breaking change to a topic's events
// means a new topic version (v2), never a change in place.
const (
	TopicBooking   = "holdfast.booking.v1"   // booking-svc via its outbox; keyed by booking ID
	TopicPayment   = "holdfast.payment.v1"   // payment-svc via its outbox; keyed by booking ID
	TopicInventory = "holdfast.inventory.v1" // inventory-svc, informational snapshots; keyed by event ID
)

// DLQ returns the dead-letter topic of a topic: holdfast.booking.v1 has
// holdfast.booking.dlq.v1.
func DLQ(topic string) string {
	if base, ok := strings.CutSuffix(topic, ".v1"); ok {
		return base + ".dlq.v1"
	}
	return topic + ".dlq"
}

// Topics lists every topic HoldFast uses, dead-letter topics included.
func Topics() []string {
	var out []string
	for _, t := range []string{TopicBooking, TopicPayment, TopicInventory} {
		out = append(out, t, DLQ(t))
	}
	return out
}

// TopicSpec is how a topic is created.
type TopicSpec struct {
	Partitions        int32
	ReplicationFactor int16
	// Retention is the retention.ms setting; empty keeps the broker default.
	Retention string
}

// TopicResult is what EnsureTopics did with one topic.
type TopicResult struct {
	Topic   string
	Created bool
	// Partitions is the topic's partition count, created or found.
	Partitions int
}

// EnsureTopics creates every topic that does not exist yet, and leaves
// existing ones alone (it never changes partitions or settings). Safe to run
// any number of times.
func EnsureTopics(ctx context.Context, cfg config.Kafka, spec TopicSpec, topics ...string) ([]TopicResult, error) {
	if spec.Partitions < 1 || spec.ReplicationFactor < 1 {
		return nil, errors.New("kafka: partitions and replication factor must be at least 1")
	}
	kc, err := kgo.NewClient(clientOpts(cfg, "holdfast-topics")...)
	if err != nil {
		return nil, fmt.Errorf("kafka: client: %w", err)
	}
	defer kc.Close()
	adm := kadm.NewClient(kc)

	var configs map[string]*string
	if spec.Retention != "" {
		configs = map[string]*string{"retention.ms": &spec.Retention}
	}
	created, err := adm.CreateTopics(ctx, spec.Partitions, spec.ReplicationFactor, configs, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create topics: %w", err)
	}
	details, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: list topics: %w", err)
	}
	out := make([]TopicResult, 0, len(topics))
	for _, t := range topics {
		r := created[t]
		switch {
		case r.Err == nil:
			out = append(out, TopicResult{Topic: t, Created: true, Partitions: int(spec.Partitions)})
		case errors.Is(r.Err, kerr.TopicAlreadyExists):
			out = append(out, TopicResult{Topic: t, Partitions: len(details[t].Partitions)})
		default:
			return out, fmt.Errorf("kafka: create topic %s: %w", t, r.Err)
		}
	}
	return out, nil
}
