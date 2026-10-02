// Package kafka is HoldFast's Kafka client, built on franz-go.
//
//   - Producer publishes events with acks=all and idempotence on (franz-go's
//     default, made explicit here), each carrying CloudEvents attributes as
//     headers (design doc 8.3).
//   - Consumer runs a consumer group with manual commits: an offset is
//     committed only after the handler has finished with the record, so
//     delivery is at least once and handlers must be idempotent. A record that
//     keeps failing goes to its topic's dead-letter topic and is then
//     committed, so one poison message cannot stall a partition.
//   - EnsureTopics creates topics explicitly; brokers run with topic
//     auto-creation off.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// clientOpts are the options every HoldFast client shares.
func clientOpts(cfg config.Kafka, clientID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.DialTimeout(cfg.DialTimeout),
		// Producing: every in-sync replica acknowledges, the broker drops
		// retried duplicates (idempotence), and a record that cannot be
		// delivered in time fails instead of retrying forever.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.ProducerLinger(5 * time.Millisecond),
	}
}

// Ping checks that a broker answers; for readiness checks.
func Ping(ctx context.Context, kc *kgo.Client) error {
	if err := kc.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}
