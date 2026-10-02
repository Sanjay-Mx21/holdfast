package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
)

// Producer publishes events.
type Producer struct {
	kc *kgo.Client
}

// NewProducer connects a producer and checks that a broker answers.
func NewProducer(ctx context.Context, cfg config.Kafka, clientID string) (*Producer, error) {
	kc, err := kgo.NewClient(clientOpts(cfg, clientID)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: producer: %w", err)
	}
	if err := Ping(ctx, kc); err != nil {
		kc.Close()
		return nil, err
	}
	return &Producer{kc: kc}, nil
}

// Publish sends the events and waits until every one is acknowledged by all
// in-sync replicas. It returns the first failure; the events that did not
// fail were still written, so callers retry the whole call with the same
// event IDs and rely on consumers dropping duplicates.
func (p *Producer) Publish(ctx context.Context, events ...Event) error {
	records := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		r, err := e.record()
		if err != nil {
			return err
		}
		records = append(records, r)
	}
	if err := p.kc.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}
	return nil
}

// Ping checks that a broker answers.
func (p *Producer) Ping(ctx context.Context) error { return Ping(ctx, p.kc) }

// Close flushes nothing (Publish is synchronous) and closes the connections.
func (p *Producer) Close() { p.kc.Close() }
