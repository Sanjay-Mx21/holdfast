package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/booking"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

func brokersFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("KAFKA_BROKERS")
	if def == "" {
		def = "localhost:29092"
	}
	return fs.String("brokers", def, "comma-separated Kafka brokers (env KAFKA_BROKERS)")
}

func kafkaConfig(brokers string) config.Kafka {
	return config.Kafka{Brokers: strings.Split(brokers, ","), DialTimeout: 5 * time.Second, DeliveryTimeout: 30 * time.Second}
}

// cmdDLQReplay is runbook RB-3: publish a dead-letter topic's messages back
// to where they came from, once the cause is fixed.
func cmdDLQReplay(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dlq replay", flag.ContinueOnError)
	brokers := brokersFlag(fs)
	topic := fs.String("topic", "", "dead-letter topic, for example holdfast.payment.dlq.v1 (or the original topic)")
	dryRun := fs.Bool("dry-run", false, "list the messages without replaying them")
	maxN := fs.Int("max", 0, "replay at most this many messages (0: all)")
	group := fs.String("group", kafka.ReplayGroup, "consumer group that records how far the topic has been replayed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *topic == "" || *maxN < 0 {
		return errors.New("usage: holdfastctl dlq replay --topic TOPIC [--dry-run] [--max N]")
	}
	dlq := *topic
	if !strings.Contains(dlq, ".dlq.") {
		dlq = kafka.DLQ(dlq)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	msgs, err := kafka.ReplayDLQ(ctx, kafkaConfig(*brokers), dlq, kafka.ReplayOptions{Group: *group, DryRun: *dryRun, Max: *maxN})
	verb := "replayed"
	if *dryRun {
		verb = "would replay"
	}
	for _, m := range msgs {
		fmt.Fprintf(stdout, "%s/%d@%d -> %s  %s %s  (%s)\n", dlq, m.Partition, m.Offset, m.Topic, m.Type, m.ID, m.Reason)
	}
	fmt.Fprintf(stdout, "%s %d message(s) from %s\n", verb, len(msgs), dlq)
	return err
}

// cmdRefund is runbook RB-4: ask payment-svc again to refund a booking
// stuck in REFUND_REQUIRED, through the normal saga path.
func cmdRefund(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("refund", flag.ContinueOnError)
	dsn := dsnFlag(fs)
	brokers := brokersFlag(fs)
	id := fs.String("booking", "", "booking ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bookingID, err := uuid.Parse(*id)
	if err != nil {
		return errors.New("usage: holdfastctl refund --booking ID")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	pool, err := openPool(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	producer, err := kafka.NewProducer(ctx, kafkaConfig(*brokers), "holdfastctl")
	if err != nil {
		return err
	}
	defer producer.Close()
	b, err := booking.RequestRefundAgain(ctx, pool, producer, bookingID)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "refund requested again for booking %s (%d paise, intent %s); payment-svc refunds it and the booking moves to REFUNDED when the provider confirms\n",
		b.ID, b.AmountPaise, b.IntentID.UUID)
	return nil
}
