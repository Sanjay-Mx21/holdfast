//go:build integration

package booking

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// TestSagaDecisionTable runs every payment event against a booking in every
// status: the saga moves only along design doc 7.2 and leaves every other
// combination alone, with no event written.
func TestSagaDecisionTable(t *testing.T) {
	f := newSagaFixture(t)
	q := bookingdb.New(f.pool)
	// The moves that put a new booking in each status (the trigger's paths).
	paths := map[string][]string{
		StatusPendingPayment: nil,
		StatusConfirmed:      {StatusConfirmed},
		StatusCancelled:      {StatusCancelled},
		StatusRefundRequired: {StatusRefundRequired},
		StatusRefunded:       {StatusRefundRequired, StatusRefunded},
	}
	events := map[string]func(View) kafka.Message{
		paymentCaptured: captured,
		paymentFailed: func(v View) kafka.Message {
			return message(paymentFailed, &eventsv1.PaymentFailed{BookingId: v.BookingID, Reason: "card_declined"})
		},
		paymentExpired: func(v View) kafka.Message {
			return message(paymentExpired, &eventsv1.PaymentExpired{BookingId: v.BookingID})
		},
		refundCompleted: func(v View) kafka.Message {
			return message(refundCompleted, &eventsv1.RefundCompleted{BookingId: v.BookingID, PspRefundId: "rfnd_x", AmountPaise: v.AmountPaise})
		},
	}
	moved := map[[2]string]string{ // (from, event) -> to; everything else stays
		{StatusPendingPayment, paymentCaptured}: StatusConfirmed,
		{StatusPendingPayment, paymentFailed}:   StatusCancelled,
		{StatusPendingPayment, paymentExpired}:  StatusCancelled,
		{StatusCancelled, paymentCaptured}:      StatusConfirmed, // a late capture the guard accepts
		{StatusRefundRequired, refundCompleted}: StatusRefunded,
	}
	for from, path := range paths {
		for typ, ev := range events {
			t.Run(from+"/"+typ, func(t *testing.T) {
				v, _ := f.book(t, f.event, 1)
				id := uuid.MustParse(v.BookingID)
				status := StatusPendingPayment
				for _, to := range path {
					if _, err := q.TransitionBooking(ctx, bookingdb.TransitionBookingParams{ID: id, FromStatus: status, ToStatus: to}); err != nil {
						t.Fatalf("set up %s: %v", to, err)
					}
					status = to
				}
				before := f.count(t, `SELECT count(*) FROM booking.outbox WHERE aggregate_id = $1`, id)
				f.handle(t, ev(v))
				want, ok := moved[[2]string{from, typ}]
				if !ok {
					want = from
				}
				if got := f.statusOf(t, v.BookingID); got != want {
					t.Fatalf("%s + %s: %s, want %s", from, typ, got, want)
				}
				wrote := f.count(t, `SELECT count(*) FROM booking.outbox WHERE aggregate_id = $1`, id) - before
				if (want != from) != (wrote == 1) || wrote > 1 {
					t.Fatalf("%s + %s wrote %d events", from, typ, wrote)
				}
			})
		}
	}
}

// TestSagaOverKafka runs the saga behind the real consumer: duplicates on
// the wire, and an inventory failure retried by the consumer, end with one
// decision per booking.
func TestSagaOverKafka(t *testing.T) {
	cfg := testenv.Kafka(t)
	f := newSagaFixture(t)
	topic := "test.saga." + uuid.NewString()[:8] + ".v1"
	if _, err := kafka.EnsureTopics(ctx, cfg, kafka.TopicSpec{Partitions: 3, ReplicationFactor: 1}, topic, kafka.DLQ(topic)); err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(ctx, cfg, "saga-test")
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	paid, paidHold := f.book(t, f.event, 2)
	declined, declinedHold := f.book(t, f.event, 1)
	capture := captured(paid)
	decline := message(paymentFailed, &eventsv1.PaymentFailed{BookingId: declined.BookingID, Reason: "card_declined"})
	event := func(m kafka.Message, key string) kafka.Event {
		return kafka.Event{Topic: topic, Key: key, Value: m.Value, ID: m.ID(), Type: m.Type(), Source: "payment-svc"}
	}
	// The capture twice under one ID (a relay that crashed after
	// publishing), the decline once.
	batch := []kafka.Event{event(capture, paid.BookingID), event(decline, declined.BookingID), event(capture, paid.BookingID)}
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if err = producer.Publish(ctx, batch...); err == nil || time.Now().After(deadline) {
			break // a new topic may need a moment for its partition leaders
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	f.inv.confirmFails = 1 // the first inventory call fails; the consumer retries
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer, err := kafka.NewConsumer(cfg, "saga-test", kafka.ConsumerConfig{
		Group: "saga-test-" + uuid.NewString()[:8], Topics: []string{topic}, Backoff: 20 * time.Millisecond,
	}, f.saga.Handle, kafka.NewMetrics(prometheus.NewRegistry()), quiet)
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = consumer.Run(cctx); close(done) }()
	defer func() { cancel(); <-done }()

	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		f.inv.mu.Lock()
		settledPaid := f.inv.holds[paidHold].State == "SOLD"
		settledDeclined := f.inv.holds[declinedHold].State == "RELEASED"
		f.inv.mu.Unlock()
		if settledPaid && settledDeclined {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not settled: paid %s, declined %s", f.statusOf(t, paid.BookingID), f.statusOf(t, declined.BookingID))
		}
	}
	if s := f.statusOf(t, paid.BookingID); s != StatusConfirmed {
		t.Fatalf("paid booking %s", s)
	}
	if s := f.statusOf(t, declined.BookingID); s != StatusCancelled {
		t.Fatalf("declined booking %s", s)
	}
	if f.sold(t, f.event) != 2 {
		t.Fatalf("sold %d, want 2: the duplicate capture was applied", f.sold(t, f.event))
	}
	if got := testutil.ToFloat64(f.m.saga.WithLabelValues("duplicate")); got < 1 {
		t.Fatalf("duplicates skipped %v, want at least 1", got)
	}
	var payload []byte
	if err := f.pool.QueryRow(ctx, `SELECT payload FROM booking.outbox WHERE aggregate_id = $1 AND event_type = 'booking.confirmed.v1'`, paid.BookingID).Scan(&payload); err != nil {
		t.Fatalf("one booking.confirmed.v1: %v", err)
	}
	var ev eventsv1.BookingConfirmed
	if err := proto.Unmarshal(payload, &ev); err != nil || ev.GetQuantity() != 2 {
		t.Fatalf("confirmed event %+v %v", &ev, err)
	}
}
