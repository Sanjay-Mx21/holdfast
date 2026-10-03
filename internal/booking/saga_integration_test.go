//go:build integration

package booking

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
)

func (f *fakeInventory) Confirm(_ context.Context, event, user, id string, qty int) (inventory.ConfirmOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.confirmFails > 0 {
		f.confirmFails--
		return "", fmt.Errorf("inventory: %w", status.Error(codes.Unavailable, "connection refused"))
	}
	f.confirms++
	h, ok := f.holds[id]
	if !ok || h.UserID != user || h.EventID != event || h.Quantity != qty {
		return "", inventory.ErrHoldNotFound
	}
	if h.State == inventory.StateSold {
		return inventory.ConfirmReplay, nil
	}
	late := h.State == inventory.StateReleased
	h.State = inventory.StateSold
	f.holds[id] = h
	if late {
		return inventory.ConfirmLate, nil
	}
	return inventory.ConfirmApplied, nil
}

func (f *fakeInventory) ReleaseForFailedPayment(_ context.Context, event, user, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	h, ok := f.holds[id]
	if !ok || h.State == inventory.StateSold || h.State == inventory.StateReleased {
		return false, nil
	}
	h.State = inventory.StateReleased
	f.holds[id] = h
	return true, nil
}

type sagaFixture struct {
	*fixture
	saga *Saga
	m    *Metrics
}

func newSagaFixture(t *testing.T) *sagaFixture {
	t.Helper()
	f := newFixture(t)
	m := NewMetrics(prometheus.NewRegistry())
	return &sagaFixture{fixture: f, m: m, saga: NewSaga(f.pool, f.inv, m, slog.New(slog.NewTextHandler(io.Discard, nil)))}
}

// book creates a PENDING_PAYMENT booking for qty units of event.
func (f *sagaFixture) book(t *testing.T, event string, qty int) (View, string) {
	t.Helper()
	user := uuid.NewString()
	hold := f.inv.hold(event, user, qty, inventory.StateHeld)
	res, err := f.svc.Create(ctx, CreateRequest{UserID: user, EventID: event, HoldID: hold, IdempotencyKey: "saga-" + uuid.NewString()})
	if err != nil || res.Code != 201 {
		t.Fatalf("create: %+v %v", res, err)
	}
	return decode(t, res), hold
}

func message(typ string, m proto.Message) kafka.Message {
	b, _ := proto.Marshal(m)
	return kafka.Message{Value: b, Headers: map[string]string{kafka.HeaderID: uuid.NewString(), kafka.HeaderType: typ}, Attempt: 1}
}

func captured(v View) kafka.Message {
	return message(paymentCaptured, &eventsv1.PaymentCaptured{BookingId: v.BookingID, IntentId: uuid.NewString(), PspPaymentId: "pay_x", AmountPaise: v.AmountPaise})
}

func (f *sagaFixture) handle(t *testing.T, m kafka.Message) {
	t.Helper()
	if err := f.saga.Handle(ctx, m); err != nil {
		t.Fatalf("handle %s: %v", m.Type(), err)
	}
}

func (f *sagaFixture) statusOf(t *testing.T, id string) string {
	t.Helper()
	var s string
	if err := f.pool.QueryRow(ctx, `SELECT status FROM booking.bookings WHERE id = $1`, id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *sagaFixture) eventTypes(t *testing.T, id string) string {
	t.Helper()
	rows, err := f.pool.Query(ctx, `SELECT event_type FROM booking.outbox WHERE aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		out = append(out, s)
	}
	return strings.Join(out, ",")
}

func (f *sagaFixture) sold(t *testing.T, event string) int {
	t.Helper()
	return f.count(t, `SELECT sold FROM booking.event_inventory WHERE event_id = $1`, event)
}

func TestSagaConfirmsACapture(t *testing.T) {
	f := newSagaFixture(t)
	v, hold := f.book(t, f.event, 2)
	m := captured(v)
	f.handle(t, m)
	if s := f.statusOf(t, v.BookingID); s != StatusConfirmed {
		t.Fatalf("status %s", s)
	}
	if got := f.eventTypes(t, v.BookingID); got != "booking.created.v1,booking.confirmed.v1" {
		t.Fatalf("events %s", got)
	}
	if f.sold(t, f.event) != 2 || f.inv.holds[hold].State != inventory.StateSold {
		t.Fatalf("guard sold %d, hold %s", f.sold(t, f.event), f.inv.holds[hold].State)
	}
	// Redelivered: no second sale; the idempotent inventory call repeats.
	f.handle(t, m)
	if f.sold(t, f.event) != 2 || f.inv.confirms != 2 {
		t.Fatalf("after redelivery: sold %d, confirms %d", f.sold(t, f.event), f.inv.confirms)
	}
	// A second capture event for the booking (a duplicate webhook upstream):
	// already decided.
	f.handle(t, captured(v))
	if f.sold(t, f.event) != 2 || testutil.ToFloat64(f.m.saga.WithLabelValues("already_decided")) != 1 {
		t.Fatal("a second capture was applied")
	}
	// A late failure changes nothing.
	f.handle(t, message(paymentFailed, &eventsv1.PaymentFailed{BookingId: v.BookingID, Reason: "card_declined"}))
	if s := f.statusOf(t, v.BookingID); s != StatusConfirmed || f.inv.releases != 0 {
		t.Fatalf("after a late failure: %s, releases %d", s, f.inv.releases)
	}
}

func TestSagaRefundsWhatTheGuardRefuses(t *testing.T) {
	f := newSagaFixture(t)
	small, err := catalog.Create(ctx, f.pool, catalog.NewEvent{Name: "small", SaleOpensAt: time.Now(), PerUserLimit: 4, UnitPricePaise: 100, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Valkey's fast path believed two units were free; PostgreSQL has one.
	v, hold := f.book(t, small.String(), 2)
	intent := uuid.NewString()
	f.handle(t, message(paymentCaptured, &eventsv1.PaymentCaptured{BookingId: v.BookingID, IntentId: intent, AmountPaise: v.AmountPaise}))
	if s := f.statusOf(t, v.BookingID); s != StatusRefundRequired {
		t.Fatalf("status %s", s)
	}
	if f.sold(t, small.String()) != 0 || f.inv.confirms != 0 || f.inv.holds[hold].State != inventory.StateReleased {
		t.Fatalf("a refused sale: sold %d, confirms %d, hold %s; want 0, 0 and the hold released",
			f.sold(t, small.String()), f.inv.confirms, f.inv.holds[hold].State)
	}
	var payload []byte
	if err := f.pool.QueryRow(ctx, `SELECT payload FROM booking.outbox WHERE aggregate_id = $1 AND event_type = 'booking.refund_required.v1'`, v.BookingID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var ev eventsv1.BookingRefundRequired
	if err := proto.Unmarshal(payload, &ev); err != nil || ev.GetIntentId() != intent || ev.GetAmountPaise() != v.AmountPaise ||
		ev.GetReason() != eventsv1.RefundReason_REFUND_REASON_GUARD_REJECTED {
		t.Fatalf("refund event %+v %v", &ev, err)
	}
	// The refund completes: the saga is over.
	f.handle(t, message(refundCompleted, &eventsv1.RefundCompleted{BookingId: v.BookingID, IntentId: intent, PspRefundId: "rfnd_1", AmountPaise: v.AmountPaise}))
	if s := f.statusOf(t, v.BookingID); s != StatusRefunded {
		t.Fatalf("status %s", s)
	}
	if got := f.eventTypes(t, v.BookingID); got != "booking.created.v1,booking.refund_required.v1,booking.refunded.v1" {
		t.Fatalf("events %s", got)
	}
}

func TestSagaCancelsAndHonoursLateCaptures(t *testing.T) {
	f := newSagaFixture(t)
	v, hold := f.book(t, f.event, 1)
	f.handle(t, message(paymentFailed, &eventsv1.PaymentFailed{BookingId: v.BookingID, Reason: "card_declined"}))
	if s := f.statusOf(t, v.BookingID); s != StatusCancelled || f.inv.holds[hold].State != inventory.StateReleased {
		t.Fatalf("after the failure: %s, hold %s", s, f.inv.holds[hold].State)
	}
	// The buyer paid after all: a late capture, honoured while units remain.
	f.handle(t, captured(v))
	if s := f.statusOf(t, v.BookingID); s != StatusConfirmed || f.inv.holds[hold].State != inventory.StateSold {
		t.Fatalf("late capture: %s, hold %s", s, f.inv.holds[hold].State)
	}
	if got := testutil.ToFloat64(f.m.late.WithLabelValues("confirmed")); got != 1 {
		t.Fatalf("late confirms %v", got)
	}
	if got := testutil.ToFloat64(f.m.settled.WithLabelValues("confirm_late")); got != 1 {
		t.Fatalf("inventory late confirms %v", got)
	}

	// A late capture when nothing is left is refunded.
	tiny, _ := catalog.Create(ctx, f.pool, catalog.NewEvent{Name: "tiny", SaleOpensAt: time.Now(), PerUserLimit: 4, UnitPricePaise: 100, Capacity: 1})
	first, _ := f.book(t, tiny.String(), 1)
	second, _ := f.book(t, tiny.String(), 1)
	f.handle(t, message(paymentExpired, &eventsv1.PaymentExpired{BookingId: second.BookingID}))
	f.handle(t, captured(first))
	f.handle(t, captured(second))
	if s := f.statusOf(t, second.BookingID); s != StatusRefundRequired {
		t.Fatalf("a late capture with no units left: %s", s)
	}
	if got := testutil.ToFloat64(f.m.late.WithLabelValues("refund_required")); got != 1 {
		t.Fatalf("late refunds %v", got)
	}
	if got := f.eventTypes(t, second.BookingID); got != "booking.created.v1,booking.cancelled.v1,booking.refund_required.v1" {
		t.Fatalf("events %s", got)
	}
}

func TestSagaRetriesInventoryAfterCommitting(t *testing.T) {
	f := newSagaFixture(t)
	v, hold := f.book(t, f.event, 1)
	f.inv.confirmFails = 1
	m := captured(v)
	if err := f.saga.Handle(ctx, m); err == nil || kafka.IsPermanent(err) {
		t.Fatalf("inventory unavailable: %v, want a retryable error", err)
	}
	// The decision committed; the retry skips it and settles inventory.
	if s := f.statusOf(t, v.BookingID); s != StatusConfirmed {
		t.Fatalf("status %s", s)
	}
	f.handle(t, m)
	if f.inv.holds[hold].State != inventory.StateSold || f.sold(t, f.event) != 1 {
		t.Fatalf("after the retry: hold %s, sold %d", f.inv.holds[hold].State, f.sold(t, f.event))
	}
}

func TestSagaSkipsWhatItCannotUse(t *testing.T) {
	f := newSagaFixture(t)
	// An unknown booking (a test's, or another environment's: P30) is skipped.
	f.handle(t, captured(View{BookingID: uuid.NewString(), AmountPaise: 100}))
	if got := testutil.ToFloat64(f.m.saga.WithLabelValues("unknown_booking")); got != 1 {
		t.Fatalf("unknown bookings %v", got)
	}
	// Other event types are not the saga's.
	f.handle(t, message("payment.something.v9", &eventsv1.PaymentFailed{}))
	// Unreadable messages go to the dead-letter topic.
	bad := kafka.Message{Value: []byte("not protobuf"), Headers: map[string]string{kafka.HeaderID: "x", kafka.HeaderType: paymentCaptured}}
	if err := f.saga.Handle(ctx, bad); !kafka.IsPermanent(err) {
		t.Fatalf("garbage: %v, want permanent", err)
	}
	if err := f.saga.Handle(ctx, message(paymentCaptured, &eventsv1.PaymentCaptured{BookingId: "nope"})); !kafka.IsPermanent(err) {
		t.Fatalf("a bad booking ID: %v, want permanent", err)
	}
	noID := message(paymentCaptured, &eventsv1.PaymentCaptured{BookingId: uuid.NewString()})
	delete(noID.Headers, kafka.HeaderID)
	if err := f.saga.Handle(ctx, noID); !kafka.IsPermanent(err) {
		t.Fatalf("no event ID: %v, want permanent", err)
	}
	// An inventory hold that does not exist is for a human, not a retry loop.
	v, hold := f.book(t, f.event, 1)
	delete(f.inv.holds, hold)
	if err := f.saga.Handle(ctx, captured(v)); !kafka.IsPermanent(err) || !errors.Is(err, inventory.ErrHoldNotFound) {
		t.Fatalf("a missing hold: %v, want permanent", err)
	}
}
