//go:build integration

package bookingdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

type fixture struct {
	pool  *pgxpool.Pool
	q     *Queries
	event uuid.UUID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := testenv.Postgres(t)
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "booking schema test", SaleOpensAt: time.Now(), PerUserLimit: 4, UnitPricePaise: 1000, Capacity: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{pool: pool, q: New(pool), event: id}
}

func (f *fixture) booking(t *testing.T, user uuid.UUID, deadline time.Time) Booking {
	t.Helper()
	b, err := f.q.CreateBooking(ctx, CreateBookingParams{
		ID: uuid.Must(uuid.NewV7()), EventID: f.event, UserID: user, HoldID: uuid.New(),
		Qty: 2, AmountPaise: 2000, PaymentDeadline: deadline,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func TestBookingStartsPendingAndIsReadOnlyByItsOwner(t *testing.T) {
	f := newFixture(t)
	user := uuid.New()
	b := f.booking(t, user, time.Now().Add(10*time.Minute))
	if b.Status != "PENDING_PAYMENT" || b.Version != 0 || b.IntentID.Valid {
		t.Fatalf("new booking %+v", b)
	}
	if got, err := f.q.GetBookingForUser(ctx, GetBookingForUserParams{ID: b.ID, UserID: user}); err != nil || got.ID != b.ID {
		t.Fatalf("owner's read: %v", err)
	}
	if _, err := f.q.GetBookingForUser(ctx, GetBookingForUserParams{ID: b.ID, UserID: uuid.New()}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("someone else's read: %v, want no rows", err)
	}
	// One booking per hold.
	_, err := f.q.CreateBooking(ctx, CreateBookingParams{
		ID: uuid.Must(uuid.NewV7()), EventID: f.event, UserID: user, HoldID: b.HoldID,
		Qty: 1, AmountPaise: 1000, PaymentDeadline: time.Now().Add(time.Minute),
	})
	if pgCode(err) != "23505" {
		t.Fatalf("a second booking for the same hold: %v, want a unique violation", err)
	}
	if got, err := f.q.GetBookingByHold(ctx, b.HoldID); err != nil || got.ID != b.ID {
		t.Fatalf("by hold: %v", err)
	}
}

func TestStatusMovesFollowTheStateMachine(t *testing.T) {
	f := newFixture(t)
	move := func(id uuid.UUID, from, to string) (Booking, error) {
		return f.q.TransitionBooking(ctx, TransitionBookingParams{ID: id, FromStatus: from, ToStatus: to})
	}

	// A late capture: cancelled for want of payment, then confirmed.
	b := f.booking(t, uuid.New(), time.Now().Add(time.Minute))
	b, err := move(b.ID, "PENDING_PAYMENT", "CANCELLED")
	if err != nil || b.Version != 1 {
		t.Fatalf("PENDING_PAYMENT -> CANCELLED: %v (version %d)", err, b.Version)
	}
	if b, err = move(b.ID, "CANCELLED", "CONFIRMED"); err != nil || b.Version != 2 {
		t.Fatalf("CANCELLED -> CONFIRMED: %v", err)
	}
	// Compare-and-set: a stale "from" changes nothing.
	if _, err := move(b.ID, "PENDING_PAYMENT", "CANCELLED"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("a move from a stale status: %v, want no rows", err)
	}
	// The trigger refuses moves the state machine does not have.
	for _, bad := range []string{"CANCELLED", "REFUNDED", "PENDING_PAYMENT", "REFUND_REQUIRED"} {
		if _, err := move(b.ID, "CONFIRMED", bad); pgCode(err) != "23514" {
			t.Fatalf("CONFIRMED -> %s: %v, want a check violation", bad, err)
		}
	}

	// Refunds: required, then done, then final.
	r := f.booking(t, uuid.New(), time.Now().Add(time.Minute))
	if _, err := move(r.ID, "PENDING_PAYMENT", "REFUND_REQUIRED"); err != nil {
		t.Fatal(err)
	}
	if _, err := move(r.ID, "REFUND_REQUIRED", "CONFIRMED"); pgCode(err) != "23514" {
		t.Fatalf("REFUND_REQUIRED -> CONFIRMED: %v, want a check violation", err)
	}
	if _, err := move(r.ID, "REFUND_REQUIRED", "REFUNDED"); err != nil {
		t.Fatal(err)
	}
	if _, err := move(r.ID, "REFUNDED", "REFUND_REQUIRED"); pgCode(err) != "23514" {
		t.Fatalf("REFUNDED -> REFUND_REQUIRED: %v, want a check violation", err)
	}
	// Even a raw UPDATE cannot skip the machine.
	if _, err := f.pool.Exec(ctx, `UPDATE booking.bookings SET status = 'PENDING_PAYMENT' WHERE id = $1`, r.ID); pgCode(err) != "23514" {
		t.Fatalf("raw UPDATE REFUNDED -> PENDING_PAYMENT: %v, want a check violation", err)
	}
}

func TestIntentIsRecordedOnce(t *testing.T) {
	f := newFixture(t)
	b := f.booking(t, uuid.New(), time.Now().Add(time.Minute))
	intent := uuid.NullUUID{UUID: uuid.New(), Valid: true}
	if _, err := f.q.SetBookingIntent(ctx, SetBookingIntentParams{ID: b.ID, IntentID: intent}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.q.SetBookingIntent(ctx, SetBookingIntentParams{ID: b.ID, IntentID: intent}); err != nil {
		t.Fatalf("the same intent again: %v", err)
	}
	other := uuid.NullUUID{UUID: uuid.New(), Valid: true}
	if _, err := f.q.SetBookingIntent(ctx, SetBookingIntentParams{ID: b.ID, IntentID: other}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("a different intent: %v, want no rows", err)
	}
}

// claimIn runs claim in its own transaction and keeps it open until release
// is closed, so two claims overlap like two replicas would.
func claimIn[T any](t *testing.T, pool *pgxpool.Pool, claim func(q *Queries) ([]T, error), release <-chan struct{}) <-chan []T {
	t.Helper()
	out := make(chan []T, 1)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := claim(New(tx))
	if err != nil {
		t.Fatal(err)
	}
	out <- rows
	go func() { <-release; _ = tx.Rollback(ctx) }()
	return out
}

func TestConcurrentDeadlineJobsTakeDisjointBatches(t *testing.T) {
	f := newFixture(t)
	testenv.Exclusive(t, f.pool, "booking-deadline")
	past := time.Now().Add(-time.Minute)
	expired := map[uuid.UUID]bool{}
	for range 3 {
		expired[f.booking(t, uuid.New(), past).ID] = true
	}
	f.booking(t, uuid.New(), time.Now().Add(time.Hour)) // not due

	t.Cleanup(func() { // leave nothing expired behind for later runs
		for id := range expired {
			_, _ = f.q.TransitionBooking(ctx, TransitionBookingParams{ID: id, FromStatus: "PENDING_PAYMENT", ToStatus: "CANCELLED"})
		}
	})

	// Two jobs at once: the first takes a batch of 2, the second everything
	// left. Bookings that earlier runs left expired may be in either.
	release := make(chan struct{})
	defer close(release)
	a := <-claimIn(t, f.pool, func(q *Queries) ([]Booking, error) { return q.ClaimExpiredBookings(ctx, 2) }, release)
	b := <-claimIn(t, f.pool, func(q *Queries) ([]Booking, error) { return q.ClaimExpiredBookings(ctx, 100_000) }, release)
	if len(a) != 2 {
		t.Fatalf("the first job claimed %d, want its batch of 2", len(a))
	}
	seen := map[uuid.UUID]bool{}
	for _, bk := range append(a, b...) {
		if seen[bk.ID] {
			t.Fatalf("booking %s claimed twice", bk.ID)
		}
		seen[bk.ID] = true
	}
	// Other tests' expired bookings may be claimed too; ours must all be.
	for id := range expired {
		if !seen[id] {
			t.Fatalf("expired booking %s was not claimed (got %d and %d)", id, len(a), len(b))
		}
	}
}

func TestIdempotencyKeys(t *testing.T) {
	f := newFixture(t)
	user, key := uuid.New(), "booking-key-0001"
	hash := make([]byte, 32)
	first, err := f.q.BeginIdempotentRequest(ctx, BeginIdempotentRequestParams{UserID: user, IdemKey: key, RequestHash: hash})
	if err != nil || first.Status != "IN_PROGRESS" {
		t.Fatalf("first request: %v %+v", err, first)
	}
	if _, err := f.q.BeginIdempotentRequest(ctx, BeginIdempotentRequestParams{UserID: user, IdemKey: key, RequestHash: hash}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("a retry with the same key: %v, want no rows", err)
	}
	b := f.booking(t, user, time.Now().Add(time.Minute))
	done, err := f.q.CompleteIdempotentRequest(ctx, CompleteIdempotentRequestParams{
		UserID: user, IdemKey: key, BookingID: uuid.NullUUID{UUID: b.ID, Valid: true},
		ResponseCode: pgInt2(201), ResponseBody: []byte(`{"bookingId":"x"}`),
	})
	if err != nil || done.Status != "COMPLETED" {
		t.Fatalf("complete: %v", err)
	}
	if _, err := f.q.CompleteIdempotentRequest(ctx, CompleteIdempotentRequestParams{
		UserID: user, IdemKey: key, ResponseCode: pgInt2(500), ResponseBody: []byte(`{}`),
	}); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("completing twice: %v, want no rows", err)
	}
	got, err := f.q.GetIdempotencyKey(ctx, GetIdempotencyKeyParams{UserID: user, IdemKey: key})
	if err != nil || got.ResponseCode.Int16 != 201 {
		t.Fatalf("stored response: %v %+v", err, got)
	}
	// A key shorter than 8 characters is refused by the schema.
	if _, err := f.q.BeginIdempotentRequest(ctx, BeginIdempotentRequestParams{UserID: user, IdemKey: "short", RequestHash: hash}); pgCode(err) != "23514" {
		t.Fatalf("a 5-character key: %v, want a check violation", err)
	}
}

func TestOutboxAndProcessedMessages(t *testing.T) {
	f := newFixture(t)
	agg := uuid.New()
	ids := map[uuid.UUID]bool{}
	for range 3 {
		id := uuid.Must(uuid.NewV7())
		ids[id] = true
		if err := f.q.InsertOutboxEvent(ctx, InsertOutboxEventParams{
			EventID: id, Topic: "holdfast.booking.v1", AggregateID: agg, EventType: "booking.created.v1",
			Payload: []byte{1}, Headers: []byte(`{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Two relays at once take disjoint batches.
	release := make(chan struct{})
	claim := func(q *Queries) ([]OutboxEvent, error) { return q.ClaimUnpublishedEvents(ctx, 1000) }
	a := <-claimIn(t, f.pool, claim, release)
	b := <-claimIn(t, f.pool, claim, release)
	close(release)
	ours := 0
	for _, e := range append(a, b...) {
		if ids[e.EventID] {
			ours++
		}
	}
	if ours != 3 {
		t.Fatalf("our 3 events claimed %d times in total, want exactly 3", ours)
	}

	// Publish them; they are not claimed again.
	var rowIDs []int64
	for _, e := range a {
		rowIDs = append(rowIDs, e.ID)
	}
	if err := f.q.MarkEventsPublished(ctx, rowIDs); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let the second claim's rollback land
	left, err := f.q.ClaimUnpublishedEvents(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range left {
		for _, id := range rowIDs {
			if e.ID == id {
				t.Fatalf("published event %d claimed again", id)
			}
		}
	}

	// Leave nothing of ours unpublished for later runs.
	var all []int64
	for _, e := range append(a, b...) {
		if ids[e.EventID] {
			all = append(all, e.ID)
		}
	}
	if err := f.q.MarkEventsPublished(ctx, all); err != nil {
		t.Fatal(err)
	}

	// A consumer applies a message once.
	consumer, msg := "booking-svc.payments", uuid.NewString()
	if n, err := f.q.RecordProcessedMessage(ctx, RecordProcessedMessageParams{Consumer: consumer, MessageID: msg}); err != nil || n != 1 {
		t.Fatalf("first delivery: %d %v", n, err)
	}
	if n, err := f.q.RecordProcessedMessage(ctx, RecordProcessedMessageParams{Consumer: consumer, MessageID: msg}); err != nil || n != 0 {
		t.Fatalf("redelivery: %d %v, want 0 rows", n, err)
	}
}

func pgInt2(v int16) pgtype.Int2 { return pgtype.Int2{Int16: v, Valid: true} }
