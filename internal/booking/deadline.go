package booking

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
)

// DeadlineJob cancels bookings whose payment deadline has passed, writing a
// booking.cancelled event for each, and deletes idempotency keys older than a
// day. It runs in every replica: FOR UPDATE SKIP LOCKED gives concurrent
// passes disjoint batches, so none needs to lead.
type DeadlineJob struct {
	pool     *pgxpool.Pool
	q        *bookingdb.Queries
	interval time.Duration
	batch    int32
	m        *Metrics
	log      *slog.Logger
	lastKeys time.Time
}

// NewDeadlineJob returns the job; it scans every interval, batch bookings at
// a time.
func NewDeadlineJob(pool *pgxpool.Pool, interval time.Duration, batch int, m *Metrics, log *slog.Logger) *DeadlineJob {
	return &DeadlineJob{pool: pool, q: bookingdb.New(pool), interval: interval, batch: int32(batch), m: m, log: log} //nolint:gosec // validated in config
}

// Name implements app.Component.
func (j *DeadlineJob) Name() string { return "booking-deadlines" }

// Run implements app.Component.
func (j *DeadlineJob) Run(ctx context.Context) error {
	t := time.NewTicker(j.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := j.Pass(ctx); err != nil && ctx.Err() == nil {
			j.m.runs.WithLabelValues("error").Inc()
			j.log.Warn("booking deadlines: pass failed", "err", err)
			continue
		}
		j.m.runs.WithLabelValues("ok").Inc()
	}
}

// Pass cancels every overdue booking, a batch per transaction, and once an
// hour deletes idempotency keys older than 24 hours.
func (j *DeadlineJob) Pass(ctx context.Context) error {
	for {
		n, err := j.expireBatch(ctx)
		if err != nil {
			return err
		}
		if n < int(j.batch) {
			break
		}
	}
	if time.Since(j.lastKeys) >= time.Hour {
		n, err := j.q.DeleteIdempotencyKeysBefore(ctx, time.Now().Add(-24*time.Hour))
		if err != nil {
			return fmt.Errorf("booking: delete old idempotency keys: %w", err)
		}
		j.lastKeys = time.Now()
		if n > 0 {
			j.log.Info("booking deadlines: deleted old idempotency keys", "count", n)
		}
	}
	return nil
}

func (j *DeadlineJob) expireBatch(ctx context.Context) (int, error) {
	// Each batch gets its own trace, carried by the events it writes.
	ctx, span := otel.Tracer("github.com/Sanjay-Mx21/holdfast/internal/booking").Start(ctx, "booking.expire_overdue")
	defer span.End()
	var n int
	err := pgx.BeginFunc(ctx, j.pool, func(tx pgx.Tx) error {
		q := j.q.WithTx(tx)
		overdue, err := q.ClaimExpiredBookings(ctx, j.batch)
		if err != nil {
			return err
		}
		for _, b := range overdue {
			cancelled, err := q.TransitionBooking(ctx, bookingdb.TransitionBookingParams{ID: b.ID, FromStatus: StatusPendingPayment, ToStatus: StatusCancelled})
			if errors.Is(err, pgx.ErrNoRows) {
				continue // moved meanwhile (a capture won the race): leave it
			}
			if err != nil {
				return err
			}
			if err := writeEvent(ctx, q, cancelled.ID, eventCancelled, cancelledEvent(cancelled, eventsv1.CancellationReason_CANCELLATION_REASON_PAYMENT_EXPIRED)); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("booking: expire overdue bookings: %w", err)
	}
	j.m.expired.Add(float64(n))
	return n, nil
}
