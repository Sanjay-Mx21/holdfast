// Package payment is payment-svc: payment intents, the provider's orders,
// its webhooks, the double-entry ledger, and status polling for intents
// whose webhook never came (design doc 6.4, 7.3, 9.8).
//
// Money rules live in the database (internal/payment/paymentdb, migrations
// payment/00001 onward): one intent per booking, only the 7.3 moves, and
// ledger transactions that balance. Every change and its event are written in
// one transaction; the outbox relay publishes the events.
package payment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

// Errors.
var (
	ErrInvalidRequest  = errors.New("payment: invalid request")
	ErrIntentConflict  = errors.New("payment: the booking already has an intent for another amount")
	ErrPSPUnavailable  = errors.New("payment: the payment provider is unavailable")
	errMissingEventRef = errors.New("payment: intent has no event ID for the ledger")
)

// Provider is what payment-svc needs from the payment provider (*psp.Client).
type Provider interface {
	CreateOrder(ctx context.Context, intentID string, req psp.CreateOrder) (psp.Order, error)
	GetOrder(ctx context.Context, orderID string) (psp.Order, error)
	CreateRefund(ctx context.Context, intentID, paymentID string, amountPaise int64) (psp.Refund, error)
}

// Service manages payment intents.
type Service struct {
	pool *pgxpool.Pool
	q    *paymentdb.Queries
	psp  Provider
	m    *Metrics
	log  *slog.Logger
}

// NewService returns the payment service.
func NewService(pool *pgxpool.Pool, provider Provider, m *Metrics, log *slog.Logger) *Service {
	return &Service{pool: pool, q: paymentdb.New(pool), psp: provider, m: m, log: log}
}

// Intent is what booking-svc gets back.
type Intent struct {
	ID          uuid.UUID
	CheckoutURL string
}

// CreateIntent returns the booking's intent and checkout URL, creating the
// intent (one per booking) and the provider's order (keyed by the intent ID)
// the first time. Every step is idempotent, so a retry after any failure
// finishes the job and returns the same answer.
func (s *Service) CreateIntent(ctx context.Context, bookingID, eventID uuid.UUID, amountPaise int64, expiresAt time.Time) (Intent, error) {
	if bookingID == uuid.Nil || eventID == uuid.Nil || amountPaise <= 0 || expiresAt.IsZero() {
		return Intent{}, fmt.Errorf("%w: booking, event, a positive amount and an expiry are required", ErrInvalidRequest)
	}
	in, err := s.q.CreateIntent(ctx, paymentdb.CreateIntentParams{
		ID: uuid.Must(uuid.NewV7()), BookingID: bookingID, EventID: uuid.NullUUID{UUID: eventID, Valid: true},
		AmountPaise: amountPaise, ExpiresAt: expiresAt,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if in, err = s.q.GetIntentByBooking(ctx, bookingID); err != nil {
			return Intent{}, fmt.Errorf("payment: read intent: %w", err)
		}
		if in.AmountPaise != amountPaise {
			return Intent{}, ErrIntentConflict
		}
	case err != nil:
		return Intent{}, fmt.Errorf("payment: create intent: %w", err)
	default:
		s.m.intents.Inc()
	}
	if in.PspOrderID.Valid && in.CheckoutUrl.Valid {
		return Intent{ID: in.ID, CheckoutURL: in.CheckoutUrl.String}, nil
	}

	order, err := s.psp.CreateOrder(ctx, in.ID.String(), psp.CreateOrder{
		AmountPaise: in.AmountPaise, Currency: in.Currency, ExpiresAt: in.ExpiresAt, Reference: in.ID.String(),
	})
	if errors.Is(err, psp.ErrUnavailable) {
		return Intent{}, fmt.Errorf("%w: %w", ErrPSPUnavailable, err)
	}
	if err != nil {
		return Intent{}, fmt.Errorf("payment: create order: %w", err)
	}
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		if err := q.InsertPSPOrder(ctx, paymentdb.InsertPSPOrderParams{
			PspOrderID: order.OrderID, IntentID: in.ID, AmountPaise: in.AmountPaise,
			CheckoutUrl: order.CheckoutURL, ExpiresAt: in.ExpiresAt,
		}); err != nil {
			return err
		}
		_, err := q.AttachOrder(ctx, paymentdb.AttachOrderParams{
			ID: in.ID, PspOrderID: text(order.OrderID), CheckoutUrl: text(order.CheckoutURL),
		})
		return err
	})
	if err != nil {
		return Intent{}, fmt.Errorf("payment: record order: %w", err)
	}
	return Intent{ID: in.ID, CheckoutURL: order.CheckoutURL}, nil
}

func text(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }
