// Package booking is booking-svc: a buyer turns a hold into a booking that
// waits for payment, and the booking's life from there to CONFIRMED,
// CANCELLED or REFUNDED (design doc 6.4 and 7.2).
//
// The database enforces the rules a bug could break: one booking per hold,
// and only the state machine's moves (internal/booking/bookingdb, migration
// booking/00002). Every step of creating a booking is idempotent, so a
// retried request resumes where the last attempt stopped instead of doing
// anything twice.
package booking

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/bookingdb"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

// Booking statuses (design doc 7.2).
const (
	StatusPendingPayment = "PENDING_PAYMENT"
	StatusConfirmed      = "CONFIRMED"
	StatusCancelled      = "CANCELLED"
	StatusRefundRequired = "REFUND_REQUIRED"
	StatusRefunded       = "REFUNDED"
)

// ErrNotFound means no booking the caller may see has that ID.
var ErrNotFound = errors.New("booking: not found")

// Inventory is what booking needs from inventory-svc (*inventory.Client).
type Inventory interface {
	GetHold(ctx context.Context, eventID, userID, holdID string) (inventory.Hold, error)
	MarkPaying(ctx context.Context, eventID, userID, holdID string) (time.Time, error)
}

// Intents creates payment intents (payment-svc, *payment.Client). The call
// must be idempotent per booking: called again, it returns the same intent.
type Intents interface {
	CreateIntent(ctx context.Context, bookingID, eventID uuid.UUID, amountPaise int64, expiresAt time.Time) (intentID uuid.UUID, checkoutURL string, err error)
}

// Config tunes the service.
type Config struct {
	// Grace is how long a PAYING hold outlives the booking's payment deadline,
	// so a capture reported a little late still finds its units (design doc
	// 6.1: a 7-minute deadline and 3 minutes' grace in a 10-minute payment
	// window). The deadline is the hold's protected-until time minus Grace.
	Grace time.Duration
}

// Service creates and reads bookings.
type Service struct {
	pool    *pgxpool.Pool
	q       *bookingdb.Queries
	inv     Inventory
	intents Intents // nil: bookings are created without a payment intent
	cfg     Config
	m       *Metrics
	log     *slog.Logger
}

// NewService returns the booking service. intents may be nil: bookings are
// then created without a payment intent (and without a checkout URL).
func NewService(pool *pgxpool.Pool, inv Inventory, intents Intents, cfg Config, m *Metrics, log *slog.Logger) *Service {
	return &Service{pool: pool, q: bookingdb.New(pool), inv: inv, intents: intents, cfg: cfg, m: m, log: log}
}

// CreateRequest is POST /v1/bookings.
type CreateRequest struct {
	UserID         string
	EventID        string
	HoldID         string
	IdempotencyKey string
}

// Result is the HTTP response for a create request: what was answered the
// first time is stored with the idempotency key and answered again, byte for
// byte, to every retry with that key.
type Result struct {
	Code     int
	Body     json.RawMessage
	Replayed bool   // the stored response of an earlier request
	outcome  string // for metrics
}

// View is a booking as the API shows it.
type View struct {
	BookingID       string    `json:"bookingId"`
	EventID         string    `json:"eventId"`
	HoldID          string    `json:"holdId"`
	Quantity        int       `json:"quantity"`
	AmountPaise     int64     `json:"amountPaise"`
	Status          string    `json:"status"`
	PaymentDeadline time.Time `json:"paymentDeadline"`
	IntentID        string    `json:"intentId,omitempty"`
	CheckoutURL     string    `json:"checkoutUrl,omitempty"`
}

func viewOf(b bookingdb.Booking) View {
	v := View{
		BookingID: b.ID.String(), EventID: b.EventID.String(), HoldID: b.HoldID.String(), Quantity: int(b.Qty),
		AmountPaise: b.AmountPaise, Status: b.Status, PaymentDeadline: b.PaymentDeadline.UTC(),
	}
	if b.IntentID.Valid {
		v.IntentID = b.IntentID.UUID.String()
	}
	return v
}

// invalid is a problem with the request itself: never stored with a key.
type invalid struct{ p *httpx.Problem }

func (e invalid) Error() string { return e.p.Error() }

// Problem returns the problem document of a request error, if err is one.
func Problem(err error) (*httpx.Problem, bool) {
	var e invalid
	if errors.As(err, &e) {
		return e.p, true
	}
	return nil, false
}

func parseIDs(req CreateRequest) (user, event, hold uuid.UUID, err error) {
	for _, f := range []struct {
		name string
		v    string
		dst  *uuid.UUID
	}{{"userId", req.UserID, &user}, {"eventId", req.EventID, &event}, {"holdId", req.HoldID, &hold}} {
		id, perr := uuid.Parse(f.v)
		if perr != nil || len(f.v) != 36 {
			return user, event, hold, invalid{httpx.BadRequest("INVALID_REQUEST", f.name+" must be a UUID")}
		}
		*f.dst = id
	}
	k := req.IdempotencyKey
	if len(k) < 8 || len(k) > 255 || strings.ContainsFunc(k, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return user, event, hold, invalid{httpx.BadRequest("INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must be 8 to 255 printable ASCII characters")}
	}
	return user, event, hold, nil
}

// requestHash identifies what a key was first used for, so reusing the key
// for a different request is refused instead of answered with the wrong
// booking.
func requestHash(event, hold uuid.UUID) []byte {
	h := sha256.Sum256([]byte("POST /v1/bookings\n" + event.String() + "\n" + hold.String()))
	return h[:]
}

// Create books a hold for a buyer, idempotently (design doc 9.5). It returns
// the response to send, or an error for a failure the client should retry
// with the same key (the key stays in progress, and the retry resumes).
func (s *Service) Create(ctx context.Context, req CreateRequest) (Result, error) {
	user, event, hold, err := parseIDs(req)
	if err != nil {
		return Result{}, err
	}
	hash := requestHash(event, hold)
	_, err = s.q.BeginIdempotentRequest(ctx, bookingdb.BeginIdempotentRequestParams{UserID: user, IdemKey: req.IdempotencyKey, RequestHash: hash})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		key, err := s.q.GetIdempotencyKey(ctx, bookingdb.GetIdempotencyKeyParams{UserID: user, IdemKey: req.IdempotencyKey})
		if err != nil {
			return Result{}, fmt.Errorf("booking: read idempotency key: %w", err)
		}
		if !bytes.Equal(key.RequestHash, hash) {
			s.m.create(resultKeyReused)
			return Result{}, invalid{httpx.Unprocessable("IDEMPOTENCY_KEY_REUSED", "this Idempotency-Key was used for a different request")}
		}
		if key.Status == "COMPLETED" {
			s.m.create(resultReplayed)
			return Result{Code: int(key.ResponseCode.Int16), Body: key.ResponseBody, Replayed: true}, nil
		}
		s.m.resumed.Inc() // an earlier attempt stopped part-way: carry on from there
	case err != nil:
		return Result{}, fmt.Errorf("booking: claim idempotency key: %w", err)
	}

	res, bookingID, err := s.create(ctx, user, event, hold)
	if err != nil {
		s.m.create(resultError)
		return Result{}, err
	}
	s.m.create(res.outcome)
	var bid uuid.NullUUID
	if bookingID != uuid.Nil {
		bid = uuid.NullUUID{UUID: bookingID, Valid: true}
	}
	stored, err := s.q.CompleteIdempotentRequest(ctx, bookingdb.CompleteIdempotentRequestParams{
		UserID: user, IdemKey: req.IdempotencyKey, BookingID: bid,
		ResponseCode: pgtype.Int2{Int16: int16(res.Code), Valid: true}, ResponseBody: res.Body, //nolint:gosec // an HTTP status
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent request with the same key finished first: answer
		// exactly what it answered.
		key, err := s.q.GetIdempotencyKey(ctx, bookingdb.GetIdempotencyKeyParams{UserID: user, IdemKey: req.IdempotencyKey})
		if err != nil {
			return Result{}, fmt.Errorf("booking: read idempotency key: %w", err)
		}
		return Result{Code: int(key.ResponseCode.Int16), Body: key.ResponseBody, Replayed: true}, nil
	}
	if err != nil {
		return Result{}, fmt.Errorf("booking: store response: %w", err)
	}
	// Answer with the body as stored (jsonb normalises it), so the first
	// response and every replay are the same bytes.
	res.Body = stored.ResponseBody
	return res, nil
}

// create is the saga; every step is idempotent, so running it again after a
// partial failure finishes it. It returns a definitive response (stored with
// the key), or an error for a transient failure.
func (s *Service) create(ctx context.Context, user, event, hold uuid.UUID) (Result, uuid.UUID, error) {
	// Already booked (by an earlier attempt, or a concurrent one)?
	b, err := s.q.GetBookingByHold(ctx, hold)
	switch {
	case err == nil:
		if b.UserID != user || b.EventID != event {
			return s.problem(resultHoldNotFound, httpx.NotFound("HOLD_NOT_FOUND", "no such hold")), uuid.Nil, nil
		}
		return s.finish(ctx, b)
	case !errors.Is(err, pgx.ErrNoRows):
		return Result{}, uuid.Nil, fmt.Errorf("booking: read booking: %w", err)
	}

	ev, err := catalog.Get(ctx, s.pool, event)
	if errors.Is(err, catalog.ErrNotFound) {
		return s.problem(resultEventNotFound, httpx.NotFound("EVENT_NOT_FOUND", "no such event")), uuid.Nil, nil
	}
	if err != nil {
		return Result{}, uuid.Nil, err
	}
	h, err := s.inv.GetHold(ctx, event.String(), user.String(), hold.String())
	if r, ok := s.holdProblem(err); ok {
		return r, uuid.Nil, nil
	}
	if err != nil {
		return Result{}, uuid.Nil, fmt.Errorf("booking: get hold: %w", err)
	}
	if h.State != inventory.StateHeld && h.State != inventory.StatePaying {
		return s.problem(resultHoldNotAvailable, httpx.Conflict("HOLD_NOT_AVAILABLE", "the hold has been released or sold")), uuid.Nil, nil
	}
	// Protect the hold for the payment window before the booking exists: if
	// this succeeds and the insert below fails, a retry finds the hold PAYING
	// and carries on; if the buyer never retries, the window simply runs out.
	protected, err := s.inv.MarkPaying(ctx, event.String(), user.String(), hold.String())
	if r, ok := s.holdProblem(err); ok {
		return r, uuid.Nil, nil
	}
	if err != nil {
		return Result{}, uuid.Nil, fmt.Errorf("booking: protect hold: %w", err)
	}

	deadline := protected.Add(-s.cfg.Grace)
	b, err = s.insert(ctx, bookingdb.CreateBookingParams{
		ID: uuid.Must(uuid.NewV7()), EventID: event, UserID: user, HoldID: hold,
		Qty: int16(h.Quantity), AmountPaise: int64(h.Quantity) * ev.UnitPricePaise, //nolint:gosec // 1 to 10
		PaymentDeadline: deadline,
	})
	if err != nil {
		return Result{}, uuid.Nil, err
	}
	return s.finish(ctx, b)
}

// insert writes the booking and its booking.created event in one
// transaction. A concurrent request that booked the same hold first wins;
// its booking is returned.
func (s *Service) insert(ctx context.Context, p bookingdb.CreateBookingParams) (bookingdb.Booking, error) {
	var b bookingdb.Booking
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.q.WithTx(tx)
		var err error
		if b, err = q.CreateBooking(ctx, p); err != nil {
			return err
		}
		return writeEvent(ctx, q, b.ID, eventCreated, createdEvent(b))
	})
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" { // unique hold_id: someone else booked it a moment ago
		return s.q.GetBookingByHold(ctx, p.HoldID)
	}
	if err != nil {
		return bookingdb.Booking{}, fmt.Errorf("booking: insert: %w", err)
	}
	s.m.created.Inc()
	return b, nil
}

// finish creates (or fetches again) the payment intent and answers 201.
func (s *Service) finish(ctx context.Context, b bookingdb.Booking) (Result, uuid.UUID, error) {
	v := viewOf(b)
	if s.intents != nil && b.Status == StatusPendingPayment {
		intent, url, err := s.intents.CreateIntent(ctx, b.ID, b.EventID, b.AmountPaise, b.PaymentDeadline)
		if err != nil {
			return Result{}, uuid.Nil, fmt.Errorf("booking: create payment intent: %w", err)
		}
		if _, err := s.q.SetBookingIntent(ctx, bookingdb.SetBookingIntentParams{ID: b.ID, IntentID: uuid.NullUUID{UUID: intent, Valid: true}}); err != nil {
			return Result{}, uuid.Nil, fmt.Errorf("booking: record payment intent: %w", err)
		}
		v.IntentID, v.CheckoutURL = intent.String(), url
	}
	body, err := json.Marshal(v)
	if err != nil {
		return Result{}, uuid.Nil, err
	}
	return Result{Code: http.StatusCreated, Body: body, outcome: resultCreated}, b.ID, nil
}

// holdProblem turns inventory's definitive answers into responses.
func (s *Service) holdProblem(err error) (Result, bool) {
	switch {
	case err == nil:
		return Result{}, false
	case errors.Is(err, inventory.ErrHoldNotFound), errors.Is(err, inventory.ErrEventNotProvisioned):
		return s.problem(resultHoldNotFound, httpx.NotFound("HOLD_NOT_FOUND", "no such hold")), true
	case errors.Is(err, inventory.ErrHoldExpired):
		return s.problem(resultHoldNotAvailable, httpx.Conflict("HOLD_NOT_AVAILABLE", "the hold has expired or been released")), true
	case errors.Is(err, inventory.ErrInvalidRequest):
		return s.problem(resultHoldNotFound, httpx.NotFound("HOLD_NOT_FOUND", "no such hold")), true
	}
	return Result{}, false
}

func (s *Service) problem(outcome string, p *httpx.Problem) Result {
	body, _ := json.Marshal(p)
	return Result{Code: p.Status, Body: body, outcome: outcome}
}

// Get returns a booking owned by userID; anyone else's is ErrNotFound, so
// booking IDs cannot be probed.
func (s *Service) Get(ctx context.Context, userID, bookingID string) (View, error) {
	user, err := uuid.Parse(userID)
	if err != nil {
		return View{}, ErrNotFound
	}
	id, err := uuid.Parse(bookingID)
	if err != nil || len(bookingID) != 36 {
		return View{}, ErrNotFound
	}
	b, err := s.q.GetBookingForUser(ctx, bookingdb.GetBookingForUserParams{ID: id, UserID: user})
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, ErrNotFound
	}
	if err != nil {
		return View{}, fmt.Errorf("booking: get: %w", err)
	}
	return viewOf(b), nil
}
