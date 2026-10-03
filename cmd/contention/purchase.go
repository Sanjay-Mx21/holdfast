package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sanjay-Mx21/holdfast/db/migrations"
	"github.com/Sanjay-Mx21/holdfast/internal/booking"
	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/mockpsp"
	"github.com/Sanjay-Mx21/holdfast/internal/payment"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/config"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/postgres"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/valkey"
)

// intents adapts payment-svc's service to booking-svc's Intents, as the
// gRPC client does across the network.
type intents struct{ svc *payment.Service }

func (a intents) CreateIntent(ctx context.Context, bookingID, eventID uuid.UUID, amount int64, expiresAt time.Time) (uuid.UUID, string, error) {
	in, err := a.svc.CreateIntent(ctx, bookingID, eventID, amount, expiresAt)
	return in.ID, in.CheckoutURL, err
}

// runPurchase is E1 part B: whole purchases, from hold to sale, through the
// real services in one process. Buyers race for holds (Valkey Lua), book
// them (booking-svc: PostgreSQL, PAYING holds, payment intents), and pay at
// an in-process mockpsp that fails a share of payments; its signed webhooks
// reach payment-svc's handler. The payment events are then delivered to the
// booking saga twice each, as an at-least-once broker may. Every booking
// must end confirmed or cancelled, and the stores must agree on what sold.
func runPurchase(ctx context.Context, o options) (*Report, error) {
	if o.dsn == "" {
		return nil, errors.New("-mode purchase needs -dsn or POSTGRES_DSN")
	}
	// Its own database, <name>_e1: a running stack's outbox relays and saga
	// would otherwise act on these purchases mid-run (releasing declined
	// holds early, for one).
	dsn, err := postgres.SiblingDatabase(ctx, o.dsn, "_e1")
	if err != nil {
		return nil, err
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	// 32 connections: E1 must fit beside the running stack in PostgreSQL's 100.
	workers := min(o.concurrency, 32)
	pool, err := postgres.NewPool(ctx, config.Postgres{
		DSN: dsn, MaxConns: int32(workers) + 8, MinConns: 0, MaxConnLifetime: time.Hour, //nolint:gosec // bounded above
		MaxConnIdleTime: time.Minute, StatementTimeout: 30 * time.Second,
	}, "holdfast-contention")
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	for _, s := range migrations.All() {
		if _, err := postgres.Migrate(ctx, pool, s.Name, s.Files, quiet); err != nil {
			return nil, err
		}
	}
	rdb, err := valkey.New(ctx, config.Valkey{
		Addrs: strings.Split(o.valkeyAddrs, ","), PoolSize: workers * 2,
		DialTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
	}, "holdfast-contention")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rdb.Close() }()

	reg := prometheus.NewRegistry()
	store := inventory.NewStore(rdb)
	inv, err := inventory.NewService(store, inventory.Config{HoldTTL: 10 * time.Minute, PaymentWindow: 15 * time.Minute}, inventory.NewMetrics(reg))
	if err != nil {
		return nil, err
	}
	const pricePaise = 100
	eventUUID, err := catalog.Create(ctx, pool, catalog.NewEvent{
		Name: "E1 purchases", SaleOpensAt: time.Now(), PerUserLimit: o.perUser, UnitPricePaise: pricePaise, Capacity: o.capacity,
	})
	if err != nil {
		return nil, err
	}
	eventID := eventUUID.String()
	if _, err := inv.Provision(ctx, eventID, inventory.EventConfig{Capacity: o.capacity, PerUserLimit: o.perUser}); err != nil {
		return nil, err
	}
	if !o.keep {
		defer func() { _ = store.Purge(context.WithoutCancel(ctx), eventID) }()
	}

	// mockpsp, failing a share of payments, and payment-svc receiving its
	// webhooks, both in this process over loopback HTTP.
	secret := randomSecret()
	faults := mockpsp.NewInjector(1)
	if err := faults.Set(mockpsp.Faults{FailureRate: o.payFailure}); err != nil {
		return nil, err
	}
	pm := mockpsp.NewMetrics(reg)
	hooksRouter := httpx.NewRouter()
	hookSrv := httptest.NewServer(hooksRouter)
	defer hookSrv.Close()
	dispatcher := mockpsp.NewDispatcher(mockpsp.DispatcherConfig{
		URL: hookSrv.URL + "/v1/webhooks/psp", Secret: secret, RetryBase: 50 * time.Millisecond, Concurrency: workers,
	}, faults, pm, quiet)
	dctx, stopHooks := context.WithCancel(context.WithoutCancel(ctx))
	hooksDone := make(chan struct{})
	go func() { _ = dispatcher.Run(dctx); close(hooksDone) }()
	defer func() { stopHooks(); <-hooksDone }()
	pspRouter := httpx.NewRouter()
	pspSrv := httptest.NewServer(pspRouter)
	defer pspSrv.Close()
	provider := mockpsp.New(mockpsp.Config{APIKey: "e1"}, mockpsp.NewStore(pspSrv.URL), faults, dispatcher, pm, quiet)
	provider.Register(pspRouter, httpx.NewRouter(), authn.RequireStaticToken(string(secret)))
	client, err := psp.New(psp.Config{BaseURL: pspSrv.URL, APIKey: "e1", Timeout: 10 * time.Second, BreakerThreshold: 1_000_000}, psp.NewMetrics(reg))
	if err != nil {
		return nil, err
	}
	payments := payment.NewService(pool, client, payment.NewMetrics(reg), quiet)
	payment.NewWebhookHandler(payments, secret, 5*time.Minute).Register(hooksRouter)

	bm := booking.NewMetrics(reg)
	bookings := booking.NewService(pool, inv, intents{payments}, booking.Config{Grace: 3 * time.Minute}, bm, quiet)
	saga := booking.NewSaga(pool, inv, bm, quiet)

	// 1. The purchases.
	type result struct {
		outcome string
		booking string
		hold    string
		user    string
		err     error
	}
	results := make([]result, o.purchases)
	latencies := make([]time.Duration, o.purchases)
	start := time.Now()
	parallel(o.purchases, workers, func(i int) {
		t0 := time.Now()
		defer func() { latencies[i] = time.Since(t0) }()
		user := uuid.NewString()
		h, err := inv.CreateHold(ctx, inventory.CreateHoldRequest{
			EventID: eventID, UserID: user, IdempotencyKey: fmt.Sprintf("e1-purchase-hold-%08d", i), Quantity: o.qty,
		})
		if errors.Is(err, inventory.ErrSoldOut) {
			results[i] = result{outcome: "sold_out"}
			return
		}
		if err != nil {
			results[i] = result{outcome: "error", err: fmt.Errorf("hold: %w", err)}
			return
		}
		res, err := bookings.Create(ctx, booking.CreateRequest{
			UserID: user, EventID: eventID, HoldID: h.Hold.ID, IdempotencyKey: fmt.Sprintf("e1-purchase-book-%08d", i),
		})
		if err != nil || res.Code != 201 {
			results[i] = result{outcome: "error", err: fmt.Errorf("booking: %d %s: %w", res.Code, res.Body, err)}
			return
		}
		var v booking.View
		if err := json.Unmarshal(res.Body, &v); err != nil || v.CheckoutURL == "" {
			results[i] = result{outcome: "error", err: fmt.Errorf("booking without a checkout URL: %s", res.Body)}
			return
		}
		order, err := provider.Pay(path.Base(v.CheckoutURL))
		if err != nil {
			results[i] = result{outcome: "error", err: fmt.Errorf("pay: %w", err)}
			return
		}
		outcome := "paid"
		if order.Status == psp.OrderFailed {
			outcome = "payment_failed"
		}
		results[i] = result{outcome: outcome, booking: v.BookingID, hold: h.Hold.ID, user: user}
	})
	elapsed := time.Since(start)

	r := newReport("purchase", o.purchases, o, elapsed, latencies)
	r.Concurrency = workers
	var ids []uuid.UUID
	var firstErr error
	for _, res := range results {
		r.Outcomes[res.outcome]++
		if res.booking != "" {
			ids = append(ids, uuid.MustParse(res.booking))
		}
		if res.err != nil && firstErr == nil {
			firstErr = res.err
		}
	}

	// 2. Wait for the webhooks to settle every intent.
	if err := waitFor(ctx, 2*time.Minute, func() (bool, error) {
		var open int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM payment.payment_intents WHERE booking_id = ANY($1) AND status = 'CREATED'`, ids).Scan(&open)
		return open == 0, err
	}); err != nil {
		return nil, fmt.Errorf("waiting for webhooks: %w", err)
	}

	// 3. Deliver the payment events to the saga, twice each.
	msgs, err := paymentEvents(ctx, pool, ids)
	if err != nil {
		return nil, err
	}
	var sagaErrs sync.Map
	deliver := func() {
		parallel(len(msgs), workers, func(i int) {
			if err := saga.Handle(ctx, msgs[i]); err != nil {
				sagaErrs.Store(i, err)
			}
		})
	}
	deliver()
	first, err := bookingStates(ctx, pool, eventUUID)
	if err != nil {
		return nil, err
	}
	deliver()
	second, err := bookingStates(ctx, pool, eventUUID)
	if err != nil {
		return nil, err
	}
	sagaErrs.Range(func(_, v any) bool {
		r.Outcomes["saga_error"]++
		if firstErr == nil {
			firstErr = v.(error)
		}
		return true
	})

	// 4. The invariants.
	intentStates := map[string]int{}
	rows, err := pool.Query(ctx, `SELECT status, count(*) FROM payment.payment_intents WHERE booking_id = ANY($1) GROUP BY status`, ids)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		intentStates[s] = n
	}
	rows.Close()
	var receivable int64
	if err := pool.QueryRow(ctx, `SELECT coalesce(sum(CASE l.direction WHEN 'D' THEN l.amount_paise ELSE -l.amount_paise END), 0)
		FROM payment.ledger_entries l JOIN payment.payment_intents i ON i.id = l.intent_id
		WHERE i.booking_id = ANY($1) AND l.account = 'psp_receivable'`, ids).Scan(&receivable); err != nil {
		return nil, err
	}
	ev, err := catalog.Get(ctx, pool, eventUUID)
	if err != nil {
		return nil, err
	}
	avail, err := inv.Availability(ctx, eventID)
	if err != nil {
		return nil, err
	}
	holdStates := map[inventory.HoldState]int{}
	for _, res := range results {
		if res.booking == "" {
			continue
		}
		h, err := inv.GetHold(ctx, eventID, res.user, res.hold)
		if err != nil {
			holdStates["unreadable"]++
			continue
		}
		holdStates[h.State]++
	}

	confirmed, cancelled := second[booking.StatusConfirmed], second[booking.StatusCancelled]
	refunding := second[booking.StatusRefundRequired] + second[booking.StatusRefunded]
	booked := len(ids)
	expectedHolds := min(o.purchases, o.capacity/o.qty)
	r.Outcomes["confirmed"], r.Outcomes["cancelled"], r.Outcomes["refund_required"] = confirmed, cancelled, refunding
	r.check("I1 holds granted == units on sale", r.Outcomes["paid"]+r.Outcomes["payment_failed"] == expectedHolds,
		fmt.Sprintf("held and booked %d, expected %d", booked, expectedHolds))
	r.check("I1 sold == confirmed units <= capacity", ev.Sold == confirmed*o.qty && ev.Sold <= ev.Capacity,
		fmt.Sprintf("PostgreSQL sold %d, confirmed %d bookings of %d units, capacity %d", ev.Sold, confirmed, o.qty, ev.Capacity))
	r.check("every booking ended: confirmed or cancelled", confirmed+cancelled == booked && refunding == 0,
		fmt.Sprintf("%d confirmed + %d cancelled of %d booked, %d refund-required", confirmed, cancelled, booked, refunding))
	r.check("I2 one capture per booking, each confirmed (I3)", intentStates["CAPTURED"] == confirmed && confirmed == r.Outcomes["paid"],
		fmt.Sprintf("%d captured intents, %d paid, %d confirmed", intentStates["CAPTURED"], r.Outcomes["paid"], confirmed))
	r.check("every failed payment cancelled its booking", intentStates["FAILED"] == cancelled && cancelled == r.Outcomes["payment_failed"],
		fmt.Sprintf("%d failed intents, %d declined, %d cancelled", intentStates["FAILED"], r.Outcomes["payment_failed"], cancelled))
	r.check("I5 every hold SOLD or RELEASED", holdStates[inventory.StateSold] == confirmed && holdStates[inventory.StateReleased] == cancelled,
		fmt.Sprintf("SOLD %d, RELEASED %d, other %d", holdStates[inventory.StateSold], holdStates[inventory.StateReleased],
			booked-holdStates[inventory.StateSold]-holdStates[inventory.StateReleased]))
	r.check("Valkey agrees with PostgreSQL on what is left", avail.Available == o.capacity-ev.Sold,
		fmt.Sprintf("Valkey available %d, capacity %d - PostgreSQL sold %d", avail.Available, o.capacity, ev.Sold))
	r.check("ledger: receivable == captured payments", receivable == int64(confirmed*o.qty*pricePaise),
		fmt.Sprintf("psp_receivable %d paise for %d captures of %d paise", receivable, confirmed, o.qty*pricePaise))
	r.check("redelivering every event changed nothing", mapsEqual(first, second),
		fmt.Sprintf("%d events delivered twice; states %v then %v", len(msgs), first, second))
	r.check("payment failures were injected", o.payFailure == 0 || r.Outcomes["payment_failed"] > 0,
		fmt.Sprintf("%d of %d payments failed (rate %.2f)", r.Outcomes["payment_failed"], booked, o.payFailure))
	r.check("no infrastructure errors", r.Outcomes["error"] == 0 && r.Outcomes["saga_error"] == 0, errDetail(firstErr))
	return r, nil
}

// paymentEvents reads the payment events written for the bookings, as the
// outbox relay would publish them.
func paymentEvents(ctx context.Context, pool *pgxpool.Pool, ids []uuid.UUID) ([]kafka.Message, error) {
	rows, err := pool.Query(ctx, `SELECT event_id, event_type, payload FROM payment.outbox WHERE aggregate_id = ANY($1) ORDER BY id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []kafka.Message
	for rows.Next() {
		var id uuid.UUID
		var typ string
		var payload []byte
		if err := rows.Scan(&id, &typ, &payload); err != nil {
			return nil, err
		}
		out = append(out, kafka.Message{Value: payload, Headers: map[string]string{kafka.HeaderID: id.String(), kafka.HeaderType: typ}, Attempt: 1})
	}
	return out, rows.Err()
}

func bookingStates(ctx context.Context, pool *pgxpool.Pool, event uuid.UUID) (map[string]int, error) {
	rows, err := pool.Query(ctx, `SELECT status, count(*) FROM booking.bookings WHERE event_id = $1 GROUP BY status`, event)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

func mapsEqual(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// waitFor polls cond until it holds or the timeout passes.
func waitFor(ctx context.Context, timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if err != nil || ok {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func randomSecret() []byte {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return []byte(hex.EncodeToString(b))
}
