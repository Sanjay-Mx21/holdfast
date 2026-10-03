//go:build integration

package payment

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	paymentv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/payment/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/paymentdb"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/grpcx"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/kafka"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

// fakePSP is the payment provider: orders keyed by intent ID (its
// idempotency key), statuses set by the test, and outages on demand.
type fakePSP struct {
	mu       sync.Mutex
	orders   map[string]psp.Order // by order ID
	byIntent map[string]string
	creates  int
	refunds  []string // intent IDs, one per refund call
	down     bool
}

func newFakePSP() *fakePSP {
	return &fakePSP{orders: map[string]psp.Order{}, byIntent: map[string]string{}}
}

func (f *fakePSP) CreateOrder(_ context.Context, intentID string, req psp.CreateOrder) (psp.Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return psp.Order{}, psp.ErrUnavailable
	}
	f.creates++
	if id, ok := f.byIntent[intentID]; ok {
		return f.orders[id], nil
	}
	id := "order_" + uuid.NewString()
	o := psp.Order{OrderID: id, CheckoutURL: "https://psp.test/pay/" + id, Status: psp.OrderCreated, AmountPaise: req.AmountPaise, ExpiresAt: req.ExpiresAt}
	f.orders[id], f.byIntent[intentID] = o, id
	return o, nil
}

func (f *fakePSP) GetOrder(_ context.Context, orderID string) (psp.Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.orders[orderID]
	if f.down || !ok { // other tests' orders: leave them alone
		return psp.Order{}, psp.ErrUnavailable
	}
	return o, nil
}

func (f *fakePSP) CreateRefund(_ context.Context, intentID, paymentID string, amount int64) (psp.Refund, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return psp.Refund{}, psp.ErrUnavailable
	}
	f.refunds = append(f.refunds, intentID)
	return psp.Refund{RefundID: "rfnd_" + intentID, PaymentID: paymentID, AmountPaise: amount, Status: "PENDING"}, nil
}

func (f *fakePSP) set(orderID string, change func(*psp.Order)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o := f.orders[orderID]
	change(&o)
	f.orders[orderID] = o
}

func (f *fakePSP) setDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

type fixture struct {
	pool *pgxpool.Pool
	psp  *fakePSP
	svc  *Service
	m    *Metrics
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := testenv.Postgres(t)
	p := newFakePSP()
	m := NewMetrics(prometheus.NewRegistry())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &fixture{pool: pool, psp: p, svc: NewService(pool, p, m, quiet), m: m}
}

type booking struct{ id, event uuid.UUID }

// intent creates an intent with an order for a new booking.
func (f *fixture) intent(t *testing.T, amount int64) (booking, Intent, string) {
	t.Helper()
	b := booking{uuid.New(), uuid.New()}
	in, err := f.svc.CreateIntent(ctx, b.id, b.event, amount, time.Now().Add(7*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return b, in, f.psp.byIntent[in.ID.String()]
}

// payID is a provider payment ID; they are unique across runs.
func payID() string { return "pay_" + uuid.NewString() }

// hook builds a webhook body with a fresh provider event ID.
func hook(typ, orderID, paymentID string, amount int64) []byte {
	b, _ := json.Marshal(psp.Webhook{ID: "evt_" + uuid.NewString(), Type: typ, OrderID: orderID, PaymentID: paymentID, AmountPaise: amount, CreatedAt: time.Now()})
	return b
}

func (f *fixture) apply(t *testing.T, raw []byte) {
	t.Helper()
	if err := f.svc.ApplyWebhook(ctx, raw); err != nil {
		t.Fatalf("apply %s: %v", raw, err)
	}
}

func (f *fixture) status(t *testing.T, id uuid.UUID) string {
	t.Helper()
	in, err := f.svc.q.GetIntent(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return in.Status
}

// balance is debits minus credits on one account for one intent.
func (f *fixture) balance(t *testing.T, intent uuid.UUID, account string) int64 {
	t.Helper()
	var n int64
	if err := f.pool.QueryRow(ctx, `SELECT coalesce(sum(CASE direction WHEN 'D' THEN amount_paise ELSE -amount_paise END), 0)
		FROM payment.ledger_entries WHERE intent_id = $1 AND account = $2`, intent, account).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// events returns the outbox event types for a booking, oldest first.
func (f *fixture) events(t *testing.T, b booking) []string {
	t.Helper()
	rows, err := f.pool.Query(ctx, `SELECT event_type FROM payment.outbox WHERE aggregate_id = $1 ORDER BY id`, b.id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func TestCreateIntentIsIdempotent(t *testing.T) {
	f := newFixture(t)
	b, first, order := f.intent(t, 5000)
	if first.CheckoutURL != "https://psp.test/pay/"+order {
		t.Fatalf("intent %+v", first)
	}
	again, err := f.svc.CreateIntent(ctx, b.id, b.event, 5000, time.Now().Add(7*time.Minute))
	if err != nil || again != first {
		t.Fatalf("again: %+v %v; want %+v", again, err, first)
	}
	if f.psp.creates != 1 {
		t.Fatalf("%d provider orders created; the retry should reuse the recorded one", f.psp.creates)
	}
	if _, err := f.svc.CreateIntent(ctx, b.id, b.event, 6000, time.Now()); !errors.Is(err, ErrIntentConflict) {
		t.Fatalf("another amount: %v, want ErrIntentConflict", err)
	}
	for _, bad := range []struct {
		booking, event uuid.UUID
		amount         int64
		expires        time.Time
	}{
		{uuid.Nil, uuid.New(), 100, time.Now()},
		{uuid.New(), uuid.Nil, 100, time.Now()},
		{uuid.New(), uuid.New(), 0, time.Now()},
		{uuid.New(), uuid.New(), 100, time.Time{}},
	} {
		if _, err := f.svc.CreateIntent(ctx, bad.booking, bad.event, bad.amount, bad.expires); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("%+v: %v, want ErrInvalidRequest", bad, err)
		}
	}
}

func TestCreateIntentResumesAfterTheProviderWasDown(t *testing.T) {
	f := newFixture(t)
	b := booking{uuid.New(), uuid.New()}
	f.psp.setDown(true)
	if _, err := f.svc.CreateIntent(ctx, b.id, b.event, 2500, time.Now().Add(time.Minute)); !errors.Is(err, ErrPSPUnavailable) {
		t.Fatalf("provider down: %v, want ErrPSPUnavailable", err)
	}
	stored, err := f.svc.q.GetIntentByBooking(ctx, b.id)
	if err != nil || stored.PspOrderID.Valid {
		t.Fatalf("after the outage: %+v %v; want an intent without an order", stored, err)
	}
	f.psp.setDown(false)
	in, err := f.svc.CreateIntent(ctx, b.id, b.event, 2500, time.Now().Add(time.Minute))
	if err != nil || in.ID != stored.ID || in.CheckoutURL == "" {
		t.Fatalf("retry: %+v %v; want intent %s with a checkout URL", in, err, stored.ID)
	}
}

func TestCaptureWebhook(t *testing.T) {
	f := newFixture(t)
	pay1 := payID()
	b, in, order := f.intent(t, 5000)
	raw := hook(psp.EventPaymentCaptured, order, pay1, 5000)
	f.apply(t, raw)
	if s := f.status(t, in.ID); s != "CAPTURED" {
		t.Fatalf("status %s", s)
	}
	revenue := "unearned_revenue:" + b.event.String()
	if f.balance(t, in.ID, "psp_receivable") != 5000 || f.balance(t, in.ID, revenue) != -5000 {
		t.Fatal("the capture is not booked as D psp_receivable / C unearned revenue")
	}
	if got := f.events(t, b); len(got) != 1 || got[0] != "payment.captured.v1" {
		t.Fatalf("events %v", got)
	}
	var payload []byte
	if err := f.pool.QueryRow(ctx, `SELECT payload FROM payment.outbox WHERE aggregate_id = $1`, b.id).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var ev eventsv1.PaymentCaptured
	if err := proto.Unmarshal(payload, &ev); err != nil || ev.GetBookingId() != b.id.String() || ev.GetIntentId() != in.ID.String() ||
		ev.GetPspPaymentId() != pay1 || ev.GetAmountPaise() != 5000 {
		t.Fatalf("event %+v %v", &ev, err)
	}

	// The provider retries: the same event changes nothing.
	f.apply(t, raw)
	// A different event for the same capture: nothing either.
	f.apply(t, hook(psp.EventPaymentCaptured, order, pay1, 5000))
	// A failure reported after the capture: forward only.
	f.apply(t, hook(psp.EventPaymentFailed, order, "", 5000))
	if s := f.status(t, in.ID); s != "CAPTURED" || f.balance(t, in.ID, "psp_receivable") != 5000 || len(f.events(t, b)) != 1 {
		t.Fatalf("after duplicates and a late failure: status %s, events %v", s, f.events(t, b))
	}
	if d := testutil.ToFloat64(f.m.webhooks.WithLabelValues("payment.captured", "true")); d != 1 {
		t.Fatalf("duplicate webhooks counted %v, want 1", d)
	}
}

func TestFailureThenCaptureAndExpiry(t *testing.T) {
	f := newFixture(t)
	pay2 := payID()
	b, in, order := f.intent(t, 3000)
	f.apply(t, hook(psp.EventPaymentFailed, order, "", 3000))
	if s := f.status(t, in.ID); s != "FAILED" {
		t.Fatalf("status %s", s)
	}
	// A later attempt on the same order succeeded: the capture wins.
	f.apply(t, hook(psp.EventPaymentCaptured, order, pay2, 3000))
	if s := f.status(t, in.ID); s != "CAPTURED" {
		t.Fatalf("status %s after a capture following a failure", s)
	}
	if got := strings.Join(f.events(t, b), ","); got != "payment.failed.v1,payment.captured.v1" {
		t.Fatalf("events %s", got)
	}

	b2, in2, order2 := f.intent(t, 3000)
	f.apply(t, hook(psp.EventOrderExpired, order2, "", 3000))
	if s := f.status(t, in2.ID); s != "EXPIRED" {
		t.Fatalf("status %s", s)
	}
	if got := f.events(t, b2); len(got) != 1 || got[0] != "payment.expired.v1" {
		t.Fatalf("events %v", got)
	}
}

func TestWebhooksThatChangeNothing(t *testing.T) {
	f := newFixture(t)
	pay3 := payID()
	// An order HoldFast never made: recorded and acknowledged.
	raw := hook(psp.EventPaymentCaptured, "order_unknown_"+uuid.NewString(), "pay_x", 100)
	f.apply(t, raw)
	var id string
	_ = json.Unmarshal(raw, &struct{ ID *string }{&id})
	var n int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM payment.webhook_events WHERE psp_event_id = $1`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("unknown order's webhook recorded %d times, %v", n, err)
	}
	// A body we cannot read: acknowledged, nothing else.
	f.apply(t, []byte(`{"not":"a webhook"}`))
	f.apply(t, []byte(`not json`))

	// A capture for a different amount is never booked.
	b, in, order := f.intent(t, 4000)
	f.apply(t, hook(psp.EventPaymentCaptured, order, pay3, 3999))
	if s := f.status(t, in.ID); s != "CREATED" || f.balance(t, in.ID, "psp_receivable") != 0 || len(f.events(t, b)) != 0 {
		t.Fatalf("mismatched capture applied: status %s", s)
	}
	if got := testutil.ToFloat64(f.m.mismatch); got != 1 {
		t.Fatalf("mismatches counted %v", got)
	}
}

func TestRefundCompleted(t *testing.T) {
	f := newFixture(t)
	pay4 := payID()
	b, in, order := f.intent(t, 7000)
	f.apply(t, hook(psp.EventPaymentCaptured, order, pay4, 7000))
	// Requesting the refund is task 3.11's; here the intent is moved by hand.
	if _, err := f.svc.q.TransitionIntent(ctx, paymentdb.TransitionIntentParams{ID: in.ID, FromStatus: "CAPTURED", ToStatus: "REFUND_PENDING"}); err != nil {
		t.Fatal(err)
	}
	refund, _ := json.Marshal(psp.Webhook{ID: "evt_" + uuid.NewString(), Type: psp.EventRefundCompleted, OrderID: order, RefundID: "rfnd_1", AmountPaise: 7000})
	f.apply(t, refund)
	if s := f.status(t, in.ID); s != "REFUNDED" {
		t.Fatalf("status %s", s)
	}
	if f.balance(t, in.ID, "psp_receivable") != 0 || f.balance(t, in.ID, "unearned_revenue:"+b.event.String()) != 0 {
		t.Fatal("the refund did not reverse the capture's entries")
	}
	if got := strings.Join(f.events(t, b), ","); got != "payment.captured.v1,refund.completed.v1" {
		t.Fatalf("events %s", got)
	}
}

func TestWebhookHandler(t *testing.T) {
	f := newFixture(t)
	pay5 := payID()
	secret := []byte("0123456789abcdef0123456789abcdef")
	h := NewWebhookHandler(f.svc, secret, 5*time.Minute)
	_, in, order := f.intent(t, 1500)
	body := hook(psp.EventPaymentCaptured, order, pay5, 1500)
	send := func(ts, sig string, body []byte) int {
		r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/webhooks/psp", strings.NewReader(string(body)))
		r.Header.Set(psp.HeaderTimestamp, ts)
		r.Header.Set(psp.HeaderSignature, sig)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	now := strconv.FormatInt(time.Now().Unix(), 10)
	if code := send(now, psp.Sign([]byte("the-wrong-secret-the-wrong-secret"), now, body), body); code != http.StatusUnauthorized {
		t.Fatalf("a bad signature: %d", code)
	}
	if s := f.status(t, in.ID); s != "CREATED" {
		t.Fatalf("an unverified webhook changed the intent to %s", s)
	}
	if code := send(now, psp.Sign(secret, now, body), body); code != http.StatusOK {
		t.Fatalf("a good webhook: %d", code)
	}
	if code := send(now, psp.Sign(secret, now, body), body); code != http.StatusOK {
		t.Fatalf("its redelivery: %d", code)
	}
	if s := f.status(t, in.ID); s != "CAPTURED" {
		t.Fatalf("status %s", s)
	}
	big := make([]byte, maxWebhookBytes+1)
	if code := send(now, psp.Sign(secret, now, big), big); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("an oversized body: %d", code)
	}
}

func TestPollerAppliesWhatTheProviderKnows(t *testing.T) {
	f := newFixture(t)
	testenv.Exclusive(t, f.pool, "payment-poll")
	pay6 := payID()
	b, in, order := f.intent(t, 2000)
	_, quiet, quietOrder := f.intent(t, 2000)
	// Make both the oldest intents, so the claim takes them first whatever
	// other tests (and earlier runs) left behind.
	if _, err := f.pool.Exec(ctx, `UPDATE payment.payment_intents
		SET created_at = (SELECT min(created_at) FROM payment.payment_intents) - interval '1 day' WHERE id = ANY($1)`,
		[]uuid.UUID{in.ID, quiet.ID}); err != nil {
		t.Fatal(err)
	}
	f.psp.set(order, func(o *psp.Order) { o.Status, o.PaymentID = psp.OrderCaptured, pay6 })
	_ = quietOrder // still CREATED at the provider: the buyer may be paying

	p := NewPoller(f.svc, time.Second, time.Minute, 2, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if s := f.status(t, in.ID); s != "CAPTURED" {
		t.Fatalf("polled intent %s", s)
	}
	if s := f.status(t, quiet.ID); s != "CREATED" {
		t.Fatalf("an open order's intent moved to %s", s)
	}
	if got := f.events(t, b); len(got) != 1 || got[0] != "payment.captured.v1" {
		t.Fatalf("events %v", got)
	}
	if got := testutil.ToFloat64(f.m.captures.WithLabelValues("poll")); got != 1 {
		t.Fatalf("poll captures %v", got)
	}
	// The webhook arriving later changes nothing.
	f.apply(t, hook(psp.EventPaymentCaptured, order, pay6, 2000))
	if f.balance(t, in.ID, "psp_receivable") != 2000 {
		t.Fatal("the late webhook booked the capture twice")
	}
	// Expiry learned by polling. The intent was just polled, so the next pass
	// takes never-polled intents first (P34); put it back at the head.
	f.psp.set(quietOrder, func(o *psp.Order) { o.Status = psp.OrderExpired })
	if _, err := f.pool.Exec(ctx, `UPDATE payment.payment_intents
		SET created_at = (SELECT min(created_at) FROM payment.payment_intents) - interval '1 day', polled_at = NULL WHERE id = $1`, quiet.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if s := f.status(t, quiet.ID); s != "EXPIRED" {
		t.Fatalf("polled expiry: %s", s)
	}
}

// TestGRPCContract serves CreateIntent with the production interceptors and
// calls it with booking-svc's client.
func TestGRPCContract(t *testing.T) {
	f := newFixture(t)
	_, bookingKey, _ := ed25519.GenerateKey(rand.Reader)
	_, queueKey, _ := ed25519.GenerateKey(rand.Reader)
	v := authn.NewServiceVerifier("payment", map[string]ed25519.PublicKey{
		"booking": bookingKey.Public().(ed25519.PublicKey), "queue": queueKey.Public().(ed25519.PublicKey),
	}, time.Second)
	srv := grpcx.NewServer(grpcx.ServerConfig{Verifier: v, Allow: GRPCAllow()}, prometheus.NewRegistry(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	paymentv1.RegisterPaymentServiceServer(srv, NewGRPCServer(f.svc))
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	dial := func(key ed25519.PrivateKey, caller string) *Client {
		conn, err := grpcx.Dial(grpcx.ClientConfig{Target: ln.Addr().String(), Tokens: authn.NewServiceTokenSource(key, caller, "payment")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return NewClient(conn)
	}
	c := dial(bookingKey, "booking")

	bookingID, eventID := uuid.New(), uuid.New()
	id, url, err := c.CreateIntent(ctx, bookingID, eventID, 9000, time.Now().Add(7*time.Minute))
	if err != nil || id == uuid.Nil || !strings.HasPrefix(url, "https://psp.test/pay/") {
		t.Fatalf("CreateIntent = %s %q %v", id, url, err)
	}
	if again, againURL, err := c.CreateIntent(ctx, bookingID, eventID, 9000, time.Now().Add(7*time.Minute)); err != nil || again != id || againURL != url {
		t.Fatalf("retry = %s %q %v", again, againURL, err)
	}
	if _, _, err := c.CreateIntent(ctx, bookingID, eventID, 9001, time.Now()); !errors.Is(err, ErrIntentConflict) {
		t.Fatalf("another amount: %v, want ErrIntentConflict", err)
	}
	if _, _, err := c.CreateIntent(ctx, uuid.New(), eventID, -1, time.Now()); grpcx.Code(err) != codes.InvalidArgument {
		t.Fatalf("a negative amount: %v, want INVALID_ARGUMENT", err)
	}
	f.psp.setDown(true)
	if _, _, err := c.CreateIntent(ctx, uuid.New(), eventID, 100, time.Now().Add(time.Minute)); !errors.Is(err, ErrPSPUnavailable) {
		t.Fatalf("provider down: %v, want ErrPSPUnavailable", err)
	}
	f.psp.setDown(false)
	// Only booking-svc may create intents.
	if _, _, err := dial(queueKey, "queue").CreateIntent(ctx, uuid.New(), eventID, 100, time.Now()); grpcx.Code(err) != codes.PermissionDenied {
		t.Fatalf("queue-svc's call: %v, want PERMISSION_DENIED", err)
	}
}

// TestPollerRotates pins P34: intents whose polls keep failing must not be
// claimed again and again ahead of the rest.
func TestPollerRotates(t *testing.T) {
	f := newFixture(t)
	testenv.Exclusive(t, f.pool, "payment-poll")
	_, a, _ := f.intent(t, 100)
	_, b, _ := f.intent(t, 100)
	// The two oldest open intents, a before b.
	for _, id := range []uuid.UUID{b.ID, a.ID} {
		if _, err := f.pool.Exec(ctx, `UPDATE payment.payment_intents
			SET created_at = (SELECT min(created_at) FROM payment.payment_intents) - interval '1 day', polled_at = NULL WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
	}
	f.psp.setDown(true) // every poll fails
	p := NewPoller(f.svc, time.Second, time.Minute, 1, slog.New(slog.NewTextHandler(io.Discard, nil)))
	polled := func(id uuid.UUID) bool {
		in, err := f.svc.q.GetIntent(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return in.PolledAt.Valid
	}
	if err := p.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if !polled(a.ID) || polled(b.ID) {
		t.Fatalf("first pass: a polled %v, b polled %v; want a only", polled(a.ID), polled(b.ID))
	}
	if err := p.Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if !polled(b.ID) {
		t.Fatal("second pass polled a again instead of b")
	}
	if s := f.status(t, a.ID); s != "CREATED" {
		t.Fatalf("a failed poll moved the intent to %s", s)
	}
}

func refundRequired(b booking) kafka.Message {
	v, _ := proto.Marshal(&eventsv1.BookingRefundRequired{BookingId: b.id.String(), Reason: eventsv1.RefundReason_REFUND_REASON_GUARD_REJECTED})
	return kafka.Message{Value: v, Headers: map[string]string{kafka.HeaderID: uuid.NewString(), kafka.HeaderType: "booking.refund_required.v1"}}
}

func TestRefundRequestedByBooking(t *testing.T) {
	f := newFixture(t)
	b, in, order := f.intent(t, 6000)
	pay := payID()
	f.apply(t, hook(psp.EventPaymentCaptured, order, pay, 6000))

	// The provider is down: the move commits, the call is retried.
	f.psp.setDown(true)
	m := refundRequired(b)
	if err := f.svc.HandleBookingEvent(ctx, m); err == nil || kafka.IsPermanent(err) {
		t.Fatalf("provider down: %v, want a retryable error", err)
	}
	if s := f.status(t, in.ID); s != "REFUND_PENDING" {
		t.Fatalf("status %s", s)
	}
	f.psp.setDown(false)
	if err := f.svc.HandleBookingEvent(ctx, m); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if len(f.psp.refunds) != 1 || f.psp.refunds[0] != in.ID.String() {
		t.Fatalf("refund calls %v, want one keyed by the intent", f.psp.refunds)
	}
	// The provider's webhook completes it: the capture's entries reversed.
	refund, _ := json.Marshal(psp.Webhook{ID: "evt_" + uuid.NewString(), Type: psp.EventRefundCompleted, OrderID: order, PaymentID: pay, RefundID: "rfnd_x", AmountPaise: 6000})
	f.apply(t, refund)
	if s := f.status(t, in.ID); s != "REFUNDED" || f.balance(t, in.ID, "psp_receivable") != 0 {
		t.Fatalf("after the refund: %s", s)
	}
	// Once refunded, a redelivered request asks for nothing.
	if err := f.svc.HandleBookingEvent(ctx, m); err != nil || len(f.psp.refunds) != 1 {
		t.Fatalf("redelivered after the refund: %v, %d calls", err, len(f.psp.refunds))
	}
}

func TestRefundConsumerSkipsWhatItCannotUse(t *testing.T) {
	f := newFixture(t)
	// A booking with no intent (a test's, or another environment's).
	if err := f.svc.HandleBookingEvent(ctx, refundRequired(booking{uuid.New(), uuid.New()})); err != nil {
		t.Fatalf("unknown booking: %v", err)
	}
	// Other booking events are not for this consumer.
	other := kafka.Message{Value: []byte("x"), Headers: map[string]string{kafka.HeaderID: "1", kafka.HeaderType: "booking.created.v1"}}
	if err := f.svc.HandleBookingEvent(ctx, other); err != nil {
		t.Fatalf("booking.created: %v", err)
	}
	bad := kafka.Message{Value: []byte("not protobuf"), Headers: map[string]string{kafka.HeaderID: "2", kafka.HeaderType: "booking.refund_required.v1"}}
	if err := f.svc.HandleBookingEvent(ctx, bad); !kafka.IsPermanent(err) {
		t.Fatalf("garbage: %v, want permanent", err)
	}
	// An intent that was never captured is not refunded.
	b, in, _ := f.intent(t, 100)
	if err := f.svc.HandleBookingEvent(ctx, refundRequired(b)); err != nil || f.status(t, in.ID) != "CREATED" || len(f.psp.refunds) != 0 {
		t.Fatalf("an uncaptured intent: %v, status %s, %d refunds", err, f.status(t, in.ID), len(f.psp.refunds))
	}
}

// TestLateAndOutOfOrderWebhooks: a capture reported after the order expired
// wins (money moved), and an expiry reported after a capture changes nothing.
func TestLateAndOutOfOrderWebhooks(t *testing.T) {
	f := newFixture(t)
	b, in, order := f.intent(t, 4500)
	f.apply(t, hook(psp.EventOrderExpired, order, "", 4500))
	if s := f.status(t, in.ID); s != "EXPIRED" {
		t.Fatalf("status %s", s)
	}
	f.apply(t, hook(psp.EventPaymentCaptured, order, payID(), 4500))
	if s := f.status(t, in.ID); s != "CAPTURED" || f.balance(t, in.ID, "psp_receivable") != 4500 {
		t.Fatalf("a late capture: %s", s)
	}
	if got := strings.Join(f.events(t, b), ","); got != "payment.expired.v1,payment.captured.v1" {
		t.Fatalf("events %s", got)
	}

	b2, in2, order2 := f.intent(t, 4500)
	f.apply(t, hook(psp.EventPaymentCaptured, order2, payID(), 4500))
	f.apply(t, hook(psp.EventOrderExpired, order2, "", 4500))
	f.apply(t, hook(psp.EventPaymentFailed, order2, "", 4500))
	if s := f.status(t, in2.ID); s != "CAPTURED" || len(f.events(t, b2)) != 1 {
		t.Fatalf("after an expiry and a failure that came late: %s, events %v", s, f.events(t, b2))
	}
}

// TestPaymentContinuesTheBookingsTrace: a webhook arrives in a trace of its
// own, but settles the intent in the trace of the booking request that
// created it, so the capture's event (and the saga after it) joins one
// purchase trace.
func TestPaymentContinuesTheBookingsTrace(t *testing.T) {
	f := newFixture(t)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := sdktrace.NewTracerProvider().Tracer("test")
	rctx, request := tracer.Start(ctx, "POST /v1/bookings")
	b := booking{uuid.New(), uuid.New()}
	in, err := f.svc.CreateIntent(rctx, b.id, b.event, 800, time.Now().Add(7*time.Minute))
	request.End()
	if err != nil {
		t.Fatal(err)
	}
	wctx, webhook := tracer.Start(ctx, "POST /v1/webhooks/psp")
	if err := f.svc.ApplyWebhook(wctx, hook(psp.EventPaymentCaptured, f.psp.byIntent[in.ID.String()], payID(), 800)); err != nil {
		t.Fatal(err)
	}
	webhook.End()
	var headers []byte
	if err := f.pool.QueryRow(ctx, `SELECT headers FROM payment.outbox WHERE aggregate_id = $1`, b.id).Scan(&headers); err != nil {
		t.Fatal(err)
	}
	var h map[string]string
	_ = json.Unmarshal(headers, &h)
	if want := request.SpanContext().TraceID().String(); !strings.Contains(h["traceparent"], want) {
		t.Fatalf("the capture's event carries %q; want the booking request's trace %s, not the webhook's %s",
			h["traceparent"], want, webhook.SpanContext().TraceID())
	}
}
