//go:build integration

package booking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Sanjay-Mx21/holdfast/internal/booking/catalog"
	eventsv1 "github.com/Sanjay-Mx21/holdfast/internal/gen/holdfast/events/v1"
	"github.com/Sanjay-Mx21/holdfast/internal/inventory"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

var ctx = context.Background()

// fakeInventory is inventory-svc as booking-svc sees it: holds by ID, with
// failures on demand.
type fakeInventory struct {
	mu          sync.Mutex
	holds       map[string]inventory.Hold
	unavailable int // fail this many calls with UNAVAILABLE first
	markCalls   int
}

func (f *fakeInventory) hold(event, user string, qty int, state inventory.HoldState) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := uuid.NewString()
	f.holds[id] = inventory.Hold{ID: id, EventID: event, UserID: user, Quantity: qty, State: state, ExpiresAt: time.Now().Add(5 * time.Minute)}
	return id
}

func (f *fakeInventory) fail() error {
	if f.unavailable > 0 {
		f.unavailable--
		return fmt.Errorf("inventory: %w", status.Error(codes.Unavailable, "connection refused"))
	}
	return nil
}

func (f *fakeInventory) GetHold(_ context.Context, event, user, id string) (inventory.Hold, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail(); err != nil {
		return inventory.Hold{}, err
	}
	h, ok := f.holds[id]
	if !ok || h.UserID != user || h.EventID != event {
		return inventory.Hold{}, inventory.ErrHoldNotFound
	}
	return h, nil
}

func (f *fakeInventory) MarkPaying(_ context.Context, event, user, id string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fail(); err != nil {
		return time.Time{}, err
	}
	f.markCalls++
	h, ok := f.holds[id]
	if !ok || h.UserID != user || h.EventID != event {
		return time.Time{}, inventory.ErrHoldNotFound
	}
	switch h.State {
	case inventory.StateHeld:
		h.State, h.ExpiresAt = inventory.StatePaying, time.Now().Add(10*time.Minute).Truncate(time.Millisecond)
		f.holds[id] = h
	case inventory.StatePaying:
	default:
		return time.Time{}, inventory.ErrHoldExpired
	}
	return h.ExpiresAt, nil
}

type fixture struct {
	pool  *pgxpool.Pool
	inv   *fakeInventory
	svc   *Service
	event string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	pool := testenv.Postgres(t)
	id, err := catalog.Create(ctx, pool, catalog.NewEvent{Name: "booking test", SaleOpensAt: time.Now(), PerUserLimit: 4, UnitPricePaise: 2500, Capacity: 100})
	if err != nil {
		t.Fatal(err)
	}
	inv := &fakeInventory{holds: map[string]inventory.Hold{}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &fixture{pool: pool, inv: inv, svc: NewService(pool, inv, nil, Config{}, NewMetrics(prometheus.NewRegistry()), quiet), event: id.String()}
}

func (f *fixture) create(user, hold, key string) (Result, error) {
	return f.svc.Create(ctx, CreateRequest{UserID: user, EventID: f.event, HoldID: hold, IdempotencyKey: key})
}

func decode(t *testing.T, res Result) View {
	t.Helper()
	var v View
	if err := json.Unmarshal(res.Body, &v); err != nil {
		t.Fatalf("body %s: %v", res.Body, err)
	}
	return v
}

func problemCode(t *testing.T, res Result) string {
	t.Helper()
	var p httpx.Problem
	if err := json.Unmarshal(res.Body, &p); err != nil {
		t.Fatalf("problem %s: %v", res.Body, err)
	}
	return p.Code
}

func (f *fixture) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCreateBookingAndItsEvent(t *testing.T) {
	f := newFixture(t)
	tp := sdktrace.NewTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	rctx, span := tp.Tracer("test").Start(ctx, "POST /v1/bookings")
	user := uuid.NewString()
	hold := f.inv.hold(f.event, user, 2, inventory.StateHeld)

	res, err := f.svc.Create(rctx, CreateRequest{UserID: user, EventID: f.event, HoldID: hold, IdempotencyKey: "create-key-0001"})
	span.End()
	if err != nil || res.Code != 201 || res.Replayed {
		t.Fatalf("create: %+v %v", res, err)
	}
	v := decode(t, res)
	if v.Status != StatusPendingPayment || v.AmountPaise != 5000 || v.Quantity != 2 || v.HoldID != hold {
		t.Fatalf("booking %+v", v)
	}
	if want := f.inv.holds[hold].ExpiresAt; !v.PaymentDeadline.Equal(want) {
		t.Fatalf("deadline %s, want the hold's protected-until %s", v.PaymentDeadline, want)
	}

	// The booking.created event was written with it, carrying the trace.
	var eventType string
	var payload, headers []byte
	if err := f.pool.QueryRow(ctx, `SELECT event_type, payload, headers FROM booking.outbox WHERE aggregate_id = $1`, v.BookingID).Scan(&eventType, &payload, &headers); err != nil {
		t.Fatal(err)
	}
	var ev eventsv1.BookingCreated
	if err := proto.Unmarshal(payload, &ev); err != nil || eventType != "booking.created.v1" || ev.GetBookingId() != v.BookingID || ev.GetAmountPaise() != 5000 {
		t.Fatalf("event %s %+v %v", eventType, &ev, err)
	}
	var h map[string]string
	_ = json.Unmarshal(headers, &h)
	if !strings.Contains(h["traceparent"], span.SpanContext().TraceID().String()) {
		t.Fatalf("event headers %v lack the request's trace", h)
	}

	// The same key replays the same answer.
	again, err := f.create(user, hold, "create-key-0001")
	if err != nil || !again.Replayed || string(again.Body) != string(res.Body) || again.Code != 201 {
		t.Fatalf("replay: %+v %v", again, err)
	}
	// Owner-only reads.
	if got, err := f.svc.Get(ctx, user, v.BookingID); err != nil || got.BookingID != v.BookingID {
		t.Fatalf("owner's read: %v", err)
	}
	if _, err := f.svc.Get(ctx, uuid.NewString(), v.BookingID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("someone else's read: %v", err)
	}
}

func TestOneBookingPerHoldWhateverTheKeys(t *testing.T) {
	f := newFixture(t)
	user := uuid.NewString()
	hold := f.inv.hold(f.event, user, 1, inventory.StateHeld)
	a, err := f.create(user, hold, "first-key-0001")
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.create(user, hold, "second-key-0001")
	if err != nil || b.Code != 201 || decode(t, b).BookingID != decode(t, a).BookingID {
		t.Fatalf("a second key for the same hold: %+v %v; want the same booking", b, err)
	}
	if n := f.count(t, `SELECT count(*) FROM booking.bookings WHERE hold_id = $1`, hold); n != 1 {
		t.Fatalf("%d bookings for one hold", n)
	}
	if n := f.count(t, `SELECT count(*) FROM booking.outbox WHERE aggregate_id = $1`, decode(t, a).BookingID); n != 1 {
		t.Fatalf("%d events for one booking", n)
	}
	// Reusing a key for a different hold is refused.
	other := f.inv.hold(f.event, user, 1, inventory.StateHeld)
	_, err = f.create(user, other, "first-key-0001")
	if p, ok := Problem(err); !ok || p.Code != "IDEMPOTENCY_KEY_REUSED" || p.Status != 422 {
		t.Fatalf("key reused for a different request: %v", err)
	}
}

func TestDefinitiveAnswersAreStored(t *testing.T) {
	f := newFixture(t)
	user := uuid.NewString()
	cases := []struct {
		name, hold, code string
		status           int
	}{
		{"someone else's hold", f.inv.hold(f.event, uuid.NewString(), 1, inventory.StateHeld), "HOLD_NOT_FOUND", 404},
		{"a released hold", f.inv.hold(f.event, user, 1, inventory.StateReleased), "HOLD_NOT_AVAILABLE", 409},
		{"a sold hold", f.inv.hold(f.event, user, 1, inventory.StateSold), "HOLD_NOT_AVAILABLE", 409},
		{"an unknown hold", uuid.NewString(), "HOLD_NOT_FOUND", 404},
	}
	for i, c := range cases {
		key := fmt.Sprintf("answer-key-%04d", i)
		res, err := f.create(user, c.hold, key)
		if err != nil || res.Code != c.status || problemCode(t, res) != c.code {
			t.Fatalf("%s: %+v %v", c.name, res, err)
		}
		again, err := f.create(user, c.hold, key)
		if err != nil || !again.Replayed || again.Code != c.status {
			t.Fatalf("%s, retried: %+v %v", c.name, again, err)
		}
	}
	res, err := f.svc.Create(ctx, CreateRequest{UserID: user, EventID: uuid.NewString(), HoldID: uuid.NewString(), IdempotencyKey: "answer-key-9999"})
	if err != nil || res.Code != 404 || problemCode(t, res) != "EVENT_NOT_FOUND" {
		t.Fatalf("unknown event: %+v %v", res, err)
	}
	for _, bad := range []CreateRequest{
		{UserID: user, EventID: "x", HoldID: uuid.NewString(), IdempotencyKey: "valid-key-0001"},
		{UserID: user, EventID: f.event, HoldID: uuid.NewString(), IdempotencyKey: "short"},
	} {
		if _, err := f.svc.Create(ctx, bad); err == nil {
			t.Fatalf("invalid request %+v accepted", bad)
		} else if _, ok := Problem(err); !ok {
			t.Fatalf("invalid request %+v: %v, want a problem", bad, err)
		}
	}
}

func TestARetryResumesAfterInventoryWasUnavailable(t *testing.T) {
	f := newFixture(t)
	user := uuid.NewString()
	hold := f.inv.hold(f.event, user, 3, inventory.StateHeld)
	f.inv.unavailable = 1
	if _, err := f.create(user, hold, "resume-key-0001"); err == nil {
		t.Fatal("no error while inventory was unavailable")
	}
	if n := f.count(t, `SELECT count(*) FROM booking.idempotency_keys WHERE idem_key = 'resume-key-0001' AND status = 'IN_PROGRESS'`); n != 1 {
		t.Fatal("the key is not left in progress for a retry")
	}
	res, err := f.create(user, hold, "resume-key-0001")
	if err != nil || res.Code != 201 || res.Replayed {
		t.Fatalf("retry: %+v %v", res, err)
	}
	if n := f.count(t, `SELECT count(*) FROM booking.bookings WHERE hold_id = $1`, hold); n != 1 {
		t.Fatalf("%d bookings", n)
	}
}

func TestConcurrentIdenticalRequestsBookOnce(t *testing.T) {
	f := newFixture(t)
	user := uuid.NewString()
	hold := f.inv.hold(f.event, user, 2, inventory.StateHeld)
	const n = 20
	bodies := make([]string, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := f.create(user, hold, "race-key-00001")
			if err != nil {
				bodies[i] = "error: " + err.Error()
				return
			}
			bodies[i] = fmt.Sprintf("%d %s", res.Code, res.Body)
		}()
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if bodies[i] != bodies[0] {
			t.Fatalf("answers differ:\n%s\n%s", bodies[0], bodies[i])
		}
	}
	if !strings.HasPrefix(bodies[0], "201 ") {
		t.Fatalf("answer %s", bodies[0])
	}
	if c := f.count(t, `SELECT count(*) FROM booking.bookings WHERE hold_id = $1`, hold); c != 1 {
		t.Fatalf("%d bookings", c)
	}
	if c := f.count(t, `SELECT count(*) FROM booking.outbox o JOIN booking.bookings b ON b.id = o.aggregate_id WHERE b.hold_id = $1`, hold); c != 1 {
		t.Fatalf("%d events", c)
	}
}

func TestOverdueBookingsAreCancelled(t *testing.T) {
	f := newFixture(t)
	user := uuid.NewString()
	var due []string
	for range 3 {
		res, err := f.create(user, f.inv.hold(f.event, user, 1, inventory.StateHeld), uuid.NewString())
		if err != nil {
			t.Fatal(err)
		}
		due = append(due, decode(t, res).BookingID)
	}
	keep, _ := f.create(user, f.inv.hold(f.event, user, 1, inventory.StateHeld), uuid.NewString())
	if _, err := f.pool.Exec(ctx, `UPDATE booking.bookings SET payment_deadline = now() - interval '1 minute' WHERE id = ANY($1::uuid[])`, due); err != nil {
		t.Fatal(err)
	}

	m := NewMetrics(prometheus.NewRegistry())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	var wg sync.WaitGroup
	for range 2 { // two replicas at once
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := NewDeadlineJob(f.pool, time.Second, 2, m, quiet).Pass(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for _, id := range due {
		v, err := f.svc.Get(ctx, user, id)
		if err != nil || v.Status != StatusCancelled {
			t.Fatalf("overdue booking %s: %+v %v", id, v, err)
		}
		if n := f.count(t, `SELECT count(*) FROM booking.outbox WHERE aggregate_id = $1 AND event_type = 'booking.cancelled.v1'`, id); n != 1 {
			t.Fatalf("booking %s has %d cancelled events, want 1", id, n)
		}
	}
	if v, _ := f.svc.Get(ctx, user, decode(t, keep).BookingID); v.Status != StatusPendingPayment {
		t.Fatalf("a booking before its deadline was touched: %s", v.Status)
	}
}

func TestHTTPAPI(t *testing.T) {
	f := newFixture(t)
	rt := httpx.NewRouter()
	NewHandler(f.svc).Register(rt, authn.RequireDevIdentity())
	user := uuid.NewString()
	hold := f.inv.hold(f.event, user, 1, inventory.StateHeld)
	do := func(method, path, key, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("X-Dev-User-Id", user)
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		rec := httptest.NewRecorder()
		rt.ServeHTTP(rec, req)
		return rec
	}
	body := fmt.Sprintf(`{"eventId":%q,"holdId":%q}`, f.event, hold)
	if rec := do("POST", "/v1/bookings", "", body); rec.Code != 400 || !strings.Contains(rec.Body.String(), "IDEMPOTENCY_KEY_REQUIRED") {
		t.Fatalf("without a key: %d %s", rec.Code, rec.Body)
	}
	first := do("POST", "/v1/bookings", "http-key-00001", body)
	if first.Code != 201 || first.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("create: %d %s", first.Code, first.Body)
	}
	again := do("POST", "/v1/bookings", "http-key-00001", body)
	if again.Code != 201 || again.Header().Get("Idempotent-Replayed") != "true" || again.Body.String() != first.Body.String() {
		t.Fatalf("replay: %d %v %s", again.Code, again.Header(), again.Body)
	}
	var v View
	_ = json.Unmarshal(first.Body.Bytes(), &v)
	if rec := do("GET", "/v1/bookings/"+v.BookingID, "", ""); rec.Code != 200 {
		t.Fatalf("get: %d %s", rec.Code, rec.Body)
	}
	if rec := do("GET", "/v1/bookings/"+uuid.NewString(), "", ""); rec.Code != 404 || !strings.Contains(rec.Body.String(), "BOOKING_NOT_FOUND") {
		t.Fatalf("get unknown: %d %s", rec.Code, rec.Body)
	}
	f.inv.unavailable = 2
	other := f.inv.hold(f.event, user, 1, inventory.StateHeld)
	rec := do("POST", "/v1/bookings", "http-key-00002", fmt.Sprintf(`{"eventId":%q,"holdId":%q}`, f.event, other))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("inventory unavailable: %d %v %s", rec.Code, rec.Header(), rec.Body)
	}
}

// fakeIntents is payment-svc as booking-svc sees it: one intent per booking,
// outages on demand.
type fakeIntents struct {
	mu          sync.Mutex
	byBooking   map[uuid.UUID]uuid.UUID
	expires     map[uuid.UUID]time.Time
	unavailable int
}

func (f *fakeIntents) CreateIntent(_ context.Context, bookingID, _ uuid.UUID, _ int64, expiresAt time.Time) (uuid.UUID, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable > 0 {
		f.unavailable--
		return uuid.Nil, "", status.Error(codes.Unavailable, "payment provider unavailable")
	}
	id, ok := f.byBooking[bookingID]
	if !ok {
		id = uuid.New()
		f.byBooking[bookingID] = id
	}
	f.expires[bookingID] = expiresAt
	return id, "https://psp.test/pay/" + id.String(), nil
}

func TestBookingCarriesItsPaymentIntent(t *testing.T) {
	f := newFixture(t)
	intents := &fakeIntents{byBooking: map[uuid.UUID]uuid.UUID{}, expires: map[uuid.UUID]time.Time{}, unavailable: 1}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.svc = NewService(f.pool, f.inv, intents, Config{Grace: 3 * time.Minute}, NewMetrics(prometheus.NewRegistry()), quiet)
	user := uuid.NewString()
	hold := f.inv.hold(f.event, user, 1, inventory.StateHeld)

	// payment-svc down: the booking exists, the key stays in progress.
	if _, err := f.create(user, hold, "intent-key-0001"); err == nil {
		t.Fatal("no error while payment-svc was unavailable")
	}
	res, err := f.create(user, hold, "intent-key-0001")
	if err != nil || res.Code != 201 {
		t.Fatalf("retry: %+v %v", res, err)
	}
	v := decode(t, res)
	id := uuid.MustParse(v.BookingID)
	intent := intents.byBooking[id]
	if v.IntentID != intent.String() || v.CheckoutURL != "https://psp.test/pay/"+intent.String() {
		t.Fatalf("booking %+v; want intent %s and its checkout URL", v, intent)
	}
	// The deadline leaves the grace inside the hold's payment window, and the
	// intent expires with the deadline.
	protected := f.inv.holds[hold].ExpiresAt
	if !v.PaymentDeadline.Equal(protected.Add(-3*time.Minute)) || !intents.expires[id].Equal(v.PaymentDeadline) {
		t.Fatalf("deadline %s (intent expires %s); want the hold's %s minus 3m", v.PaymentDeadline, intents.expires[id], protected)
	}
	if n := f.count(t, `SELECT count(*) FROM booking.bookings WHERE id = $1 AND intent_id = $2`, id, intent); n != 1 {
		t.Fatal("the intent is not recorded on the booking")
	}
}
