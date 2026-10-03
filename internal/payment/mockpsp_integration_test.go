//go:build integration

package payment

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sanjay-Mx21/holdfast/internal/mockpsp"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
	"github.com/Sanjay-Mx21/holdfast/internal/testenv"
)

// withMockPSP wires payment-svc to an in-process mockpsp: the real provider
// client calls it, and its signed webhooks reach payment-svc's handler.
type withMockPSP struct {
	*fixture
	faults *mockpsp.Injector
	psp    *mockpsp.Provider
	public string
}

func newMockPSPFixture(t *testing.T) *withMockPSP {
	t.Helper()
	pool := testenv.Postgres(t)
	secret := []byte("0123456789abcdef0123456789abcdef")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := NewMetrics(prometheus.NewRegistry())

	// payment-svc's webhook endpoint; the service is set once the client exists.
	f := &fixture{pool: pool, m: m}
	hooksRouter := httpx.NewRouter()
	hookSrv := httptest.NewServer(hooksRouter)
	t.Cleanup(hookSrv.Close)

	mm := mockpsp.NewMetrics(prometheus.NewRegistry())
	faults := mockpsp.NewInjector(11)
	dispatcher := mockpsp.NewDispatcher(mockpsp.DispatcherConfig{URL: hookSrv.URL + "/v1/webhooks/psp", Secret: secret, RetryBase: 20 * time.Millisecond}, faults, mm, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = dispatcher.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	public, admin := httpx.NewRouter(), httpx.NewRouter()
	pspSrv := httptest.NewServer(public)
	t.Cleanup(pspSrv.Close)
	provider := mockpsp.New(mockpsp.Config{APIKey: "psp-key"}, mockpsp.NewStore(pspSrv.URL), faults, dispatcher, mm, quiet)
	provider.Register(public, admin, authn.RequireStaticToken("unused-unused-unused-unused-unused"))

	client, err := psp.New(psp.Config{BaseURL: pspSrv.URL, APIKey: "psp-key"}, psp.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	f.svc = NewService(pool, client, m, quiet)
	NewWebhookHandler(f.svc, secret, 5*time.Minute).Register(hooksRouter)
	return &withMockPSP{fixture: f, faults: faults, psp: provider, public: pspSrv.URL}
}

func (w *withMockPSP) pay(t *testing.T, checkoutURL string) psp.Order {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, checkoutURL+"/pay", nil)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var o psp.Order
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("pay: %d %v", resp.StatusCode, err)
	}
	return o
}

func (w *withMockPSP) waitStatus(t *testing.T, id uuid.UUID, want string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); w.status(t, id) != want; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("intent %s is %s, want %s", id, w.status(t, id), want)
		}
	}
}

func TestPaymentWithMockPSP(t *testing.T) {
	w := newMockPSPFixture(t)
	// Every webhook is delivered twice.
	if err := w.faults.Set(mockpsp.Faults{DuplicateRate: 1}); err != nil {
		t.Fatal(err)
	}
	b := booking{uuid.New(), uuid.New()}
	in, err := w.svc.CreateIntent(ctx, b.id, b.event, 250000, time.Now().Add(7*time.Minute))
	if err != nil || !strings.HasPrefix(in.CheckoutURL, w.public+"/checkout/order_") {
		t.Fatalf("CreateIntent = %+v, %v", in, err)
	}
	paid := w.pay(t, in.CheckoutURL)
	w.waitStatus(t, in.ID, "CAPTURED")
	time.Sleep(200 * time.Millisecond) // let the duplicate arrive
	if got := w.balance(t, in.ID, "psp_receivable"); got != 250000 {
		t.Fatalf("receivable %d: the duplicate was booked again", got)
	}
	if got := w.events(t, b); len(got) != 1 || got[0] != "payment.captured.v1" {
		t.Fatalf("events %v", got)
	}
	stored, _ := w.svc.q.GetIntent(ctx, in.ID)
	if stored.PspPaymentID.String != paid.PaymentID {
		t.Fatalf("payment ID %q, want the provider's %q", stored.PspPaymentID.String, paid.PaymentID)
	}
	var dups int
	if err := w.pool.QueryRow(ctx, `SELECT count(*) FROM payment.webhook_events WHERE psp_order_id = $1`, stored.PspOrderID.String).Scan(&dups); err != nil || dups != 1 {
		t.Fatalf("%d webhook rows for the order, want 1 (%v)", dups, err)
	}
}

func TestLostWebhookIsRecoveredByPolling(t *testing.T) {
	w := newMockPSPFixture(t)
	if err := w.faults.Set(mockpsp.Faults{LossRate: 1}); err != nil {
		t.Fatal(err)
	}
	b := booking{uuid.New(), uuid.New()}
	in, err := w.svc.CreateIntent(ctx, b.id, b.event, 4200, time.Now().Add(7*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	w.pay(t, in.CheckoutURL)
	time.Sleep(200 * time.Millisecond)
	if s := w.status(t, in.ID); s != "CREATED" {
		t.Fatalf("status %s before polling; the webhook should have been lost", s)
	}
	// Make it the oldest open intent, so the poller's batch takes it first.
	if _, err := w.pool.Exec(ctx, `UPDATE payment.payment_intents
		SET created_at = (SELECT min(created_at) FROM payment.payment_intents) - interval '1 day' WHERE id = $1`, in.ID); err != nil {
		t.Fatal(err)
	}
	if err := NewPoller(w.svc, time.Second, time.Minute, 1, slog.New(slog.NewTextHandler(io.Discard, nil))).Pass(ctx); err != nil {
		t.Fatal(err)
	}
	if s := w.status(t, in.ID); s != "CAPTURED" || w.balance(t, in.ID, "psp_receivable") != 4200 {
		t.Fatalf("after polling: %s", s)
	}
}
