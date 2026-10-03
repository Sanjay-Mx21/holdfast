package mockpsp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/authn"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

var (
	secret     = []byte("0123456789abcdef0123456789abcdef")
	adminToken = "admin-token-admin-token-admin-token"
	quiet      = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// receiver is payment-svc's webhook endpoint as mockpsp sees it: it checks
// every signature, can refuse the first deliveries, and records the rest.
type receiver struct {
	mu        sync.Mutex
	got       []psp.Webhook
	refuse    atomic.Int32 // answer 503 to this many deliveries first
	attempts  atomic.Int32
	badSigned atomic.Int32
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rc.attempts.Add(1)
	body, _ := io.ReadAll(r.Body)
	if psp.Verify(secret, r.Header.Get(psp.HeaderTimestamp), r.Header.Get(psp.HeaderSignature), body, time.Now(), time.Minute) != nil {
		rc.badSigned.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if rc.refuse.Add(-1) >= 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	var wh psp.Webhook
	_ = json.Unmarshal(body, &wh)
	rc.mu.Lock()
	rc.got = append(rc.got, wh)
	rc.mu.Unlock()
}

// ofType returns the one webhook of a type. Deliveries run concurrently, so
// webhooks sent moments apart can arrive in either order (P40), as a real
// provider's can.
func (rc *receiver) ofType(t *testing.T, typ string) psp.Webhook {
	t.Helper()
	var found []psp.Webhook
	for _, wh := range rc.webhooks() {
		if wh.Type == typ {
			found = append(found, wh)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d %s webhooks in %+v, want 1", len(found), typ, rc.webhooks())
	}
	return found[0]
}

func (rc *receiver) webhooks() []psp.Webhook {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]psp.Webhook(nil), rc.got...)
}

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

type harness struct {
	p      *Provider
	m      *Metrics
	rc     *receiver
	client *psp.Client
	public string
	admin  string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	rc := &receiver{}
	recvSrv := httptest.NewServer(rc)
	t.Cleanup(recvSrv.Close)

	m := NewMetrics(prometheus.NewRegistry())
	faults := NewInjector(7)
	hooks := NewDispatcher(DispatcherConfig{URL: recvSrv.URL, Secret: secret, RetryBase: 10 * time.Millisecond, RetryMax: 20 * time.Millisecond}, faults, m, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = hooks.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	public, admin := httpx.NewRouter(), httpx.NewRouter()
	pubSrv := httptest.NewServer(public)
	t.Cleanup(pubSrv.Close)
	adminSrv := httptest.NewServer(admin)
	t.Cleanup(adminSrv.Close)
	p := New(Config{APIKey: "psp-key", RefundDelay: 20 * time.Millisecond}, NewStore(pubSrv.URL), faults, hooks, m, quiet)
	p.Register(public, admin, authn.RequireStaticToken(adminToken))

	client, err := psp.New(psp.Config{BaseURL: pubSrv.URL, APIKey: "psp-key", Timeout: 300 * time.Millisecond, BreakerThreshold: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{p: p, m: m, rc: rc, client: client, public: pubSrv.URL, admin: adminSrv.URL}
}

func (h *harness) order(t *testing.T, amount int64) (string, psp.Order) {
	t.Helper()
	intent := uuid.NewString()
	o, err := h.client.CreateOrder(context.Background(), intent, psp.CreateOrder{AmountPaise: amount, Currency: "INR", ExpiresAt: time.Now().Add(7 * time.Minute), Reference: intent})
	if err != nil {
		t.Fatal(err)
	}
	return intent, o
}

func (h *harness) adminCall(t *testing.T, method, path, token, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), method, h.admin+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (h *harness) setFaults(t *testing.T, f string) {
	t.Helper()
	if resp := h.adminCall(t, http.MethodPut, "/internal/v1/faults", adminToken, f); resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("set faults %s: %d %s", f, resp.StatusCode, b)
	}
}

func (h *harness) payJSON(t *testing.T, orderID, action string) (int, psp.Order) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, h.public+"/checkout/"+orderID+"/"+action, nil)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var o psp.Order
	_ = json.NewDecoder(resp.Body).Decode(&o)
	return resp.StatusCode, o
}

func TestOrderPayAndWebhook(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	intent, o := h.order(t, 250000)
	if o.Status != psp.OrderCreated || !strings.HasPrefix(o.CheckoutURL, h.public+"/checkout/order_") {
		t.Fatalf("order %+v", o)
	}
	// The intent ID is the idempotency key: a retry returns the same order.
	again, err := h.client.CreateOrder(ctx, intent, psp.CreateOrder{AmountPaise: 250000, Currency: "INR", ExpiresAt: o.ExpiresAt, Reference: intent})
	if err != nil || again.OrderID != o.OrderID {
		t.Fatalf("retry: %+v %v", again, err)
	}
	if _, err := h.client.CreateOrder(ctx, intent, psp.CreateOrder{AmountPaise: 1, Currency: "INR", ExpiresAt: o.ExpiresAt, Reference: intent}); !errors.Is(err, psp.ErrRejected) {
		t.Fatalf("the key reused for another amount: %v, want a rejection", err)
	}

	code, paid := h.payJSON(t, o.OrderID, "pay")
	if code != http.StatusOK || paid.Status != psp.OrderCaptured || paid.PaymentID == "" {
		t.Fatalf("pay: %d %+v", code, paid)
	}
	waitFor(t, "the capture webhook", func() bool { return len(h.rc.webhooks()) == 1 })
	wh := h.rc.webhooks()[0]
	if wh.Type != psp.EventPaymentCaptured || wh.OrderID != o.OrderID || wh.PaymentID != paid.PaymentID || wh.AmountPaise != 250000 || !strings.HasPrefix(wh.ID, "evt_") {
		t.Fatalf("webhook %+v", wh)
	}
	if got, err := h.client.GetOrder(ctx, o.OrderID); err != nil || got.Status != psp.OrderCaptured || got.PaymentID != paid.PaymentID {
		t.Fatalf("GetOrder = %+v %v", got, err)
	}
	if h.rc.badSigned.Load() != 0 {
		t.Fatal("a webhook failed signature verification")
	}
}

func TestDeclineThenPayAndExpiry(t *testing.T) {
	h := newHarness(t)
	_, o := h.order(t, 1000)
	if code, failed := h.payJSON(t, o.OrderID, "fail"); code != http.StatusOK || failed.Status != psp.OrderFailed {
		t.Fatalf("decline: %d %+v", code, failed)
	}
	if code, paid := h.payJSON(t, o.OrderID, "pay"); code != http.StatusOK || paid.Status != psp.OrderCaptured {
		t.Fatalf("pay after a decline: %d %+v", code, paid)
	}
	waitFor(t, "two webhooks", func() bool { return len(h.rc.webhooks()) == 2 })
	if failed := h.rc.ofType(t, psp.EventPaymentFailed); failed.Reason != "declined_by_buyer" {
		t.Fatalf("failure webhook %+v", failed)
	}
	h.rc.ofType(t, psp.EventPaymentCaptured)

	ref := uuid.NewString()
	open, err := h.client.CreateOrder(context.Background(), ref, psp.CreateOrder{AmountPaise: 1000, Currency: "INR", ExpiresAt: time.Now().Add(300 * time.Millisecond), Reference: ref})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	h.p.ExpireOrders()
	waitFor(t, "the expiry webhook", func() bool { return len(h.rc.webhooks()) == 3 })
	if wh := h.rc.ofType(t, psp.EventOrderExpired); wh.OrderID != open.OrderID {
		t.Fatalf("webhook %+v", wh)
	}
	if code, _ := h.payJSON(t, open.OrderID, "pay"); code != http.StatusConflict {
		t.Fatalf("paying an expired order: %d", code)
	}
	if code, _ := h.payJSON(t, "order_nope", "pay"); code != http.StatusNotFound {
		t.Fatalf("paying an unknown order: %d", code)
	}
}

func TestRefundAndSettlement(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	intent, o := h.order(t, 5000)
	_, paid := h.payJSON(t, o.OrderID, "pay")
	r, err := h.client.CreateRefund(ctx, intent, paid.PaymentID, 5000)
	if err != nil || r.Status != RefundPending {
		t.Fatalf("refund %+v %v", r, err)
	}
	if again, err := h.client.CreateRefund(ctx, intent, paid.PaymentID, 5000); err != nil || again.RefundID != r.RefundID {
		t.Fatalf("refund retry %+v %v", again, err)
	}
	waitFor(t, "the refund webhook", func() bool { return len(h.rc.webhooks()) == 2 })
	if wh := h.rc.ofType(t, psp.EventRefundCompleted); wh.RefundID != r.RefundID || wh.OrderID != o.OrderID || wh.PaymentID != paid.PaymentID {
		t.Fatalf("webhook %+v", wh)
	}
	rep, err := h.client.Settlements(ctx, time.Now().Add(-time.Hour), time.Now().Add(time.Minute))
	if err != nil || len(rep.Items) != 2 || rep.Items[0].Type != psp.SettledCapture || rep.Items[0].Reference != intent || rep.Items[1].Type != psp.SettledRefund {
		t.Fatalf("settlement %+v %v", rep, err)
	}
	if _, err := h.client.Settlements(ctx, time.Now(), time.Now().Add(-time.Hour)); !errors.Is(err, psp.ErrRejected) {
		t.Fatalf("a reversed window: %v", err)
	}
}

func TestAPIKeyIsRequired(t *testing.T) {
	h := newHarness(t)
	bad, _ := psp.New(psp.Config{BaseURL: h.public, APIKey: "wrong"}, nil)
	if _, err := bad.GetOrder(context.Background(), "order_x"); !errors.Is(err, psp.ErrRejected) || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a wrong key: %v", err)
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, h.public+"/v1/orders", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer psp-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("no Idempotency-Key: %d", resp.StatusCode)
	}
}

func TestWebhookFaults(t *testing.T) {
	h := newHarness(t)
	// The receiver refuses twice: the provider retries the same event.
	h.rc.refuse.Store(2)
	_, o := h.order(t, 100)
	h.payJSON(t, o.OrderID, "pay")
	waitFor(t, "a delivery after two refusals", func() bool { return len(h.rc.webhooks()) == 1 })
	if n := h.rc.attempts.Load(); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}
	if got := testutil.ToFloat64(h.m.webhooks.WithLabelValues(psp.EventPaymentCaptured, "retried")); got != 2 {
		t.Fatalf("retried %v", got)
	}

	// Duplicates carry the same event ID.
	h.setFaults(t, `{"duplicateRate":1}`)
	_, o2 := h.order(t, 100)
	h.payJSON(t, o2.OrderID, "pay")
	waitFor(t, "a duplicated webhook", func() bool { return len(h.rc.webhooks()) == 3 })
	got := h.rc.webhooks()
	if got[1].ID != got[2].ID || got[1].OrderID != o2.OrderID {
		t.Fatalf("duplicates %+v %+v", got[1], got[2])
	}

	// Lost webhooks are never sent, but the order still changed.
	h.setFaults(t, `{"lossRate":1}`)
	_, o3 := h.order(t, 100)
	_, paid := h.payJSON(t, o3.OrderID, "pay")
	time.Sleep(100 * time.Millisecond)
	if n := len(h.rc.webhooks()); n != 3 || paid.Status != psp.OrderCaptured {
		t.Fatalf("%d webhooks after a loss; order %s", n, paid.Status)
	}

	// A delayed webhook is abandoned by a reset.
	h.setFaults(t, `{"delayRate":1,"delayMin":"300ms","delayMax":"300ms"}`)
	_, o4 := h.order(t, 100)
	h.payJSON(t, o4.OrderID, "pay")
	if resp := h.adminCall(t, http.MethodPost, "/internal/v1/reset", adminToken, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("reset %d", resp.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)
	if n := len(h.rc.webhooks()); n != 3 {
		t.Fatalf("%d webhooks; the reset should have abandoned the delayed one", n)
	}
	if _, err := h.client.GetOrder(context.Background(), o4.OrderID); !errors.Is(err, psp.ErrRejected) {
		t.Fatalf("an order after the reset: %v", err)
	}
}

func TestPaymentFailureFault(t *testing.T) {
	h := newHarness(t)
	h.setFaults(t, `{"failureRate":1}`)
	_, o := h.order(t, 100)
	if _, got := h.payJSON(t, o.OrderID, "pay"); got.Status != psp.OrderFailed || got.FailureReason != "card_declined" {
		t.Fatalf("pay with the failure fault: %+v", got)
	}
}

func TestTimeoutAndOutageFaults(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	// Every answer is held back past the client's 300 ms: the call fails,
	// but the provider did create the order.
	h.setFaults(t, `{"timeoutRate":1,"timeoutDelay":"2s"}`)
	intent := uuid.NewString()
	req := psp.CreateOrder{AmountPaise: 900, Currency: "INR", ExpiresAt: time.Now().Add(7 * time.Minute), Reference: intent}
	if _, err := h.client.CreateOrder(ctx, intent, req); !errors.Is(err, psp.ErrUnavailable) {
		t.Fatalf("with every answer held back: %v", err)
	}
	h.setFaults(t, `{}`)
	o, err := h.client.CreateOrder(ctx, intent, req)
	if err != nil || o.OrderID == "" {
		t.Fatalf("the retry: %+v %v", o, err)
	}
	if st := h.p.store.Stats(); st["orders"] != 1 {
		t.Fatalf("%d orders; the retry must find the one created under the timeout", st["orders"])
	}

	h.setFaults(t, `{"outage":true}`)
	if _, err := h.client.GetOrder(ctx, o.OrderID); !errors.Is(err, psp.ErrUnavailable) {
		t.Fatalf("during an outage: %v", err)
	}
	if code, _ := h.payJSON(t, o.OrderID, "pay"); code != http.StatusServiceUnavailable {
		t.Fatalf("checkout during an outage: %d", code)
	}
}

func TestCheckoutPage(t *testing.T) {
	h := newHarness(t)
	_, o := h.order(t, 250050)
	resp, err := http.Get(o.CheckoutURL) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	page := string(body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, "₹2500.50") || !strings.Contains(page, `action="/checkout/`+o.OrderID+`/pay"`) ||
		!strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'none'") {
		t.Fatalf("checkout page %d:\n%s", resp.StatusCode, page)
	}
	// A browser's form post is redirected back to the page, now captured.
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	post, err := noRedirect.Post(h.public+"/checkout/"+o.OrderID+"/pay", "application/x-www-form-urlencoded", nil) //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	_ = post.Body.Close()
	if post.StatusCode != http.StatusSeeOther || post.Header.Get("Location") != "/checkout/"+o.OrderID {
		t.Fatalf("form post %d %s", post.StatusCode, post.Header.Get("Location"))
	}
	resp, _ = http.Get(o.CheckoutURL) //nolint:noctx // test
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "captured") || strings.Contains(string(body), "<form") {
		t.Fatalf("after paying:\n%s", body)
	}
}

func TestAdminAPI(t *testing.T) {
	h := newHarness(t)
	if resp := h.adminCall(t, http.MethodGet, "/internal/v1/faults", "wrong-token", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a wrong token: %d", resp.StatusCode)
	}
	if resp := h.adminCall(t, http.MethodPut, "/internal/v1/faults", adminToken, `{"lossRate":2}`); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid faults: %d", resp.StatusCode)
	}
	if resp := h.adminCall(t, http.MethodPut, "/internal/v1/faults", adminToken, `{"bogus":1}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("an unknown field: %d", resp.StatusCode)
	}
	h.setFaults(t, `{"duplicateRate":0.2,"lossRate":0.05}`)
	resp := h.adminCall(t, http.MethodGet, "/internal/v1/faults", adminToken, "")
	var f Faults
	if err := json.NewDecoder(resp.Body).Decode(&f); err != nil || f.DuplicateRate != 0.2 || f.LossRate != 0.05 || f.DelayMax.D() != 90*time.Second {
		t.Fatalf("faults %+v %v", f, err)
	}
	h.order(t, 100)
	resp = h.adminCall(t, http.MethodGet, "/internal/v1/stats", adminToken, "")
	var st map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil || st["orders"] != 1 || st["orders_CREATED"] != 1 {
		t.Fatalf("stats %v %v", st, err)
	}
}
