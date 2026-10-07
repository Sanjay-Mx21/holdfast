package mockpsp

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/httpx"
)

// Config configures the provider.
type Config struct {
	// APIKey, if set, is the bearer key every API call must carry.
	APIKey string
	// RefundDelay is how long a refund stays PENDING (default 2 s).
	RefundDelay time.Duration
	// ExpiryInterval is how often open orders are checked for expiry
	// (default 1 s).
	ExpiryInterval time.Duration
}

// Provider is mockpsp: its store, faults and webhooks behind HTTP.
type Provider struct {
	cfg    Config
	store  *Store
	faults *Injector
	hooks  *Dispatcher
	m      *Metrics
	log    *slog.Logger
	now    func() time.Time
}

// New returns a provider.
func New(cfg Config, store *Store, faults *Injector, hooks *Dispatcher, m *Metrics, log *slog.Logger) *Provider {
	if cfg.RefundDelay <= 0 {
		cfg.RefundDelay = 2 * time.Second
	}
	if cfg.ExpiryInterval <= 0 {
		cfg.ExpiryInterval = time.Second
	}
	return &Provider{cfg: cfg, store: store, faults: faults, hooks: hooks, m: m, log: log, now: time.Now}
}

// Register mounts the provider API and the checkout page on public, and
// the fault-injection API on admin behind operator.
func (p *Provider) Register(public, admin *httpx.Router, operator httpx.Middleware) {
	api := func(h http.HandlerFunc) http.Handler { return p.api(httpx.BodyLimit(16 << 10)(h)) }
	public.Handle("POST /v1/orders", api(p.createOrder))
	public.Handle("GET /v1/orders/{orderID}", api(p.getOrder))
	public.Handle("POST /v1/refunds", api(p.createRefund))
	public.Handle("GET /v1/settlements", api(p.settlements))
	public.HandleFunc("GET /checkout/{orderID}", p.checkoutPage)
	public.Handle("POST /checkout/{orderID}/pay", httpx.BodyLimit(4<<10)(http.HandlerFunc(p.pay)))
	public.Handle("POST /checkout/{orderID}/fail", httpx.BodyLimit(4<<10)(http.HandlerFunc(p.decline)))

	admin.Handle("GET /internal/v1/faults", operator(http.HandlerFunc(p.getFaults)))
	admin.Handle("PUT /internal/v1/faults", operator(httpx.BodyLimit(4<<10)(http.HandlerFunc(p.putFaults))))
	admin.Handle("POST /internal/v1/reset", operator(http.HandlerFunc(p.reset)))
	admin.Handle("GET /internal/v1/stats", operator(http.HandlerFunc(p.stats)))
}

// api guards the provider API: the outage fault, the API key, and the
// timeout fault, which processes the call but holds its answer back.
func (p *Provider) api(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.faults.Outage() {
			p.m.faults.WithLabelValues("outage").Inc()
			httpx.WriteProblem(w, r, httpx.Unavailable("the provider is down (outage fault)", 5))
			return
		}
		if p.cfg.APIKey != "" {
			scheme, key, _ := strings.Cut(r.Header.Get("Authorization"), " ")
			if !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare([]byte(key), []byte(p.cfg.APIKey)) != 1 {
				httpx.WriteProblem(w, r, httpx.Unauthorized("UNAUTHENTICATED", "a valid API key is required"))
				return
			}
		}
		delay := p.faults.Timeout()
		if delay == 0 {
			next.ServeHTTP(w, r)
			return
		}
		p.m.faults.WithLabelValues("timeout").Inc()
		buf := &heldResponse{header: http.Header{}, status: http.StatusOK}
		next.ServeHTTP(buf, r)
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-r.Context().Done(): // the client gave up: the work is done, the answer lost
			return
		case <-t.C:
		}
		for k, v := range buf.header {
			w.Header()[k] = v
		}
		w.WriteHeader(buf.status)
		_, _ = w.Write(buf.body.Bytes())
	})
}

// heldResponse buffers an answer for the timeout fault.
type heldResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (h *heldResponse) Header() http.Header         { return h.header }
func (h *heldResponse) Write(b []byte) (int, error) { return h.body.Write(b) }
func (h *heldResponse) WriteHeader(status int)      { h.status = status }

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		httpx.WriteProblem(w, r, httpx.BadRequest("IDEMPOTENCY_KEY_REQUIRED", "an Idempotency-Key header of 1 to 255 characters is required"))
		return "", false
	}
	return key, true
}

func (p *Provider) createOrder(w http.ResponseWriter, r *http.Request) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req psp.CreateOrder
	if prob := httpx.DecodeJSON(r, &req); prob != nil {
		httpx.WriteProblem(w, r, prob)
		return
	}
	if req.AmountPaise <= 0 || req.Currency != "INR" || req.Reference == "" || !req.ExpiresAt.After(p.now()) {
		httpx.WriteProblem(w, r, httpx.Unprocessable("INVALID_ORDER", "amountPaise must be positive, currency INR, reference set and expiresAt in the future"))
		return
	}
	o, created, err := p.store.CreateOrder(key, req, p.now())
	if errors.Is(err, ErrKeyReused) {
		p.m.orders.WithLabelValues("key_reused").Inc()
		httpx.WriteProblem(w, r, httpx.Unprocessable("IDEMPOTENCY_KEY_REUSED", err.Error()))
		return
	}
	if !created {
		p.m.orders.WithLabelValues("replayed").Inc()
		w.Header().Set("Idempotent-Replayed", "true")
		httpx.WriteJSON(w, http.StatusOK, o)
		return
	}
	p.m.orders.WithLabelValues("created").Inc()
	httpx.WriteJSON(w, http.StatusCreated, o)
}

func (p *Provider) getOrder(w http.ResponseWriter, r *http.Request) {
	o, err := p.store.Order(r.PathValue("orderID"))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.NotFound("ORDER_NOT_FOUND", "no such order"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, o)
}

type refundRequest struct {
	PaymentID   string `json:"paymentId"`
	AmountPaise int64  `json:"amountPaise"`
}

func (p *Provider) createRefund(w http.ResponseWriter, r *http.Request) {
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req refundRequest
	if prob := httpx.DecodeJSON(r, &req); prob != nil {
		httpx.WriteProblem(w, r, prob)
		return
	}
	ref, created, err := p.store.CreateRefund(key, req.PaymentID, req.AmountPaise)
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.WriteProblem(w, r, httpx.NotFound("PAYMENT_NOT_FOUND", "no such payment"))
		return
	case errors.Is(err, ErrKeyReused):
		httpx.WriteProblem(w, r, httpx.Unprocessable("IDEMPOTENCY_KEY_REUSED", err.Error()))
		return
	case errors.Is(err, ErrNotRefundable):
		httpx.WriteProblem(w, r, httpx.Unprocessable("NOT_REFUNDABLE", "only a captured payment can be refunded, in full"))
		return
	case errors.Is(err, ErrAlreadyRefunded):
		httpx.WriteProblem(w, r, httpx.Conflict("ALREADY_REFUNDED", "the payment already has a refund"))
		return
	case err != nil:
		httpx.WriteProblem(w, r, httpx.Internal())
		return
	}
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		httpx.WriteJSON(w, http.StatusOK, ref)
		return
	}
	time.AfterFunc(p.cfg.RefundDelay, func() { p.completeRefund(ref.RefundID) })
	httpx.WriteJSON(w, http.StatusCreated, ref)
}

func (p *Provider) completeRefund(id string) {
	ref, orderID, changed, err := p.store.CompleteRefund(id, p.now())
	if err != nil || !changed { // reset meanwhile, or already done
		return
	}
	p.m.payments.WithLabelValues("refunded").Inc()
	p.hooks.Send(psp.Webhook{
		Type: psp.EventRefundCompleted, OrderID: orderID, PaymentID: ref.PaymentID, RefundID: ref.RefundID, AmountPaise: ref.AmountPaise,
	})
}

func (p *Provider) settlements(w http.ResponseWriter, r *http.Request) {
	from, err1 := time.Parse(time.RFC3339Nano, r.URL.Query().Get("from"))
	to, err2 := time.Parse(time.RFC3339Nano, r.URL.Query().Get("to"))
	if err1 != nil || err2 != nil || !to.After(from) || to.Sub(from) > 31*24*time.Hour {
		httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_WINDOW", "from and to must be RFC 3339 times, from before to, at most 31 days apart"))
		return
	}
	// Paged (P58): up to limit items per answer, and "next" to ask for more.
	limit := defaultSettlementPage
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxSettlementPage {
			httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_LIMIT", fmt.Sprintf("limit must be 1 to %d", maxSettlementPage)))
			return
		}
		limit = n
	}
	page, err := p.store.SettlementPage(from, to, r.URL.Query().Get("after"), limit)
	if err != nil {
		httpx.WriteProblem(w, r, httpx.BadRequest("INVALID_CURSOR", "after must be a cursor from a previous page's next"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, page)
}

// The settlement report's page sizes.
const (
	defaultSettlementPage = 1000
	maxSettlementPage     = 5000
)

// Name implements app.Component: the expiry loop.
func (p *Provider) Name() string { return "order-expiry" }

// Run implements app.Component: open orders past their expiry are closed
// and announced (order.expired).
func (p *Provider) Run(ctx context.Context) error {
	t := time.NewTicker(p.cfg.ExpiryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		p.ExpireOrders()
	}
}

// ExpireOrders runs one expiry pass.
func (p *Provider) ExpireOrders() {
	for _, o := range p.store.Expire(p.now()) {
		p.m.payments.WithLabelValues("expired").Inc()
		p.hooks.Send(psp.Webhook{Type: psp.EventOrderExpired, OrderID: o.OrderID, AmountPaise: o.AmountPaise})
	}
}

// Pay attempts a payment on an order, as the buyer's "Pay" button does:
// it captures unless the failure fault strikes. It returns the order.
func (p *Provider) Pay(orderID string) (psp.Order, error) {
	if p.faults.Fail() {
		return p.attempt(orderID, false, "card_declined")
	}
	return p.attempt(orderID, true, "")
}

func (p *Provider) attempt(orderID string, succeed bool, reason string) (psp.Order, error) {
	o, changed, err := p.store.Pay(orderID, succeed, reason, p.now())
	if err != nil || !changed {
		return o, err
	}
	if o.Status == psp.OrderCaptured {
		p.m.payments.WithLabelValues("captured").Inc()
		p.hooks.Send(psp.Webhook{Type: psp.EventPaymentCaptured, OrderID: o.OrderID, PaymentID: o.PaymentID, AmountPaise: o.AmountPaise})
	} else {
		p.m.payments.WithLabelValues("failed").Inc()
		p.hooks.Send(psp.Webhook{Type: psp.EventPaymentFailed, OrderID: o.OrderID, AmountPaise: o.AmountPaise, Reason: o.FailureReason})
	}
	return o, nil
}

func (p *Provider) pay(w http.ResponseWriter, r *http.Request) {
	p.checkoutAction(w, r, func(id string) (psp.Order, error) { return p.Pay(id) })
}

func (p *Provider) decline(w http.ResponseWriter, r *http.Request) {
	p.checkoutAction(w, r, func(id string) (psp.Order, error) { return p.attempt(id, false, "declined_by_buyer") })
}

// checkoutAction serves the checkout buttons: JSON clients (load tests) get
// the order; browsers are sent back to the page.
func (p *Provider) checkoutAction(w http.ResponseWriter, r *http.Request, act func(string) (psp.Order, error)) {
	if p.faults.Outage() {
		p.m.faults.WithLabelValues("outage").Inc()
		httpx.WriteProblem(w, r, httpx.Unavailable("the provider is down (outage fault)", 5))
		return
	}
	id := r.PathValue("orderID")
	o, err := act(id)
	wantsJSON := strings.Contains(r.Header.Get("Accept"), "application/json")
	switch {
	case errors.Is(err, ErrNotFound):
		httpx.WriteProblem(w, r, httpx.NotFound("ORDER_NOT_FOUND", "no such order"))
	case errors.Is(err, ErrOrderClosed):
		httpx.WriteProblem(w, r, httpx.Conflict("ORDER_CLOSED", "the order has expired"))
	case err != nil:
		httpx.WriteProblem(w, r, httpx.Internal())
	case wantsJSON:
		httpx.WriteJSON(w, http.StatusOK, o)
	default:
		// A relative path built from the stored order's ID, never from input.
		http.Redirect(w, r, "/checkout/"+url.PathEscape(o.OrderID), http.StatusSeeOther)
	}
}

var checkoutTmpl = template.Must(template.New("checkout").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>mockpsp checkout</title>
<style>
body{font-family:system-ui,sans-serif;max-width:28rem;margin:3rem auto;padding:0 1rem;color:#1d1d1f;background:#fafafa}
.card{background:#fff;border:1px solid #ddd;border-radius:12px;padding:1.5rem}
.amount{font-size:2rem;font-weight:600;margin:.5rem 0}
.muted{color:#666;font-size:.875rem}
.status{display:inline-block;padding:.2rem .6rem;border-radius:999px;background:#eee;font-weight:600}
form{display:inline}
button{font:inherit;padding:.6rem 1.2rem;border-radius:8px;border:1px solid #1d1d1f;cursor:pointer;margin-right:.5rem}
.pay{background:#1d1d1f;color:#fff}
.banner{background:#fff4d6;border:1px solid #e8c766;border-radius:8px;padding:.5rem .75rem;margin-bottom:1rem;font-size:.875rem}
</style></head><body>
<div class="banner">Test payment provider. No real money moves and no card details are asked for.</div>
<div class="card">
<div class="muted">Order {{.OrderID}}</div>
<div class="amount">₹{{.Rupees}}</div>
<p>Status: <span class="status">{{.Status}}</span>{{if .FailureReason}} ({{.FailureReason}}){{end}}</p>
{{if .Open}}<p class="muted">Pay before {{.ExpiresAt}}.</p>
<form method="post" action="/checkout/{{.OrderID}}/pay"><button class="pay" type="submit">Pay ₹{{.Rupees}}</button></form>
<form method="post" action="/checkout/{{.OrderID}}/fail"><button type="submit">Decline</button></form>
{{else if eq .Status "CAPTURED"}}<p>Payment {{.PaymentID}} captured. You can close this page.</p>
{{else}}<p>This order can no longer be paid.</p>{{end}}
</div></body></html>
`))

type checkoutView struct {
	psp.Order
	Rupees string
	Open   bool
}

func (p *Provider) checkoutPage(w http.ResponseWriter, r *http.Request) {
	if p.faults.Outage() {
		httpx.WriteProblem(w, r, httpx.Unavailable("the provider is down (outage fault)", 5))
		return
	}
	o, err := p.store.Order(r.PathValue("orderID"))
	if err != nil {
		httpx.WriteProblem(w, r, httpx.NotFound("ORDER_NOT_FOUND", "no such order"))
		return
	}
	v := checkoutView{
		Order:  o,
		Rupees: rupees(o.AmountPaise),
		Open:   (o.Status == psp.OrderCreated || o.Status == psp.OrderFailed) && p.now().Before(o.ExpiresAt),
	}
	var buf bytes.Buffer
	if err := checkoutTmpl.Execute(&buf, v); err != nil {
		httpx.WriteProblem(w, r, httpx.Internal())
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'")
	_, _ = w.Write(buf.Bytes())
}

// rupees formats paise as rupees with two decimals.
func rupees(paise int64) string { return fmt.Sprintf("%d.%02d", paise/100, paise%100) }

func (p *Provider) getFaults(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, p.faults.Faults())
}

func (p *Provider) putFaults(w http.ResponseWriter, r *http.Request) {
	var f Faults
	if prob := httpx.DecodeJSON(r, &f); prob != nil {
		httpx.WriteProblem(w, r, prob)
		return
	}
	if err := p.faults.Set(f); err != nil {
		httpx.WriteProblem(w, r, httpx.Unprocessable("INVALID_FAULTS", err.Error()))
		return
	}
	p.log.Info("mockpsp: faults set", "faults", p.faults.Faults())
	httpx.WriteJSON(w, http.StatusOK, p.faults.Faults())
}

// reset forgets every order and refund, abandons pending webhooks and
// clears the faults.
func (p *Provider) reset(w http.ResponseWriter, _ *http.Request) {
	p.store.Reset()
	p.hooks.Reset()
	_ = p.faults.Set(Faults{})
	p.log.Info("mockpsp: reset")
	w.WriteHeader(http.StatusNoContent)
}

func (p *Provider) stats(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, p.store.Stats())
}
