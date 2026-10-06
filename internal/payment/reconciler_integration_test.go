//go:build integration

package payment

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Sanjay-Mx21/holdfast/internal/mockpsp"
	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

// testRecon is a reconciler with no margins, so a capture just booked counts.
func testRecon(svc *Service, s Settlements) *Reconciler {
	return NewReconciler(svc, s, ReconcilerConfig{Interval: time.Hour, Window: time.Hour, RefundStuckAfter: 10 * time.Minute}, quietLog)
}

// findingsFor keeps the findings about one intent: the test database is
// shared, so a pass also reports other tests' intents.
func findingsFor(fs []Finding, id uuid.UUID) []string {
	var kinds []string
	for _, f := range fs {
		if f.IntentID == id {
			kinds = append(kinds, f.Kind)
		}
	}
	return kinds
}

// TestReconcilerRecoversALostCaptureWebhook: the provider captured, its
// webhook was lost, and no poll ran. The settlement report brings the
// capture in through the same path as a webhook: the intent is captured,
// booked once and announced, and the next pass finds nothing more.
func TestReconcilerRecoversALostCaptureWebhook(t *testing.T) {
	w := newMockPSPFixture(t)
	if err := w.faults.Set(mockpsp.Faults{LossRate: 1}); err != nil {
		t.Fatal(err)
	}
	b := booking{uuid.New(), uuid.New()}
	in, err := w.svc.CreateIntent(ctx, b.id, b.event, 4200, time.Now().Add(7*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	paid := w.pay(t, in.CheckoutURL)
	time.Sleep(150 * time.Millisecond)
	if s := w.status(t, in.ID); s != "CREATED" {
		t.Fatalf("status %s before reconciling; the webhook should have been lost", s)
	}

	r := testRecon(w.svc, w.svc.psp.(*psp.Client))
	fs, err := r.Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := findingsFor(fs, in.ID); len(got) != 1 || got[0] != FindingMissedCapture {
		t.Fatalf("findings %v, want [missed_capture]", got)
	}
	if s := w.status(t, in.ID); s != "CAPTURED" {
		t.Fatalf("status %s after reconciling", s)
	}
	stored, _ := w.svc.q.GetIntent(ctx, in.ID)
	if stored.PspPaymentID.String != paid.PaymentID || w.balance(t, in.ID, "psp_receivable") != 4200 {
		t.Fatalf("payment %q, receivable %d", stored.PspPaymentID.String, w.balance(t, in.ID, "psp_receivable"))
	}
	if got := w.events(t, b); len(got) != 1 || got[0] != "payment.captured.v1" {
		t.Fatalf("events %v", got)
	}
	if testutil.ToFloat64(w.m.captures.WithLabelValues("reconciler")) != 1 {
		t.Fatal("the capture was not counted as the reconciler's")
	}

	// Nothing more to do: the capture is now on both sides.
	fs, err = r.Pass(ctx)
	if err != nil || len(findingsFor(fs, in.ID)) != 0 {
		t.Fatalf("second pass: %v, %v", findingsFor(fs, in.ID), err)
	}
	if w.balance(t, in.ID, "psp_receivable") != 4200 {
		t.Fatal("the capture was booked twice")
	}
}

// TestReconcilerCompletesARefundWhoseWebhookWasLost: the provider completed
// a refund, but its webhook was lost; the report completes it.
func TestReconcilerCompletesARefundWhoseWebhookWasLost(t *testing.T) {
	w := newMockPSPFixture(t)
	b := booking{uuid.New(), uuid.New()}
	in, err := w.svc.CreateIntent(ctx, b.id, b.event, 3300, time.Now().Add(7*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	w.pay(t, in.CheckoutURL)
	w.waitStatus(t, in.ID, "CAPTURED")
	if err := w.faults.Set(mockpsp.Faults{LossRate: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.svc.HandleBookingEvent(ctx, refundRequired(b)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // the provider completes the refund; its webhook is lost
	if s := w.status(t, in.ID); s != "REFUND_PENDING" {
		t.Fatalf("status %s before reconciling", s)
	}

	fs, err := testRecon(w.svc, w.svc.psp.(*psp.Client)).Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := findingsFor(fs, in.ID); len(got) != 1 || got[0] != FindingMissedRefund {
		t.Fatalf("findings %v, want [missed_refund]", got)
	}
	if s := w.status(t, in.ID); s != "REFUNDED" || w.balance(t, in.ID, "psp_receivable") != 0 {
		t.Fatalf("status %s, receivable %d", s, w.balance(t, in.ID, "psp_receivable"))
	}
	if got := w.events(t, b); len(got) != 2 || got[1] != "refund.completed.v1" {
		t.Fatalf("events %v", got)
	}
}

// scripted is a settlement report the test writes.
type scripted struct{ items []psp.SettlementItem }

func (s *scripted) Settlements(_ context.Context, from, to time.Time) (psp.Settlement, error) {
	return psp.Settlement{From: from, To: to, Items: s.items}, nil
}

// TestReconcilerReportsWhatNeedsAHuman covers every finding a human must
// look at, which the reconciler never changes, and a stuck refund, which
// it asks for again.
func TestReconcilerReportsWhatNeedsAHuman(t *testing.T) {
	f := newFixture(t)
	now := time.Now()

	// Captured in HoldFast, unknown to the provider.
	_, onlyOurs, order := f.intent(t, 1000)
	f.apply(t, hook(psp.EventPaymentCaptured, order, payID(), 1000))
	// Open, but the provider captured a different amount.
	_, wrongAmount, wrongOrder := f.intent(t, 2000)
	// Captured on both sides, and the provider also refunded it, unasked.
	_, refundedUnasked, order3 := f.intent(t, 3000)
	pay3 := payID()
	f.apply(t, hook(psp.EventPaymentCaptured, order3, pay3, 3000))
	// A refund requested an hour ago whose provider call never succeeded.
	b4, stuck, order4 := f.intent(t, 4000)
	pay4 := payID()
	f.apply(t, hook(psp.EventPaymentCaptured, order4, pay4, 4000))
	f.psp.setDown(true)
	if err := f.svc.HandleBookingEvent(ctx, refundRequired(b4)); err == nil {
		t.Fatal("the refund call should have failed while the provider is down")
	}
	f.psp.setDown(false)
	if _, err := f.pool.Exec(ctx, `UPDATE payment.payment_intents SET updated_at = now() - interval '1 hour' WHERE id = $1`, stuck.ID); err != nil {
		t.Fatal(err)
	}
	stranger := uuid.New()

	report := &scripted{items: []psp.SettlementItem{
		{Type: psp.SettledCapture, OrderID: wrongOrder, PaymentID: payID(), Reference: wrongAmount.ID.String(), AmountPaise: 2500, At: now},
		{Type: psp.SettledCapture, OrderID: order3, PaymentID: pay3, Reference: refundedUnasked.ID.String(), AmountPaise: 3000, At: now},
		{Type: psp.SettledRefund, OrderID: order3, PaymentID: pay3, RefundID: "rfnd_x", Reference: refundedUnasked.ID.String(), AmountPaise: 3000, At: now},
		{Type: psp.SettledCapture, OrderID: order4, PaymentID: pay4, Reference: stuck.ID.String(), AmountPaise: 4000, At: now},
		{Type: psp.SettledCapture, OrderID: "order_elsewhere", PaymentID: payID(), Reference: stranger.String(), AmountPaise: 500, At: now},
		{Type: psp.SettledCapture, OrderID: "order_odd", PaymentID: payID(), Reference: "not-an-intent", AmountPaise: 500, At: now},
	}}
	fs, err := testRecon(f.svc, report).Pass(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uuid.UUID]string{
		onlyOurs.ID:        FindingCaptureUnknown,
		wrongAmount.ID:     FindingAmountMismatch,
		refundedUnasked.ID: FindingRefundUnknown,
		stuck.ID:           FindingRefundStuck,
		stranger:           FindingUnknownOrder,
	}
	for id, kind := range want {
		if got := findingsFor(fs, id); len(got) != 1 || got[0] != kind {
			t.Errorf("intent %s: findings %v, want [%s]", id, got, kind)
		}
	}
	if got := findingsFor(fs, uuid.Nil); len(got) != 1 || got[0] != FindingUnknownOrder {
		t.Errorf("a reference that is not an intent ID: findings %v, want [unknown_order]", got)
	}

	// Nothing a human must decide was changed.
	if s := f.status(t, wrongAmount.ID); s != "CREATED" {
		t.Errorf("the mismatched intent became %s", s)
	}
	if s := f.status(t, refundedUnasked.ID); s != "CAPTURED" {
		t.Errorf("the unasked refund moved the intent to %s", s)
	}
	// The stuck refund was asked for again.
	if n := len(f.psp.refunds); n == 0 || f.psp.refunds[n-1] != stuck.ID.String() {
		t.Errorf("refund calls %v, want one for %s", f.psp.refunds, stuck.ID)
	}
	if testutil.ToFloat64(f.m.reconMismatch.WithLabelValues(FindingAmountMismatch)) != 1 {
		t.Error("the amount mismatch was not counted")
	}
	if testutil.ToFloat64(f.m.reconLastSuccess) == 0 {
		t.Error("the pass did not record its success")
	}
}

// TestOneReconcilerAtATime: while another replica holds the lock, a pass
// does nothing.
func TestOneReconcilerAtATime(t *testing.T) {
	f := newFixture(t)
	conn, err := f.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", reconLockKey); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", reconLockKey) }()

	_, onlyOurs, order := f.intent(t, 1000)
	f.apply(t, hook(psp.EventPaymentCaptured, order, payID(), 1000))
	fs, err := testRecon(f.svc, &scripted{}).Pass(ctx)
	if err != nil || len(findingsFor(fs, onlyOurs.ID)) != 0 {
		t.Fatalf("with the lock held elsewhere: %v, %v", fs, err)
	}
	if testutil.ToFloat64(f.m.reconRuns.WithLabelValues("skipped")) != 1 {
		t.Fatal("the skipped pass was not counted")
	}
}
