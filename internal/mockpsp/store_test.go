package mockpsp

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

var t0 = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func newOrder(t *testing.T, s *Store, key string, amount int64) psp.Order {
	t.Helper()
	o, created, err := s.CreateOrder(key, psp.CreateOrder{AmountPaise: amount, Currency: "INR", ExpiresAt: t0.Add(7 * time.Minute), Reference: "intent-" + key}, t0)
	if err != nil || !created {
		t.Fatalf("CreateOrder = %+v, %v, %v", o, created, err)
	}
	return o
}

func TestOrdersAreIdempotentPerKey(t *testing.T) {
	s := NewStore("http://psp.test")
	o := newOrder(t, s, "k1", 5000)
	if o.Status != psp.OrderCreated || o.CheckoutURL != "http://psp.test/checkout/"+o.OrderID {
		t.Fatalf("order %+v", o)
	}
	again, created, err := s.CreateOrder("k1", psp.CreateOrder{AmountPaise: 5000, Currency: "INR", ExpiresAt: t0.Add(7 * time.Minute), Reference: "intent-k1"}, t0)
	if err != nil || created || again.OrderID != o.OrderID {
		t.Fatalf("the same request again: %+v %v %v", again, created, err)
	}
	if _, _, err := s.CreateOrder("k1", psp.CreateOrder{AmountPaise: 5001, Currency: "INR", ExpiresAt: t0.Add(7 * time.Minute), Reference: "intent-k1"}, t0); !errors.Is(err, ErrKeyReused) {
		t.Fatalf("the key with another amount: %v", err)
	}
}

func TestPaymentAttempts(t *testing.T) {
	s := NewStore("http://psp.test")
	o := newOrder(t, s, "k1", 5000)
	failed, changed, err := s.Pay(o.OrderID, false, "card_declined", t0.Add(time.Minute))
	if err != nil || !changed || failed.Status != psp.OrderFailed || failed.FailureReason != "card_declined" {
		t.Fatalf("failed attempt: %+v %v %v", failed, changed, err)
	}
	// A failed order can be paid again before it expires.
	paid, changed, err := s.Pay(o.OrderID, true, "", t0.Add(2*time.Minute))
	if err != nil || !changed || paid.Status != psp.OrderCaptured || paid.PaymentID == "" || paid.FailureReason != "" {
		t.Fatalf("second attempt: %+v %v %v", paid, changed, err)
	}
	// Once captured, further attempts change nothing.
	if again, changed, err := s.Pay(o.OrderID, false, "x", t0.Add(3*time.Minute)); err != nil || changed || again.PaymentID != paid.PaymentID {
		t.Fatalf("after the capture: %+v %v %v", again, changed, err)
	}
	if _, _, err := s.Pay("order_nope", true, "", t0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown order: %v", err)
	}
	// An order past its expiry cannot be paid, even before the sweep.
	late := newOrder(t, s, "k2", 100)
	if _, _, err := s.Pay(late.OrderID, true, "", t0.Add(8*time.Minute)); !errors.Is(err, ErrOrderClosed) {
		t.Fatalf("a late payment: %v", err)
	}
}

func TestExpiry(t *testing.T) {
	s := NewStore("http://psp.test")
	open := newOrder(t, s, "k1", 100)
	failed := newOrder(t, s, "k2", 100)
	paid := newOrder(t, s, "k3", 100)
	_, _, _ = s.Pay(failed.OrderID, false, "card_declined", t0)
	_, _, _ = s.Pay(paid.OrderID, true, "", t0)
	if got := s.Expire(t0.Add(6 * time.Minute)); len(got) != 0 {
		t.Fatalf("expired early: %v", got)
	}
	got := s.Expire(t0.Add(7 * time.Minute))
	if len(got) != 2 {
		t.Fatalf("expired %d orders, want the open and the failed one", len(got))
	}
	for _, id := range []string{open.OrderID, failed.OrderID} {
		if o, _ := s.Order(id); o.Status != psp.OrderExpired {
			t.Fatalf("%s is %s", id, o.Status)
		}
	}
	if o, _ := s.Order(paid.OrderID); o.Status != psp.OrderCaptured {
		t.Fatalf("captured order is %s", o.Status)
	}
	if again := s.Expire(t0.Add(8 * time.Minute)); len(again) != 0 {
		t.Fatalf("expired twice: %v", again)
	}
}

// P58: the report comes in pages that neither skip nor repeat an item, even
// when several items share a time across a page boundary.
func TestSettlementPages(t *testing.T) {
	s := NewStore("http://psp.test")
	want := map[string]bool{}
	for i := range 7 {
		o := newOrder(t, s, fmt.Sprintf("p%d", i), 100)
		at := t0.Add(time.Minute)
		if i >= 4 {
			at = t0.Add(2 * time.Minute)
		}
		paid, _, _ := s.Pay(o.OrderID, true, "", at) // four captures share one time, three another
		want[paid.PaymentID] = true
	}
	seen := map[string]bool{}
	after, pages := "", 0
	for {
		page, err := s.SettlementPage(t0, t0.Add(time.Hour), after, 3)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, it := range page.Items {
			if seen[it.PaymentID] {
				t.Fatalf("page %d repeats %s", pages, it.PaymentID)
			}
			seen[it.PaymentID] = true
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	if pages != 3 || len(seen) != len(want) {
		t.Fatalf("%d pages, %d items; want 3 pages and %d items", pages, len(seen), len(want))
	}
	for id := range want {
		if !seen[id] {
			t.Fatalf("%s missing from the pages", id)
		}
	}
	if _, err := s.SettlementPage(t0, t0.Add(time.Hour), "not-a-cursor!", 3); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("a made-up cursor: %v", err)
	}
	if page, _ := s.SettlementPage(t0, t0.Add(time.Hour), "", 10); page.Next != "" || len(page.Items) != 7 {
		t.Fatalf("one page for everything: %d items, next %q", len(page.Items), page.Next)
	}
}

func TestRefundsAndSettlement(t *testing.T) {
	s := NewStore("http://psp.test")
	o := newOrder(t, s, "k1", 5000)
	open := newOrder(t, s, "k2", 700)
	paid, _, _ := s.Pay(o.OrderID, true, "", t0.Add(time.Minute))

	if _, _, err := s.CreateRefund("r1", paid.PaymentID, 4000); !errors.Is(err, ErrNotRefundable) {
		t.Fatalf("partial refund: %v", err)
	}
	if _, _, err := s.CreateRefund("r1", "pay_nope", 5000); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown payment: %v", err)
	}
	r, created, err := s.CreateRefund("r1", paid.PaymentID, 5000)
	if err != nil || !created || r.Status != RefundPending {
		t.Fatalf("refund: %+v %v %v", r, created, err)
	}
	if again, created, err := s.CreateRefund("r1", paid.PaymentID, 5000); err != nil || created || again.RefundID != r.RefundID {
		t.Fatalf("the same refund again: %+v %v %v", again, created, err)
	}
	if _, _, err := s.CreateRefund("r2", paid.PaymentID, 5000); !errors.Is(err, ErrAlreadyRefunded) {
		t.Fatalf("a second refund: %v", err)
	}
	done, orderID, changed, err := s.CompleteRefund(r.RefundID, t0.Add(2*time.Minute))
	if err != nil || !changed || done.Status != RefundCompleted || orderID != o.OrderID {
		t.Fatalf("complete: %+v %s %v %v", done, orderID, changed, err)
	}
	if _, _, changed, _ := s.CompleteRefund(r.RefundID, t0.Add(3*time.Minute)); changed {
		t.Fatal("completed twice")
	}

	rep := s.Settlement(t0, t0.Add(time.Hour))
	if len(rep.Items) != 2 {
		t.Fatalf("settlement %+v", rep)
	}
	capture, refund := rep.Items[0], rep.Items[1]
	if capture.Type != psp.SettledCapture || capture.PaymentID != paid.PaymentID || capture.Reference != "intent-k1" || capture.AmountPaise != 5000 {
		t.Fatalf("capture item %+v", capture)
	}
	if refund.Type != psp.SettledRefund || refund.RefundID != r.RefundID || refund.OrderID != o.OrderID || refund.Reference != "intent-k1" {
		t.Fatalf("refund item %+v", refund)
	}
	if rep := s.Settlement(t0.Add(90*time.Second), t0.Add(time.Hour)); len(rep.Items) != 1 || rep.Items[0].Type != psp.SettledRefund {
		t.Fatalf("a window after the capture: %+v", rep.Items)
	}
	_ = open // never captured: never settled

	s.Reset()
	if _, err := s.Order(o.OrderID); !errors.Is(err, ErrNotFound) {
		t.Fatal("Reset kept an order")
	}
}
