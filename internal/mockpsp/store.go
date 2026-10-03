// Package mockpsp is a stand-in payment provider for HoldFast's development
// and tests (design doc section 4): the API payment-svc calls (orders,
// refunds, settlement reports; contract in internal/payment/psp), a hosted
// checkout page, HMAC-signed webhooks, and an admin API that injects the
// faults a real provider's sandbox cannot: duplicated, delayed and lost
// webhooks, failed payments, slow answers and outages.
//
// State is in memory and resettable between runs. It is a test tool: it
// never handles card details and must never face the internet.
package mockpsp

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

// Errors.
var (
	ErrNotFound        = errors.New("mockpsp: not found")
	ErrKeyReused       = errors.New("mockpsp: idempotency key reused with a different request")
	ErrOrderClosed     = errors.New("mockpsp: the order can no longer be paid")
	ErrNotRefundable   = errors.New("mockpsp: the payment cannot be refunded")
	ErrAlreadyRefunded = errors.New("mockpsp: the payment already has a refund")
)

// Refund statuses.
const (
	RefundPending   = "PENDING"
	RefundCompleted = "COMPLETED"
)

type order struct {
	psp.Order
	reference  string
	key        string
	request    psp.CreateOrder
	createdAt  time.Time
	capturedAt time.Time
}

type refund struct {
	psp.Refund
	key         string
	orderID     string
	completedAt time.Time
}

// Store holds the provider's orders, payments and refunds.
type Store struct {
	mu         sync.Mutex
	orders     map[string]*order  // by order ID
	orderKeys  map[string]string  // idempotency key to order ID
	payments   map[string]string  // payment ID to order ID
	refunds    map[string]*refund // by refund ID
	refundKeys map[string]string  // idempotency key to refund ID
	byPayment  map[string]string  // payment ID to refund ID
	publicURL  string
}

// NewStore returns an empty store whose checkout URLs start with publicURL.
func NewStore(publicURL string) *Store {
	s := &Store{publicURL: publicURL}
	s.Reset()
	return s
}

// Reset forgets everything.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders, s.orderKeys, s.payments = map[string]*order{}, map[string]string{}, map[string]string{}
	s.refunds, s.refundKeys, s.byPayment = map[string]*refund{}, map[string]string{}, map[string]string{}
}

// CreateOrder opens an order, once per idempotency key: the same key with
// the same request returns the same order (created false); with a different
// request, ErrKeyReused.
func (s *Store) CreateOrder(key string, req psp.CreateOrder, now time.Time) (psp.Order, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.orderKeys[key]; ok {
		o := s.orders[id]
		if !sameOrder(o.request, req) {
			return psp.Order{}, false, ErrKeyReused
		}
		return o.Order, false, nil
	}
	id := "order_" + uuid.NewString()
	o := &order{
		Order: psp.Order{
			OrderID: id, CheckoutURL: s.publicURL + "/checkout/" + id, Status: psp.OrderCreated,
			AmountPaise: req.AmountPaise, ExpiresAt: req.ExpiresAt.UTC(),
		},
		reference: req.Reference, key: key, request: req, createdAt: now,
	}
	s.orders[id], s.orderKeys[key] = o, id
	return o.Order, true, nil
}

func sameOrder(a, b psp.CreateOrder) bool {
	return a.AmountPaise == b.AmountPaise && a.Currency == b.Currency && a.Reference == b.Reference && a.ExpiresAt.Equal(b.ExpiresAt)
}

// Order returns an order.
func (s *Store) Order(id string) (psp.Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[id]
	if !ok {
		return psp.Order{}, ErrNotFound
	}
	return o.Order, nil
}

// Pay records a payment attempt on an open order: a capture, or a failure
// with reason. An order that failed can be paid again until it expires; a
// captured one answers with its capture (changed false).
func (s *Store) Pay(id string, succeed bool, reason string, now time.Time) (o psp.Order, changed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ord, ok := s.orders[id]
	switch {
	case !ok:
		return psp.Order{}, false, ErrNotFound
	case ord.Status == psp.OrderCaptured:
		return ord.Order, false, nil
	case ord.Status == psp.OrderExpired || !now.Before(ord.ExpiresAt):
		return ord.Order, false, ErrOrderClosed
	}
	if succeed {
		ord.Status, ord.PaymentID, ord.FailureReason, ord.capturedAt = psp.OrderCaptured, "pay_"+uuid.NewString(), "", now
		s.payments[ord.PaymentID] = id
	} else {
		ord.Status, ord.FailureReason = psp.OrderFailed, reason
	}
	return ord.Order, true, nil
}

// Expire closes the open (created or failed) orders whose time is up and
// returns them.
func (s *Store) Expire(now time.Time) []psp.Order {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []psp.Order
	for _, o := range s.orders {
		if (o.Status == psp.OrderCreated || o.Status == psp.OrderFailed) && !now.Before(o.ExpiresAt) {
			o.Status = psp.OrderExpired
			out = append(out, o.Order)
		}
	}
	return out
}

// CreateRefund starts a full refund of a captured payment, once per
// idempotency key and once per payment.
func (s *Store) CreateRefund(key, paymentID string, amount int64) (psp.Refund, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.refundKeys[key]; ok {
		r := s.refunds[id]
		if r.PaymentID != paymentID || r.AmountPaise != amount {
			return psp.Refund{}, false, ErrKeyReused
		}
		return r.Refund, false, nil
	}
	orderID, ok := s.payments[paymentID]
	if !ok {
		return psp.Refund{}, false, ErrNotFound
	}
	if o := s.orders[orderID]; o.Status != psp.OrderCaptured || o.AmountPaise != amount {
		return psp.Refund{}, false, ErrNotRefundable // full refunds of captures only
	}
	if _, ok := s.byPayment[paymentID]; ok {
		return psp.Refund{}, false, ErrAlreadyRefunded
	}
	r := &refund{
		Refund: psp.Refund{RefundID: "rfnd_" + uuid.NewString(), PaymentID: paymentID, AmountPaise: amount, Status: RefundPending},
		key:    key, orderID: orderID,
	}
	s.refunds[r.RefundID], s.refundKeys[key], s.byPayment[paymentID] = r, r.RefundID, r.RefundID
	return r.Refund, true, nil
}

// CompleteRefund marks a pending refund completed and returns it with its
// order ID; a refund already completed returns changed false.
func (s *Store) CompleteRefund(id string, now time.Time) (r psp.Refund, orderID string, changed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.refunds[id]
	if !ok {
		return psp.Refund{}, "", false, ErrNotFound
	}
	if ref.Status == RefundCompleted {
		return ref.Refund, ref.orderID, false, nil
	}
	ref.Status, ref.completedAt = RefundCompleted, now
	return ref.Refund, ref.orderID, true, nil
}

// Settlement reports the captures and completed refunds in [from, to),
// oldest first.
func (s *Store) Settlement(from, to time.Time) psp.Settlement {
	s.mu.Lock()
	defer s.mu.Unlock()
	rep := psp.Settlement{From: from.UTC(), To: to.UTC(), Items: []psp.SettlementItem{}}
	in := func(t time.Time) bool { return !t.IsZero() && !t.Before(from) && t.Before(to) }
	for _, o := range s.orders {
		if o.PaymentID != "" && in(o.capturedAt) {
			rep.Items = append(rep.Items, psp.SettlementItem{
				Type: psp.SettledCapture, OrderID: o.OrderID, PaymentID: o.PaymentID, Reference: o.reference,
				AmountPaise: o.AmountPaise, At: o.capturedAt.UTC(),
			})
		}
	}
	for _, r := range s.refunds {
		if r.Status == RefundCompleted && in(r.completedAt) {
			rep.Items = append(rep.Items, psp.SettlementItem{
				Type: psp.SettledRefund, OrderID: r.orderID, PaymentID: r.PaymentID, RefundID: r.RefundID,
				Reference: s.orders[r.orderID].reference, AmountPaise: r.AmountPaise, At: r.completedAt.UTC(),
			})
		}
	}
	sort.Slice(rep.Items, func(i, j int) bool { return rep.Items[i].At.Before(rep.Items[j].At) })
	return rep
}

// Stats counts orders by status, and refunds.
func (s *Store) Stats() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{"orders": len(s.orders), "refunds": len(s.refunds)}
	for _, o := range s.orders {
		out["orders_"+o.Status]++
	}
	return out
}
