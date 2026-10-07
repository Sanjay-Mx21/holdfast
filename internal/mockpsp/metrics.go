package mockpsp

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Sanjay-Mx21/holdfast/internal/payment/psp"
)

// Metrics count what mockpsp did, faults included, so a chaos run can be
// checked against what was injected.
type Metrics struct {
	orders   *prometheus.CounterVec
	payments *prometheus.CounterVec
	webhooks *prometheus.CounterVec
	faults   *prometheus.CounterVec
}

// NewMetrics registers mockpsp's metrics. Every label takes a few fixed
// values.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		orders: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_mockpsp_orders_total", Help: "Order requests: created, replayed (same idempotency key), key_reused.",
		}, []string{"result"}),
		payments: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_mockpsp_payments_total", Help: "Payment attempts and order outcomes: captured, failed, expired, refunded.",
		}, []string{"result"}),
		webhooks: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_mockpsp_webhooks_total",
			Help: "Webhooks by type and result: delivered, retried, gave_up, and the faults dropped, duplicated, delayed.",
		}, []string{"type", "result"}),
		faults: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_mockpsp_faults_total", Help: "API faults injected: timeout, outage.",
		}, []string{"fault"}),
	}
	// Every series at zero from the start, so increase() and the chaos
	// scripts' before-and-after counts see the first event (P42, P54).
	for _, r := range []string{"created", "replayed", "key_reused"} {
		m.orders.WithLabelValues(r)
	}
	for _, r := range []string{"captured", "failed", "expired", "refunded"} {
		m.payments.WithLabelValues(r)
	}
	for typ := range webhookTypes {
		for _, r := range []string{"delivered", "retried", "gave_up", "dropped", "duplicated", "delayed"} {
			m.webhooks.WithLabelValues(typ, r)
		}
	}
	for _, f := range []string{"timeout", "outage"} {
		m.faults.WithLabelValues(f)
	}
	return m
}

var webhookTypes = map[string]bool{
	psp.EventPaymentCaptured: true, psp.EventPaymentFailed: true, psp.EventOrderExpired: true, psp.EventRefundCompleted: true,
}

func (m *Metrics) webhook(typ, result string) {
	if !webhookTypes[typ] {
		typ = "other"
	}
	m.webhooks.WithLabelValues(typ, result).Inc()
}
