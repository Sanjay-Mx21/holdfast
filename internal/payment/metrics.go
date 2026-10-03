package payment

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are payment-svc's counters.
type Metrics struct {
	intents  prometheus.Counter
	webhooks *prometheus.CounterVec
	captures *prometheus.CounterVec
	mismatch prometheus.Counter
	polls    *prometheus.CounterVec
}

// NewMetrics registers payment-svc's metrics. Labels are bounded: webhook
// types are the provider's few (anything else counts as "other").
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		intents: f.NewCounter(prometheus.CounterOpts{Name: "holdfast_payment_intents_created_total", Help: "Payment intents created (one per booking)."}),
		webhooks: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_webhooks_total",
			Help: "Verified provider webhooks by type and whether they were duplicates.",
		}, []string{"type", "duplicate"}),
		captures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_payment_captures_total",
			Help: "Intents captured, by how the capture was learned: webhook or poll.",
		}, []string{"via"}),
		mismatch: f.NewCounter(prometheus.CounterOpts{
			Name: "holdfast_payment_amount_mismatch_total",
			Help: "Captures refused because the amount differed from the intent's: page a human.",
		}),
		polls: f.NewCounterVec(prometheus.CounterOpts{Name: "holdfast_payment_polls_total", Help: "Status polls of open intents, by result."}, []string{"result"}),
	}
}

var webhookTypes = map[string]bool{
	"payment.captured": true, "payment.failed": true, "order.expired": true, "refund.completed": true, "malformed": true, "bad_signature": true,
}

func (m *Metrics) webhook(typ string, duplicate bool) {
	if !webhookTypes[typ] {
		typ = "other"
	}
	m.webhooks.WithLabelValues(typ, strconv.FormatBool(duplicate)).Inc()
}

func (m *Metrics) captured(via string) { m.captures.WithLabelValues(via).Inc() }
