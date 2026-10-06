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
	refunds  *prometheus.CounterVec

	// The reconciler (design doc 9.9).
	reconMismatch    *prometheus.CounterVec
	reconRuns        *prometheus.CounterVec
	reconLastSuccess prometheus.Gauge
}

// NewMetrics registers payment-svc's metrics. Labels are bounded: webhook
// types are the provider's few (anything else counts as "other").
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		intents: f.NewCounter(prometheus.CounterOpts{Name: "holdfast_payment_intents_created_total", Help: "Payment intents created (one per booking)."}),
		webhooks: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_webhooks_total",
			Help: "Verified provider webhooks by type and whether they were duplicates.",
		}, []string{"type", "duplicate"}),
		captures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_payment_captures_total",
			Help: "Intents captured, by how the capture was learned: webhook, poll or reconciler.",
		}, []string{"via"}),
		mismatch: f.NewCounter(prometheus.CounterOpts{
			Name: "holdfast_payment_amount_mismatch_total",
			Help: "Captures refused because the amount differed from the intent's: page a human.",
		}),
		polls: f.NewCounterVec(prometheus.CounterOpts{Name: "holdfast_payment_polls_total", Help: "Status polls of open intents, by result."}, []string{"result"}),
		refunds: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_payment_refund_requests_total",
			Help: "Refund requests to the provider: requested, retry (provider unreachable), rejected (dead-lettered for a human).",
		}, []string{"result"}),
		reconMismatch: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_recon_mismatch_total",
			Help: "Differences between the provider's settlement report and HoldFast's records, by kind; counted on every pass while they persist. missed_capture, missed_refund and refund_stuck are repaired; the others page a human.",
		}, []string{"type"}),
		reconRuns: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_recon_runs_total",
			Help: "Reconciliation passes by result: ok, error, skipped (another replica holds the lock).",
		}, []string{"result"}),
		reconLastSuccess: f.NewGauge(prometheus.GaugeOpts{
			Name: "holdfast_recon_last_success_timestamp_seconds",
			Help: "When the last reconciliation pass completed (Unix time); an alert fires if it gets old.",
		}),
	}
	for _, k := range findingKinds {
		m.reconMismatch.WithLabelValues(k)
	}
	for _, r := range []string{"ok", "error", "skipped"} {
		m.reconRuns.WithLabelValues(r)
	}
	for _, v := range []string{"webhook", "poll", "reconciler"} {
		m.captures.WithLabelValues(v)
	}
	return m
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
