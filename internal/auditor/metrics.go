package auditor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are the auditor's results.
type Metrics struct {
	violations  *prometheus.GaugeVec
	drift       *prometheus.GaugeVec
	runs        *prometheus.CounterVec
	lastSuccess prometheus.Gauge
}

// NewMetrics registers the auditor's metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		violations: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "holdfast_invariant_violations",
			Help: "Violations of each invariant (I1 no oversell, I2 no double charge, I3 money safety, I4 per-user cap, I5 no lost units) found by the latest check. Must always be 0.",
		}, []string{"invariant"}),
		drift: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "holdfast_inventory_drift_units",
			Help: "Units inventory (Valkey) offers beyond what PostgreSQL has free (capacity - sold - pending bookings), per provisioned event. Must be 0; otherwise rebuild the event's inventory (runbook RB-INV-4).",
		}, []string{"event"}),
		runs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_auditor_runs_total",
			Help: "Auditor checks by result: ok, error (some invariant could not be checked).",
		}, []string{"result"}),
		lastSuccess: f.NewGauge(prometheus.GaugeOpts{
			Name: "holdfast_auditor_last_success_timestamp_seconds",
			Help: "When every invariant was last checked successfully (Unix time); an alert fires if it gets old.",
		}),
	}
	for _, r := range []string{"ok", "error"} {
		m.runs.WithLabelValues(r)
	}
	return m
}
