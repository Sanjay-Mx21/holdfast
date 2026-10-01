package inventory

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are the inventory service's domain metrics.
type Metrics struct {
	holds         *prometheus.CounterVec
	holdLatency   prometheus.Histogram
	releases      *prometheus.CounterVec
	confirms      *prometheus.CounterVec
	available     *prometheus.GaugeVec
	sweeps        *prometheus.CounterVec
	sweepDuration prometheus.Histogram
}

// Hold results, pre-initialised so every series exists from the first scrape
// (rate() over a series that appears mid-incident is misleading).
var holdResults = []string{"held", "replay", "sold_out", "user_limit", "invalid", "not_provisioned", "error"}

// NewMetrics registers inventory metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
		holds: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_holds_total",
			Help: "Hold attempts by result.",
		}, []string{"result"}),
		holdLatency: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "holdfast_hold_script_duration_seconds",
			Help:    "Latency of the atomic hold script round trip to Valkey.",
			Buckets: []float64{.0002, .0005, .001, .002, .005, .01, .025, .05, .1, .25},
		}),
		releases: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_hold_releases_total",
			Help: "Holds released back to the pool, by reason.",
		}, []string{"mode"}),
		confirms: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_hold_confirms_total",
			Help: "Hold confirmations by outcome (late means re-taken after release).",
		}, []string{"outcome"}),
		available: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "holdfast_inventory_available",
			Help: "Last observed available units per event.",
		}, []string{"event"}),
		sweeps: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_sweeper_runs_total",
			Help: "Expiry sweeper passes by result.",
		}, []string{"result"}),
		sweepDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "holdfast_sweeper_duration_seconds",
			Help:    "Duration of one sweeper pass over all events.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
	}
	for _, r := range holdResults {
		m.holds.WithLabelValues(r)
	}
	for _, mode := range []ReleaseMode{ReleaseExpire, ReleaseUserCancel, ReleasePaymentFailed} {
		m.releases.WithLabelValues(string(mode))
	}
	for _, o := range []ConfirmOutcome{ConfirmApplied, ConfirmReplay, ConfirmLate} {
		m.confirms.WithLabelValues(string(o))
	}
	return m
}

func (m *Metrics) holdResult(result string) { m.holds.WithLabelValues(result).Inc() }

func (m *Metrics) setAvailable(eventID string, n int64) {
	m.available.WithLabelValues(eventID).Set(float64(n))
}
