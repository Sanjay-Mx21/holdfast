package booking

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outcomes of POST /v1/bookings.
const (
	resultCreated          = "created" // a booking was created, or an in-progress one finished
	resultReplayed         = "replayed"
	resultKeyReused        = "key_reused"
	resultHoldNotFound     = "hold_not_found"
	resultHoldNotAvailable = "hold_not_available"
	resultEventNotFound    = "event_not_found"
	resultError            = "error" // transient: the key stays in progress for a retry
)

// Metrics are booking-svc's counters.
type Metrics struct {
	requests *prometheus.CounterVec
	created  prometheus.Counter
	resumed  prometheus.Counter
	expired  prometheus.Counter
	runs     *prometheus.CounterVec
}

// NewMetrics registers booking-svc's metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_booking_requests_total",
			Help: "POST /v1/bookings by outcome: created, replayed, key_reused, hold_not_found, hold_not_available, event_not_found, error.",
		}, []string{"result"}),
		created: f.NewCounter(prometheus.CounterOpts{
			Name: "holdfast_bookings_created_total",
			Help: "Bookings inserted (one per hold).",
		}),
		resumed: f.NewCounter(prometheus.CounterOpts{
			Name: "holdfast_booking_requests_resumed_total",
			Help: "Requests that found their idempotency key in progress and resumed the earlier attempt.",
		}),
		expired: f.NewCounter(prometheus.CounterOpts{
			Name: "holdfast_bookings_expired_total",
			Help: "Bookings cancelled because their payment deadline passed.",
		}),
		runs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_booking_deadline_runs_total",
			Help: "Deadline job passes, by result: ok, error.",
		}, []string{"result"}),
	}
}

func (m *Metrics) create(result string) { m.requests.WithLabelValues(result).Inc() }
