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
	outcomes *prometheus.CounterVec
	late     *prometheus.CounterVec
	saga     *prometheus.CounterVec
	settled  *prometheus.CounterVec
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
		outcomes: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_booking_saga_outcomes_total",
			Help: "Bookings moved by the saga, by new status: CONFIRMED, REFUND_REQUIRED, CANCELLED, REFUNDED.",
		}, []string{"status"}),
		late: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_late_confirm_total",
			Help: "Captures that arrived after the booking was cancelled, by outcome: confirmed (the guard took the sale) or refund_required.",
		}, []string{"result"}),
		saga: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_booking_saga_skipped_total",
			Help: "Payment events the saga applied no change for: duplicate, unknown_booking, already_decided.",
		}, []string{"reason"}),
		settled: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_booking_inventory_settled_total",
			Help: "Inventory calls after a decision: confirm_confirmed, confirm_replay, confirm_late, released, release_noop.",
		}, []string{"result"}),
	}
}

// decided counts a saga decision; late marks a capture after cancellation.
func (m *Metrics) decided(status string, late bool) {
	m.outcomes.WithLabelValues(status).Inc()
	if late {
		result := "confirmed"
		if status == StatusRefundRequired {
			result = "refund_required"
		}
		m.late.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) create(result string) { m.requests.WithLabelValues(result).Inc() }
