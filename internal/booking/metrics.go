package booking

import (
	"time"

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
	// captureToConfirm is the SLO of design doc 12.1 (p99 at most 5 s).
	captureToConfirm prometheus.Histogram
}

// NewMetrics registers booking-svc's metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	m := &Metrics{
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
		captureToConfirm: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "holdfast_capture_to_confirm_seconds",
			Help:    "From the payment's capture (its event's time) to the booking's confirmation committed by the saga. SLO: p99 at most 5 s.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 3, 5, 7.5, 10, 20, 30, 60, 120},
		}),
	}
	// Export every series from the start, at zero, so increase() and rate()
	// see a counter's first event (a series born at 1 shows no increase).
	for _, r := range []string{resultCreated, resultReplayed, resultKeyReused, resultHoldNotFound, resultHoldNotAvailable, resultEventNotFound, resultError} {
		m.requests.WithLabelValues(r)
	}
	for _, s := range []string{StatusConfirmed, StatusRefundRequired, StatusCancelled, StatusRefunded} {
		m.outcomes.WithLabelValues(s)
	}
	for _, r := range []string{"confirmed", "refund_required"} {
		m.late.WithLabelValues(r)
	}
	for _, r := range []string{"duplicate", "unknown_booking", "already_decided"} {
		m.saga.WithLabelValues(r)
	}
	for _, r := range []string{"confirm_confirmed", "confirm_replay", "confirm_late", "released", "release_noop"} {
		m.settled.WithLabelValues(r)
	}
	for _, r := range []string{"ok", "error"} {
		m.runs.WithLabelValues(r)
	}
	return m
}

// confirmedAfter records how long a confirmed booking took from its capture.
// A capture time from the future (clock skew between hosts) counts as 0.
func (m *Metrics) confirmedAfter(capturedAt time.Time) {
	m.captureToConfirm.Observe(max(0, time.Since(capturedAt).Seconds()))
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
