package queue

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are queue-svc's domain metrics. Labels are fixed enums, never
// client-supplied values. The rest of the queue metrics (size, admittedUpTo,
// sessions, admission rate, leader epoch) arrive with task 2.10.
type Metrics struct {
	joins      *prometheus.CounterVec
	opened     *prometheus.CounterVec
	openerRuns *prometheus.CounterVec
	openerTime prometheus.Histogram
}

// Join outcomes, the values of the result label.
const (
	joinJoined        = "joined"
	joinAlready       = "already_joined"
	joinRateLimitedIP = "rate_limited_ip"
	joinRateLimitUser = "rate_limited_user"
	joinClosed        = "closed"
	joinNotFound      = "not_found"
	joinInvalid       = "invalid"
	joinError         = "error"
)

// T0 transitions by who performed them, the values of the by label.
const (
	openedByJoin   = "join"
	openedByOpener = "opener"
)

// NewMetrics registers the queue metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		joins: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_queue_joins_total",
			Help: "Join attempts by outcome.",
		}, []string{"result"}),
	}
	m.opened = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_opened_total",
		Help: "T0 transitions (PRE to OPEN), by who performed them: a join that arrived first, or the opener.",
	}, []string{"by"})
	m.openerRuns = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_opener_runs_total",
		Help: "Opener passes over all events, by result.",
	}, []string{"result"})
	m.openerTime = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "holdfast_queue_opener_duration_seconds",
		Help:    "Duration of one opener pass over all events.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})
	// Export every series from the start, at zero.
	for _, r := range []string{joinJoined, joinAlready, joinRateLimitedIP, joinRateLimitUser, joinClosed, joinNotFound, joinInvalid, joinError} {
		m.joins.WithLabelValues(r)
	}
	for _, by := range []string{openedByJoin, openedByOpener} {
		m.opened.WithLabelValues(by)
	}
	for _, r := range []string{"ok", "error"} {
		m.openerRuns.WithLabelValues(r)
	}
	return m
}

func (m *Metrics) join(result string)   { m.joins.WithLabelValues(result).Inc() }
func (m *Metrics) transition(by string) { m.opened.WithLabelValues(by).Inc() }
