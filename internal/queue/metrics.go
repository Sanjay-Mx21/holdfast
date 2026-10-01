package queue

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are queue-svc's domain metrics. Labels are fixed enums, never
// client-supplied values. The rest of the queue metrics (size, admittedUpTo,
// sessions, admission rate, leader epoch) arrive with task 2.10.
type Metrics struct {
	joins *prometheus.CounterVec
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

// NewMetrics registers the queue metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		joins: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_queue_joins_total",
			Help: "Join attempts by outcome.",
		}, []string{"result"}),
	}
	for _, r := range []string{joinJoined, joinAlready, joinRateLimitedIP, joinRateLimitUser, joinClosed, joinNotFound, joinInvalid, joinError} {
		m.joins.WithLabelValues(r) // export every series from the start, at zero
	}
	return m
}

func (m *Metrics) join(result string) { m.joins.WithLabelValues(result).Inc() }
