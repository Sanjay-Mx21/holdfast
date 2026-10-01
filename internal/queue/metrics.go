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
	positions  *prometheus.CounterVec
	admits     *prometheus.CounterVec
	opened     *prometheus.CounterVec
	openerRuns *prometheus.CounterVec
	openerTime prometheus.Histogram
	ticks      *prometheus.CounterVec
	admitted   prometheus.Counter
	terms      prometheus.Counter
	leader     *prometheus.GaugeVec
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

// Position lookup outcomes, the values of the result label.
const (
	positionRanked      = "ranked"
	positionRandomizing = "randomizing"
	positionNotInQueue  = "not_in_queue"
	positionNotFound    = "not_found"
	positionRateLimited = "rate_limited"
	positionInvalid     = "invalid"
	positionError       = "error"
)

// Admit outcomes, the values of the result label.
const (
	admitIssued      = "issued"
	admitNotYourTurn = "not_your_turn"
	admitExpired     = "expired"
	admitNotInQueue  = "not_in_queue"
	admitClosed      = "closed"
	admitNotFound    = "not_found"
	admitRateLimited = "rate_limited"
	admitInvalid     = "invalid"
	admitError       = "error"
)

// T0 transitions by who performed them, the values of the by label.
const (
	openedByJoin   = "join"
	openedByOpener = "opener"
)

// Admission tick outcomes, the values of the result label.
const (
	tickAdvanced = "advanced"
	tickIdle     = "idle"
	tickFenced   = "fenced"
	tickError    = "error"
)

// NewMetrics registers the queue metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		joins: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_queue_joins_total",
			Help: "Join attempts by outcome.",
		}, []string{"result"}),
	}
	m.positions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_position_lookups_total",
		Help: "Position lookups (GET /v1/queue/{id}/me) by outcome.",
	}, []string{"result"})
	m.admits = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_admits_total",
		Help: "Turn claims (POST /v1/queue/{id}/admit) by outcome; issued means an admission token was signed.",
	}, []string{"result"})
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
	m.ticks = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_admission_ticks_total",
		Help: "Admission ticks run by a leader, by outcome.",
	}, []string{"result"})
	m.admitted = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "holdfast_queue_admitted_total",
		Help: "People admitted from the queue into the purchase path.",
	})
	m.terms = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "holdfast_queue_leader_terms_total",
		Help: "Admission leadership terms won by this process.",
	})
	m.leader = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "holdfast_queue_admission_leader",
		Help: "1 while this process leads the event's admission controller. Labelled by event ID: bounded by the number of provisioned events.",
	}, []string{"event"})
	// Export every series from the start, at zero.
	for _, r := range []string{joinJoined, joinAlready, joinRateLimitedIP, joinRateLimitUser, joinClosed, joinNotFound, joinInvalid, joinError} {
		m.joins.WithLabelValues(r)
	}
	for _, r := range []string{positionRanked, positionRandomizing, positionNotInQueue, positionNotFound, positionRateLimited, positionInvalid, positionError} {
		m.positions.WithLabelValues(r)
	}
	for _, r := range []string{admitIssued, admitNotYourTurn, admitExpired, admitNotInQueue, admitClosed, admitNotFound, admitRateLimited, admitInvalid, admitError} {
		m.admits.WithLabelValues(r)
	}
	for _, by := range []string{openedByJoin, openedByOpener} {
		m.opened.WithLabelValues(by)
	}
	for _, r := range []string{"ok", "error"} {
		m.openerRuns.WithLabelValues(r)
	}
	for _, r := range []string{tickAdvanced, tickIdle, tickFenced, tickError} {
		m.ticks.WithLabelValues(r)
	}
	return m
}

func (m *Metrics) join(result string)     { m.joins.WithLabelValues(result).Inc() }
func (m *Metrics) position(result string) { m.positions.WithLabelValues(result).Inc() }
func (m *Metrics) transition(by string)   { m.opened.WithLabelValues(by).Inc() }

func (m *Metrics) tick(result string) { m.ticks.WithLabelValues(result).Inc() }

func (m *Metrics) setLeader(eventID string, leading bool) {
	v := 0.0
	if leading {
		v = 1
	}
	m.leader.WithLabelValues(eventID).Set(v)
}

func (m *Metrics) admit(result string) { m.admits.WithLabelValues(result).Inc() }
