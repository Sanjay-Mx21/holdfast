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
	admitted   *prometheus.CounterVec
	terms      prometheus.Counter
	leader     *prometheus.GaugeVec
	invReads   *prometheus.CounterVec

	// Per-event gauges, set by the event's admission leader every tick and
	// removed when its term ends, so only the current leader reports them.
	size         *prometheus.GaugeVec
	admittedUpTo *prometheus.GaugeVec
	sessions     *prometheus.GaugeVec
	maxSessions  *prometheus.GaugeVec
	epoch        *prometheus.GaugeVec
	state        *prometheus.GaugeVec

	// statusAge is set by the opener in every replica: what clients see.
	statusAge *prometheus.GaugeVec
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
	joinAgentLockout  = "agent_lockout"
	joinVerifiedOnly  = "verified_only"
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

// Results of the leader's inventory reads, the values of the result label.
const (
	inventoryReadOK             = "ok"
	inventoryReadNotProvisioned = "not_provisioned"
	inventoryReadError          = "error"
)

// allStates are the values of the state label.
var allStates = []State{StatePre, StateOpen, StateFrozen, StateSoldOut, StateClosed}

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
	m.admitted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_admitted_total",
		Help: "People admitted from the queue into the purchase path. Labelled by event ID: bounded by the number of provisioned events.",
	}, []string{"event"})
	perEvent := func(name, help string) *prometheus.GaugeVec {
		return promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help + " Labelled by event ID: bounded by the number of provisioned events."}, []string{"event"})
	}
	m.size = perEvent("holdfast_queue_size", "People in the queue (joined, admitted or not).")
	m.admittedUpTo = perEvent("holdfast_queue_admitted_up_to", "admittedUpTo: the highest admitted rank.")
	m.sessions = perEvent("holdfast_queue_active_sessions", "Unexpired session slots: the concurrency in use.")
	m.maxSessions = perEvent("holdfast_queue_max_sessions", "The session budget (Little's Law L).")
	m.epoch = perEvent("holdfast_queue_leader_epoch", "Fencing epoch of the current admission leader.")
	m.statusAge = perEvent("holdfast_queue_status_age_seconds", "Age of the status document clients are served; grows while no leader writes it.")
	m.terms = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "holdfast_queue_leader_terms_total",
		Help: "Admission leadership terms won by this process.",
	})
	m.state = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "holdfast_queue_state",
		Help: "1 for the queue's current state (PRE, OPEN, FROZEN, SOLD_OUT, CLOSED), 0 for the others, as the status document shows it; set by the event's admission leader every tick. Labelled by event ID: bounded by the number of provisioned events.",
	}, []string{"event", "state"})
	m.invReads = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "holdfast_queue_inventory_reads_total",
		Help: "Availability reads by admission leaders from inventory-svc, by result; on error or not_provisioned the tick admits without the units cap.",
	}, []string{"result"})
	m.leader = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "holdfast_queue_admission_leader",
		Help: "1 while this process leads the event's admission controller. Labelled by event ID: bounded by the number of provisioned events.",
	}, []string{"event"})
	// Export every series from the start, at zero.
	for _, r := range []string{joinJoined, joinAlready, joinRateLimitedIP, joinRateLimitUser, joinClosed, joinNotFound, joinInvalid, joinAgentLockout, joinVerifiedOnly, joinError} {
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
	for _, r := range []string{inventoryReadOK, inventoryReadNotProvisioned, inventoryReadError} {
		m.invReads.WithLabelValues(r)
	}
	return m
}

func (m *Metrics) inventoryRead(result string) { m.invReads.WithLabelValues(result).Inc() }

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

// leaderTick records what the leader saw on one tick.
func (m *Metrics) leaderTick(eventID string, a Advance) {
	m.size.WithLabelValues(eventID).Set(float64(a.QueueSize))
	m.admittedUpTo.WithLabelValues(eventID).Set(float64(a.AdmittedUpTo))
	m.sessions.WithLabelValues(eventID).Set(float64(a.ActiveSessions))
	if a.State != "" {
		for _, s := range allStates {
			v := 0.0
			if s == a.State {
				v = 1
			}
			m.state.WithLabelValues(eventID, string(s)).Set(v)
		}
	}
}

// leaderTerm starts or ends this process's view of an event's leadership.
// Ending it removes the per-event gauges, so a former leader does not keep
// exporting stale values next to the new leader's.
func (m *Metrics) leaderTerm(eventID string, epoch int64, maxSessions int, leading bool) {
	m.setLeader(eventID, leading)
	if leading {
		m.epoch.WithLabelValues(eventID).Set(float64(epoch))
		m.maxSessions.WithLabelValues(eventID).Set(float64(maxSessions))
		return
	}
	for _, g := range []*prometheus.GaugeVec{m.size, m.admittedUpTo, m.sessions, m.maxSessions, m.epoch} {
		g.DeleteLabelValues(eventID)
	}
	m.state.DeletePartialMatch(prometheus.Labels{"event": eventID})
}
