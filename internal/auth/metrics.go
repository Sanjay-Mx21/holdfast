package auth

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are auth-svc's counters. Every label takes a few fixed values;
// phone numbers, codes and tokens never appear.
type Metrics struct {
	otps         *prometheus.CounterVec
	verifies     *prometheus.CounterVec
	refreshes    *prometheus.CounterVec
	usersCreated prometheus.Counter
}

// NewMetrics registers auth-svc's metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		otps: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_auth_otp_requests_total",
			Help: "Code requests: sent, rate_limited, invalid_phone, error.",
		}, []string{"result"}),
		verifies: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_auth_otp_verifications_total",
			Help: "Code checks: ok (signed in) or invalid (wrong, expired, used or out of attempts).",
		}, []string{"result"}),
		refreshes: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_auth_refresh_total",
			Help: "Refresh-token presentations: rotated, invalid, reused (a rotated token came back; its family was revoked).",
		}, []string{"result"}),
		usersCreated: f.NewCounter(prometheus.CounterOpts{
			Name: "holdfast_auth_users_created_total",
			Help: "Users created by a first sign-in.",
		}),
	}
}

func (m *Metrics) otp(result string)     { m.otps.WithLabelValues(result).Inc() }
func (m *Metrics) verify(result string)  { m.verifies.WithLabelValues(result).Inc() }
func (m *Metrics) refresh(result string) { m.refreshes.WithLabelValues(result).Inc() }
