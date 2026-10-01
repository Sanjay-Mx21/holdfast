package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/logging"
)

// HeaderRequestID carries the request ID between clients, the edge and services.
const HeaderRequestID = "X-Request-Id"

type requestIDKey struct{}

// RequestIDFrom returns the request ID stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// RequestID accepts a well-formed incoming X-Request-Id (so one ID follows a
// request across hops) or generates a time-ordered UUIDv7, echoes it in the
// response and stores it in the context.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderRequestID)
			if !validRequestID(id) {
				if v7, err := uuid.NewV7(); err == nil {
					id = v7.String()
				} else {
					id = uuid.NewString()
				}
			}
			w.Header().Set(HeaderRequestID, id)
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
		})
	}
}

func validRequestID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isIDChar(s[i]) {
			return false
		}
	}
	return true
}

func isIDChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '-' || c == '_' || c == '.' || c == ':'
}

// AccessLog stores a request-scoped logger (tagged with the request ID) in
// the context and logs one line per request. Client errors log at WARN and
// server errors at ERROR; successes log at INFO only when logSuccess is true,
// because at flash-sale volumes per-request success logs are pure cost.
func AccessLog(base *slog.Logger, logSuccess bool) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			l := base.With(slog.String("request_id", RequestIDFrom(r.Context())))
			rec := wrapRecorder(w)
			next.ServeHTTP(rec, r.WithContext(logging.WithContext(r.Context(), l)))

			level := slog.LevelInfo
			switch {
			case rec.status >= 500:
				level = slog.LevelError
			case rec.status >= 400:
				level = slog.LevelWarn
			case !logSuccess:
				return
			}
			l.LogAttrs(r.Context(), level, "http request",
				slog.String("method", r.Method),
				slog.String("route", RouteFrom(r.Context())),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int64("bytes", rec.bytes),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}

// Recover turns a panic into a logged 500 problem instead of a dropped
// connection. http.ErrAbortHandler is re-raised, as net/http expects.
func Recover() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := wrapRecorder(w)
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(v)
				}
				logging.FromContext(r.Context()).Error("panic recovered",
					slog.Any("panic", v), slog.String("stack", string(debug.Stack())))
				if !rec.wroteHeader {
					WriteProblem(rec, r, Internal())
				}
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// SecurityHeaders sets safe defaults for a JSON API. Handlers may override
// Cache-Control for responses that are meant to be cached.
func SecurityHeaders() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Set("X-Content-Type-Options", "nosniff")
			h.Set("X-Frame-Options", "DENY")
			h.Set("Referrer-Policy", "no-referrer")
			h.Set("Cache-Control", "no-store")
			next.ServeHTTP(w, r)
		})
	}
}

// BodyLimit caps request bodies; reads beyond n fail with *http.MaxBytesError.
func BodyLimit(n int64) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}

// Timeout bounds the request context. Handlers and every downstream call
// (Valkey, PostgreSQL, gRPC) must pass r.Context() so the deadline propagates.
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// HTTPMetrics records RED metrics (rate, errors, duration) per route.
type HTTPMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inflight prometheus.Gauge
}

// NewHTTPMetrics registers HTTP metrics on reg.
func NewHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	f := promauto.With(reg)
	return &HTTPMetrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "holdfast_http_requests_total",
			Help: "HTTP requests by route and status code.",
		}, []string{"route", "code"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "holdfast_http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"route"}),
		inflight: f.NewGauge(prometheus.GaugeOpts{
			Name: "holdfast_http_requests_in_flight",
			Help: "HTTP requests currently being served.",
		}),
	}
}

// Middleware returns the metrics middleware.
func (m *HTTPMetrics) Middleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.inflight.Inc()
			defer m.inflight.Dec()
			start := time.Now()
			rec := wrapRecorder(w)
			next.ServeHTTP(rec, r)
			route := RouteFrom(r.Context())
			m.requests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
			m.duration.WithLabelValues(route).Observe(time.Since(start).Seconds())
		})
	}
}

// recorder captures the status code and byte count. Middlewares share one
// recorder per request instead of stacking wrappers.
type recorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func wrapRecorder(w http.ResponseWriter) *recorder {
	if rec, ok := w.(*recorder); ok {
		return rec
	}
	return &recorder{ResponseWriter: w, status: http.StatusOK}
}

func (r *recorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status, r.wroteHeader = status, true
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
