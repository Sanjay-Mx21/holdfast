package httpx

import (
	"net/http"
	"net/http/pprof"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
	"github.com/Sanjay-Mx21/holdfast/internal/platform/health"
)

// NewAdminRouter returns the router for the internal-only admin port. It
// serves /metrics, /livez, /readyz, /buildz and /debug/pprof/*, and services
// mount their internal APIs on it too. Never expose this port publicly.
func NewAdminRouter(metrics http.Handler, h *health.Health) *Router {
	rt := NewRouter(RequestID(), Recover(), SecurityHeaders())
	rt.Handle("GET /metrics", metrics)
	rt.HandleFunc("GET /livez", h.Livez)
	rt.HandleFunc("GET /readyz", h.Readyz)
	rt.HandleFunc("GET /buildz", func(w http.ResponseWriter, _ *http.Request) {
		WriteJSON(w, http.StatusOK, buildinfo.Get())
	})
	rt.HandleFunc("GET /debug/pprof/", pprof.Index)
	rt.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	rt.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	rt.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	rt.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return rt
}
