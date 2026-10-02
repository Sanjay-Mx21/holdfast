// Package httpx is HoldFast's HTTP toolkit: a router that labels every route,
// production middleware, RFC 9457 problem responses, strict JSON decoding and
// a server with timeouts and graceful shutdown. It deliberately builds on the
// standard library's ServeMux (method + wildcard patterns) instead of a framework.
package httpx

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Middleware wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middleware so the first argument is the outermost.
func Chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

type routeKey struct{}

type routeInfo struct{ pattern string }

// RouteFrom returns the pattern of the route serving the request, or
// "unmatched". Metrics and logs use it instead of the raw path, so label
// cardinality stays bounded no matter what URLs clients send.
func RouteFrom(ctx context.Context) string {
	if ri, ok := ctx.Value(routeKey{}).(*routeInfo); ok && ri.pattern != "" {
		return ri.pattern
	}
	return "unmatched"
}

// Router wraps http.ServeMux with global middleware, route labelling and
// problem-document responses for unknown routes and wrong methods.
type Router struct {
	mux     *http.ServeMux
	handler http.Handler
}

// NewRouter returns a router whose global middleware runs, outermost first,
// around every request, including requests that match no route.
func NewRouter(global ...Middleware) *Router {
	rt := &Router{mux: http.NewServeMux()}
	rt.handler = Chain(http.HandlerFunc(rt.dispatch), global...)
	return rt
}

// Handle registers h for a Go 1.22+ pattern such as "POST /v1/events/{eventID}/holds".
func (rt *Router) Handle(pattern string, h http.Handler) {
	rt.mux.Handle(pattern, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ri, ok := r.Context().Value(routeKey{}).(*routeInfo); ok {
			ri.pattern = pattern
		}
		// Name the server span after the route, never the raw path, so span
		// names stay bounded like metric labels.
		if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
			span.SetName(pattern)
			span.SetAttributes(attribute.String("http.route", pattern))
		}
		h.ServeHTTP(w, r)
	}))
}

// HandleFunc registers a handler function.
func (rt *Router) HandleFunc(pattern string, fn http.HandlerFunc) { rt.Handle(pattern, fn) }

// ServeHTTP implements http.Handler.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = r.WithContext(context.WithValue(r.Context(), routeKey{}, &routeInfo{}))
	rt.handler.ServeHTTP(w, r)
}

func (rt *Router) dispatch(w http.ResponseWriter, r *http.Request) {
	if _, pattern := rt.mux.Handler(r); pattern != "" {
		rt.mux.ServeHTTP(w, r)
		return
	}
	// No route matched. Let ServeMux decide between 404, 405 (it sets Allow)
	// and path-cleaning redirects, but answer errors with a problem document.
	rec := &captureWriter{header: http.Header{}}
	rt.mux.ServeHTTP(rec, r)
	switch {
	case rec.status >= 300 && rec.status < 400:
		w.Header().Set("Location", rec.header.Get("Location"))
		w.WriteHeader(rec.status)
	case rec.status == http.StatusMethodNotAllowed:
		w.Header().Set("Allow", rec.header.Get("Allow"))
		WriteProblem(w, r, NewProblem(http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed for this resource"))
	default:
		WriteProblem(w, r, NotFound("ROUTE_NOT_FOUND", "no such endpoint"))
	}
}

// captureWriter records what ServeMux would have written, without writing it.
type captureWriter struct {
	header http.Header
	status int
}

func (c *captureWriter) Header() http.Header         { return c.header }
func (c *captureWriter) Write(b []byte) (int, error) { return len(b), nil }
func (c *captureWriter) WriteHeader(status int)      { c.status = status }
