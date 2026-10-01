// Package metrics provides a dedicated Prometheus registry per process.
// Using our own registry (never the global default) keeps tests isolated and
// makes every exported metric an explicit decision.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Sanjay-Mx21/holdfast/internal/platform/buildinfo"
)

// NewRegistry returns a registry with Go runtime, process and build-info
// collectors already registered.
func NewRegistry(service string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	bi := buildinfo.Get()
	info := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "holdfast_build_info",
		Help: "Build metadata of the running binary; the value is always 1.",
		ConstLabels: prometheus.Labels{
			"service": service, "version": bi.Version, "commit": bi.Commit, "go_version": bi.GoVersion,
		},
	})
	info.Set(1)
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		info,
	)
	return reg
}

// Handler serves reg in the Prometheus/OpenMetrics text formats.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg, EnableOpenMetrics: true})
}
