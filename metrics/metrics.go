// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package metrics provides Prometheus metrics collection for the fit.go framework.
package metrics

import (
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Options configures the metrics registry.
type Options struct {
	MetricsDir        string
	ServerEnabled     bool
	HTTPClientEnabled bool
	ServerBuckets     string // comma-separated bucket values
	HTTPClientBuckets string // comma-separated bucket values
	DeploymentName    string
	// PrometheusRegistry allows injecting a custom prometheus registry (useful for testing).
	PrometheusRegistry *prometheus.Registry
}

// Registry holds all metrics collectors.
type Registry struct {
	mu             sync.RWMutex
	serverEnabled  bool
	clientEnabled  bool
	serverHist     *prometheus.HistogramVec
	clientHist     *prometheus.HistogramVec
	deploymentName string
	metricsDir     string
	promRegistry   *prometheus.Registry
	collectors     []prometheus.Collector
}

// New creates a new metrics registry.
func New(opts Options) (*Registry, error) {
	promReg := opts.PrometheusRegistry
	if promReg == nil {
		promReg = prometheus.NewRegistry()
	}

	r := &Registry{
		serverEnabled:  opts.ServerEnabled,
		clientEnabled:  opts.HTTPClientEnabled,
		deploymentName: opts.DeploymentName,
		metricsDir:     opts.MetricsDir,
		promRegistry:   promReg,
	}

	if r.deploymentName == "" {
		r.deploymentName = os.Getenv("DEPLOYMENT_NAME")
	}

	// Initialize server histogram
	if r.serverEnabled {
		buckets := defaultServerBuckets
		if opts.ServerBuckets != "" {
			buckets = parseBuckets(opts.ServerBuckets)
		}
		r.serverHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fit_http_request_duration_ms",
			Help:    "HTTP server request duration in milliseconds",
			Buckets: buckets,
		}, []string{"method", "route", "status_code", "deployment_name"})
		promReg.MustRegister(r.serverHist)
		r.collectors = append(r.collectors, r.serverHist)
	}

	// Initialize HTTP client histogram
	if r.clientEnabled {
		buckets := defaultClientBuckets
		if opts.HTTPClientBuckets != "" {
			buckets = parseBuckets(opts.HTTPClientBuckets)
		}
		r.clientHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fit_http_client_request_duration_ms",
			Help:    "HTTP client request duration in milliseconds",
			Buckets: buckets,
		}, []string{"method", "host", "status_code", "deployment_name"})
		promReg.MustRegister(r.clientHist)
		r.collectors = append(r.collectors, r.clientHist)
	}

	return r, nil
}

// ServerMetrics contains data for recording server request metrics.
type ServerMetrics struct {
	Method     string
	Route      string
	StatusCode int
	Duration   time.Duration
}

// RecordServerMetrics records an HTTP server request metric.
func (r *Registry) RecordServerMetrics(m ServerMetrics) {
	if r == nil || !r.serverEnabled || r.serverHist == nil {
		return
	}
	r.serverHist.With(prometheus.Labels{
		"method":          m.Method,
		"route":           m.Route,
		"status_code":     strconv.Itoa(m.StatusCode),
		"deployment_name": r.deploymentName,
	}).Observe(float64(m.Duration.Milliseconds()))
}

// HTTPClientMetrics contains data for recording HTTP client request metrics.
type HTTPClientMetrics struct {
	Method     string
	Host       string
	StatusCode int
	Duration   time.Duration
}

// RecordHTTPClientMetrics records an HTTP client request metric.
func (r *Registry) RecordHTTPClientMetrics(m HTTPClientMetrics) {
	if r == nil || !r.clientEnabled || r.clientHist == nil {
		return
	}
	r.clientHist.With(prometheus.Labels{
		"method":          m.Method,
		"host":            m.Host,
		"status_code":     strconv.Itoa(m.StatusCode),
		"deployment_name": r.deploymentName,
	}).Observe(float64(m.Duration.Milliseconds()))
}

// ShouldRecordServerMetrics returns true if server metrics are enabled.
func (r *Registry) ShouldRecordServerMetrics() bool {
	return r != nil && r.serverEnabled
}

// ShouldRecordHTTPClientMetrics returns true if HTTP client metrics are enabled.
func (r *Registry) ShouldRecordHTTPClientMetrics() bool {
	return r != nil && r.clientEnabled
}

// Handler returns an http.Handler that serves Prometheus metrics for scraping.
func (r *Registry) Handler() http.Handler {
	if r == nil || r.promRegistry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(r.promRegistry, promhttp.HandlerOpts{})
}

// Shutdown cleans up metrics resources by unregistering all collectors.
func (r *Registry) Shutdown() error {
	if r == nil || r.promRegistry == nil {
		return nil
	}
	for _, c := range r.collectors {
		r.promRegistry.Unregister(c)
	}
	r.collectors = nil
	return nil
}

// Default histogram buckets
var defaultServerBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
var defaultClientBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

func parseBuckets(s string) []float64 {
	parts := strings.Split(s, ",")
	var buckets []float64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if v, err := strconv.ParseFloat(p, 64); err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) {
			buckets = append(buckets, v)
		}
	}
	if len(buckets) == 0 {
		return defaultServerBuckets
	}
	return buckets
}

// GetMetricsOutput returns Prometheus-formatted metrics output as a string.
func (r *Registry) GetMetricsOutput() string {
	if r == nil || r.promRegistry == nil {
		return ""
	}
	mfs, err := r.promRegistry.Gather()
	if err != nil || len(mfs) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, mf := range mfs {
		sb.WriteString("# HELP " + mf.GetName() + " " + mf.GetHelp() + "\n")
		sb.WriteString("# TYPE " + mf.GetName() + " " + mf.GetType().String() + "\n")
		for _, m := range mf.GetMetric() {
			labels := ""
			if len(m.GetLabel()) > 0 {
				parts := make([]string, 0, len(m.GetLabel()))
				for _, l := range m.GetLabel() {
					parts = append(parts, l.GetName()+"=\""+l.GetValue()+"\"")
				}
				labels = "{" + strings.Join(parts, ",") + "}"
			}
			if h := m.GetHistogram(); h != nil {
				for _, b := range h.GetBucket() {
					sb.WriteString(mf.GetName() + "_bucket" + addLeBucket(labels, b.GetUpperBound()) + " " + strconv.FormatUint(b.GetCumulativeCount(), 10) + "\n")
				}
				sb.WriteString(mf.GetName() + "_sum" + labels + " " + strconv.FormatFloat(h.GetSampleSum(), 'f', -1, 64) + "\n")
				sb.WriteString(mf.GetName() + "_count" + labels + " " + strconv.FormatUint(h.GetSampleCount(), 10) + "\n")
			}
		}
	}
	return sb.String()
}

// addLeBucket inserts the le label into existing labels string for histogram buckets.
func addLeBucket(labels string, bound float64) string {
	le := strconv.FormatFloat(bound, 'f', -1, 64)
	if bound == math.Inf(1) {
		le = "+Inf"
	}
	if labels == "" {
		return "{le=\"" + le + "\"}"
	}
	// Insert le before closing brace
	return labels[:len(labels)-1] + ",le=\"" + le + "\"}"
}

// ServerRecorderFunc returns a callback suitable for server.Config.MetricsRecorder
// that forwards inbound HTTP request metrics to this registry. Wire it with:
//
//	srvCfg.MetricsRecorder = reg.ServerRecorderFunc()
func (r *Registry) ServerRecorderFunc() func(method, route, status string, durationMs float64) {
	return func(method, route, status string, durationMs float64) {
		code, _ := strconv.Atoi(status)
		r.RecordServerMetrics(ServerMetrics{
			Method:     method,
			Route:      route,
			StatusCode: code,
			Duration:   time.Duration(durationMs * float64(time.Millisecond)),
		})
	}
}

// HTTPClientRecorderFunc returns a callback suitable for httpclient.WithMetrics
// that forwards outbound HTTP call metrics to this registry. Wire it with:
//
//	httpclient.NewHTTPClient(httpclient.WithMetrics(reg.HTTPClientRecorderFunc()))
func (r *Registry) HTTPClientRecorderFunc() func(method, host string, status int, d time.Duration) {
	return func(method, host string, status int, d time.Duration) {
		r.RecordHTTPClientMetrics(HTTPClientMetrics{
			Method:     method,
			Host:       host,
			StatusCode: status,
			Duration:   d,
		})
	}
}
