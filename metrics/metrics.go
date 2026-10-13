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
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofynd/fit-go/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"
)

const defaultFlushInterval = 4 * time.Second

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

// AdvancedOptions contains opt-in textfile lifecycle controls introduced after
// the original Options contract. The separate type preserves old positional
// literals and equality expressions.
// Deprecated: use TextfileOptions.
type AdvancedOptions struct {
	MetricsDir         string
	ServerEnabled      bool
	HTTPClientEnabled  bool
	ServerBuckets      string
	HTTPClientBuckets  string
	DeploymentName     string
	PrometheusRegistry *prometheus.Registry
	// FlushInterval controls periodic textfile output when MetricsDir is set.
	// Defaults to 4s, matching legacy prom-file-client.
	FlushInterval time.Duration
	// MetricsFile optionally overrides the output filename. Relative paths are
	// resolved below MetricsDir. The default is <pod-name>-<pid>.prom.
	MetricsFile string
	// OnFlushError receives periodic textfile failures. When nil, fit-go emits a
	// generic error log without the path or raw OS error, while LastFlushError
	// retains the detailed error for health/readiness inspection.
	OnFlushError   func(error)
	legacyDefaults bool
}

// Registry holds all metrics collectors.
type Registry struct {
	mu             sync.Mutex
	serverEnabled  bool
	clientEnabled  bool
	serverHist     *prometheus.HistogramVec
	clientHist     *prometheus.HistogramVec
	deploymentName string
	promRegistry   *prometheus.Registry
	collectors     []prometheus.Collector
	metricsFile    string
	flushInterval  time.Duration
	flushMu        sync.Mutex
	lastFlushMu    sync.RWMutex
	lastFlushErr   error
	stop           chan struct{}
	done           chan struct{}
	shutdownOnce   sync.Once
	shutdownErr    error
	closed         bool
	onFlushError   func(error)
	legacyDefaults bool
}

var processDefault struct {
	sync.RWMutex
	registry *Registry
	owner    *defaultRegistryOwner
}

type defaultRegistryOwner struct {
	registry *Registry
	previous *defaultRegistryOwner
	baseline *Registry
	active   bool
}

// New creates a new metrics registry.
func New(opts Options) (*Registry, error) {
	return NewAdvanced(AdvancedOptions{
		MetricsDir: opts.MetricsDir, ServerEnabled: opts.ServerEnabled,
		HTTPClientEnabled: opts.HTTPClientEnabled, ServerBuckets: opts.ServerBuckets,
		HTTPClientBuckets: opts.HTTPClientBuckets, DeploymentName: opts.DeploymentName,
		PrometheusRegistry: opts.PrometheusRegistry,
		legacyDefaults:     true,
	})
}

// NewAdvanced creates a registry with opt-in textfile lifecycle controls while
// preserving the original New(Options) API.
// Deprecated: use NewTextfileRegistry.
func NewAdvanced(opts AdvancedOptions) (*Registry, error) {
	opts.MetricsDir = strings.TrimSpace(opts.MetricsDir)
	promReg := opts.PrometheusRegistry
	if promReg == nil {
		promReg = prometheus.NewRegistry()
	}

	r := &Registry{
		serverEnabled:  opts.ServerEnabled,
		clientEnabled:  opts.HTTPClientEnabled,
		deploymentName: opts.DeploymentName,
		promRegistry:   promReg,
		onFlushError:   opts.OnFlushError,
		legacyDefaults: opts.legacyDefaults,
	}

	if r.deploymentName == "" {
		r.deploymentName = os.Getenv("DEPLOYMENT_NAME")
	}
	if r.deploymentName == "" && !opts.legacyDefaults {
		r.deploymentName = config.GetAppNameForDBOptions()
	}
	if r.deploymentName == "" && !opts.legacyDefaults {
		r.deploymentName = "unknown"
	}

	// Initialize server histogram
	if r.serverEnabled {
		buckets := defaultServerBuckets
		help := "HTTP request duration in milliseconds"
		if opts.legacyDefaults {
			buckets = legacyDefaultBuckets
			help = "HTTP server request duration in milliseconds"
		}
		if opts.ServerBuckets != "" {
			if opts.legacyDefaults {
				buckets = parseLegacyBuckets(opts.ServerBuckets, legacyDefaultBuckets)
			} else {
				buckets = parseBuckets(opts.ServerBuckets, defaultServerBuckets)
			}
		}
		r.serverHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fit_http_request_duration_ms",
			Help:    help,
			Buckets: buckets,
		}, []string{"method", "route", "status_code", "deployment_name"})
		promReg.MustRegister(r.serverHist)
		r.collectors = append(r.collectors, r.serverHist)
	}

	// Initialize HTTP client histogram
	if r.clientEnabled {
		buckets := defaultClientBuckets
		if opts.legacyDefaults {
			buckets = legacyDefaultBuckets
		}
		if opts.HTTPClientBuckets != "" {
			if opts.legacyDefaults {
				buckets = parseLegacyBuckets(opts.HTTPClientBuckets, legacyDefaultBuckets)
			} else {
				buckets = parseBuckets(opts.HTTPClientBuckets, defaultClientBuckets)
			}
		}
		r.clientHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fit_http_client_request_duration_ms",
			Help:    "HTTP client request duration in milliseconds",
			Buckets: buckets,
		}, []string{"method", "host", "status_code", "deployment_name"})
		promReg.MustRegister(r.clientHist)
		r.collectors = append(r.collectors, r.clientHist)
	}

	if opts.MetricsDir != "" && !opts.legacyDefaults {
		if err := r.startFileOutput(opts); err != nil {
			for _, collector := range r.collectors {
				promReg.Unregister(collector)
			}
			return nil, err
		}
	}

	return r, nil
}

// Default returns the process-default registry installed by fit.Init. Direct
// metrics.New callers remain isolated unless they explicitly call SetDefault.
func Default() *Registry {
	processDefault.RLock()
	defer processDefault.RUnlock()
	return processDefault.registry
}

// SetDefault installs r as the process-default registry used by fit's server
// and HTTP clients. The returned function restores the previous registry and is
// safe to call more than once.
func SetDefault(r *Registry) func() {
	processDefault.Lock()
	owner := &defaultRegistryOwner{
		registry: r,
		previous: processDefault.owner,
		baseline: processDefault.registry,
		active:   true,
	}
	processDefault.registry = r
	processDefault.owner = owner
	processDefault.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			processDefault.Lock()
			owner.active = false
			if processDefault.owner == owner {
				fallback := owner.baseline
				previous := owner.previous
				for previous != nil && !previous.active {
					fallback = previous.baseline
					previous = previous.previous
				}
				processDefault.owner = previous
				if previous != nil {
					processDefault.registry = previous.registry
				} else {
					processDefault.registry = fallback
				}
			}
			processDefault.Unlock()
		})
	}
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

// Register adds custom collectors to the same registry and textfile lifecycle
// used by FIT's built-in HTTP histograms. This is required by legacy services
// that registered business counters directly with prom-file-client even while
// FIT_PROMETHEUS_ENABLED (the automatic histogram switch) was false.
//
// Registration is all-or-nothing. If any collector conflicts, collectors added
// by this call are rolled back and are not retained for Shutdown.
func (r *Registry) Register(collectors ...prometheus.Collector) error {
	if r == nil || r.promRegistry == nil {
		return fmt.Errorf("metrics: registry is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("metrics: registry is shut down")
	}

	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, collector := range collectors {
		if collector == nil {
			continue
		}
		if err := r.promRegistry.Register(collector); err != nil {
			for _, prior := range registered {
				r.promRegistry.Unregister(prior)
			}
			return fmt.Errorf("metrics: register collector: %w", err)
		}
		registered = append(registered, collector)
	}
	r.collectors = append(r.collectors, registered...)
	return nil
}

// Shutdown cleans up metrics resources by unregistering all collectors.
func (r *Registry) Shutdown() error {
	if r == nil {
		return nil
	}
	r.shutdownOnce.Do(func() {
		if r.stop != nil {
			close(r.stop)
			<-r.done
		}
		if r.metricsFile != "" {
			r.shutdownErr = r.Flush()
		}
		r.mu.Lock()
		r.closed = true
		collectors := append([]prometheus.Collector(nil), r.collectors...)
		r.collectors = nil
		r.mu.Unlock()
		if r.promRegistry != nil {
			for _, collector := range collectors {
				r.promRegistry.Unregister(collector)
			}
		}
	})
	return r.shutdownErr
}

// Default histogram buckets
var defaultServerBuckets = []float64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
var defaultClientBuckets = []float64{100, 250, 500, 1000, 2500, 5000, 10000, 30000}
var legacyDefaultBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

func parseBuckets(s string, defaults []float64) []float64 {
	parts := strings.Split(s, ",")
	var buckets []float64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if v, err := strconv.ParseFloat(p, 64); err == nil && v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) {
			buckets = append(buckets, v)
		}
	}
	if len(buckets) == 0 {
		return append([]float64(nil), defaults...)
	}
	sort.Float64s(buckets)
	// prometheus.Histogram requires strictly increasing buckets. Legacy sorted
	// user input; de-duplicating additionally avoids a startup panic.
	unique := buckets[:0]
	for _, bucket := range buckets {
		if len(unique) == 0 || bucket != unique[len(unique)-1] {
			unique = append(unique, bucket)
		}
	}
	return unique
}

// parseLegacyBuckets preserves the original metrics.New behavior: finite
// numeric values retain caller order and an all-invalid list falls back to the
// shared official-main bucket set. NewAdvanced uses the validated/sorted parser
// above so malformed post-main configuration cannot panic registration.
func parseLegacyBuckets(s string, defaults []float64) []float64 {
	parts := strings.Split(s, ",")
	var buckets []float64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if v, err := strconv.ParseFloat(p, 64); err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) {
			buckets = append(buckets, v)
		}
	}
	if len(buckets) == 0 {
		return append([]float64(nil), defaults...)
	}
	return buckets
}

func (r *Registry) startFileOutput(opts AdvancedOptions) error {
	if err := os.MkdirAll(opts.MetricsDir, 0o755); err != nil {
		return fmt.Errorf("metrics: create output directory: %w", err)
	}

	name := opts.MetricsFile
	if name == "" {
		name = os.Getenv("K8S_POD_NAME")
		if name == "" {
			name = "metrics"
		}
		name = fmt.Sprintf("%s-%d.prom", filepath.Base(name), os.Getpid())
	}
	if !filepath.IsAbs(name) {
		name = filepath.Join(opts.MetricsDir, name)
	}
	r.metricsFile = name
	r.flushInterval = opts.FlushInterval
	if r.flushInterval <= 0 {
		r.flushInterval = defaultFlushInterval
	}

	if err := r.Flush(); err != nil {
		return fmt.Errorf("metrics: initialize textfile output: %w", err)
	}
	r.stop = make(chan struct{})
	r.done = make(chan struct{})
	go r.fileOutputLoop()
	return nil
}

func (r *Registry) fileOutputLoop() {
	ticker := time.NewTicker(r.flushInterval)
	defer func() {
		ticker.Stop()
		close(r.done)
	}()
	for {
		select {
		case <-ticker.C:
			if err := r.Flush(); err != nil {
				if r.onFlushError != nil {
					r.onFlushError(err)
				} else {
					slog.Error("fit/metrics: periodic textfile flush failed")
				}
			}
		case <-r.stop:
			return
		}
	}
}

// Flush atomically writes the current registry in Prometheus textfile format.
// It is a no-op when MetricsDir was not configured.
func (r *Registry) Flush() error {
	if r == nil || r.metricsFile == "" || r.promRegistry == nil {
		return nil
	}
	r.flushMu.Lock()
	err := prometheus.WriteToTextfile(r.metricsFile, r.promRegistry)
	r.flushMu.Unlock()

	r.lastFlushMu.Lock()
	r.lastFlushErr = err
	r.lastFlushMu.Unlock()
	return err
}

// LastFlushError reports the most recent periodic or explicit textfile error.
func (r *Registry) LastFlushError() error {
	if r == nil {
		return nil
	}
	r.lastFlushMu.RLock()
	defer r.lastFlushMu.RUnlock()
	return r.lastFlushErr
}

// MetricsFile returns the active Prometheus textfile path, or an empty string
// when this registry is scrape-only.
func (r *Registry) MetricsFile() string {
	if r == nil {
		return ""
	}
	return r.metricsFile
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
	if r.legacyDefaults {
		for _, mf := range mfs {
			sb.WriteString("# HELP " + mf.GetName() + " " + mf.GetHelp() + "\n")
			sb.WriteString("# TYPE " + mf.GetName() + " " + mf.GetType().String() + "\n")
			for _, metric := range mf.GetMetric() {
				labels := ""
				if len(metric.GetLabel()) > 0 {
					parts := make([]string, 0, len(metric.GetLabel()))
					for _, label := range metric.GetLabel() {
						parts = append(parts, label.GetName()+"=\""+label.GetValue()+"\"")
					}
					labels = "{" + strings.Join(parts, ",") + "}"
				}
				if histogram := metric.GetHistogram(); histogram != nil {
					for _, bucket := range histogram.GetBucket() {
						sb.WriteString(mf.GetName() + "_bucket" + addLeBucket(labels, bucket.GetUpperBound()) + " " + strconv.FormatUint(bucket.GetCumulativeCount(), 10) + "\n")
					}
					sb.WriteString(mf.GetName() + "_sum" + labels + " " + strconv.FormatFloat(histogram.GetSampleSum(), 'f', -1, 64) + "\n")
					sb.WriteString(mf.GetName() + "_count" + labels + " " + strconv.FormatUint(histogram.GetSampleCount(), 10) + "\n")
				}
			}
		}
		return sb.String()
	}
	for _, mf := range mfs {
		if _, err := expfmt.MetricFamilyToText(&sb, mf); err != nil {
			return ""
		}
	}
	return sb.String()
}

func addLeBucket(labels string, bound float64) string {
	le := strconv.FormatFloat(bound, 'f', -1, 64)
	if math.IsInf(bound, 1) {
		le = "+Inf"
	}
	if labels == "" {
		return "{le=\"" + le + "\"}"
	}
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
