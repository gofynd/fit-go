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

// Package otelmetrics owns the generic OpenTelemetry metrics SDK lifecycle for
// fit-go. It is separate from the legacy FIT Prometheus textfile metrics in the
// metrics package; applications may enable either or both pipelines.
package otelmetrics

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/gofynd/fit-go/tracing"
)

const (
	defaultExportInterval = 60 * time.Second
	defaultExportTimeout  = 30 * time.Second
)

// Options configures the generic OpenTelemetry metrics provider.
type Options struct {
	ServiceName         string
	FallbackServiceName string
	Env                 string
	Attributes          map[string]string
	Exporters           string
	Endpoint            string
	// EndpointIsCommon marks Endpoint as OTEL_EXPORTER_OTLP_ENDPOINT rather
	// than the metrics-specific endpoint. HTTP exporters append /v1/metrics to
	// common endpoints as required by the OTel environment specification.
	EndpointIsCommon bool
	Protocol         string
	ExportInterval   time.Duration
	ExportTimeout    time.Duration
	Enabled          *bool
	Resource         *resource.Resource
	Readers          []sdkmetric.Reader
	Views            []sdkmetric.View
	MetricExporter   sdkmetric.Exporter
	// ErrorHandler receives asynchronous SDK/export errors. fit.Init installs a
	// privacy-safe platform logger handler; standalone users may provide one.
	ErrorHandler otel.ErrorHandler
}

// DefaultOptions resolves standard OpenTelemetry metrics environment
// variables. Export remains opt-in: the default exporter is "none".
func DefaultOptions() Options {
	endpoint, common := resolveEndpointFromEnv()
	return Options{
		ServiceName:         strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")),
		FallbackServiceName: strings.TrimSpace(os.Getenv("SERVICE_NAME")),
		Env:                 envString("GO_ENV", envString("NODE_ENV", "development")),
		Exporters:           envString("OTEL_METRICS_EXPORTER", "none"),
		Endpoint:            endpoint,
		Protocol:            envString("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", envString("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")),
		ExportInterval:      envDurationMillis("OTEL_METRIC_EXPORT_INTERVAL", defaultExportInterval),
		ExportTimeout:       envDurationMillis("OTEL_METRIC_EXPORT_TIMEOUT", defaultExportTimeout),
		EndpointIsCommon:    common,
	}
}

// Provider wraps an OpenTelemetry MeterProvider and owns its readers and
// exporters. Shutdown is idempotent.
type Provider struct {
	sdk           *sdkmetric.MeterProvider
	meterProvider metric.MeterProvider
	enabled       bool
	closed        atomic.Bool
	shutdownOnce  sync.Once
	shutdownErr   error
	owners        map[*globalOwner]struct{}
	errorHandler  otel.ErrorHandler
}

// New creates a provider. A disabled configuration returns a usable no-op
// provider and performs no network or process-global side effects.
func New(ctx context.Context, opts Options) (*Provider, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	enabled := metricsEnabled(opts)
	if sdkDisabled() || !enabled {
		noop := metricnoop.NewMeterProvider()
		return &Provider{meterProvider: noop}, nil
	}

	readers := append([]sdkmetric.Reader(nil), opts.Readers...)
	if opts.MetricExporter != nil {
		readers = append(readers, periodicReader(opts.MetricExporter, opts))
	} else if len(readers) == 0 {
		exporters, err := newExporters(ctx, opts)
		if err != nil {
			return nil, err
		}
		for _, exporter := range exporters {
			readers = append(readers, periodicReader(exporter, opts))
		}
	}
	if len(readers) == 0 {
		return nil, errors.New("otelmetrics: enabled provider has no metric reader")
	}

	res := opts.Resource
	if res == nil {
		res = tracing.ResourceFromOptions(ctx, tracing.Options{
			ServiceName:         opts.ServiceName,
			FallbackServiceName: opts.FallbackServiceName,
			Env:                 opts.Env,
			Attributes:          opts.Attributes,
		})
	}
	providerOptions := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, reader := range readers {
		providerOptions = append(providerOptions, sdkmetric.WithReader(reader))
	}
	if len(opts.Views) > 0 {
		providerOptions = append(providerOptions, sdkmetric.WithView(opts.Views...))
	}
	sdk := sdkmetric.NewMeterProvider(providerOptions...)
	return &Provider{
		sdk:           sdk,
		meterProvider: sdk,
		enabled:       true,
		owners:        make(map[*globalOwner]struct{}),
		errorHandler:  opts.ErrorHandler,
	}, nil
}

// IsEnabled reports whether this provider records and exports metrics.
func (p *Provider) IsEnabled() bool { return p != nil && p.enabled && !p.closed.Load() }

// Meter returns a meter from this provider. A nil provider is safe and returns
// a no-op meter.
func (p *Provider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	if p == nil || p.meterProvider == nil {
		return metricnoop.NewMeterProvider().Meter(name, opts...)
	}
	return p.meterProvider.Meter(name, opts...)
}

// MeterProvider returns the API provider owned by p.
func (p *Provider) MeterProvider() metric.MeterProvider {
	if p == nil || p.meterProvider == nil {
		return metricnoop.NewMeterProvider()
	}
	return p.meterProvider
}

// ForceFlush exports all pending measurements.
func (p *Provider) ForceFlush(ctx context.Context) error {
	if p == nil || p.sdk == nil || p.closed.Load() {
		return nil
	}
	return p.sdk.ForceFlush(ctx)
}

// Shutdown restores any process-global installations and releases readers and
// exporters. It is safe to call more than once.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p.shutdownOnce.Do(func() {
		p.detachMeterGlobals()
		if p.sdk != nil {
			p.shutdownErr = p.sdk.Shutdown(ctx)
		}
		p.detachErrorGlobals()
	})
	return p.shutdownErr
}

func periodicReader(exporter sdkmetric.Exporter, opts Options) sdkmetric.Reader {
	interval := opts.ExportInterval
	if interval <= 0 {
		interval = defaultExportInterval
	}
	timeout := opts.ExportTimeout
	if timeout <= 0 {
		timeout = defaultExportTimeout
	}
	return sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval), sdkmetric.WithTimeout(timeout))
}

func metricsEnabled(opts Options) bool {
	if opts.Enabled != nil {
		return *opts.Enabled
	}
	if len(opts.Readers) > 0 || opts.MetricExporter != nil {
		return true
	}
	exporters := strings.TrimSpace(opts.Exporters)
	return exporters != "" && !strings.EqualFold(exporters, "none")
}

func newExporters(ctx context.Context, opts Options) ([]sdkmetric.Exporter, error) {
	names, err := parseExporters(opts.Exporters)
	if err != nil {
		return nil, err
	}
	exporters := make([]sdkmetric.Exporter, 0, len(names))
	for _, name := range names {
		switch name {
		case "otlp":
			exporter, err := newOTLPExporter(ctx, opts)
			if err != nil {
				return nil, fmt.Errorf("otelmetrics: create OTLP exporter: %w", err)
			}
			exporters = append(exporters, exporter)
		case "console", "stdout":
			exporter, err := stdoutmetric.New()
			if err != nil {
				return nil, fmt.Errorf("otelmetrics: create console exporter: %w", err)
			}
			exporters = append(exporters, exporter)
		}
	}
	return exporters, nil
}

func parseExporters(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("otelmetrics: OTEL_METRICS_EXPORTER is empty")
	}
	seen := make(map[string]struct{})
	var names []string
	for _, raw := range strings.Split(value, ",") {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		switch name {
		case "otlp", "console", "stdout", "none":
		default:
			return nil, fmt.Errorf("otelmetrics: unsupported metrics exporter %q", name)
		}
		if _, exists := seen[name]; !exists {
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	if _, none := seen["none"]; none {
		if len(names) != 1 {
			return nil, errors.New("otelmetrics: exporter none cannot be combined with another exporter")
		}
		return nil, nil
	}
	if len(names) == 0 {
		return nil, errors.New("otelmetrics: no metrics exporter configured")
	}
	return names, nil
}

func newOTLPExporter(ctx context.Context, opts Options) (sdkmetric.Exporter, error) {
	protocol := strings.ToLower(strings.TrimSpace(opts.Protocol))
	switch protocol {
	case "", "http", "http/protobuf":
		httpOptions := make([]otlpmetrichttp.Option, 0, 1)
		if endpoint := httpEndpoint(opts.Endpoint, opts.EndpointIsCommon); endpoint != "" {
			httpOptions = append(httpOptions, otlpmetrichttp.WithEndpointURL(endpoint))
		}
		return otlpmetrichttp.New(ctx, httpOptions...)
	case "grpc":
		grpcOptions := make([]otlpmetricgrpc.Option, 0, 1)
		if endpoint := strings.TrimSpace(opts.Endpoint); endpoint != "" {
			grpcOptions = append(grpcOptions, otlpmetricgrpc.WithEndpointURL(endpoint))
		}
		return otlpmetricgrpc.New(ctx, grpcOptions...)
	default:
		return nil, fmt.Errorf("unsupported OTLP metrics protocol %q", opts.Protocol)
	}
}

func resolveEndpointFromEnv() (string, bool) {
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")); endpoint != "" {
		return endpoint, false
	}
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" {
		return endpoint, true
	}
	return "", false
}

func httpEndpoint(endpoint string, common bool) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || !common {
		return endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return endpoint
	}
	parsed.Path = path.Join(parsed.Path, "v1/metrics")
	return parsed.String()
}

func sdkDisabled() bool {
	value := strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED"))
	return strings.EqualFold(value, "true") || value == "1"
}

func envString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func envDurationMillis(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	if milliseconds, err := strconv.ParseInt(value, 10, 64); err == nil && milliseconds > 0 {
		const maxMilliseconds = int64(^uint64(0)>>1) / int64(time.Millisecond)
		if milliseconds <= maxMilliseconds {
			return time.Duration(milliseconds) * time.Millisecond
		}
	}
	if duration, err := time.ParseDuration(value); err == nil && duration > 0 {
		return duration
	}
	return fallback
}

type globalOwner struct {
	provider             *Provider
	wrapper              *ownedMeterProvider
	previous             metric.MeterProvider
	errorWrapper         *ownedErrorHandler
	previousErrorHandler otel.ErrorHandler
	meterActive          bool
	errorActive          bool
}

type ownedMeterProvider struct {
	metric.MeterProvider
	owner *globalOwner
}

type ownedErrorHandler struct {
	owner   *globalOwner
	handler otel.ErrorHandler
}

func (handler *ownedErrorHandler) Handle(err error) {
	if handler != nil && handler.handler != nil {
		handler.handler.Handle(err)
	}
}

var (
	globalMu     sync.Mutex
	globalRouter *routingMeterProvider
)

// InstallGlobal installs p as the process-global OTel meter provider and
// returns an idempotent restoration function. Overlapping installations may be
// restored out of order without reviving a provider that has already shut down.
func InstallGlobal(p *Provider) func() {
	if p == nil {
		return func() {}
	}
	globalMu.Lock()
	if !p.enabled || p.closed.Load() {
		globalMu.Unlock()
		return func() {}
	}
	current := otel.GetMeterProvider()
	if globalRouter == nil {
		globalRouter = newRoutingMeterProvider(initialMeterFallback(current))
	}
	previous := current
	if current == globalRouter {
		previous = globalRouter.current()
	} else {
		previous = initialMeterFallback(current)
	}
	owner := &globalOwner{provider: p, previous: previous, meterActive: true}
	owner.wrapper = &ownedMeterProvider{MeterProvider: p.MeterProvider(), owner: owner}
	if p.owners == nil {
		p.owners = make(map[*globalOwner]struct{})
	}
	p.owners[owner] = struct{}{}
	globalRouter.setTarget(owner.wrapper)
	if current != globalRouter {
		otel.SetMeterProvider(globalRouter)
	}
	if p.errorHandler != nil {
		owner.previousErrorHandler = otel.GetErrorHandler()
		owner.errorWrapper = &ownedErrorHandler{owner: owner, handler: p.errorHandler}
		owner.errorActive = true
		otel.SetErrorHandler(owner.errorWrapper)
	}
	globalMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() { restoreGlobal(owner) })
	}
}

func restoreGlobal(owner *globalOwner) {
	if owner == nil {
		return
	}
	globalMu.Lock()
	defer globalMu.Unlock()
	restoreGlobalLocked(owner)
}

func restoreGlobalLocked(owner *globalOwner) {
	if owner == nil {
		return
	}
	restoreMeterGlobalLocked(owner)
	restoreErrorGlobalLocked(owner)
	delete(owner.provider.owners, owner)
}

func restoreMeterGlobalLocked(owner *globalOwner) {
	if owner == nil || !owner.meterActive {
		return
	}
	owner.meterActive = false
	if globalRouter != nil && globalRouter.current() == owner.wrapper {
		globalRouter.setTarget(activePredecessor(owner.previous))
	}
}

func restoreErrorGlobalLocked(owner *globalOwner) {
	if owner == nil || !owner.errorActive {
		return
	}
	owner.errorActive = false
	if owner.errorWrapper != nil && otel.GetErrorHandler() == owner.errorWrapper {
		otel.SetErrorHandler(activeErrorPredecessor(owner.previousErrorHandler))
	}
}

func activePredecessor(provider metric.MeterProvider) metric.MeterProvider {
	for {
		owned, ok := provider.(*ownedMeterProvider)
		if !ok || owned.owner == nil || owned.owner.meterActive {
			return provider
		}
		provider = owned.owner.previous
	}
}

func activeErrorPredecessor(handler otel.ErrorHandler) otel.ErrorHandler {
	for {
		owned, ok := handler.(*ownedErrorHandler)
		if !ok || owned.owner == nil || owned.owner.errorActive {
			return handler
		}
		handler = owned.owner.previousErrorHandler
	}
}

func initialMeterFallback(provider metric.MeterProvider) metric.MeterProvider {
	typeOf := reflect.TypeOf(provider)
	if typeOf != nil && typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	if typeOf != nil && typeOf.PkgPath() == "go.opentelemetry.io/otel/internal/global" && typeOf.Name() == "meterProvider" {
		return metricnoop.NewMeterProvider()
	}
	return provider
}

func (p *Provider) detachMeterGlobals() {
	globalMu.Lock()
	defer globalMu.Unlock()
	p.closed.Store(true)
	for owner := range p.owners {
		restoreMeterGlobalLocked(owner)
	}
}

func (p *Provider) detachErrorGlobals() {
	globalMu.Lock()
	defer globalMu.Unlock()
	for owner := range p.owners {
		restoreErrorGlobalLocked(owner)
		delete(p.owners, owner)
	}
}
