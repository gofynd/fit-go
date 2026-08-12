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

// Package tracing provides OpenTelemetry tracing integration for the fit.go framework.
//
// This module supports:
// - OpenTelemetry span creation and context propagation
// - Span attribute setting matching traceclue API
// - Function decorators for tracing
// - HTTP instrumentation hooks (ignoring health check paths)
// - OTLP export to an OpenTelemetry collector
//
// The package is observability-backend neutral. Deployments choose the backend
// behind their OpenTelemetry collector. Backend configuration and credentials
// do not belong in fit-go.
//
// gRPC instrumentation is explicit through fit-go's grpc helpers. Deployed pyfit
// installed grpcio as an optional runtime but did not install an OTel gRPC
// interceptor, so automatic pyfit gRPC tracing is not a legacy capability.
//
// Environment variables:
// - TRACING_ENABLED: Enable OpenTelemetry tracing (default: false)
// - OTEL_EXPORTER_OTLP_TRACES_ENDPOINT / OTEL_EXPORTER_OTLP_ENDPOINT: collector endpoint
// - OTEL_EXPORTER_OTLP_TRACES_PROTOCOL / OTEL_EXPORTER_OTLP_PROTOCOL: grpc or http/protobuf
// - OTEL_PROPAGATORS: tracecontext, baggage, b3, b3multi, and/or jaeger
// - OTEL_SERVICE_NAME: Service name for spans
// - OTEL_RESOURCE_ATTRIBUTES: Additional resource attributes
// - OTEL_TRACES_EXPORTER: otlp, console, none, or a comma-separated list
// - OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG: sampling policy and ratio
// - OTEL_SDK_DISABLED: disable OpenTelemetry SDK initialization
// - FIT_TRACING_ACTIVATION_MODE: explicit (default) or pyfit compatibility
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/gofynd/fit-go/logging"
	"github.com/gofynd/fit-go/redact"
)

// Options configures the tracer.
type Options struct {
	ServiceName string // Explicit resource service.name; higher precedence than every env/resource value
	// FallbackServiceName is used only when neither ServiceName,
	// OTEL_SERVICE_NAME, nor resource attributes provide service.name. It maps
	// FIT's legacy SERVICE_NAME without overriding the OTel resource contract.
	FallbackServiceName string
	Env                 string
	Endpoint            string            // Explicit OTLP URL; overrides traces-specific/common env endpoints
	Protocol            string            // OTLP protocol: "grpc" | "http/protobuf" (default: http/protobuf)
	Exporters           string            // OTEL_TRACES_EXPORTER: otlp, console, none, or a comma-separated list
	Propagators         string            // OTEL_PROPAGATORS: tracecontext,baggage,b3,b3multi,jaeger
	ActivationMode      string            // "explicit" (default) or "pyfit" compatibility
	Sampler             string            // OTEL_TRACES_SAMPLER; empty/unknown = parentbased_always_on
	SampleRate          float64           // Sampling ratio (0.0-1.0); from OTEL_TRACES_SAMPLER_ARG
	ResourceAttributes  string            // OTEL_RESOURCE_ATTRIBUTES from an explicit config source
	BatchTimeout        time.Duration     // Span batch export timeout
	MaxExportBatch      int               // Maximum spans per export batch
	Attributes          map[string]string // Additional resource attributes
	// Enabled, when non-nil, overrides the TRACING_ENABLED env var. Set it from a
	// merged config so tracing enabled via a config FILE (which doesn't populate
	// the process env) isn't silently a no-op. nil = use the env var.
	Enabled *bool `json:"-"`
	// SDKDisabled carries OTEL_SDK_DISABLED when it came from a merged config
	// source that is not visible through os.Getenv. nil = use the environment.
	SDKDisabled *bool `json:"-"`
	// SpanExporter allows injecting a custom span exporter (useful for testing).
	SpanExporter sdktrace.SpanExporter `json:"-"`
	// UseSimpleSpanProcessor uses a synchronous span processor instead of batch.
	// Only use this for testing.
	UseSimpleSpanProcessor bool `json:"-"`
}

// DefaultOptions returns default tracer options from environment.
func DefaultOptions() Options {
	return Options{
		ServiceName:         envString("OTEL_SERVICE_NAME", ""),
		FallbackServiceName: envString("SERVICE_NAME", ""),
		Env:                 envString("GO_ENV", envString("NODE_ENV", "development")),
		Endpoint:            envString("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", envString("OTEL_EXPORTER_OTLP_ENDPOINT", "")),
		Protocol:            envString("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", envString("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")),
		Exporters:           envString("OTEL_TRACES_EXPORTER", "otlp"),
		Propagators:         envString("OTEL_PROPAGATORS", ""),
		ActivationMode:      envString("FIT_TRACING_ACTIVATION_MODE", "explicit"),
		Sampler:             envString("OTEL_TRACES_SAMPLER", ""),
		SampleRate:          sampleRateFromEnv(),
	}
}

// sampleRateFromEnv reads the OTel-standard OTEL_TRACES_SAMPLER_ARG (the ratio for
// the *ratio samplers) and defaults to 1.0 when unset, malformed, or outside
// [0,1]. The argument is ignored unless a ratio sampler is selected.
func sampleRateFromEnv() float64 {
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return validSampleRate(f)
		}
	}
	return 1.0
}

// validSampleRate applies the OTel fallback for malformed ratio arguments.
// Invalid values do not silently become NeverSample or AlwaysSample.
func validSampleRate(rate float64) float64 {
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate < 0 || rate > 1 {
		return 1.0
	}
	return rate
}

// SpanKind represents the role of the span.
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer
	SpanKindClient
	SpanKindProducer
	SpanKindConsumer
)

// SpanStatusCode represents span completion status.
type SpanStatusCode int

const (
	StatusUnset SpanStatusCode = iota
	StatusOK
	StatusError
)

// Span represents an OpenTelemetry span.
// Wraps a real trace.Span from the OTel SDK when tracing is enabled.
type Span struct {
	name       string
	traceID    string
	spanID     string
	parentID   string
	startTime  time.Time
	endTime    time.Time
	attributes map[string]any
	status     SpanStatusCode
	statusMsg  string
	kind       SpanKind
	ended      bool
	mu         sync.Mutex
	// otelSpan holds the real OTel span when tracing is enabled.
	otelSpan trace.Span
	// adopted marks a Span that WRAPS a span created by native OTel instrumentation
	// (otelgin / otelgrpc / redisotel / otelpgx) rather than by StartSpan. Such a
	// span is annotate-only: the instrumentation that created it owns its lifecycle,
	// so End() must not end it (see End). Set only by adoptOtelSpan.
	adopted bool
}

// SetAttribute sets an attribute on the span.
func (s *Span) SetAttribute(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attributes == nil {
		s.attributes = make(map[string]any)
	}
	s.attributes[key] = value
	if s.otelSpan != nil {
		s.otelSpan.SetAttributes(toOtelAttribute(key, value))
	}
}

// SetAttributes sets multiple attributes on the span.
func (s *Span) SetAttributes(attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attributes == nil {
		s.attributes = make(map[string]any)
	}
	maps.Copy(s.attributes, attrs)
	if s.otelSpan != nil {
		otelAttrs := make([]attribute.KeyValue, 0, len(attrs))
		for k, v := range attrs {
			otelAttrs = append(otelAttrs, toOtelAttribute(k, v))
		}
		s.otelSpan.SetAttributes(otelAttrs...)
	}
}

// SetStatus sets the span status.
func (s *Span) SetStatus(code SpanStatusCode, message string) {
	message = redact.Text(message)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
	s.statusMsg = message
	if s.otelSpan != nil {
		switch code {
		case StatusOK:
			s.otelSpan.SetStatus(codes.Ok, message)
		case StatusError:
			s.otelSpan.SetStatus(codes.Error, message)
		default:
			s.otelSpan.SetStatus(codes.Unset, message)
		}
	}
}

// End marks the span as ended.
//
// An ADOPTED span (one wrapping a span created by otelgin/otelgrpc/redisotel/…) is
// a no-op here: the instrumentation that created it owns its lifecycle and ends it
// when the request/operation completes. Ending it from a helper would truncate the
// server span mid-request and corrupt its duration.
func (s *Span) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.adopted {
		return
	}
	if !s.ended {
		s.endTime = time.Now()
		s.ended = true
		if s.otelSpan != nil {
			s.otelSpan.End()
		}
	}
}

// TraceID returns the span's trace ID.
func (s *Span) TraceID() string { return s.traceID }

// SpanID returns the span's ID.
func (s *Span) SpanID() string { return s.spanID }

// IsSampled reports whether this span's trace is sampled (recorded/exported).
// It reflects the real sampling decision (e.g. ParentBased / TraceIDRatio), so
// propagators can set the W3C traceparent sampled flag correctly instead of
// hard-coding it. Returns false when tracing is disabled (no OTel span).
func (s *Span) IsSampled() bool {
	return s.otelSpan != nil && s.otelSpan.SpanContext().IsSampled()
}

// Status returns the span's completion status (StatusUnset until SetStatus is
// called). Primarily useful for asserting span outcome in tests.
func (s *Span) Status() SpanStatusCode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Tracer wraps OpenTelemetry tracing functionality.
type Tracer struct {
	serviceName        string
	env                string
	enabled            bool
	options            Options
	mu                 sync.RWMutex
	provider           *sdktrace.TracerProvider
	otelTracer         trace.Tracer
	propagator         propagation.TextMapPropagator
	previousTP         trace.TracerProvider
	previousPropagator propagation.TextMapPropagator
	previousOwner      *Tracer
	otelOwner          *otelGlobalOwner
	shutdownOnce       sync.Once
	shutdownErr        error
	closed             atomic.Bool
}

var (
	globalTracer       atomic.Pointer[Tracer]
	globalTracerMu     sync.Mutex
	globalInitErr      error
	implicitTraceMu    sync.Mutex
	globalTracerOwners = struct {
		current *globalTracerOwner
		active  map[*globalTracerOwner]struct{}
	}{active: make(map[*globalTracerOwner]struct{})}
	otelOwners = struct {
		sync.Mutex
		current *otelGlobalOwner
		active  map[*otelGlobalOwner]struct{}
	}{active: make(map[*otelGlobalOwner]struct{})}
)

type globalTracerOwner struct {
	tracer      *Tracer
	initErr     error
	previous    *globalTracerOwner
	baseline    *Tracer
	baselineErr error
	active      bool
}

type otelGlobalOwner struct {
	tracer             *Tracer
	provider           trace.TracerProvider
	propagator         *ownedTextMapPropagator
	previous           *otelGlobalOwner
	baselineTP         trace.TracerProvider
	baselinePropagator propagation.TextMapPropagator
	active             bool
	primary            bool
}

// ownedTextMapPropagator gives each installation a comparable identity while
// preserving the configured propagator's behavior. TextMapPropagator
// implementations are not required to be comparable, so comparing the raw
// interface during restoration can panic.
type ownedTextMapPropagator struct {
	delegate propagation.TextMapPropagator
}

func (p *ownedTextMapPropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	p.delegate.Inject(ctx, carrier)
}

func (p *ownedTextMapPropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return p.delegate.Extract(ctx, carrier)
}

func (p *ownedTextMapPropagator) Fields() []string {
	return p.delegate.Fields()
}

// New creates a new tracer. When tracing is enabled, this initializes the real
// OpenTelemetry SDK with the configured OTLP transport.
func New(ctx context.Context, opts Options) (*Tracer, error) {
	t := &Tracer{
		serviceName: opts.ServiceName,
		env:         opts.Env,
		enabled:     tracingEnabled(opts),
		options:     opts,
	}

	if t.enabled {
		if err := t.initOTel(ctx, opts); err != nil {
			t.enabled = false
			return t, fmt.Errorf("fit/tracing: failed to initialize OTel: %w", err)
		}
	}

	return t, nil
}

// tracingEnabled resolves the enable-gate:
//
//  1. OTEL_SDK_DISABLED=true is the OTel-standard KILL SWITCH and wins over everything,
//     including an explicit Options.Enabled and TRACING_ENABLED. traceclue/pyfit honour
//     it; fit-go ignored it entirely, so an operator disabling telemetry fleet-wide via
//     the standard env had no effect on Go services.
//  2. otherwise the explicit Options.Enabled override when set,
//  3. in explicit mode, the TRACING_ENABLED env var,
//  4. in pyfit compatibility mode, enabled when OTEL_SDK_DISABLED is absent.
func tracingEnabled(opts Options) bool {
	if opts.SDKDisabled != nil && *opts.SDKDisabled {
		return false
	}
	if sdkDisabled() {
		return false
	}
	if opts.Enabled != nil {
		return *opts.Enabled
	}
	if strings.EqualFold(strings.TrimSpace(opts.ActivationMode), "pyfit") {
		if opts.SDKDisabled != nil {
			return !*opts.SDKDisabled
		}
		// pyfit/TraceClue use Python truthiness here: an absent or explicitly
		// empty value enables the SDK, while every non-empty value disables it.
		return strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")) == ""
	}
	return isTracingEnabled()
}

// sdkDisabled reports the OTel-standard OTEL_SDK_DISABLED kill switch.
func sdkDisabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true")
}

type otlpEndpointSource uint8

const (
	otlpEndpointDefault otlpEndpointSource = iota
	otlpEndpointCommon
	otlpEndpointTraces
	otlpEndpointExplicit
)

// resolveOTLPEndpoint preserves signal-specific > common precedence. An Endpoint
// supplied directly in Options is explicit unless it is the value populated by
// DefaultOptions from the current environment.
func resolveOTLPEndpoint(opts Options) (string, otlpEndpointSource) {
	tracesEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
	commonEndpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	endpoint := strings.TrimSpace(opts.Endpoint)

	if endpoint != "" {
		switch {
		case tracesEndpoint != "" && endpoint == tracesEndpoint:
			return endpoint, otlpEndpointTraces
		case tracesEndpoint == "" && commonEndpoint != "" && endpoint == commonEndpoint:
			return endpoint, otlpEndpointCommon
		default:
			return endpoint, otlpEndpointExplicit
		}
	}
	if tracesEndpoint != "" {
		return tracesEndpoint, otlpEndpointTraces
	}
	if commonEndpoint != "" {
		return commonEndpoint, otlpEndpointCommon
	}
	return "", otlpEndpointDefault
}

// resolveOTLPProtocol applies the OTel/NodeSDK precedence and default used by
// the deployed TraceClue generation. Go has no OTLP http/json trace exporter;
// unsupported values therefore fall back to the standard http/protobuf default.
func resolveOTLPProtocol(opts Options) string {
	protocol := strings.ToLower(strings.TrimSpace(opts.Protocol))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")))
	}
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	switch protocol {
	case "grpc":
		return "grpc"
	case "http/protobuf", "":
		return "http/protobuf"
	default:
		return "http/protobuf"
	}
}

func httpTraceEndpoint(endpoint string, source otlpEndpointSource) string {
	if endpoint == "" || source != otlpEndpointCommon {
		return endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/traces"
	return u.String()
}

// newOTLPExporter builds the OTLP span exporter. Explicit Options override the
// environment; otherwise traces-specific environment variables override common
// OTLP variables. The OTel default protocol is http/protobuf.
func newOTLPExporter(ctx context.Context, opts Options) (sdktrace.SpanExporter, error) {
	endpoint, source := resolveOTLPEndpoint(opts)
	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme == "" || u.Host == "" {
			if err == nil {
				err = fmt.Errorf("endpoint must include scheme and host")
			}
			return nil, fmt.Errorf("invalid OTLP endpoint %q: %w", endpoint, err)
		}
	}
	if resolveOTLPProtocol(opts) == "grpc" {
		var grpcOpts []otlptracegrpc.Option
		if endpoint != "" {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithEndpointURL(endpoint))
		}
		return otlptracegrpc.New(ctx, grpcOpts...)
	}
	var httpOpts []otlptracehttp.Option
	if endpoint != "" {
		httpOpts = append(httpOpts, otlptracehttp.WithEndpointURL(httpTraceEndpoint(endpoint, source)))
	}
	return otlptracehttp.New(ctx, httpOpts...)
}

func resolveTraceExporters(opts Options) ([]string, error) {
	value := strings.TrimSpace(opts.Exporters)
	if value == "" {
		value = strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER"))
	}
	if value == "" {
		value = "otlp"
	}

	seen := make(map[string]struct{})
	exporters := make([]string, 0, 2)
	for _, raw := range strings.Split(value, ",") {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		switch name {
		case "otlp", "console", "none":
		default:
			return nil, fmt.Errorf("unsupported OTEL_TRACES_EXPORTER value %q", name)
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		exporters = append(exporters, name)
	}
	if len(exporters) == 0 {
		return nil, fmt.Errorf("OTEL_TRACES_EXPORTER must select at least one exporter")
	}
	if _, hasNone := seen["none"]; hasNone && len(exporters) != 1 {
		return nil, fmt.Errorf("OTEL_TRACES_EXPORTER=none cannot be combined with other exporters")
	}
	return exporters, nil
}

func newSpanExporters(ctx context.Context, opts Options) ([]sdktrace.SpanExporter, error) {
	if opts.SpanExporter != nil {
		return []sdktrace.SpanExporter{opts.SpanExporter}, nil
	}

	names, err := resolveTraceExporters(opts)
	if err != nil {
		return nil, err
	}
	exporters := make([]sdktrace.SpanExporter, 0, len(names))
	shutdown := func() {
		for _, exporter := range exporters {
			_ = exporter.Shutdown(context.Background())
		}
	}
	for _, name := range names {
		var exporter sdktrace.SpanExporter
		switch name {
		case "none":
			continue
		case "otlp":
			exporter, err = newOTLPExporter(ctx, opts)
		case "console":
			exporter, err = stdouttrace.New()
		}
		if err != nil {
			shutdown()
			return nil, fmt.Errorf("creating %s trace exporter: %w", name, err)
		}
		exporters = append(exporters, exporter)
	}
	return exporters, nil
}

// buildSampler resolves the OTel-standard OTEL_TRACES_SAMPLER value (opts.Sampler)
// into a sampler, using opts.SampleRate only for the *ratio variants. Unset or
// unknown sampler names use the OTel SDK default, parentbased_always_on.
// It is then wrapped by the traceclue ServiceEntryPointSampler when
// TRACECLUE_ALWAYS_SAMPLE_SERVICE_ENTRY_POINTS=true (the platform default for the
// Node/Python fleet). Faithful to legacy, a force-sampled entry span also causes
// ParentBased descendants to remain sampled; this wrapper does not independently
// thin interior spans.
func buildSampler(opts Options) sdktrace.Sampler {
	return wrapWithEntryPointSampler(buildBaseSampler(opts))
}

// buildBaseSampler resolves the OTel-standard sampler value alone (no entry-point
// wrapping) — kept separate so the entry-point sampler can delegate to it.
func buildBaseSampler(opts Options) sdktrace.Sampler {
	switch strings.ToLower(strings.TrimSpace(opts.Sampler)) {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return ratioSampler(opts.SampleRate)
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(ratioSampler(validSampleRate(opts.SampleRate)))
	case "":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	default:
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
}

// ratioSampler maps a ratio to a sampler, collapsing the >=1 / <=0 edges to
// Always/Never so TraceIDRatioBased only sees a genuine fraction.
func ratioSampler(ratio float64) sdktrace.Sampler {
	ratio = validSampleRate(ratio)
	if ratio >= 1.0 {
		return sdktrace.AlwaysSample()
	}
	if ratio <= 0.0 {
		return sdktrace.NeverSample()
	}
	return sdktrace.TraceIDRatioBased(ratio)
}

// buildPropagator mirrors NodeSDK 0.205.0 used by TraceClue 3.1.3. TraceContext
// and Baggage are the default pair when the setting is empty. Configured names
// are deduplicated; unknown names are warned and ignored. If no configured name
// is valid, a no-op propagator is returned rather than falling back to defaults.
func buildPropagator(value string) (propagation.TextMapPropagator, error) {
	if strings.TrimSpace(value) == "" {
		value = "tracecontext,baggage"
	}

	names := make([]string, 0, 2)
	for _, raw := range strings.Split(value, ",") {
		if name := strings.ToLower(strings.TrimSpace(raw)); name != "" {
			names = append(names, name)
		}
	}
	seen := make(map[string]struct{})
	propagators := make([]propagation.TextMapPropagator, 0, 2)
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		switch name {
		case "tracecontext":
			propagators = append(propagators, propagation.TraceContext{})
		case "baggage":
			propagators = append(propagators, propagation.Baggage{})
		case "b3":
			propagators = append(propagators, b3.New())
		case "b3multi":
			propagators = append(propagators, b3.New(b3.WithInjectEncoding(b3.B3MultipleHeader)))
		case "jaeger":
			// The Go Jaeger propagator handles only uber-trace-id. The installed
			// Node propagator also maps OTel baggage to dynamic uberctx-* headers,
			// so compose the missing baggage half explicitly.
			propagators = append(propagators, jaeger.Jaeger{}, jaegerBaggagePropagator{})
		default:
			slog.Warn("fit/tracing: unavailable configured propagator ignored")
		}
	}
	if len(propagators) == 0 {
		return propagation.NewCompositeTextMapPropagator(), nil
	}
	return propagation.NewCompositeTextMapPropagator(propagators...), nil
}

// jaegerBaggagePropagator reproduces the dynamic uberctx-* baggage behavior of
// @opentelemetry/propagator-jaeger used by the legacy Node services.
type jaegerBaggagePropagator struct{}

func (jaegerBaggagePropagator) Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	for _, member := range baggage.FromContext(ctx).Members() {
		carrier.Set("uberctx-"+member.Key(), encodeURIComponent(member.Value()))
	}
}

func (jaegerBaggagePropagator) Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	current := baggage.FromContext(ctx)
	changed := false
	for _, key := range carrier.Keys() {
		if !strings.HasPrefix(strings.ToLower(key), "uberctx-") || len(key) <= len("uberctx-") {
			continue
		}
		value, err := url.PathUnescape(carrier.Get(key))
		if err != nil {
			continue
		}
		member, err := baggage.NewMemberRaw(key[len("uberctx-"):], value)
		if err != nil {
			continue
		}
		current, err = current.SetMember(member)
		if err == nil {
			changed = true
		}
	}
	if !changed {
		return ctx
	}
	return baggage.ContextWithBaggage(ctx, current)
}

// Dynamic uberctx-* fields cannot be enumerated by TextMapPropagator.Fields.
func (jaegerBaggagePropagator) Fields() []string { return nil }

func encodeURIComponent(value string) string {
	const hex = "0123456789ABCDEF"
	var encoded strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			strings.ContainsRune("-_.!~*'()", rune(c)) {
			encoded.WriteByte(c)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte(hex[c>>4])
		encoded.WriteByte(hex[c&0x0f])
	}
	return encoded.String()
}

// PropagationFields returns every standard propagation field fit-go supports,
// plus fields owned by the active propagator. Outbound transports must remove
// this complete set before injection. Otherwise changing OTEL_PROPAGATORS at a
// service boundary can forward a stale B3 or Jaeger parent alongside the newly
// injected W3C context, making downstream extraction order-dependent.
func PropagationFields(propagator propagation.TextMapPropagator) []string {
	fields := []string{
		"traceparent", "tracestate", "baggage",
		"b3",
		"x-b3-traceid", "x-b3-spanid", "x-b3-sampled", "x-b3-flags", "x-b3-parentspanid",
		"uber-trace-id",
	}
	if propagator != nil {
		fields = append(fields, propagator.Fields()...)
	}
	seen := make(map[string]struct{}, len(fields))
	unique := fields[:0]
	for _, field := range fields {
		canonical := strings.ToLower(strings.TrimSpace(field))
		if canonical == "" {
			continue
		}
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		unique = append(unique, canonical)
	}
	return unique
}

// IsPropagationField recognizes both enumerable propagation headers and the
// Jaeger propagator's dynamic uberctx-* baggage family.
func IsPropagationField(name string, propagator propagation.TextMapPropagator) bool {
	canonical := strings.ToLower(strings.TrimSpace(name))
	if canonical == "" {
		return false
	}
	if strings.HasPrefix(canonical, "uberctx-") {
		return true
	}
	for _, field := range PropagationFields(propagator) {
		if canonical == field {
			return true
		}
	}
	return false
}

// buildResource mirrors the default NodeSDK detector set used by TraceClue:
// telemetry SDK, process, host, then environment. Explicit fit-go attributes
// override detector values, and ServiceName/Env are authoritative last.
//
// Process command arguments and owner are deliberately excluded even though the
// legacy Node detector emitted them: command lines can contain credentials and the
// fit-go's telemetry contract forbids secret/PII export.
//
// Resource detectors can return useful partial resources together with errors.
// Those errors are reported to the OTel error handler but do not disable tracing.
func buildResource(ctx context.Context, opts Options) *resource.Resource {
	serviceName := strings.TrimSpace(opts.ServiceName)
	if serviceName == "" {
		serviceName = strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	}
	fallbackServiceName := strings.TrimSpace(opts.FallbackServiceName)
	if fallbackServiceName == "" {
		fallbackServiceName = strings.TrimSpace(os.Getenv("SERVICE_NAME"))
	}
	environment := opts.Env
	if environment == "" {
		environment = envString("GO_ENV", envString("NODE_ENV", "development"))
	}
	defaults := make([]attribute.KeyValue, 0, 4)
	if host, err := os.Hostname(); err == nil && host != "" {
		// Legacy TraceClue identifies a replica by hostname. Environment and explicit
		// attributes below may intentionally override this default.
		defaults = append(defaults, semconv.ServiceInstanceID(host))
	}
	buildVersion, buildRevision := detectedBuildMetadata()
	version := firstEnvironmentValue("SENTRY_RELEASE", "SERVICE_VERSION", "PLATFORM_VERSION")
	if version == "" {
		version = buildVersion
	}
	if version != "" {
		defaults = append(defaults, attribute.String("service.version", version))
	}
	revision := firstEnvironmentValue("GITSHA", "GIT_SHA")
	if revision == "" {
		revision = buildRevision
	}
	if revision != "" {
		defaults = append(defaults, attribute.String("vcs.ref.head.revision", revision))
	}
	if deploymentName := firstEnvironmentValue("DEPLOY_ENV", "SENTRY_ENVIRONMENT"); deploymentName != "" {
		defaults = append(defaults, attribute.String("deployment.environment.name", deploymentName))
	}

	explicitAttributes := maps.Clone(opts.Attributes)
	if explicitAttributes == nil {
		explicitAttributes = make(map[string]string)
	}
	for k, v := range parseResourceAttributes(opts.ResourceAttributes) {
		if _, exists := explicitAttributes[k]; !exists {
			explicitAttributes[k] = v
		}
	}
	explicit := make([]attribute.KeyValue, 0, len(explicitAttributes))
	for k, v := range explicitAttributes {
		explicit = append(explicit, attribute.String(k, v))
	}
	authoritative := []attribute.KeyValue{attribute.String("deployment.environment", environment)}
	if serviceName != "" {
		authoritative = append(authoritative, semconv.ServiceName(serviceName))
	}

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithProcessPID(),
		resource.WithProcessExecutableName(),
		resource.WithProcessExecutablePath(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithProcessRuntimeDescription(),
		resource.WithHost(),
		resource.WithAttributes(defaults...),
		resource.WithFromEnv(),
		resource.WithAttributes(explicit...),
		resource.WithAttributes(authoritative...),
	)
	if err != nil {
		// Detector errors can include raw OTEL_RESOURCE_ATTRIBUTES input. Do not
		// echo it because operators may have accidentally placed sensitive values
		// there; valid partial attributes are still retained below.
		slog.Warn("fit/tracing: resource detection incomplete; valid attributes retained")
	}
	if res == nil {
		return resource.Empty()
	}
	if value, ok := res.Set().Value(semconv.ServiceNameKey); !ok || strings.TrimSpace(value.AsString()) == "" {
		if fallbackServiceName == "" {
			fallbackServiceName = "unknown_service"
		}
		fallback := resource.NewSchemaless(semconv.ServiceName(fallbackServiceName))
		merged, mergeErr := resource.Merge(res, fallback)
		if mergeErr == nil && merged != nil {
			res = merged
		}
	}
	return res
}

func parseResourceAttributes(raw string) map[string]string {
	attributes := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		value = strings.TrimSpace(value)
		if decoded, err := url.PathUnescape(value); err == nil {
			value = decoded
		}
		attributes[key] = value
	}
	return attributes
}

func firstEnvironmentValue(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func detectedBuildMetadata() (version, revision string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", ""
	}
	if value := strings.TrimSpace(info.Main.Version); value != "" && value != "(devel)" {
		version = value
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			revision = strings.TrimSpace(setting.Value)
			break
		}
	}
	return version, revision
}

// ResourceFromOptions builds the process resource used by fit-go telemetry.
// Metrics and traces should call this helper so service identity, environment,
// process, host, and OTEL_RESOURCE_ATTRIBUTES precedence cannot drift between
// signals.
func ResourceFromOptions(ctx context.Context, opts Options) *resource.Resource {
	return buildResource(ctx, opts)
}

func isOTelDefaultDelegate(value interface{}) bool {
	typ := reflect.TypeOf(value)
	if typ == nil {
		return false
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	return typ.PkgPath() == "go.opentelemetry.io/otel/internal/global"
}

func snapshotTracerProvider(provider trace.TracerProvider) trace.TracerProvider {
	if provider == nil || isOTelDefaultDelegate(provider) {
		return tracenoop.NewTracerProvider()
	}
	return provider
}

func snapshotPropagator(propagator propagation.TextMapPropagator) propagation.TextMapPropagator {
	if propagator == nil || isOTelDefaultDelegate(propagator) {
		return propagation.NewCompositeTextMapPropagator()
	}
	return propagator
}

// initOTel sets up the real OTel TracerProvider with the configured exporters.
func (t *Tracer) initOTel(ctx context.Context, opts Options) error {
	propagator, err := buildPropagator(opts.Propagators)
	if err != nil {
		return fmt.Errorf("creating propagator: %w", err)
	}
	res := buildResource(ctx, opts)

	exporters, err := newSpanExporters(ctx, opts)
	if err != nil {
		return err
	}

	providerOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(buildSampler(opts)),
	}
	for _, exporter := range exporters {
		var processor sdktrace.SpanProcessor
		if opts.UseSimpleSpanProcessor {
			processor = sdktrace.NewSimpleSpanProcessor(exporter)
		} else {
			bspOpts := make([]sdktrace.BatchSpanProcessorOption, 0, 2)
			if opts.BatchTimeout > 0 {
				bspOpts = append(bspOpts, sdktrace.WithBatchTimeout(opts.BatchTimeout))
			}
			if opts.MaxExportBatch > 0 {
				bspOpts = append(bspOpts, sdktrace.WithMaxExportBatchSize(opts.MaxExportBatch))
			}
			processor = sdktrace.NewBatchSpanProcessor(exporter, bspOpts...)
		}
		providerOpts = append(providerOpts, sdktrace.WithSpanProcessor(processor))
	}

	tp := sdktrace.NewTracerProvider(providerOpts...)

	t.provider = tp
	if serviceName, ok := res.Set().Value(semconv.ServiceNameKey); ok {
		t.serviceName = serviceName.AsString()
	}
	t.otelTracer = tp.Tracer("fit.go/" + t.serviceName)
	t.propagator = propagator
	t.otelOwner = installOTelGlobals(t, true)
	refreshImplicitTraceEnabled()

	return nil
}

// installOTelGlobals records an ownership node before replacing both OTel
// globals. New and SetGlobal each receive a node so either lifecycle can end
// out of order without reviving an already-shut-down owner.
func installOTelGlobals(t *Tracer, primary bool) *otelGlobalOwner {
	if t == nil || t.provider == nil || t.closed.Load() {
		return nil
	}

	otelOwners.Lock()
	defer otelOwners.Unlock()

	currentTP := otel.GetTracerProvider()
	currentPropagator := otel.GetTextMapPropagator()
	previous := otelOwners.current
	if previous == nil || currentTP != previous.provider || !ownedByPropagator(currentPropagator, previous.propagator) {
		// One or both process globals were independently replaced. Start a new
		// chain from the actual values instead of linking to a stale fit owner.
		previous = nil
	}
	owner := &otelGlobalOwner{
		tracer:             t,
		provider:           t.provider,
		propagator:         &ownedTextMapPropagator{delegate: t.propagator},
		previous:           previous,
		baselineTP:         snapshotTracerProvider(currentTP),
		baselinePropagator: snapshotPropagator(currentPropagator),
		active:             true,
		primary:            primary,
	}
	if primary {
		t.previousTP = owner.baselineTP
		t.previousPropagator = owner.baselinePropagator
		if previous != nil {
			t.previousOwner = previous.tracer
		}
	}
	otelOwners.current = owner
	otelOwners.active[owner] = struct{}{}
	otel.SetTracerProvider(t.provider)
	// Install the configured propagator globally so native instrumentation uses
	// the same TraceContext/Baggage/B3/Jaeger format as the legacy process.
	otel.SetTextMapPropagator(owner.propagator)
	return owner
}

func ownedByPropagator(current propagation.TextMapPropagator, owner *ownedTextMapPropagator) bool {
	installed, ok := current.(*ownedTextMapPropagator)
	return ok && installed == owner
}

func effectiveOTelPredecessor(owner *otelGlobalOwner) (*otelGlobalOwner, trace.TracerProvider, propagation.TextMapPropagator) {
	previous := owner.previous
	fallbackTP := owner.baselineTP
	fallbackPropagator := owner.baselinePropagator
	for previous != nil && !previous.active {
		fallbackTP = previous.baselineTP
		fallbackPropagator = previous.baselinePropagator
		previous = previous.previous
	}
	return previous, fallbackTP, fallbackPropagator
}

func restoreOTelOwnerLocked(owner *otelGlobalOwner) {
	if otelOwners.current != owner {
		return
	}
	previous, fallbackTP, fallbackPropagator := effectiveOTelPredecessor(owner)
	otelOwners.current = previous

	// Provider and propagator ownership are independent. An external component
	// may replace only one of them, and fit-go must preserve that replacement.
	if otel.GetTracerProvider() == owner.provider {
		if previous != nil {
			otel.SetTracerProvider(previous.provider)
		} else {
			otel.SetTracerProvider(fallbackTP)
		}
	}
	if ownedByPropagator(otel.GetTextMapPropagator(), owner.propagator) {
		if previous != nil {
			otel.SetTextMapPropagator(previous.propagator)
		} else {
			otel.SetTextMapPropagator(fallbackPropagator)
		}
	}
}

func refreshLegacyOTelSnapshotsLocked() {
	for owner := range otelOwners.active {
		if !owner.active || !owner.primary {
			continue
		}
		previous, fallbackTP, fallbackPropagator := effectiveOTelPredecessor(owner)
		owner.tracer.previousTP = fallbackTP
		owner.tracer.previousPropagator = fallbackPropagator
		owner.tracer.previousOwner = nil
		if previous != nil {
			owner.tracer.previousTP = previous.provider
			owner.tracer.previousPropagator = previous.propagator
			owner.tracer.previousOwner = previous.tracer
		}
	}
}

func deactivateOTelOwner(owner *otelGlobalOwner) {
	if owner == nil {
		return
	}
	otelOwners.Lock()
	if !owner.active {
		otelOwners.Unlock()
		return
	}
	owner.active = false
	delete(otelOwners.active, owner)
	restoreOTelOwnerLocked(owner)
	refreshLegacyOTelSnapshotsLocked()
	otelOwners.Unlock()
	refreshImplicitTraceEnabled()
}

func relinquishOTelGlobals(t *Tracer) {
	if t == nil {
		return
	}
	otelOwners.Lock()
	for owner := range otelOwners.active {
		if owner.tracer == t {
			owner.active = false
			delete(otelOwners.active, owner)
		}
	}
	if otelOwners.current != nil && !otelOwners.current.active {
		restoreOTelOwnerLocked(otelOwners.current)
	}
	refreshLegacyOTelSnapshotsLocked()
	otelOwners.Unlock()
	refreshImplicitTraceEnabled()
}

// Init initializes the global tracer with default options.
func Init() error {
	_, err := InitWithOptions(DefaultOptions())
	return err
}

// InitWithOptions initializes the global tracer with custom options.
func InitWithOptions(opts Options) (*Tracer, error) {
	globalTracerMu.Lock()
	defer globalTracerMu.Unlock()
	if current := globalTracer.Load(); current != nil {
		return current, globalInitErr
	}

	t, err := New(context.Background(), opts)
	installGlobalTracerLocked(t, err)
	refreshImplicitTraceEnabled()
	return t, err
}

// Global returns the global tracer instance.
//
// This compatibility API cannot return initialization failures. Boot paths that
// require strict telemetry startup should call Init or GlobalWithError.
func Global() *Tracer {
	t, _ := GlobalWithError()
	return t
}

// GlobalWithError returns the global tracer and the cached initialization error.
// Unlike the old sync.Once path, repeated callers observe the same failure.
func GlobalWithError() (*Tracer, error) {
	globalTracerMu.Lock()
	if current := globalTracer.Load(); current != nil {
		err := globalInitErr
		globalTracerMu.Unlock()
		return current, err
	}
	globalTracerMu.Unlock()
	return InitWithOptions(DefaultOptions())
}

// InitError reports the last global initialization failure, if any.
func InitError() error {
	globalTracerMu.Lock()
	defer globalTracerMu.Unlock()
	return globalInitErr
}

// SetGlobal replaces the global tracer and returns a function that restores the
// previous one. It installs a specific tracer regardless of whether Init has
// already run, primarily for tests and advanced wiring. Restore with the returned
// function (typically `defer SetGlobal(tr)()`).
func SetGlobal(t *Tracer) (restore func()) {
	otelOwner := installOTelGlobals(t, false)
	globalTracerMu.Lock()
	if t != nil && t.closed.Load() {
		globalTracerMu.Unlock()
		deactivateOTelOwner(otelOwner)
		return func() {}
	}
	owner := installGlobalTracerLocked(t, nil)
	globalTracerMu.Unlock()
	refreshImplicitTraceEnabled()

	var once sync.Once
	return func() {
		once.Do(func() {
			deactivateGlobalTracerOwner(owner)
			deactivateOTelOwner(otelOwner)
		})
	}
}

func installGlobalTracerLocked(t *Tracer, initErr error) *globalTracerOwner {
	previous := globalTracerOwners.current
	current := globalTracer.Load()
	if previous == nil || current != previous.tracer {
		// The package state was replaced outside the tracked owner chain (tests
		// and advanced wiring can do this from within package tracing).
		previous = nil
	}
	owner := &globalTracerOwner{
		tracer:      t,
		initErr:     initErr,
		previous:    previous,
		baseline:    current,
		baselineErr: globalInitErr,
		active:      true,
	}
	globalTracerOwners.current = owner
	globalTracerOwners.active[owner] = struct{}{}
	globalTracer.Store(t)
	globalInitErr = initErr
	return owner
}

func effectiveGlobalTracerPredecessor(owner *globalTracerOwner) (*globalTracerOwner, *Tracer, error) {
	previous := owner.previous
	fallback := owner.baseline
	fallbackErr := owner.baselineErr
	for previous != nil && !previous.active {
		fallback = previous.baseline
		fallbackErr = previous.baselineErr
		previous = previous.previous
	}
	return previous, fallback, fallbackErr
}

func restoreGlobalTracerOwnerLocked(owner *globalTracerOwner) {
	if globalTracerOwners.current != owner {
		return
	}
	previous, fallback, fallbackErr := effectiveGlobalTracerPredecessor(owner)
	globalTracerOwners.current = previous
	if globalTracer.Load() != owner.tracer {
		return
	}
	if previous != nil {
		globalTracer.Store(previous.tracer)
		globalInitErr = previous.initErr
		return
	}
	globalTracer.Store(fallback)
	globalInitErr = fallbackErr
}

func deactivateGlobalTracerOwner(owner *globalTracerOwner) {
	if owner == nil {
		return
	}
	globalTracerMu.Lock()
	if owner.active {
		owner.active = false
		delete(globalTracerOwners.active, owner)
		restoreGlobalTracerOwnerLocked(owner)
	}
	globalTracerMu.Unlock()
	refreshImplicitTraceEnabled()
}

func relinquishGlobalTracer(t *Tracer) {
	if t == nil {
		return
	}
	globalTracerMu.Lock()
	for owner := range globalTracerOwners.active {
		if owner.tracer == t {
			owner.active = false
			delete(globalTracerOwners.active, owner)
		}
	}
	if globalTracerOwners.current != nil && !globalTracerOwners.current.active {
		restoreGlobalTracerOwnerLocked(globalTracerOwners.current)
	} else if globalTracerOwners.current == nil && globalTracer.Load() == t {
		globalTracer.Store(nil)
		globalInitErr = nil
	}
	globalTracerMu.Unlock()
	refreshImplicitTraceEnabled()
}

func refreshImplicitTraceEnabled() {
	implicitTraceMu.Lock()
	defer implicitTraceMu.Unlock()
	if current := globalTracer.Load(); current != nil && current.enabled && !current.closed.Load() {
		logging.SetImplicitTraceEnabled(true)
		return
	}
	otelOwners.Lock()
	owner := otelOwners.current
	enabled := owner != nil && owner.active && owner.tracer != nil && !owner.tracer.closed.Load()
	otelOwners.Unlock()
	logging.SetImplicitTraceEnabled(enabled)
}

// Shutdown gracefully shuts down the global tracer, flushing any remaining spans.
func Shutdown(ctx context.Context) error {
	globalTracerMu.Lock()
	g := globalTracer.Load()
	globalTracerMu.Unlock()
	if g == nil {
		return nil
	}
	return g.Shutdown(ctx)
}

// Shutdown gracefully shuts down this tracer, flushing any remaining spans.
func (t *Tracer) Shutdown(ctx context.Context) error {
	return t.shutdown(ctx)
}

func (t *Tracer) shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.shutdownOnce.Do(func() {
		t.closed.Store(true)
		relinquishGlobalTracer(t)
		relinquishOTelGlobals(t)
		if t.enabled && t.provider != nil {
			t.shutdownErr = t.provider.Shutdown(ctx)
		}
	})
	return t.shutdownErr
}

// IsEnabled returns whether tracing is active.
func (t *Tracer) IsEnabled() bool {
	return t != nil && t.enabled && !t.closed.Load()
}

// toOtelSpanKind converts our SpanKind to the OTel trace.SpanKind.
func toOtelSpanKind(kind SpanKind) trace.SpanKind {
	switch kind {
	case SpanKindServer:
		return trace.SpanKindServer
	case SpanKindClient:
		return trace.SpanKindClient
	case SpanKindProducer:
		return trace.SpanKindProducer
	case SpanKindConsumer:
		return trace.SpanKindConsumer
	default:
		return trace.SpanKindInternal
	}
}

// StartSpan starts a new span. When the OTel SDK is initialized, this creates
// a real OTel span; otherwise it creates an in-memory span.
func (t *Tracer) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, *Span) {
	// A nil context must never panic the caller (e.g. tests that pass nil).
	if ctx == nil {
		ctx = context.Background()
	}
	span := &Span{
		name:      name,
		traceID:   TraceIDFromContext(ctx),
		spanID:    generateID(8),
		parentID:  SpanIDFromContext(ctx),
		startTime: time.Now(),
		kind:      kind,
	}

	// Generate new trace ID if not in existing trace
	if span.traceID == "" {
		span.traceID = generateID(16)
	}

	// If OTel is initialized, create a real span.
	if t.otelTracer != nil {
		var otelCtx context.Context
		var otelSpan trace.Span
		otelCtx, otelSpan = t.otelTracer.Start(ctx, name,
			trace.WithSpanKind(toOtelSpanKind(kind)),
		)
		span.otelSpan = otelSpan

		// Extract IDs from the real span.
		sc := otelSpan.SpanContext()
		if sc.HasTraceID() {
			span.traceID = sc.TraceID().String()
		}
		if sc.HasSpanID() {
			span.spanID = sc.SpanID().String()
		}

		ctx = otelCtx
	}

	// Add span to context for our own lookup as well.
	ctx = context.WithValue(ctx, traceIDKey, span.traceID)
	ctx = context.WithValue(ctx, spanIDKey, span.spanID)
	ctx = context.WithValue(ctx, currentSpanKey, span)

	// Bridge the IDs and W3C sampling flags to the logging package so
	// logger.WithContext(ctx) auto-stamps the complete trace identity on every
	// log line within this span — the Go equivalent of Node's OTel log-format
	// enrichment, with no per-call wiring.
	var traceFlags byte
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		traceFlags = byte(sc.TraceFlags())
	}
	ctx = logging.ContextWithTraceFlags(ctx, span.traceID, span.spanID, traceFlags)

	return ctx, span
}

// SpanFromContext extracts the current span from context.
//
// It first looks for a span created by this package (StartSpan seeds a private
// context key). When there is none it ADOPTS the native OTel span from the
// context — the span created by otelgin / otelgrpc / redisotel / otelpgx, which
// live in the STANDARD OTel context and never touch our private key.
//
// The fallback is essential, not cosmetic. server.OTelMiddleware installs otelgin,
// which seeds ONLY the native span, so without it every helper built on this
// function silently no-ops inside an HTTP handler:
//
//   - SetSpanAttributes / SetSpanStatus / SetSpanAttributesWithStatus — annotations
//     vanish (the fit.js/traceclue setSpanAttributes equivalent works on the native
//     active span);
//   - kafka.InjectTraceHeaders — finds no span and injects NO traceparent, so an
//     HTTP handler producing to Kafka SEVERS the trace: the consumer starts a fresh
//     one instead of continuing the request's trace.
//
// The returned span is annotate-only when adopted: End() is a no-op on it (see End).
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	privateSpan, _ := ctx.Value(currentSpanKey).(*Span)
	nativeSpan := trace.SpanFromContext(ctx)
	nativeSC := nativeSpan.SpanContext()
	if nativeSC.IsValid() {
		// StartSpan stores both representations of the same span. Preserve its
		// richer wrapper only while it is still the native active span. If native
		// instrumentation created a child afterwards, that child is authoritative.
		if privateSpan != nil && privateSpan.spanID == nativeSC.SpanID().String() {
			return privateSpan
		}
		return adoptOtelSpan(ctx)
	}
	return privateSpan
}

// adoptOtelSpan wraps the context's native OTel span in a *Span so the fit-go
// helpers can operate on it. Returns nil when the context carries no valid span
// (tracing disabled, or a non-recording context).
func adoptOtelSpan(ctx context.Context) *Span {
	otelSpan := trace.SpanFromContext(ctx)
	sc := otelSpan.SpanContext()
	if !sc.IsValid() {
		return nil
	}
	return &Span{
		traceID:  sc.TraceID().String(),
		spanID:   sc.SpanID().String(),
		otelSpan: otelSpan,
		adopted:  true,
	}
}

// SpanAttributes represents key-value attributes for a span.
type SpanAttributes map[string]any

// SetSpanAttributes sets attributes on the current span in context.
func SetSpanAttributes(ctx context.Context, attrs SpanAttributes) {
	span := SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(attrs)
	}
}

// SetSpanStatus sets the status on the current span.
func SetSpanStatus(ctx context.Context, code SpanStatusCode, message string) {
	span := SpanFromContext(ctx)
	if span != nil {
		span.SetStatus(code, message)
	}
}

// SetSpanAttributesWithStatus sets both attributes and status on the current span.
// This is the primary API used by for span annotation.
func SetSpanAttributesWithStatus(ctx context.Context, attrs SpanAttributes, status SpanStatusCode) {
	span := SpanFromContext(ctx)
	if span == nil {
		return
	}
	// Only set attributes if non-empty (matches behavior)
	if len(attrs) > 0 {
		span.SetAttributes(attrs)
	}
	// Validate status code before setting (matches behavior)
	if status >= StatusUnset && status <= StatusError {
		span.SetStatus(status, "")
	}
	// Invalid status codes are ignored with a warning (logged when SDK integrated)
}

// IgnoredPaths are path patterns that should not be traced.
// These match exact paths and paths with subpaths.
var IgnoredPaths = []*regexp.Regexp{
	regexp.MustCompile(`^/_healthz(/.*)?$`),
	regexp.MustCompile(`^/_readyz(/.*)?$`),
}

// AddIgnoredPath adds a path pattern to the ignore list.
func AddIgnoredPath(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	IgnoredPaths = append(IgnoredPaths, re)
	return nil
}

// ShouldTrace returns true if the given path should be traced.
func ShouldTrace(path string) bool {
	for _, p := range IgnoredPaths {
		if p.MatchString(path) {
			return false
		}
	}
	return true
}

// IgnoreIncomingRequestHook returns true if the request should be ignored.
func IgnoreIncomingRequestHook(path string) bool {
	return !ShouldTrace(path)
}

func isTracingEnabled() bool {
	val := os.Getenv("TRACING_ENABLED")
	return strings.EqualFold(val, "true") || val == "1"
}

// ContextWithTrace adds trace and span IDs to the context. It records them under
// fit-go's own keys AND, critically, installs an OpenTelemetry *remote* span
// context built from those IDs — so that a subsequent StartSpan (which delegates
// to otelTracer.Start) parents the new span to this extracted trace instead of
// beginning a fresh root trace. Without the remote span context, OTel ignores the
// custom keys and trace continuation (inbound HTTP traceparent, Kafka
// producer→consumer linkage) silently breaks.
//
// sampled is the upstream W3C sampled decision (from the inbound traceparent);
// it is carried on the remote span context so a ParentBased sampler honours the
// caller's decision instead of forcing every continued trace to record.
func ContextWithTrace(ctx context.Context, traceID, spanID string, sampled bool) context.Context {
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	ctx = context.WithValue(ctx, spanIDKey, spanID)

	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ctx // invalid trace id — keep custom keys, skip OTel parenting
	}
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}
	scc := trace.SpanContextConfig{
		TraceID:    tid,
		Remote:     true,
		TraceFlags: flags, // honour the upstream sampled decision
	}
	if sid, err := trace.SpanIDFromHex(spanID); err == nil {
		scc.SpanID = sid
	}
	if sc := trace.NewSpanContext(scc); sc.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	}
	return ctx
}

// TraceIDFromContext extracts the active trace ID. Native OTel context is the
// source of truth because instrumentation can create a child after fit-go seeded
// its private compatibility keys.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	if v, ok := ctx.Value(traceIDKey).(string); ok && v != "" {
		return v
	}
	return ""
}

// SpanIDFromContext extracts the span ID from context, with the same native-OTel
// fallback as TraceIDFromContext.
func SpanIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasSpanID() {
		return sc.SpanID().String()
	}
	if v, ok := ctx.Value(spanIDKey).(string); ok && v != "" {
		return v
	}
	return ""
}

type contextKey string

const (
	traceIDKey     contextKey = "fit_trace_id"
	spanIDKey      contextKey = "fit_span_id"
	currentSpanKey contextKey = "fit_current_span"
)

// Decorators provides function decorators for tracing.
var Decorators = &decorators{}

type decorators struct{}

// Trace wraps a function with tracing.
func (d *decorators) Trace(name string, fn func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		tracer := Global()
		if tracer == nil || !tracer.IsEnabled() {
			return fn(ctx)
		}

		ctx, span := tracer.StartSpan(ctx, name, SpanKindInternal)
		defer span.End()
		restoreActiveContext := InjectContextIntoGoroutine(ctx)
		defer restoreActiveContext()

		err := fn(ctx)
		if err != nil {
			span.SetStatus(StatusError, redact.ErrorMessage(err))
		} else {
			span.SetStatus(StatusOK, "")
		}
		return err
	}
}

// TraceWithResult wraps a function that returns a value and error.
// Note: Go methods cannot have type parameters, so this is a standalone function.
func TraceWithResult[T any](name string, fn func(ctx context.Context) (T, error)) func(ctx context.Context) (T, error) {
	return func(ctx context.Context) (T, error) {
		tracer := Global()
		if tracer == nil || !tracer.IsEnabled() {
			return fn(ctx)
		}

		ctx, span := tracer.StartSpan(ctx, name, SpanKindInternal)
		defer span.End()
		restoreActiveContext := InjectContextIntoGoroutine(ctx)
		defer restoreActiveContext()

		result, err := fn(ctx)
		if err != nil {
			span.SetStatus(StatusError, redact.ErrorMessage(err))
		} else {
			span.SetStatus(StatusOK, "")
		}
		return result, err
	}
}

// Utils provides tracing utility functions.
var Utils = &tracingUtils{}

type tracingUtils struct{}

// SetSpanAttributes is an alias for the package-level function.
func (u *tracingUtils) SetSpanAttributes(ctx context.Context, attrs SpanAttributes) {
	SetSpanAttributes(ctx, attrs)
}

// SetSpanStatus is an alias for the package-level function.
func (u *tracingUtils) SetSpanStatus(ctx context.Context, code SpanStatusCode, message string) {
	SetSpanStatus(ctx, code, message)
}

// SetSpanAttributesWithStatus is an alias for the package-level function.
// This is the primary API.
func (u *tracingUtils) SetSpanAttributesWithStatus(ctx context.Context, attrs SpanAttributes, status SpanStatusCode) {
	SetSpanAttributesWithStatus(ctx, attrs, status)
}

// FormatTraceContext formats trace context for logging.
func FormatTraceContext(traceID, spanID string) string {
	if traceID == "" && spanID == "" {
		return ""
	}
	return fmt.Sprintf("trace_id=%s span_id=%s", traceID, spanID)
}

// ExtractTraceContext extracts trace context from HTTP headers.
// Supports W3C Trace Context (traceparent) format.
func ExtractTraceContext(traceparent string) (traceID, spanID string, sampled bool) {
	// W3C format: version-trace_id-parent_id-trace_flags
	// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
	parts := strings.Split(traceparent, "-")
	if len(parts) < 4 {
		return "", "", false
	}
	traceID = parts[1]
	spanID = parts[2]
	sampled = parts[3] == "01"
	return
}

// FormatTraceparent creates a W3C traceparent header value.
func FormatTraceparent(traceID, spanID string, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return fmt.Sprintf("00-%s-%s-%s", traceID, spanID, flags)
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func generateID(byteLen int) string {
	b := make([]byte, byteLen)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func envString(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// toOtelAttribute converts a key-value pair to an OTel attribute.KeyValue.
func toOtelAttribute(key string, value any) attribute.KeyValue {
	switch v := value.(type) {
	case string:
		return attribute.String(key, v)
	case int:
		return attribute.Int(key, v)
	case int64:
		return attribute.Int64(key, v)
	case float64:
		return attribute.Float64(key, v)
	case bool:
		return attribute.Bool(key, v)
	default:
		return attribute.String(key, fmt.Sprintf("%v", v))
	}
}
