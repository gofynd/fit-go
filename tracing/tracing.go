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
// - New Relic compatibility layer
//
// Environment variables:
// - TRACING_ENABLED: Enable OpenTelemetry tracing (default: false)
// - OTEL_EXPORTER_OTLP_ENDPOINT: OTLP collector endpoint
// - OTEL_SERVICE_NAME: Service name for spans
// - OTEL_RESOURCE_ATTRIBUTES: Additional resource attributes
// - NEWRELIC_LICENSE_KEY: New Relic license key (enables NR integration)
// - NEWRELIC_APP_NAME: New Relic application name
package tracing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/logging"
)

// Options configures the tracer.
type Options struct {
	ServiceName    string
	Env            string
	Endpoint       string            // OTLP endpoint (URL: scheme+host+port)
	Protocol       string            // OTLP protocol: "grpc" | "http/protobuf" (default http/protobuf)
	Sampler        string            // OTEL_TRACES_SAMPLER (e.g. "parentbased_traceidratio"); empty = parentbased over SampleRate
	SampleRate     float64           // Sampling ratio (0.0-1.0); from OTEL_TRACES_SAMPLER_ARG
	BatchTimeout   time.Duration     // Span batch export timeout
	MaxExportBatch int               // Maximum spans per export batch
	Attributes     map[string]string // Additional resource attributes
	// Enabled, when non-nil, overrides the TRACING_ENABLED env var. Set it from a
	// merged config so tracing enabled via a config FILE (which doesn't populate
	// the process env) isn't silently a no-op. nil = use the env var.
	Enabled *bool `json:"-"`
	// SpanExporter allows injecting a custom span exporter (useful for testing).
	SpanExporter sdktrace.SpanExporter `json:"-"`
	// UseSimpleSpanProcessor uses a synchronous span processor instead of batch.
	// Only use this for testing.
	UseSimpleSpanProcessor bool `json:"-"`
}

// DefaultOptions returns default tracer options from environment.
func DefaultOptions() Options {
	return Options{
		ServiceName:    envString("OTEL_SERVICE_NAME", envString("SERVICE_NAME", "unknown")),
		Env:            envString("GO_ENV", envString("NODE_ENV", "development")),
		Endpoint:       envString("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		Protocol:       envString("OTEL_EXPORTER_OTLP_PROTOCOL", ""),
		Sampler:        envString("OTEL_TRACES_SAMPLER", ""),
		SampleRate:     sampleRateFromEnv(),
		BatchTimeout:   5 * time.Second,
		MaxExportBatch: 512,
	}
}

// sampleRateFromEnv reads the OTel-standard OTEL_TRACES_SAMPLER_ARG (the ratio for
// the *ratio samplers) and defaults to 1.0 (sample all) when unset/unparseable —
// matching the SDK's default and the prior fit-go behaviour.
func sampleRateFromEnv() float64 {
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return 1.0
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
	serviceName string
	env         string
	enabled     bool
	options     Options
	mu          sync.RWMutex
	provider    *sdktrace.TracerProvider
	otelTracer  trace.Tracer
}

var (
	globalTracer     atomic.Pointer[Tracer]
	globalTracerOnce sync.Once
)

// New creates a new tracer. When tracing is enabled, this initializes the
// real OpenTelemetry SDK with an OTLP HTTP exporter.
func New(ctx context.Context, opts Options) (*Tracer, error) {
	t := &Tracer{
		serviceName: opts.ServiceName,
		env:         opts.Env,
		enabled:     tracingEnabled(opts),
		options:     opts,
	}

	// Keep the logger's implicit trace-in-logs fallback in lock-step with the
	// tracer's enabled state, so the goroutine-local lookup (runtime.Stack) only
	// runs when tracing is actually on.
	logging.SetImplicitTraceEnabled(t.enabled)

	if t.enabled {
		if err := t.initOTel(ctx, opts); err != nil {
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
//  3. otherwise the TRACING_ENABLED env var.
func tracingEnabled(opts Options) bool {
	if sdkDisabled() {
		return false
	}
	if opts.Enabled != nil {
		return *opts.Enabled
	}
	return isTracingEnabled()
}

// sdkDisabled reports the OTel-standard OTEL_SDK_DISABLED kill switch.
func sdkDisabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true")
}

// newOTLPExporter builds the OTLP span exporter, selecting gRPC vs HTTP by
// OTEL_EXPORTER_OTLP_PROTOCOL ("grpc" | "http/protobuf", the OTel-spec values;
// default http/protobuf). The endpoint may be a full URL with scheme+host+port —
// WithEndpointURL parses it correctly, whereas WithEndpoint expects bare host:port
// and mangles a scheme into "http://http://...".
func newOTLPExporter(ctx context.Context, opts Options) (sdktrace.SpanExporter, error) {
	protocol := opts.Protocol
	if protocol == "" {
		protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	if strings.EqualFold(protocol, "grpc") {
		var grpcOpts []otlptracegrpc.Option
		if opts.Endpoint != "" {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithEndpointURL(opts.Endpoint))
		}
		return otlptracegrpc.New(ctx, grpcOpts...)
	}
	var httpOpts []otlptracehttp.Option
	if opts.Endpoint != "" {
		httpOpts = append(httpOpts, otlptracehttp.WithEndpointURL(opts.Endpoint))
	}
	return otlptracehttp.New(ctx, httpOpts...)
}

// buildSampler resolves the OTel-standard OTEL_TRACES_SAMPLER value (opts.Sampler)
// into a sampler, using opts.SampleRate as the ratio for the *ratio variants. When
// unset it defaults to parentbased-over-ratio (the platform default), so an
// upstream sampling decision on the W3C traceparent is honoured — a trace sampled
// OUT upstream isn't re-recorded here, and the configured ratio is respected for
// locally-rooted traces (previously ignored, which sampled everything).
// It is then wrapped by the traceclue ServiceEntryPointSampler when
// TRACECLUE_ALWAYS_SAMPLE_SERVICE_ENTRY_POINTS=true (the platform default for the
// Node/Python fleet), so this service's entry-point spans are always sampled while
// the interior of a trace still honours the configured ratio.
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
	case "parentbased_traceidratio", "":
		return sdktrace.ParentBased(ratioSampler(opts.SampleRate))
	default:
		// Unknown value — fall back to the safe default rather than failing init.
		return sdktrace.ParentBased(ratioSampler(opts.SampleRate))
	}
}

// ratioSampler maps a ratio to a sampler, collapsing the >=1 / <=0 edges to
// Always/Never so TraceIDRatioBased only sees a genuine fraction.
func ratioSampler(ratio float64) sdktrace.Sampler {
	if ratio >= 1.0 {
		return sdktrace.AlwaysSample()
	}
	if ratio <= 0.0 {
		return sdktrace.NeverSample()
	}
	return sdktrace.TraceIDRatioBased(ratio)
}

// initOTel sets up the real OTel TracerProvider with OTLP exporter.
func (t *Tracer) initOTel(ctx context.Context, opts Options) error {
	// Build resource attributes.
	attrs := []attribute.KeyValue{
		semconv.ServiceName(opts.ServiceName),
		attribute.String("deployment.environment", opts.Env),
	}
	for k, v := range opts.Attributes {
		attrs = append(attrs, attribute.String(k, v))
	}

	// service.instance.id = hostname (the pod name in k8s). traceclue sets this from
	// os.hostname(); without it every replica of a service is indistinguishable in
	// traces, so you cannot tell which pod served a request.
	if host, hostErr := os.Hostname(); hostErr == nil && host != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(host))
	}

	res, err := resource.New(ctx,
		// WithFromEnv parses the OTel-standard OTEL_RESOURCE_ATTRIBUTES (k=v,k=v) and
		// OTEL_SERVICE_NAME. fit-go previously ignored both, so platform-injected
		// resource attributes were silently dropped from Go spans while the Node/Python
		// fleet reported them.
		resource.WithFromEnv(),
		resource.WithAttributes(attrs...),
		resource.WithProcessRuntimeDescription(),
	)
	if err != nil {
		return fmt.Errorf("creating resource: %w", err)
	}

	// Determine span exporter.
	var exporter sdktrace.SpanExporter
	if opts.SpanExporter != nil {
		exporter = opts.SpanExporter
	} else {
		exp, err := newOTLPExporter(ctx, opts)
		if err != nil {
			return fmt.Errorf("creating OTLP exporter: %w", err)
		}
		exporter = exp
	}

	// Configure span processor.
	var sp sdktrace.SpanProcessor
	if opts.UseSimpleSpanProcessor {
		// Synchronous processor for testing.
		sp = sdktrace.NewSimpleSpanProcessor(exporter)
	} else {
		bspOpts := []sdktrace.BatchSpanProcessorOption{}
		if opts.BatchTimeout > 0 {
			bspOpts = append(bspOpts, sdktrace.WithBatchTimeout(opts.BatchTimeout))
		}
		if opts.MaxExportBatch > 0 {
			bspOpts = append(bspOpts, sdktrace.WithMaxExportBatchSize(opts.MaxExportBatch))
		}
		sp = sdktrace.NewBatchSpanProcessor(exporter, bspOpts...)
	}

	sampler := buildSampler(opts)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sp),
		sdktrace.WithSampler(sampler),
	)

	t.provider = tp
	t.otelTracer = tp.Tracer("fit.go/" + opts.ServiceName)
	otel.SetTracerProvider(tp)

	// Install the W3C trace-context propagator globally so the OTel
	// instrumentation libraries (otelgin, otelhttp, redisotel, otelpgx) extract
	// inbound traceparent and inject it on outbound calls. Only set here, on the
	// enabled path — when tracing is off the global stays the no-op propagator,
	// so those libraries are inert (zero overhead).
	// W3C TraceContext (traceparent/tracestate) + Baggage. traceclue/OTel install both
	// by default; fit-go previously registered TraceContext only, so W3C `baggage`
	// propagated by an upstream Node/Python service was silently dropped at the Go
	// boundary. Baggage.Inject writes nothing when the context carries no baggage, so
	// this adds no header to existing traffic.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return nil
}

// Init initializes the global tracer with default options.
func Init() error {
	_, err := InitWithOptions(DefaultOptions())
	return err
}

// InitWithOptions initializes the global tracer with custom options.
func InitWithOptions(opts Options) (*Tracer, error) {
	var initErr error
	globalTracerOnce.Do(func() {
		t, err := New(context.Background(), opts)
		if err != nil {
			initErr = err
		}
		globalTracer.Store(t)
	})
	return globalTracer.Load(), initErr
}

// Global returns the global tracer instance.
func Global() *Tracer {
	if globalTracer.Load() == nil {
		Init()
	}
	return globalTracer.Load()
}

// SetGlobal replaces the global tracer and returns a function that restores the
// previous one. It bypasses the sync.Once init, so it is the way to install a
// specific tracer (e.g. an enabled tracer built with New) regardless of whether
// Init has already run — primarily for tests and advanced wiring. Restore with
// the returned func (typically `defer SetGlobal(tr)()`).
func SetGlobal(t *Tracer) (restore func()) {
	prev := globalTracer.Swap(t)
	return func() { globalTracer.Store(prev) }
}

// Shutdown gracefully shuts down the global tracer, flushing any remaining spans.
func Shutdown(ctx context.Context) error {
	if g := globalTracer.Load(); g != nil {
		return g.shutdown(ctx)
	}
	return nil
}

// Shutdown gracefully shuts down this tracer, flushing any remaining spans.
func (t *Tracer) Shutdown(ctx context.Context) error {
	return t.shutdown(ctx)
}

func (t *Tracer) shutdown(ctx context.Context) error {
	if !t.enabled {
		return nil
	}
	if t.provider != nil {
		return t.provider.Shutdown(ctx)
	}
	return nil
}

// IsEnabled returns whether tracing is active.
func (t *Tracer) IsEnabled() bool {
	return t.enabled
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

	// Bridge the IDs to the logging package so logger.WithContext(ctx) auto-stamps
	// trace_id/span_id on every log line within this span — the Go equivalent of
	// Node's OTel log-format enrichment, with no per-call wiring.
	ctx = logging.ContextWithTrace(ctx, span.traceID, span.spanID)

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
	if span, ok := ctx.Value(currentSpanKey).(*Span); ok {
		return span
	}
	return adoptOtelSpan(ctx)
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

// TraceIDFromContext extracts the trace ID from context. It prefers the value
// seeded by this package and falls back to the NATIVE OTel span context, so a
// request carrying only an otelgin/otelgrpc span still reports its trace id (used
// for span parenting in StartSpan and for log correlation).
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(traceIDKey).(string); ok && v != "" {
		return v
	}
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// SpanIDFromContext extracts the span ID from context, with the same native-OTel
// fallback as TraceIDFromContext.
func SpanIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(spanIDKey).(string); ok && v != "" {
		return v
	}
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.HasSpanID() {
		return sc.SpanID().String()
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

		err := fn(ctx)
		if err != nil {
			span.SetStatus(StatusError, err.Error())
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

		result, err := fn(ctx)
		if err != nil {
			span.SetStatus(StatusError, err.Error())
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
