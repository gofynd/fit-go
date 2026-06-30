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
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
	Endpoint       string            // OTLP endpoint
	SampleRate     float64           // Sampling rate (0.0-1.0)
	BatchTimeout   time.Duration     // Span batch export timeout
	MaxExportBatch int               // Maximum spans per export batch
	Attributes     map[string]string // Additional resource attributes
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
		SampleRate:     1.0,
		BatchTimeout:   5 * time.Second,
		MaxExportBatch: 512,
	}
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
func (s *Span) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	globalTracer     *Tracer
	globalTracerOnce sync.Once
)

// New creates a new tracer. When tracing is enabled, this initializes the
// real OpenTelemetry SDK with an OTLP HTTP exporter.
func New(ctx context.Context, opts Options) (*Tracer, error) {
	t := &Tracer{
		serviceName: opts.ServiceName,
		env:         opts.Env,
		enabled:     isTracingEnabled(),
		options:     opts,
	}

	if t.enabled {
		if err := t.initOTel(ctx, opts); err != nil {
			return t, fmt.Errorf("fit/tracing: failed to initialize OTel: %w", err)
		}
	}

	return t, nil
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

	res, err := resource.New(ctx,
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
		exporterOpts := []otlptracehttp.Option{}
		if opts.Endpoint != "" {
			exporterOpts = append(exporterOpts, otlptracehttp.WithEndpoint(opts.Endpoint))
		}
		exp, err := otlptracehttp.New(ctx, exporterOpts...)
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

	// Configure sampler.
	var sampler sdktrace.Sampler
	if opts.SampleRate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if opts.SampleRate <= 0.0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(opts.SampleRate)
	}

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
	otel.SetTextMapPropagator(propagation.TraceContext{})

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
		var err error
		globalTracer, err = New(context.Background(), opts)
		if err != nil {
			initErr = err
		}
	})
	return globalTracer, initErr
}

// Global returns the global tracer instance.
func Global() *Tracer {
	if globalTracer == nil {
		Init()
	}
	return globalTracer
}

// SetGlobal replaces the global tracer and returns a function that restores the
// previous one. It bypasses the sync.Once init, so it is the way to install a
// specific tracer (e.g. an enabled tracer built with New) regardless of whether
// Init has already run — primarily for tests and advanced wiring. Restore with
// the returned func (typically `defer SetGlobal(tr)()`).
func SetGlobal(t *Tracer) (restore func()) {
	prev := globalTracer
	globalTracer = t
	return func() { globalTracer = prev }
}

// Shutdown gracefully shuts down the global tracer, flushing any remaining spans.
func Shutdown(ctx context.Context) error {
	if globalTracer != nil {
		return globalTracer.shutdown(ctx)
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
func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(currentSpanKey).(*Span); ok {
		return span
	}
	return nil
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

// TraceIDFromContext extracts the trace ID from context.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

// SpanIDFromContext extracts the span ID from context.
func SpanIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(spanIDKey).(string); ok {
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
