// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/logging"
)

// These tests pin upstream main (df96a28) behavior on the released tracing
// entry points: New, Init, InitWithOptions, Global, Shutdown, StartSpan and
// Decorators.

func isolateOTelGlobals(t *testing.T) {
	t.Helper()
	provider := snapshotTracerProvider(otel.GetTracerProvider())
	propagator := snapshotPropagator(otel.GetTextMapPropagator())
	resetGlobalTracer()
	t.Cleanup(func() {
		resetGlobalTracer()
		otel.SetTracerProvider(provider)
		otel.SetTextMapPropagator(propagator)
	})
}

func TestUpstreamParity_FailedGlobalInitIsCached(t *testing.T) {
	isolateOTelGlobals(t)
	var attempts atomic.Int32
	original := newGlobalTracer
	newGlobalTracer = func(context.Context, AdvancedOptions, bool) (*Tracer, error) {
		attempts.Add(1)
		return nil, errors.New("sdk init failed")
	}
	t.Cleanup(func() { newGlobalTracer = original })

	enabled := true
	tracer, err := InitWithOptions(Options{ServiceName: "failing", Enabled: &enabled})
	if err == nil || tracer != nil {
		t.Fatalf("first InitWithOptions = (%v, %v), want (nil, error)", tracer, err)
	}
	for i := 0; i < 50; i++ {
		if got := Global(); got != nil {
			t.Fatalf("Global after failed init = %p, want nil (upstream sync.Once result)", got)
		}
	}
	// Upstream sync.Once: subsequent InitWithOptions/Init are no-ops returning (nil, nil).
	if again, err := InitWithOptions(Options{ServiceName: "failing", Enabled: &enabled}); again != nil || err != nil {
		t.Fatalf("repeat InitWithOptions = (%v, %v), want (nil, nil)", again, err)
	}
	if err := Init(); err != nil {
		t.Fatalf("repeat Init = %v, want nil", err)
	}
	// Decorators on the hot path must not retry initialization either.
	_ = Decorators.Trace("hot", func(context.Context) error { return nil })(context.Background())
	if got := attempts.Load(); got != 1 {
		t.Fatalf("SDK init attempts = %d, want 1", got)
	}
	if _, err := GlobalWithError(); err == nil {
		t.Fatal("GlobalWithError should still report the cached failure")
	}
}

func TestUpstreamParity_FailedNewKeepsEnabledWithoutOTelTracer(t *testing.T) {
	isolateOTelGlobals(t)
	original := buildUpstreamSpanExporters
	buildUpstreamSpanExporters = func(context.Context, AdvancedOptions) ([]sdktrace.SpanExporter, error) {
		return nil, errors.New("exporter failed")
	}
	t.Cleanup(func() { buildUpstreamSpanExporters = original })

	enabled := true
	failed, err := New(context.Background(), Options{ServiceName: "failed", Enabled: &enabled})
	if err == nil || failed == nil {
		t.Fatalf("New = (%v, %v), want failed tracer and error", failed, err)
	}
	// Upstream main kept enabled=true with no OTel tracer after a failed init.
	if !failed.IsEnabled() || failed.otelTracer != nil || failed.provider != nil {
		t.Fatalf("failed tracer enabled=%v otelTracer=%v provider=%v", failed.IsEnabled(), failed.otelTracer, failed.provider)
	}
	ctx, span := failed.StartSpan(context.Background(), "op", SpanKindInternal)
	span.SetStatus(StatusOK, "")
	span.End()
	if span.TraceID() == "" || TraceIDFromContext(ctx) == "" {
		t.Fatal("in-memory span lost its IDs")
	}
	if err := failed.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if err := failed.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// The SDK constructor keeps reporting a failed tracer as disabled.
	buildAdvancedSpanExportersOriginal := buildAdvancedSpanExporters
	buildAdvancedSpanExporters = func(context.Context, AdvancedOptions) ([]sdktrace.SpanExporter, error) {
		return nil, errors.New("exporter failed")
	}
	t.Cleanup(func() { buildAdvancedSpanExporters = buildAdvancedSpanExportersOriginal })
	advanced, err := NewSDK(context.Background(), SDKOptions{ServiceName: "failed-sdk", Enabled: &enabled})
	if err == nil || advanced == nil || advanced.IsEnabled() {
		t.Fatalf("NewSDK failure = (%v, %v), want disabled tracer and error", advanced, err)
	}
}

func TestUpstreamParity_ReleasedPathBuildsNoAdvancedExporterOrResource(t *testing.T) {
	isolateOTelGlobals(t)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp,console")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "malformed-without-value")

	var advancedCalls atomic.Int32
	original := buildAdvancedSpanExporters
	buildAdvancedSpanExporters = func(ctx context.Context, opts AdvancedOptions) ([]sdktrace.SpanExporter, error) {
		advancedCalls.Add(1)
		return original(ctx, opts)
	}
	t.Cleanup(func() { buildAdvancedSpanExporters = original })

	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	enabled := true
	exporter := &lifecycleExporter{}
	tracer, err := New(context.Background(), Options{
		ServiceName:            "released",
		Enabled:                &enabled,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	if got := advancedCalls.Load(); got != 0 {
		t.Fatalf("advanced exporter builder called %d times on released path", got)
	}
	if strings.Contains(logs.String(), "resource detection") {
		t.Fatalf("released path emitted advanced resource warning: %s", logs.String())
	}
}

func TestUpstreamParity_ShutdownKeepsPropagatorInstalled(t *testing.T) {
	isolateOTelGlobals(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	enabled := true
	exporter := &lifecycleExporter{}
	tracer, err := InitWithOptions(Options{
		ServiceName:            "released-shutdown",
		Enabled:                &enabled,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	if err := Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if exporter.shutdowns.Load() != 1 {
		t.Fatalf("exporter shutdowns = %d, want 1", exporter.shutdowns.Load())
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(trace.ContextWithSpanContext(context.Background(), sc), carrier)
	if carrier.Get("traceparent") == "" {
		t.Fatal("propagation stopped after Shutdown; upstream kept TraceContext+Baggage installed")
	}
	// Upstream left its (shut-down) provider installed as the OTel global.
	if otel.GetTracerProvider() != trace.TracerProvider(tracer.provider) {
		t.Fatal("Shutdown replaced the global tracer provider; upstream left it installed")
	}

	// Upstream sync.Once: Global and a repeat Init return the shut-down tracer
	// without starting another exporter.
	if got := Global(); got != tracer {
		t.Fatalf("Global after Shutdown = %p, want retired %p", got, tracer)
	}
	again, err := InitWithOptions(Options{ServiceName: "again", Enabled: &enabled, SpanExporter: &lifecycleExporter{}})
	if err != nil || again != tracer {
		t.Fatalf("InitWithOptions after Shutdown = (%p, %v), want (%p, nil)", again, err, tracer)
	}
}

func TestUpstreamParity_DisabledStartSpanDoesNotStampLogs(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.New(logging.Options{Env: "production", Level: "info", Output: &buf})
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}
	disabled := false
	tracer, err := New(context.Background(), Options{ServiceName: "disabled", Enabled: &disabled})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, span := tracer.StartSpan(context.Background(), "op", SpanKindInternal)
	defer span.End()
	logger.WithContext(ctx).Info("hello")

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("decode log %q: %v", buf.String(), err)
	}
	if _, ok := entry["trace_id"]; ok {
		t.Fatalf("disabled StartSpan stamped trace_id onto logs: %v", entry)
	}
	if _, ok := entry["span_id"]; ok {
		t.Fatalf("disabled StartSpan stamped span_id onto logs: %v", entry)
	}
}

func TestUpstreamParity_DecoratorsWithoutOTelTracerSkipGoroutineStore(t *testing.T) {
	isolateOTelGlobals(t)
	cases := map[string]*Tracer{
		"disabled":         {serviceName: "disabled", upstream: true},
		"failed-init-noop": {serviceName: "failed", enabled: true, upstream: true},
	}
	for name, tracer := range cases {
		t.Run(name, func(t *testing.T) {
			restore := SetGlobal(tracer)
			defer restore()
			if ContextFromGoroutine() != nil {
				t.Fatal("precondition: goroutine store not empty")
			}
			_ = Decorators.Trace("op", func(context.Context) error {
				if ContextFromGoroutine() != nil {
					t.Error("Decorators.Trace stored goroutine-local context without an OTel tracer")
				}
				return nil
			})(context.Background())
			_, _ = TraceWithResult("op", func(context.Context) (int, error) {
				if ContextFromGoroutine() != nil {
					t.Error("TraceWithResult stored goroutine-local context without an OTel tracer")
				}
				return 0, nil
			})(context.Background())
		})
	}
}

func TestInitSDKShutdownKeepsPropagatorForGracefulDrain(t *testing.T) {
	isolateOTelGlobals(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	enabled := true
	exporter := &lifecycleExporter{}
	tracer, err := InitSDK(SDKOptions{
		ServiceName:            "sdk-drain",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil || tracer == nil || !tracer.IsEnabled() {
		t.Fatalf("InitSDK = (%v, %v)", tracer, err)
	}
	if err := Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0xa, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		SpanID:     trace.SpanID{0xb, 1, 2, 3, 4, 5, 6, 7},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(trace.ContextWithRemoteSpanContext(context.Background(), sc), carrier)
	want := "00-0a0102030405060708090a0b0c0d0e0f-0b01020304050607-01"
	if got := carrier.Get("traceparent"); got != want {
		t.Fatalf("traceparent after InitSDK Shutdown = %q, want %q", got, want)
	}
	// The SDK path still swaps the shut-down provider back out.
	if otel.GetTracerProvider() == trace.TracerProvider(tracer.provider) {
		t.Fatal("InitSDK Shutdown left the shut-down provider installed")
	}
	// Global after Shutdown returns the retired tracer; no new exporter.
	if got := Global(); got != tracer {
		t.Fatalf("Global after Shutdown = %p, want retired %p", got, tracer)
	}
	if exporter.shutdowns.Load() != 1 {
		t.Fatalf("exporter shutdowns = %d, want 1", exporter.shutdowns.Load())
	}
}
