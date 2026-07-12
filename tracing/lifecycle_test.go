// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type lifecycleExporter struct {
	exports     atomic.Int32
	shutdowns   atomic.Int32
	shutdownErr error
}

type lifecyclePropagator struct {
	field string
}

func (p *lifecyclePropagator) Inject(context.Context, propagation.TextMapCarrier) {}

func (p *lifecyclePropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}

func (p *lifecyclePropagator) Fields() []string { return []string{p.field} }

func (e *lifecycleExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	e.exports.Add(1)
	return nil
}

func (e *lifecycleExporter) Shutdown(context.Context) error {
	e.shutdowns.Add(1)
	return e.shutdownErr
}

func TestTracerShutdownFlushesOnceAndCachesError(t *testing.T) {
	previousFields := otel.GetTextMapPropagator().Fields()
	enabled := true
	wantErr := errors.New("exporter shutdown failed")
	exporter := &lifecycleExporter{shutdownErr: wantErr}
	tracer, err := New(context.Background(), Options{
		ServiceName:            "lifecycle",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, span := tracer.StartSpan(context.Background(), "flush", SpanKindInternal)
	span.End()

	if err := tracer.Shutdown(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("first Shutdown error = %v, want %v", err, wantErr)
	}
	if err := tracer.Shutdown(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("second Shutdown error = %v, want cached %v", err, wantErr)
	}
	if got := exporter.exports.Load(); got != 1 {
		t.Fatalf("exports = %d, want 1", got)
	}
	if got := exporter.shutdowns.Load(); got != 1 {
		t.Fatalf("exporter shutdowns = %d, want exactly 1", got)
	}
	if otel.GetTracerProvider() != tracer.previousTP {
		t.Fatal("Shutdown did not restore the captured global tracer provider snapshot")
	}
	if got := otel.GetTextMapPropagator().Fields(); !equalStringSets(got, previousFields) {
		t.Fatalf("Shutdown propagator fields = %v, want restored %v", got, previousFields)
	}
}

func equalStringSets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	counts := make(map[string]int, len(left))
	for _, value := range left {
		counts[value]++
	}
	for _, value := range right {
		counts[value]--
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func TestGlobalInitFailureIsVisibleAndShutdownAllowsReinit(t *testing.T) {
	resetGlobalTracer()
	t.Cleanup(func() {
		_ = Shutdown(context.Background())
		resetGlobalTracer()
	})

	enabled := true
	failed, err := InitWithOptions(Options{
		ServiceName: "invalid",
		Enabled:     &enabled,
		Endpoint:    "://invalid-endpoint",
		Protocol:    "http/protobuf",
	})
	if err == nil || failed == nil {
		t.Fatalf("InitWithOptions = (%v, %v), want non-nil fallback tracer and error", failed, err)
	}
	if failed.IsEnabled() {
		t.Fatal("failed tracer must be inert")
	}
	if !errors.Is(InitError(), err) && InitError().Error() != err.Error() {
		t.Fatalf("InitError = %v, want %v", InitError(), err)
	}
	got, gotErr := GlobalWithError()
	if got != failed || gotErr == nil || gotErr.Error() != err.Error() {
		t.Fatalf("GlobalWithError = (%p, %v), want cached (%p, %v)", got, gotErr, failed, err)
	}

	if shutdownErr := Shutdown(context.Background()); shutdownErr != nil {
		t.Fatalf("Shutdown failed fallback: %v", shutdownErr)
	}
	if globalTracer.Load() != nil || InitError() != nil {
		t.Fatalf("global state was not reset: tracer=%v err=%v", globalTracer.Load(), InitError())
	}

	exporter := &lifecycleExporter{}
	valid, err := InitWithOptions(Options{
		ServiceName:            "valid",
		Enabled:                &enabled,
		Propagators:            "tracecontext,baggage",
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil || valid == nil || !valid.IsEnabled() {
		t.Fatalf("reinitialize after reset = (%v, %v)", valid, err)
	}
	if valid == failed {
		t.Fatal("reinitialization reused the failed tracer")
	}
	if err := Shutdown(context.Background()); err != nil {
		t.Fatalf("valid Shutdown: %v", err)
	}
	if exporter.shutdowns.Load() != 1 {
		t.Fatalf("valid exporter shutdowns = %d, want 1", exporter.shutdowns.Load())
	}
}

func TestShutdownRestoresPreexistingCustomGlobals(t *testing.T) {
	baselineProvider := snapshotTracerProvider(otel.GetTracerProvider())
	baselinePropagator := snapshotPropagator(otel.GetTextMapPropagator())
	t.Cleanup(func() {
		otel.SetTracerProvider(baselineProvider)
		otel.SetTextMapPropagator(baselinePropagator)
	})

	customProvider := sdktrace.NewTracerProvider()
	defer customProvider.Shutdown(context.Background())
	customPropagator := propagation.TraceContext{}
	otel.SetTracerProvider(customProvider)
	otel.SetTextMapPropagator(customPropagator)

	enabled := true
	tracer, err := New(context.Background(), Options{
		ServiceName:            "restore-custom",
		Enabled:                &enabled,
		SpanExporter:           &lifecycleExporter{},
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := otel.GetTracerProvider(); got != customProvider {
		t.Fatalf("restored provider = %T %p, want custom %T %p", got, got, customProvider, customProvider)
	}
	if got := otel.GetTextMapPropagator().Fields(); !equalStringSets(got, customPropagator.Fields()) {
		t.Fatalf("restored propagator fields = %v, want %v", got, customPropagator.Fields())
	}
}

func TestTracerShutdownRewiresOutOfOrderGlobalOwnership(t *testing.T) {
	baselineProvider := snapshotTracerProvider(otel.GetTracerProvider())
	baselinePropagator := snapshotPropagator(otel.GetTextMapPropagator())
	t.Cleanup(func() {
		otel.SetTracerProvider(baselineProvider)
		otel.SetTextMapPropagator(baselinePropagator)
	})
	otel.SetTracerProvider(baselineProvider)
	otel.SetTextMapPropagator(baselinePropagator)

	enabled := true
	first, err := New(context.Background(), Options{
		ServiceName:            "owner-first",
		Enabled:                &enabled,
		SpanExporter:           &lifecycleExporter{},
		UseSimpleSpanProcessor: true,
		Propagators:            "tracecontext",
	})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	second, err := New(context.Background(), Options{
		ServiceName:            "owner-second",
		Enabled:                &enabled,
		SpanExporter:           &lifecycleExporter{},
		UseSimpleSpanProcessor: true,
		Propagators:            "baggage",
	})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	if otel.GetTracerProvider() != second.provider {
		t.Fatal("second tracer did not own the global provider")
	}

	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if otel.GetTracerProvider() != second.provider {
		t.Fatal("out-of-order first shutdown clobbered the second provider")
	}
	if second.previousTP != baselineProvider {
		t.Fatal("second owner was not rewired around the shut-down first owner")
	}

	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	if otel.GetTracerProvider() != baselineProvider {
		t.Fatal("last owner restored a shut-down predecessor instead of the baseline")
	}
	if got := otel.GetTextMapPropagator().Fields(); !equalStringSets(got, baselinePropagator.Fields()) {
		t.Fatalf("last owner propagator fields = %v, want baseline %v", got, baselinePropagator.Fields())
	}
}

func TestSetGlobalAndNewRestoreOutOfOrder(t *testing.T) {
	resetGlobalTracer()
	baselineProvider := snapshotTracerProvider(otel.GetTracerProvider())
	baselinePropagator := snapshotPropagator(otel.GetTextMapPropagator())
	t.Cleanup(func() {
		resetGlobalTracer()
		otel.SetTracerProvider(baselineProvider)
		otel.SetTextMapPropagator(baselinePropagator)
	})

	enabled := true
	first, err := New(context.Background(), Options{
		ServiceName:            "set-global-first",
		Enabled:                &enabled,
		SpanExporter:           &lifecycleExporter{},
		UseSimpleSpanProcessor: true,
		Propagators:            "tracecontext",
	})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	restoreFirst := SetGlobal(first)
	second, err := New(context.Background(), Options{
		ServiceName:            "set-global-second",
		Enabled:                &enabled,
		SpanExporter:           &lifecycleExporter{},
		UseSimpleSpanProcessor: true,
		Propagators:            "baggage",
	})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	restoreSecond := SetGlobal(second)

	restoreFirst()
	if Global() != second || otel.GetTracerProvider() != second.provider {
		t.Fatal("restoring the older owner clobbered the newer tracer")
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if Global() != second || otel.GetTracerProvider() != second.provider {
		t.Fatal("out-of-order first shutdown clobbered the newer tracer")
	}

	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	restoreSecond() // idempotent after shutdown
	if globalTracer.Load() != nil {
		t.Fatalf("global tracer = %p, want nil baseline", globalTracer.Load())
	}
	if otel.GetTracerProvider() != baselineProvider {
		t.Fatal("final shutdown did not restore the baseline provider")
	}
	if got := otel.GetTextMapPropagator().Fields(); !equalStringSets(got, baselinePropagator.Fields()) {
		t.Fatalf("final propagator fields = %v, want %v", got, baselinePropagator.Fields())
	}
}

func TestTracerRestorationPreservesIndependentGlobalReplacement(t *testing.T) {
	t.Run("provider", func(t *testing.T) {
		baselineProvider := snapshotTracerProvider(otel.GetTracerProvider())
		baselinePropagator := snapshotPropagator(otel.GetTextMapPropagator())
		t.Cleanup(func() {
			otel.SetTracerProvider(baselineProvider)
			otel.SetTextMapPropagator(baselinePropagator)
		})

		enabled := true
		tracer, err := New(context.Background(), Options{
			ServiceName:            "external-provider",
			Enabled:                &enabled,
			SpanExporter:           &lifecycleExporter{},
			UseSimpleSpanProcessor: true,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		external := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = external.Shutdown(context.Background()) })
		otel.SetTracerProvider(external)

		if err := tracer.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if otel.GetTracerProvider() != external {
			t.Fatal("shutdown clobbered an independently replaced provider")
		}
		if got := otel.GetTextMapPropagator().Fields(); !equalStringSets(got, baselinePropagator.Fields()) {
			t.Fatalf("owned propagator was not restored: %v", got)
		}
	})

	t.Run("propagator", func(t *testing.T) {
		baselineProvider := snapshotTracerProvider(otel.GetTracerProvider())
		baselinePropagator := snapshotPropagator(otel.GetTextMapPropagator())
		t.Cleanup(func() {
			otel.SetTracerProvider(baselineProvider)
			otel.SetTextMapPropagator(baselinePropagator)
		})

		enabled := true
		tracer, err := New(context.Background(), Options{
			ServiceName:            "external-propagator",
			Enabled:                &enabled,
			SpanExporter:           &lifecycleExporter{},
			UseSimpleSpanProcessor: true,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		external := &lifecyclePropagator{field: "x-external-trace"}
		otel.SetTextMapPropagator(external)

		if err := tracer.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
		if otel.GetTracerProvider() != baselineProvider {
			t.Fatal("owned provider was not restored")
		}
		if otel.GetTextMapPropagator() != external {
			t.Fatal("shutdown clobbered an independently replaced propagator")
		}
	})
}

func TestConcurrentGlobalOwnerTeardownRestoresBaseline(t *testing.T) {
	resetGlobalTracer()
	baselineProvider := snapshotTracerProvider(otel.GetTracerProvider())
	baselinePropagator := snapshotPropagator(otel.GetTextMapPropagator())
	t.Cleanup(func() {
		resetGlobalTracer()
		otel.SetTracerProvider(baselineProvider)
		otel.SetTextMapPropagator(baselinePropagator)
	})

	const ownerCount = 8
	enabled := true
	tracers := make([]*Tracer, 0, ownerCount)
	restores := make([]func(), 0, ownerCount)
	for index := 0; index < ownerCount; index++ {
		tracer, err := New(context.Background(), Options{
			ServiceName:            "concurrent-owner",
			Enabled:                &enabled,
			SpanExporter:           &lifecycleExporter{},
			UseSimpleSpanProcessor: true,
		})
		if err != nil {
			t.Fatalf("New owner %d: %v", index, err)
		}
		tracers = append(tracers, tracer)
		restores = append(restores, SetGlobal(tracer))
	}

	var wg sync.WaitGroup
	for index, tracer := range tracers {
		wg.Add(2)
		go func(restore func()) {
			defer wg.Done()
			restore()
		}(restores[index])
		go func(tracer *Tracer) {
			defer wg.Done()
			_ = tracer.Shutdown(context.Background())
		}(tracer)
	}
	wg.Wait()

	if globalTracer.Load() != nil {
		t.Fatalf("global tracer = %p, want nil", globalTracer.Load())
	}
	if otel.GetTracerProvider() != baselineProvider {
		t.Fatal("concurrent teardown did not restore baseline provider")
	}
	if got := otel.GetTextMapPropagator().Fields(); !equalStringSets(got, baselinePropagator.Fields()) {
		t.Fatalf("concurrent teardown propagator fields = %v, want %v", got, baselinePropagator.Fields())
	}
	for index, tracer := range tracers {
		if tracer.IsEnabled() {
			t.Errorf("tracer %d remained enabled after shutdown", index)
		}
	}
}
