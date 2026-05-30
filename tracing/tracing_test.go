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

package tracing

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// resetGlobalTracer resets the global tracer for test isolation.
func resetGlobalTracer() {
	globalTracer = nil
	globalTracerOnce = sync.Once{}
}

// ---------------------------------------------------------------------------
// TestTracerInit_Disabled
// ---------------------------------------------------------------------------

func TestTracerInit_Disabled(t *testing.T) {
	os.Unsetenv("TRACING_ENABLED")
	defer os.Unsetenv("TRACING_ENABLED")

	tracer, err := New(context.Background(), Options{ServiceName: "test-disabled"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if tracer.IsEnabled() {
		t.Error("Tracer should be disabled when TRACING_ENABLED is not set")
	}
	if tracer.provider != nil {
		t.Error("TracerProvider should be nil when disabled")
	}
	if tracer.otelTracer != nil {
		t.Error("otelTracer should be nil when disabled")
	}

	// StartSpan should still work with in-memory spans.
	ctx, span := tracer.StartSpan(context.Background(), "test-span", SpanKindServer)
	if span == nil {
		t.Fatal("StartSpan should return a span even when disabled")
	}
	if span.otelSpan != nil {
		t.Error("otelSpan should be nil when disabled")
	}
	if TraceIDFromContext(ctx) == "" {
		t.Error("Context should have trace ID even when disabled")
	}
	span.End()

	// Shutdown should be a no-op.
	if err := tracer.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestTracerInit_Enabled
// ---------------------------------------------------------------------------

func TestTracerInit_Enabled(t *testing.T) {
	os.Setenv("TRACING_ENABLED", "true")
	defer os.Unsetenv("TRACING_ENABLED")

	exporter := tracetest.NewInMemoryExporter()

	tracer, err := New(context.Background(), Options{
		ServiceName:            "test-enabled",
		Env:                    "test",
		SampleRate:             1.0,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if !tracer.IsEnabled() {
		t.Error("Tracer should be enabled")
	}
	if tracer.provider == nil {
		t.Error("TracerProvider should not be nil")
	}
	if tracer.otelTracer == nil {
		t.Error("otelTracer should not be nil")
	}

	// Create a span and verify it goes through OTel.
	_, span := tracer.StartSpan(context.Background(), "otel-span", SpanKindServer)
	if span.otelSpan == nil {
		t.Error("otelSpan should not be nil when enabled")
	}
	span.End()

	// Check spans before shutdown (shutdown may reset exporter).
	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Error("Expected at least 1 exported span")
	} else if spans[0].Name != "otel-span" {
		t.Errorf("Span name = %q, want otel-span", spans[0].Name)
	}

	tracer.Shutdown(context.Background())
}

// ---------------------------------------------------------------------------
// TestStartSpan_ContextPropagation
// ---------------------------------------------------------------------------

func TestStartSpan_ContextPropagation(t *testing.T) {
	os.Setenv("TRACING_ENABLED", "true")
	defer os.Unsetenv("TRACING_ENABLED")

	exporter := tracetest.NewInMemoryExporter()
	tracer, _ := New(context.Background(), Options{
		ServiceName:            "test-ctx",
		SpanExporter:           exporter,
		SampleRate:             1.0,
		UseSimpleSpanProcessor: true,
	})

	// Start parent span.
	ctx, parent := tracer.StartSpan(context.Background(), "parent", SpanKindServer)
	parentTraceID := parent.TraceID()
	parentSpanID := parent.SpanID()

	if parentTraceID == "" {
		t.Error("Parent should have a trace ID")
	}
	if parentSpanID == "" {
		t.Error("Parent should have a span ID")
	}

	// Start child span - should inherit trace ID from OTel context.
	childCtx, child := tracer.StartSpan(ctx, "child", SpanKindInternal)
	childTraceID := child.TraceID()

	if childTraceID != parentTraceID {
		t.Errorf("Child trace ID = %q, want parent trace ID %q", childTraceID, parentTraceID)
	}
	if child.SpanID() == parentSpanID {
		t.Error("Child should have a different span ID than parent")
	}

	// Verify context propagation.
	if TraceIDFromContext(childCtx) != childTraceID {
		t.Error("Context should have child's trace ID")
	}
	if SpanIDFromContext(childCtx) != child.SpanID() {
		t.Error("Context should have child's span ID")
	}

	child.End()
	parent.End()
	tracer.Shutdown(context.Background())
}

// ---------------------------------------------------------------------------
// TestSpanAttributes
// ---------------------------------------------------------------------------

func TestSpanAttributes(t *testing.T) {
	os.Setenv("TRACING_ENABLED", "true")
	defer os.Unsetenv("TRACING_ENABLED")

	exporter := tracetest.NewInMemoryExporter()
	tracer, _ := New(context.Background(), Options{
		ServiceName:            "test-attrs",
		SpanExporter:           exporter,
		SampleRate:             1.0,
		UseSimpleSpanProcessor: true,
	})

	_, span := tracer.StartSpan(context.Background(), "attr-span", SpanKindServer)

	// Set various attribute types.
	span.SetAttribute("string_attr", "hello")
	span.SetAttribute("int_attr", 42)
	span.SetAttribute("bool_attr", true)
	span.SetAttribute("float_attr", 3.14)

	// Verify in-memory attributes.
	if span.attributes["string_attr"] != "hello" {
		t.Error("string attribute not set")
	}
	if span.attributes["int_attr"] != 42 {
		t.Error("int attribute not set")
	}
	if span.attributes["bool_attr"] != true {
		t.Error("bool attribute not set")
	}

	// SetAttributes bulk.
	span.SetAttributes(map[string]any{
		"bulk1": "val1",
		"bulk2": 123,
	})
	if len(span.attributes) != 6 {
		t.Errorf("attributes length = %d, want 6", len(span.attributes))
	}

	// SetStatus.
	span.SetStatus(StatusError, "something failed")
	if span.status != StatusError {
		t.Errorf("status = %v, want StatusError", span.status)
	}

	span.End()

	// Verify exported span has attributes (before shutdown).
	spans := exporter.GetSpans()
	tracer.Shutdown(context.Background())
	if len(spans) == 0 {
		t.Fatal("Expected exported spans")
	}
	exported := spans[0]
	// Check that at least some attributes were propagated.
	foundString := false
	for _, a := range exported.Attributes {
		if string(a.Key) == "string_attr" && a.Value.AsString() == "hello" {
			foundString = true
		}
	}
	if !foundString {
		t.Error("Exported span should have string_attr=hello")
	}
}

// ---------------------------------------------------------------------------
// TestW3CTraceContext
// ---------------------------------------------------------------------------

func TestW3CTraceContext(t *testing.T) {
	tests := []struct {
		name        string
		traceparent string
		wantTrace   string
		wantSpan    string
		wantSampled bool
	}{
		{
			name:        "valid sampled",
			traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			wantTrace:   "0af7651916cd43dd8448eb211c80319c",
			wantSpan:    "b7ad6b7169203331",
			wantSampled: true,
		},
		{
			name:        "valid not sampled",
			traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
			wantTrace:   "0af7651916cd43dd8448eb211c80319c",
			wantSpan:    "b7ad6b7169203331",
			wantSampled: false,
		},
		{
			name:        "invalid format",
			traceparent: "invalid",
			wantTrace:   "",
			wantSpan:    "",
			wantSampled: false,
		},
		{
			name:        "empty",
			traceparent: "",
			wantTrace:   "",
			wantSpan:    "",
			wantSampled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			traceID, spanID, sampled := ExtractTraceContext(tt.traceparent)
			if traceID != tt.wantTrace {
				t.Errorf("traceID = %q, want %q", traceID, tt.wantTrace)
			}
			if spanID != tt.wantSpan {
				t.Errorf("spanID = %q, want %q", spanID, tt.wantSpan)
			}
			if sampled != tt.wantSampled {
				t.Errorf("sampled = %v, want %v", sampled, tt.wantSampled)
			}
		})
	}

	// FormatTraceparent round-trip.
	tp := FormatTraceparent("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331", true)
	expected := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	if tp != expected {
		t.Errorf("FormatTraceparent = %q, want %q", tp, expected)
	}
}

// ---------------------------------------------------------------------------
// TestDecorators
// ---------------------------------------------------------------------------

func TestDecorators(t *testing.T) {
	resetGlobalTracer()
	os.Setenv("TRACING_ENABLED", "true")
	defer func() {
		os.Unsetenv("TRACING_ENABLED")
		resetGlobalTracer()
	}()

	exporter := tracetest.NewInMemoryExporter()
	tracer, _ := New(context.Background(), Options{
		ServiceName:            "test-decorators",
		SpanExporter:           exporter,
		SampleRate:             1.0,
		UseSimpleSpanProcessor: true,
	})
	// Set as global tracer.
	globalTracer = tracer

	t.Run("Trace decorator success", func(t *testing.T) {
		called := false
		fn := func(ctx context.Context) error {
			called = true
			// Verify span is in context.
			span := SpanFromContext(ctx)
			if span == nil {
				t.Error("Span should be in context inside traced function")
			}
			return nil
		}

		wrapped := Decorators.Trace("decorated-fn", fn)
		err := wrapped(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("Original function was not called")
		}
	})

	t.Run("Trace decorator error", func(t *testing.T) {
		expectedErr := fmt.Errorf("decorator error")
		fn := func(ctx context.Context) error {
			return expectedErr
		}

		wrapped := Decorators.Trace("error-fn", fn)
		err := wrapped(context.Background())
		if err != expectedErr {
			t.Errorf("error = %v, want %v", err, expectedErr)
		}
	})

	t.Run("TraceWithResult success", func(t *testing.T) {
		fn := func(ctx context.Context) (string, error) {
			return "result", nil
		}

		wrapped := TraceWithResult("result-fn", fn)
		result, err := wrapped(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != "result" {
			t.Errorf("result = %q, want result", result)
		}
	})

	t.Run("TraceWithResult error", func(t *testing.T) {
		expectedErr := fmt.Errorf("result error")
		fn := func(ctx context.Context) (int, error) {
			return 0, expectedErr
		}

		wrapped := TraceWithResult("error-result-fn", fn)
		result, err := wrapped(context.Background())
		if err != expectedErr {
			t.Errorf("error = %v, want %v", err, expectedErr)
		}
		if result != 0 {
			t.Errorf("result = %d, want 0", result)
		}
	})

	tracer.Shutdown(context.Background())
}

// ---------------------------------------------------------------------------
// TestShouldTrace
// ---------------------------------------------------------------------------

func TestShouldTrace(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/api/users", true},
		{"/", true},
		{"/api/orders", true},
		{"/_healthz", false},
		{"/_readyz", false},
		{"/_healthz/detailed", false},
		{"/_readyz/live", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := ShouldTrace(tt.path); got != tt.expected {
				t.Errorf("ShouldTrace(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestSpan_End_Idempotent
// ---------------------------------------------------------------------------

func TestSpan_End_Idempotent(t *testing.T) {
	span := &Span{name: "test", startTime: time.Now()}
	span.End()
	firstEnd := span.endTime
	time.Sleep(time.Millisecond)
	span.End()
	if span.endTime != firstEnd {
		t.Error("End() called twice should not change endTime")
	}
}

// ---------------------------------------------------------------------------
// TestSpanFromContext
// ---------------------------------------------------------------------------

func TestSpanFromContext(t *testing.T) {
	t.Run("with span", func(t *testing.T) {
		os.Unsetenv("TRACING_ENABLED")
		tracer, _ := New(context.Background(), Options{})
		ctx, expected := tracer.StartSpan(context.Background(), "test", SpanKindServer)
		got := SpanFromContext(ctx)
		if got != expected {
			t.Error("SpanFromContext did not return the correct span")
		}
	})

	t.Run("without span", func(t *testing.T) {
		got := SpanFromContext(context.Background())
		if got != nil {
			t.Error("SpanFromContext should return nil for context without span")
		}
	})
}

// ---------------------------------------------------------------------------
// TestFormatTraceContext
// ---------------------------------------------------------------------------

func TestFormatTraceContext(t *testing.T) {
	tests := []struct {
		name     string
		traceID  string
		spanID   string
		expected string
	}{
		{"both present", "trace-123", "span-456", "trace_id=trace-123 span_id=span-456"},
		{"both empty", "", "", ""},
		{"only trace", "trace-123", "", "trace_id=trace-123 span_id="},
		{"only span", "", "span-456", "trace_id= span_id=span-456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatTraceContext(tt.traceID, tt.spanID)
			if got != tt.expected {
				t.Errorf("FormatTraceContext(%q, %q) = %q, want %q", tt.traceID, tt.spanID, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestSetSpanAttributesWithStatus
// ---------------------------------------------------------------------------

func TestSetSpanAttributesWithStatus(t *testing.T) {
	os.Unsetenv("TRACING_ENABLED")
	tracer, _ := New(context.Background(), Options{})

	t.Run("sets both attributes and status", func(t *testing.T) {
		ctx, span := tracer.StartSpan(context.Background(), "test", SpanKindServer)
		SetSpanAttributesWithStatus(ctx, SpanAttributes{"key": "value", "num": 42}, StatusOK)

		if span.attributes["key"] != "value" {
			t.Error("attribute 'key' not set")
		}
		if span.status != StatusOK {
			t.Errorf("status = %v, want StatusOK", span.status)
		}
	})

	t.Run("skips empty attributes", func(t *testing.T) {
		ctx, span := tracer.StartSpan(context.Background(), "test", SpanKindServer)
		span.SetAttribute("existing", "value")
		SetSpanAttributesWithStatus(ctx, SpanAttributes{}, StatusOK)

		if len(span.attributes) != 1 {
			t.Errorf("attributes length = %d, want 1", len(span.attributes))
		}
		if span.status != StatusOK {
			t.Errorf("status = %v, want StatusOK", span.status)
		}
	})

	t.Run("ignores invalid status codes", func(t *testing.T) {
		ctx, span := tracer.StartSpan(context.Background(), "test", SpanKindServer)
		SetSpanAttributesWithStatus(ctx, SpanAttributes{"key": "value"}, SpanStatusCode(999))
		if span.status != StatusUnset {
			t.Errorf("status = %v, want StatusUnset", span.status)
		}
	})

	t.Run("no-op without span", func(t *testing.T) {
		// Should not panic.
		SetSpanAttributesWithStatus(context.Background(), SpanAttributes{"key": "value"}, StatusOK)
	})
}

// ---------------------------------------------------------------------------
// TestSamplerConfigurations
// ---------------------------------------------------------------------------

func TestSamplerConfigurations(t *testing.T) {
	os.Setenv("TRACING_ENABLED", "true")
	defer os.Unsetenv("TRACING_ENABLED")

	tests := []struct {
		name       string
		sampleRate float64
	}{
		{"always sample", 1.0},
		{"never sample", 0.0},
		{"ratio sample", 0.5},
		{"above 1.0", 2.0},
		{"negative", -1.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			tracer, err := New(context.Background(), Options{
				ServiceName:  "test-sampler",
				SpanExporter: exporter,
				SampleRate:   tt.sampleRate,
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			tracer.Shutdown(context.Background())
		})
	}
}

// ---------------------------------------------------------------------------
// TestConcurrentSpanAccess
// ---------------------------------------------------------------------------

func TestConcurrentSpanAccess(t *testing.T) {
	span := &Span{name: "concurrent-test"}

	done := make(chan bool)
	for i := range 100 {
		go func(idx int) {
			span.SetAttribute(fmt.Sprintf("key%d", idx), idx)
			span.SetStatus(StatusOK, "ok")
			done <- true
		}(i)
	}
	for range 100 {
		<-done
	}

	if len(span.attributes) != 100 {
		t.Errorf("attributes length = %d, want 100", len(span.attributes))
	}
}

// ---------------------------------------------------------------------------
// TestOTelSpanKindMapping
// ---------------------------------------------------------------------------

func TestOTelSpanKindMapping(t *testing.T) {
	os.Setenv("TRACING_ENABLED", "true")
	defer os.Unsetenv("TRACING_ENABLED")

	exporter := tracetest.NewInMemoryExporter()
	tracer, _ := New(context.Background(), Options{
		ServiceName:            "test-kinds",
		SpanExporter:           exporter,
		SampleRate:             1.0,
		UseSimpleSpanProcessor: true,
	})

	kinds := []SpanKind{SpanKindInternal, SpanKindServer, SpanKindClient, SpanKindProducer, SpanKindConsumer}
	expectedOTel := []oteltrace.SpanKind{
		oteltrace.SpanKindInternal,
		oteltrace.SpanKindServer,
		oteltrace.SpanKindClient,
		oteltrace.SpanKindProducer,
		oteltrace.SpanKindConsumer,
	}

	for i, kind := range kinds {
		_, span := tracer.StartSpan(context.Background(), fmt.Sprintf("kind-%d", i), kind)
		span.End()
		_ = expectedOTel[i] // reference to avoid unused
	}

	// Get spans before shutdown.
	spans := exporter.GetSpans()
	tracer.Shutdown(context.Background())
	if len(spans) != len(kinds) {
		t.Errorf("Expected %d spans, got %d", len(kinds), len(spans))
	}
	for i, s := range spans {
		if s.SpanKind != expectedOTel[i] {
			t.Errorf("Span[%d] kind = %v, want %v", i, s.SpanKind, expectedOTel[i])
		}
	}
}

// ---------------------------------------------------------------------------
// TestIgnoreIncomingRequestHook
// ---------------------------------------------------------------------------

func TestIgnoreIncomingRequestHook(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/api/users", false},
		{"/_healthz", true},
		{"/_readyz", true},
		{"/_healthz/live", true},
		{"/api/healthz", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := IgnoreIncomingRequestHook(tt.path); got != tt.expected {
				t.Errorf("IgnoreIncomingRequestHook(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestDefaultOptions
// ---------------------------------------------------------------------------

func TestDefaultOptions(t *testing.T) {
	os.Setenv("OTEL_SERVICE_NAME", "my-service")
	os.Setenv("GO_ENV", "production")
	defer func() {
		os.Unsetenv("OTEL_SERVICE_NAME")
		os.Unsetenv("GO_ENV")
	}()

	opts := DefaultOptions()
	if opts.ServiceName != "my-service" {
		t.Errorf("ServiceName = %q, want my-service", opts.ServiceName)
	}
	if opts.Env != "production" {
		t.Errorf("Env = %q, want production", opts.Env)
	}
}
