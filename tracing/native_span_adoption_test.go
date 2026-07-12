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

package tracing_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// nativeSpanContext builds a context carrying ONLY a native OTel span — exactly what
// otelgin/otelgrpc/redisotel install. They never touch fit-go's private span key, so
// this reproduces the state of every HTTP handler context.
func nativeSpanContext(t *testing.T) (context.Context, trace.Span) {
	t.Helper()
	tracingtest.EnabledGlobal(t)
	// Use the raw OTel tracer, NOT tracing.StartSpan — StartSpan seeds the private
	// key, which is precisely what we must not rely on here.
	ctx, span := otel.Tracer("test-otelgin").Start(context.Background(), "GET /route")
	t.Cleanup(func() { span.End() })
	return ctx, span
}

// TestSpanFromContext_AdoptsNativeOTelSpan is the regression test for the bug that
// severed traces at every HTTP→Kafka boundary.
//
// server.OTelMiddleware installs otelgin, which puts a span in the NATIVE OTel
// context only. SpanFromContext previously read just fit-go's private key, so inside
// any HTTP handler it returned nil — making SetSpanAttributes a silent no-op and
// making kafka.InjectTraceHeaders inject NO traceparent (the consumer then started a
// brand-new trace instead of continuing the request's).
func TestSpanFromContext_AdoptsNativeOTelSpan(t *testing.T) {
	ctx, native := nativeSpanContext(t)

	got := tracing.SpanFromContext(ctx)
	if got == nil {
		t.Fatal("SpanFromContext returned nil for a context carrying a native OTel span " +
			"(otelgin/otelgrpc): span annotations no-op and Kafka traceparent injection is skipped")
	}

	sc := native.SpanContext()
	if got.TraceID() != sc.TraceID().String() {
		t.Errorf("adopted TraceID = %q, want the native span's %q", got.TraceID(), sc.TraceID())
	}
	if got.SpanID() != sc.SpanID().String() {
		t.Errorf("adopted SpanID = %q, want the native span's %q", got.SpanID(), sc.SpanID())
	}
	if !got.IsSampled() {
		t.Error("adopted span reports not sampled, but the native span is sampled")
	}
}

// TestTraceIDFromContext_NativeFallback: the trace/span id helpers must also see a
// native span (they feed span parenting in StartSpan and log correlation).
func TestTraceIDFromContext_NativeFallback(t *testing.T) {
	ctx, native := nativeSpanContext(t)
	sc := native.SpanContext()

	if got := tracing.TraceIDFromContext(ctx); got != sc.TraceID().String() {
		t.Errorf("TraceIDFromContext = %q, want %q (native fallback)", got, sc.TraceID())
	}
	if got := tracing.SpanIDFromContext(ctx); got != sc.SpanID().String() {
		t.Errorf("SpanIDFromContext = %q, want %q (native fallback)", got, sc.SpanID())
	}
}

// TestSetSpanAttributes_OnNativeSpan proves the annotation helpers now land on the
// otelgin span instead of silently doing nothing. This is the fit.js/traceclue
// setSpanAttributes equivalent, which operates on the native active span.
func TestSetSpanAttributes_OnNativeSpan(t *testing.T) {
	ctx, _ := nativeSpanContext(t)

	// Must not panic and must find a span to annotate.
	tracing.SetSpanAttributes(ctx, tracing.SpanAttributes{"provider": "twilio"})
	tracing.SetSpanStatus(ctx, tracing.StatusError, "boom")

	if tracing.SpanFromContext(ctx) == nil {
		t.Fatal("no span adopted from context; annotations were dropped")
	}
}

// TestAdoptedSpan_EndIsNoop guards the lifecycle hazard: an adopted span WRAPS a span
// owned by the instrumentation that created it (otelgin ends its server span when the
// request completes). If End() forwarded to the native span, a helper could truncate
// the server span mid-request and corrupt its duration.
func TestAdoptedSpan_EndIsNoop(t *testing.T) {
	ctx, native := nativeSpanContext(t)

	adopted := tracing.SpanFromContext(ctx)
	if adopted == nil {
		t.Fatal("expected an adopted span")
	}
	adopted.End() // must NOT end the native span

	if !native.IsRecording() {
		t.Error("End() on an ADOPTED span ended the underlying native span; " +
			"otelgin owns that span's lifecycle and would record a truncated duration")
	}
}

// TestSpanFromContext_StillPrefersOwnSpan: a span created by StartSpan must still win
// over the native fallback (it carries fit-go's richer bookkeeping), and End() on it
// must still work.
func TestSpanFromContext_StillPrefersOwnSpan(t *testing.T) {
	tracer := tracingtest.EnabledGlobal(t)

	ctx, own := tracer.StartSpan(context.Background(), "own", tracing.SpanKindInternal)
	got := tracing.SpanFromContext(ctx)
	if got != own {
		t.Fatal("SpanFromContext must return the StartSpan-created span, not an adopted wrapper")
	}
	own.End() // a non-adopted span still ends normally
}

// TestSpanFromContext_NoSpan: a bare context still yields nil (no false positives).
func TestSpanFromContext_NoSpan(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	if got := tracing.SpanFromContext(context.Background()); got != nil {
		t.Errorf("SpanFromContext on a bare context = %v, want nil", got)
	}
	if got := tracing.SpanFromContext(nil); got != nil { //nolint:staticcheck // nil ctx must not panic
		t.Errorf("SpanFromContext(nil) = %v, want nil", got)
	}
}
