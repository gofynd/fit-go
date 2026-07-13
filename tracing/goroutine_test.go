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
	"os"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

type activeContextTestKey struct{}

func activeSpanContext(t *testing.T, traceHex, spanHex string) context.Context {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex(spanHex)
	if err != nil {
		t.Fatal(err)
	}
	traceState, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatal(err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		TraceState: traceState,
	})
	return trace.ContextWithSpanContext(context.Background(), spanContext)
}

func TestInjectContextIntoGoroutine(t *testing.T) {
	ctx := ContextWithTrace(context.Background(), "trace-abc", "span-def", true)

	cleanup := InjectContextIntoGoroutine(ctx)
	defer cleanup()

	got := ContextFromGoroutine()
	if got == nil {
		t.Fatal("ContextFromGoroutine returned nil after injection")
	}
	if TraceIDFromContext(got) != "trace-abc" {
		t.Errorf("trace ID = %q, want trace-abc", TraceIDFromContext(got))
	}
	if SpanIDFromContext(got) != "span-def" {
		t.Errorf("span ID = %q, want span-def", SpanIDFromContext(got))
	}
}

func TestInjectContextIntoGoroutine_Cleanup(t *testing.T) {
	ctx := ContextWithTrace(context.Background(), "trace-123", "span-456", true)

	cleanup := InjectContextIntoGoroutine(ctx)
	cleanup()

	got := ContextFromGoroutine()
	if got != nil {
		t.Error("ContextFromGoroutine should return nil after cleanup")
	}
}

func TestContextFromGoroutine_NoInjection(t *testing.T) {
	got := ContextFromGoroutine()
	if got != nil {
		t.Error("ContextFromGoroutine should return nil when nothing injected")
	}
}

func TestInjectContextIntoGoroutine_CrossGoroutine(t *testing.T) {
	ctx := ContextWithTrace(context.Background(), "trace-parent", "span-parent", true)

	cleanup := InjectContextIntoGoroutine(ctx)
	defer cleanup()

	done := make(chan context.Context, 1)
	go func() {
		done <- ContextFromGoroutine()
	}()
	got := <-done

	if got != nil {
		t.Error("ContextFromGoroutine in a different goroutine should return nil")
	}
}

func TestInjectContextIntoGoroutine_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := ContextWithTrace(context.Background(), "trace-"+string(rune('A'+idx%26)), "span", true)
			cleanup := InjectContextIntoGoroutine(ctx)
			defer cleanup()

			got := ContextFromGoroutine()
			if got == nil {
				t.Errorf("goroutine %d: context was nil", idx)
			}
		}(i)
	}
	wg.Wait()
}

func TestContextWithActiveGoroutineAdoptsTraceAndBaggage(t *testing.T) {
	active := activeSpanContext(t, "0123456789abcdef0123456789abcdef", "0123456789abcdef")
	activeMember, err := baggage.NewMember("active-key", "active-value")
	if err != nil {
		t.Fatal(err)
	}
	activeShared, err := baggage.NewMember("shared-key", "active-value")
	if err != nil {
		t.Fatal(err)
	}
	activeBaggage, err := baggage.New(activeMember, activeShared)
	if err != nil {
		t.Fatal(err)
	}
	active = baggage.ContextWithBaggage(active, activeBaggage)
	restore := InjectContextIntoGoroutine(active)
	defer restore()

	baseMember, err := baggage.NewMember("base-key", "base-value")
	if err != nil {
		t.Fatal(err)
	}
	baseShared, err := baggage.NewMember("shared-key", "base-value")
	if err != nil {
		t.Fatal(err)
	}
	baseBaggage, err := baggage.New(baseMember, baseShared)
	if err != nil {
		t.Fatal(err)
	}
	base := baggage.ContextWithBaggage(context.Background(), baseBaggage)
	base = context.WithValue(base, activeContextTestKey{}, "preserved")
	deadline := time.Now().Add(time.Minute)
	base, cancelDeadline := context.WithDeadline(base, deadline)
	defer cancelDeadline()
	base, cancel := context.WithCancel(base)
	got := ContextWithActiveGoroutine(base)

	if got.Value(activeContextTestKey{}) != "preserved" {
		t.Fatal("base context value was not preserved")
	}
	if TraceIDFromContext(got) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("trace ID = %q", TraceIDFromContext(got))
	}
	if SpanIDFromContext(got) != "0123456789abcdef" {
		t.Fatalf("span ID = %q", SpanIDFromContext(got))
	}
	gotSpanContext := trace.SpanContextFromContext(got)
	if !gotSpanContext.IsSampled() {
		t.Fatal("sampled decision was not retained")
	}
	if gotSpanContext.TraceState().String() != "vendor=value" {
		t.Fatalf("tracestate = %q", gotSpanContext.TraceState().String())
	}
	if baggage.FromContext(got).Member("active-key").Value() != "active-value" {
		t.Fatal("active baggage was not adopted")
	}
	if baggage.FromContext(got).Member("base-key").Value() != "base-value" {
		t.Fatal("base baggage was not preserved")
	}
	if baggage.FromContext(got).Member("shared-key").Value() != "base-value" {
		t.Fatal("explicit base baggage did not take precedence")
	}
	gotDeadline, ok := got.Deadline()
	if !ok || !gotDeadline.Equal(deadline) {
		t.Fatalf("deadline = %v, %t; want %v", gotDeadline, ok, deadline)
	}
	cancel()
	select {
	case <-got.Done():
	case <-time.After(time.Second):
		t.Fatal("base cancellation was not preserved")
	}
}

func TestContextWithActiveGoroutineExplicitTraceWins(t *testing.T) {
	active := activeSpanContext(t, "0123456789abcdef0123456789abcdef", "0123456789abcdef")
	restore := InjectContextIntoGoroutine(active)
	defer restore()

	explicit := activeSpanContext(t, "fedcba9876543210fedcba9876543210", "fedcba9876543210")
	got := ContextWithActiveGoroutine(explicit)
	if got != explicit {
		t.Fatal("explicit trace context should be returned unchanged")
	}
	if TraceIDFromContext(got) != "fedcba9876543210fedcba9876543210" {
		t.Fatalf("trace ID = %q", TraceIDFromContext(got))
	}
}

func TestContextWithActiveGoroutineDoesNotAdoptCancellation(t *testing.T) {
	active, cancelActive := context.WithCancel(activeSpanContext(t, "0123456789abcdef0123456789abcdef", "0123456789abcdef"))
	restore := InjectContextIntoGoroutine(active)
	defer restore()

	got := ContextWithActiveGoroutine(context.Background())
	cancelActive()
	select {
	case <-got.Done():
		t.Fatal("active context cancellation leaked into base")
	default:
	}
}

func TestContextWithActiveGoroutineWithoutActiveContextReturnsBase(t *testing.T) {
	base := context.WithValue(context.Background(), activeContextTestKey{}, "base")
	if got := ContextWithActiveGoroutine(base); got != base {
		t.Fatal("base context should be returned unchanged")
	}
	if got := ContextWithActiveGoroutine(nil); got == nil {
		t.Fatal("nil base should become a non-nil background context")
	}
}

func TestPackageLevelShutdown(t *testing.T) {
	resetGlobalTracer()
	os.Unsetenv("TRACING_ENABLED")

	if err := Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on nil global tracer should not error, got: %v", err)
	}

	Init()
	if err := Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown error = %v", err)
	}
	resetGlobalTracer()
}
