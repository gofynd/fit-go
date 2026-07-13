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

package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/event"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

func recordingMongoTracer(t *testing.T) (*tracing.Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	t.Setenv("OTEL_SDK_DISABLED", "false")

	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName:            "mongo-parent-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	t.Cleanup(func() {
		if err := tracer.Shutdown(context.Background()); err != nil {
			t.Errorf("tracer shutdown: %v", err)
		}
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return tracer, exporter
}

func exportedMongoSpan(t *testing.T, exporter *tracetest.InMemoryExporter, name string) tracetest.SpanStub {
	t.Helper()
	for _, span := range exporter.GetSpans() {
		if span.Name == name {
			return span
		}
	}
	t.Fatalf("exported span %q not found", name)
	return tracetest.SpanStub{}
}

// When tracing is off, no monitor is installed (zero overhead on the hot path).
func TestCommandMonitorFor_DisabledReturnsNil(t *testing.T) {
	if m := commandMonitorFor(nil); m != nil {
		t.Fatal("nil tracer should yield no monitor")
	}

	t.Setenv("TRACING_ENABLED", "")
	disabled, err := tracing.New(context.Background(), tracing.DefaultOptions())
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	if disabled.IsEnabled() {
		t.Skip("environment forces tracing on; disabled-path assertion not applicable")
	}
	if m := commandMonitorFor(disabled); m != nil {
		t.Fatal("disabled tracer should yield no monitor")
	}
}

// When enabled, a monitor is installed.
func TestCommandMonitorFor_EnabledReturnsMonitor(t *testing.T) {
	m := commandMonitorFor(tracingtest.Enabled(t))
	if m == nil || m.Started == nil || m.Succeeded == nil || m.Failed == nil {
		t.Fatalf("expected a fully-wired monitor, got %+v", m)
	}
}

// A command span is opened on Started and closed on Succeeded/Failed, leaving no
// in-flight spans (guards against span leaks). The failed path records the error.
func TestCommandTracer_Lifecycle(t *testing.T) {
	ct := &commandTracer{tracer: tracingtest.Enabled(t), inflight: make(map[int64]inflightSpan)}

	// Success path.
	ct.started(context.Background(), &event.CommandStartedEvent{
		CommandName:  "find",
		DatabaseName: "highbrow",
		RequestID:    1,
	})
	if got := len(ct.inflight); got != 1 {
		t.Fatalf("after started: want 1 in-flight span, got %d", got)
	}
	ct.finished(1, nil)
	if got := len(ct.inflight); got != 0 {
		t.Fatalf("after succeeded: want 0 in-flight spans, got %d (leak)", got)
	}

	// Failure path.
	ct.started(context.Background(), &event.CommandStartedEvent{
		CommandName:  "insert",
		DatabaseName: "highbrow",
		RequestID:    2,
	})
	ct.finished(2, errors.New("boom"))
	if got := len(ct.inflight); got != 0 {
		t.Fatalf("after failed: want 0 in-flight spans, got %d (leak)", got)
	}

	// Unknown completion is a safe no-op.
	ct.finished(999, nil)
}

func TestCommandTracer_InheritsActiveGoroutineParent(t *testing.T) {
	tracer, exporter := recordingMongoTracer(t)
	ct := &commandTracer{tracer: tracer, inflight: make(map[int64]inflightSpan)}
	activeCtx, active := tracer.StartSpan(context.Background(), "active-handler", tracing.SpanKindServer)
	defer active.End()
	restoreActive := tracing.InjectContextIntoGoroutine(activeCtx)
	defer restoreActive()

	base, cancel := context.WithCancel(context.Background())
	cancel()
	ct.started(base, &event.CommandStartedEvent{
		CommandName:  "find",
		DatabaseName: "highbrow",
		RequestID:    10,
	})
	ct.finished(10, nil)

	child := exportedMongoSpan(t, exporter, "mongodb.find")
	if got := child.Parent.SpanID().String(); got != active.SpanID() {
		t.Fatalf("MongoDB span parent = %s, want active goroutine span %s", got, active.SpanID())
	}
}

func TestCommandTracer_ExplicitContextParentWins(t *testing.T) {
	tracer, exporter := recordingMongoTracer(t)
	ct := &commandTracer{tracer: tracer, inflight: make(map[int64]inflightSpan)}
	activeCtx, active := tracer.StartSpan(context.Background(), "active-handler", tracing.SpanKindServer)
	defer active.End()
	explicitCtx, explicit := tracer.StartSpan(context.Background(), "explicit-parent", tracing.SpanKindServer)
	defer explicit.End()
	restoreActive := tracing.InjectContextIntoGoroutine(activeCtx)
	defer restoreActive()

	ct.started(explicitCtx, &event.CommandStartedEvent{
		CommandName:  "find",
		DatabaseName: "highbrow",
		RequestID:    11,
	})
	ct.finished(11, nil)

	child := exportedMongoSpan(t, exporter, "mongodb.find")
	if got := child.Parent.SpanID().String(); got != explicit.SpanID() {
		t.Fatalf("MongoDB span parent = %s, want explicit span %s", got, explicit.SpanID())
	}
	if got := child.Parent.SpanID().String(); got == active.SpanID() {
		t.Fatalf("MongoDB span unexpectedly inherited active goroutine span %s", got)
	}
}

func TestMongoCommandFailureStatusDoesNotExposeServerValues(t *testing.T) {
	const secret = "duplicate key: { email: secret@example.com }"
	status := mongoCommandFailureStatus(errors.New(secret))
	if status == "" || status == secret {
		t.Fatalf("unsafe Mongo status %q", status)
	}
	if status != "mongodb command failed" {
		t.Fatalf("status = %q", status)
	}
}

// An in-flight span whose completion event never arrives (connection torn down
// mid-command) must not leak: the stale sweep ends and removes it, while a fresh
// in-flight span is left untouched.
func TestCommandTracer_SweepsStaleSpans(t *testing.T) {
	tr := tracingtest.Enabled(t)
	ct := &commandTracer{tracer: tr, inflight: make(map[int64]inflightSpan)}

	_, staleSpan := tr.StartSpan(context.Background(), "mongodb.find", tracing.SpanKindClient)
	_, freshSpan := tr.StartSpan(context.Background(), "mongodb.find", tracing.SpanKindClient)
	ct.inflight[1] = inflightSpan{span: staleSpan, start: time.Now().Add(-2 * maxInflightAge)}
	ct.inflight[2] = inflightSpan{span: freshSpan, start: time.Now()}

	ct.mu.Lock()
	ct.sweepStaleLocked()
	ct.mu.Unlock()

	if _, ok := ct.inflight[1]; ok {
		t.Fatal("stale in-flight span should have been swept (ended + removed)")
	}
	if _, ok := ct.inflight[2]; !ok {
		t.Fatal("fresh in-flight span must NOT be swept")
	}
}
