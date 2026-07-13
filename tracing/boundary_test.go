// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package tracing

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRunBoundaryCreatesActiveEntrySpanAndSafeStatus(t *testing.T) {
	tracer, exporter := boundaryTracer(t)
	secret := "token=super-secret"
	operationErr := errors.New(secret)
	err := RunBoundary(context.Background(), BoundaryOptions{
		Type: BoundaryCron, Name: "reconcile-orders", Tracer: tracer,
		Attributes: SpanAttributes{"job.schedule": "hourly"},
	}, func(ctx context.Context) error {
		if TraceIDFromContext(ctx) == "" || SpanIDFromContext(ctx) == "" {
			t.Fatal("boundary context has no active trace")
		}
		active := ContextFromGoroutine()
		if active == nil || SpanIDFromContext(active) != SpanIDFromContext(ctx) {
			t.Fatal("boundary context was not installed in goroutine bridge")
		}
		return operationErr
	})
	if !errors.Is(err, operationErr) {
		t.Fatalf("RunBoundary error = %v", err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name != "cron reconcile-orders" || span.Status.Description != "boundary failed" {
		t.Fatalf("span = %q, %+v", span.Name, span.Status)
	}
	serialized := span.Name + span.Status.Description
	for _, attribute := range span.Attributes {
		serialized += string(attribute.Key) + attribute.Value.Emit()
	}
	if strings.Contains(serialized, "super-secret") {
		t.Fatalf("span leaked operation error: %s", serialized)
	}
	if ContextFromGoroutine() != nil {
		t.Fatal("boundary did not restore goroutine context")
	}
}

func TestRunBoundaryWithResultPreservesResultAndParent(t *testing.T) {
	tracer, exporter := boundaryTracer(t)
	parentContext, parent := tracer.StartSpan(context.Background(), "parent", SpanKindServer)
	result, err := RunBoundaryWithResult(parentContext, BoundaryOptions{
		Type: BoundaryWorker, Name: "consume", Tracer: tracer, SpanKind: SpanKindConsumer,
	}, func(context.Context) (int, error) { return 42, nil })
	parent.End()
	if err != nil || result != 42 {
		t.Fatalf("RunBoundaryWithResult = %d, %v", result, err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported spans = %d, want 2", len(spans))
	}
	var boundaryFound bool
	for _, span := range spans {
		if span.Name == "worker consume" {
			boundaryFound = true
			if span.Parent.SpanID() != parent.otelSpan.SpanContext().SpanID() {
				t.Fatal("boundary did not retain explicit parent")
			}
		}
	}
	if !boundaryFound {
		t.Fatal("worker boundary span missing")
	}
}

func TestRunBoundaryEndsSpanAndRethrowsPanic(t *testing.T) {
	tracer, exporter := boundaryTracer(t)
	func() {
		defer func() {
			if recover() != "panic-value" {
				t.Fatal("RunBoundary did not preserve panic value")
			}
		}()
		_ = RunBoundary(context.Background(), BoundaryOptions{
			Type: BoundaryTask, Name: "panic-task", Tracer: tracer,
		}, func(context.Context) error { panic("panic-value") })
	}()
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Description != "boundary panicked" {
		t.Fatalf("panic span = %+v", spans)
	}
}

func TestRunBoundaryWithoutTracingStillBridgesExistingContext(t *testing.T) {
	disabled := &Tracer{}
	ctx := ContextWithTrace(context.Background(), "0123456789abcdef0123456789abcdef", "0123456789abcdef", true)
	if err := RunBoundary(ctx, BoundaryOptions{Type: BoundaryJob, Name: "no-tracing", Tracer: disabled}, func(context.Context) error {
		if ContextFromGoroutine() == nil {
			t.Fatal("disabled boundary did not bridge the input context")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRunBoundaryValidatesOptions(t *testing.T) {
	for _, options := range []BoundaryOptions{
		{},
		{Name: "name", Type: "unknown"},
		{Name: "name", Attributes: SpanAttributes{"fit.boundary.name": "override"}},
	} {
		if err := RunBoundary(context.Background(), options, func(context.Context) error { return nil }); err == nil {
			t.Fatalf("RunBoundary accepted options %+v", options)
		}
	}
}

func boundaryTracer(t *testing.T) (*Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	enabled := true
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := New(context.Background(), Options{
		ServiceName: "boundary-test", Enabled: &enabled, Sampler: "always_on",
		SpanExporter: exporter, UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })
	return tracer, exporter
}
