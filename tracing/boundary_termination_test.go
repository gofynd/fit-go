// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"runtime"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
)

func TestRunBoundaryGoexitIsNotSuccessful(t *testing.T) {
	tracer, exporter := boundaryTracer(t)
	done := make(chan struct{})
	restored := make(chan bool, 1)
	go func() {
		defer close(done)
		defer func() { restored <- ContextFromGoroutine() == nil }()
		_ = RunBoundary(context.Background(), BoundaryOptions{
			Type: BoundaryTask, Name: "terminated-task", Tracer: tracer,
		}, func(context.Context) error {
			runtime.Goexit()
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminated boundary did not unwind")
	}
	if !<-restored {
		t.Fatal("terminated boundary retained its goroutine context")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || spans[0].Status.Description != "boundary terminated" {
		t.Fatalf("terminated boundary span = %+v", spans)
	}
}
