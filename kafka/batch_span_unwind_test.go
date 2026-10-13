// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/gofynd/fit-go/tracing"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func batchSpanUnwindPayload() BatchPayload {
	return BatchPayload{Topic: "orders", Messages: []MessagePayload{
		{Topic: "orders", Partition: 0, Offset: 3},
		{Topic: "orders", Partition: 0, Offset: 4},
	}}
}

func assertBatchSpansEnded(t *testing.T, exporter *tracetest.InMemoryExporter, status codes.Code) {
	t.Helper()
	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("ended %d spans; want receive + two process spans", len(spans))
	}
	var receives, processes int
	for _, span := range spans {
		if span.EndTime.IsZero() || span.Status.Code != status {
			t.Fatalf("span %q not ended with expected status %v: %+v", span.Name, status, span.Status)
		}
		switch span.Name {
		case "poll orders":
			receives++
		case "process orders":
			processes++
		default:
			t.Fatalf("unexpected span %q", span.Name)
		}
	}
	if receives != 1 || processes != 2 {
		t.Fatalf("receive=%d process=%d; want 1 and 2", receives, processes)
	}
}

func TestKafkaBatchSpansEndOnPanicWithoutChangingPanic(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "pointer", value: &struct{ Secret string }{"private-panic-payload"}},
		{name: "string", value: "private-panic-payload"},
		{name: "nil", value: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Modern Go converts panic(nil) to *runtime.PanicNilError. The library
			// must not recover/repanic it or alter the runtime's selected behavior.
			t.Setenv("GODEBUG", "panicnil=0")
			_, exporter := enabledKafkaTracer(t)
			var recovered any
			returned := false
			func() {
				defer func() { recovered = recover() }()
				_ = TracedBatchHandlerCtx(func(context.Context, BatchPayload) error { panic(test.value) })(batchSpanUnwindPayload())
				returned = true
			}()
			if returned {
				t.Fatal("batch adapter swallowed the panic")
			}
			if test.value == nil {
				if _, ok := recovered.(*runtime.PanicNilError); !ok {
					t.Fatalf("panic(nil) recovered as %T", recovered)
				}
			} else if recovered != test.value {
				t.Fatal("batch adapter changed panic identity")
			}
			assertBatchSpansEnded(t, exporter, codes.Unset)
			for _, span := range exporter.GetSpans() {
				if len(span.Events) != 0 || span.Status.Description != "" || strings.Contains(fmt.Sprint(span.Attributes), "private-panic-payload") {
					t.Fatalf("panic leaked into span %q", span.Name)
				}
			}
		})
	}
}

func TestKafkaBatchSpansEndOnLegacyNilPanicAndGoexit(t *testing.T) {
	t.Run("legacy_nil_panic", func(t *testing.T) {
		t.Setenv("GODEBUG", "panicnil=1")
		_, exporter := enabledKafkaTracer(t)
		returned := false
		func() {
			defer func() {
				if got := recover(); got != nil {
					t.Fatalf("legacy panic(nil) changed to %T", got)
				}
			}()
			_ = TracedBatchHandlerCtx(func(context.Context, BatchPayload) error { panic(nil) })(batchSpanUnwindPayload())
			returned = true
		}()
		if returned {
			t.Fatal("legacy panic(nil) was swallowed")
		}
		assertBatchSpansEnded(t, exporter, codes.Unset)
	})
	t.Run("goexit", func(t *testing.T) {
		_, exporter := enabledKafkaTracer(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = TracedBatchHandlerCtx(func(context.Context, BatchPayload) error { runtime.Goexit(); return nil })(batchSpanUnwindPayload())
		}()
		<-done
		assertBatchSpansEnded(t, exporter, codes.Unset)
	})
}

func TestKafkaBatchSpanUnwindKeepsNormalSuccessAndError(t *testing.T) {
	for _, wantErr := range []error{nil, errors.New("handler failed")} {
		name, status := "success", codes.Ok
		if wantErr != nil {
			name, status = "error", codes.Error
		}
		t.Run(name, func(t *testing.T) {
			_, exporter := enabledKafkaTracer(t)
			err := TracedBatchHandlerCtx(func(context.Context, BatchPayload) error { return wantErr })(batchSpanUnwindPayload())
			if err != wantErr {
				t.Fatalf("handler error identity changed: %v", err)
			}
			assertBatchSpansEnded(t, exporter, status)
		})
	}
}

func TestKafkaBatchSpanUnwindRestoresActiveContext(t *testing.T) {
	tracer, _ := enabledKafkaTracer(t)
	base, parent := tracer.StartSpan(context.Background(), "outer", tracing.SpanKindInternal)
	defer parent.End()
	cleanup := tracing.InjectContextIntoGoroutine(base)
	defer cleanup()
	marker := errors.New("panic identity")
	func() {
		defer func() {
			if recover() != marker {
				t.Fatal("panic identity changed")
			}
		}()
		_ = runTracedBatchHandler(base, batchSpanUnwindPayload(), func(context.Context, BatchPayload) error { panic(marker) })
	}()
	if tracing.ContextFromGoroutine() != base {
		t.Fatal("batch unwind did not restore the outer active context")
	}
}
