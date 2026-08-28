// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gofynd/fit-go/redact"
)

func TestPublicSpanStatusDescriptionsAreRedacted(t *testing.T) {
	enabled := true
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := New(context.Background(), Options{
		ServiceName:            "status-redaction",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	raw := "backend password=hunter2 token=secret user@example.com"
	want := redact.Text(raw)
	tests := []struct {
		name string
		set  func(context.Context, *Span)
	}{
		{name: "Span.SetStatus", set: func(_ context.Context, span *Span) {
			span.SetStatus(StatusError, raw)
		}},
		{name: "SetSpanStatus", set: func(ctx context.Context, _ *Span) {
			SetSpanStatus(ctx, StatusError, raw)
		}},
		{name: "Utils.SetSpanStatus", set: func(ctx context.Context, _ *Span) {
			Utils.SetSpanStatus(ctx, StatusError, raw)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, span := tracer.StartSpan(context.Background(), tt.name, SpanKindInternal)
			tt.set(ctx, span)
			if span.statusMsg != want {
				t.Fatalf("wrapper status description = %q, want %q", span.statusMsg, want)
			}
			for _, secret := range []string{"hunter2", "secret", "user@example.com"} {
				if strings.Contains(span.statusMsg, secret) {
					t.Fatalf("wrapper status description leaked %q: %q", secret, span.statusMsg)
				}
			}
			span.End()
		})
	}

	spans := exporter.GetSpans()
	if len(spans) != len(tests) {
		t.Fatalf("exported spans = %d, want %d", len(spans), len(tests))
	}
	for _, span := range spans {
		if got := span.Status.Description; got != want {
			t.Errorf("exported %q status description = %q, want %q", span.Name, got, want)
		}
	}
}
