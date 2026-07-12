// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestBuildPropagator(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		fields []string
	}{
		{name: "default", value: "", fields: []string{"baggage", "traceparent", "tracestate"}},
		{name: "trace context only", value: "tracecontext", fields: []string{"traceparent", "tracestate"}},
		{name: "baggage only", value: "baggage", fields: []string{"baggage"}},
		{name: "b3 single", value: "b3", fields: []string{"x-b3-flags", "x-b3-sampled", "x-b3-spanid", "x-b3-traceid"}},
		{name: "b3 multi", value: "b3multi", fields: []string{"x-b3-flags", "x-b3-sampled", "x-b3-spanid", "x-b3-traceid"}},
		{name: "jaeger", value: "jaeger", fields: []string{"uber-trace-id"}},
		{name: "none", value: "none", fields: []string{}},
		{name: "case whitespace and duplicates", value: " Baggage,tracecontext,baggage ", fields: []string{"baggage", "traceparent", "tracestate"}},
		{name: "unknown mixed is ignored", value: "unknown,tracecontext", fields: []string{"traceparent", "tracestate"}},
		{name: "all invalid becomes no-op", value: "unknown,none", fields: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := buildPropagator(tt.value)
			if err != nil {
				t.Fatalf("buildPropagator(%q): %v", tt.value, err)
			}
			fields := p.Fields()
			slices.Sort(fields)
			if !slices.Equal(fields, tt.fields) {
				t.Fatalf("fields = %v, want %v", fields, tt.fields)
			}
		})
	}
}

func sampledContext() context.Context {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5, 6},
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func TestBuildPropagatorWireFormats(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		wantHeader string
		absent     string
	}{
		{name: "b3 single", value: "b3", wantHeader: "b3", absent: "x-b3-traceid"},
		{name: "b3 multi", value: "b3multi", wantHeader: "x-b3-traceid", absent: "b3"},
		{name: "jaeger", value: "jaeger", wantHeader: "uber-trace-id"},
		{name: "unknown mixed", value: "unknown,tracecontext", wantHeader: "traceparent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := buildPropagator(tt.value)
			if err != nil {
				t.Fatalf("buildPropagator: %v", err)
			}
			carrier := propagation.MapCarrier{}
			p.Inject(sampledContext(), carrier)
			if carrier.Get(tt.wantHeader) == "" {
				t.Fatalf("carrier missing %q: %v", tt.wantHeader, carrier)
			}
			if tt.absent != "" && carrier.Get(tt.absent) != "" {
				t.Fatalf("carrier unexpectedly contains %q: %v", tt.absent, carrier)
			}
		})
	}
}

func TestDefaultOptionsReadsPropagators(t *testing.T) {
	t.Setenv("OTEL_PROPAGATORS", "tracecontext")
	if got := DefaultOptions().Propagators; got != "tracecontext" {
		t.Fatalf("Propagators = %q, want tracecontext", got)
	}
}

func TestPropagationFieldsIncludesEverySupportedWireFormat(t *testing.T) {
	p, err := buildPropagator("tracecontext")
	if err != nil {
		t.Fatalf("buildPropagator: %v", err)
	}
	fields := PropagationFields(p)
	for _, want := range []string{
		"traceparent", "tracestate", "baggage", "b3",
		"x-b3-traceid", "x-b3-spanid", "x-b3-sampled", "x-b3-flags", "x-b3-parentspanid",
		"uber-trace-id",
	} {
		if !slices.Contains(fields, want) {
			t.Errorf("PropagationFields missing %q: %v", want, fields)
		}
	}
}

func TestJaegerPropagatorCarriesLegacyUberctxBaggage(t *testing.T) {
	p, err := buildPropagator("jaeger")
	if err != nil {
		t.Fatalf("buildPropagator: %v", err)
	}
	member, err := baggage.NewMemberRaw("tenant", "A B/emoji-😀")
	if err != nil {
		t.Fatalf("NewMemberRaw: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("baggage.New: %v", err)
	}
	ctx := baggage.ContextWithBaggage(sampledContext(), bag)
	carrier := propagation.MapCarrier{}
	p.Inject(ctx, carrier)
	if got := carrier.Get("uberctx-tenant"); got != "A%20B%2Femoji-%F0%9F%98%80" {
		t.Fatalf("uberctx baggage = %q", got)
	}

	extracted := p.Extract(context.Background(), carrier)
	got := baggage.FromContext(extracted).Member("tenant").Value()
	if got != "A B/emoji-😀" {
		t.Fatalf("extracted baggage = %q", got)
	}
}

func TestIsPropagationFieldIncludesDynamicJaegerBaggage(t *testing.T) {
	p, err := buildPropagator("tracecontext")
	if err != nil {
		t.Fatalf("buildPropagator: %v", err)
	}
	for _, key := range []string{"uberctx-user", "UBERCTX-Tenant", "traceparent"} {
		if !IsPropagationField(key, p) {
			t.Errorf("IsPropagationField(%q) = false", key)
		}
	}
	if IsPropagationField("x-user-data", p) {
		t.Fatal("non-propagation field was classified as propagation")
	}
}
