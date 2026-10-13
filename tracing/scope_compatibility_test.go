// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInstrumentationScopeLegacyAndSDKIdentity(t *testing.T) {
	for _, test := range []struct {
		name        string
		sdk         bool
		serviceName string
		wantScope   string
		wantService string
	}{
		{"legacy-distinct-resource", false, "logical", "fit.go/logical", "resource"},
		{"legacy-empty-option", false, "", "fit.go/", "resource"},
		{"sdk-explicit-option", true, "logical", "fit.go/logical", "logical"},
		{"sdk-resource-fallback", true, "", "fit.go/resource", "resource"},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolateOTelGlobals(t)
			t.Setenv("OTEL_SDK_DISABLED", "false")
			t.Setenv("OTEL_SERVICE_NAME", "")
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
			t.Setenv("SERVICE_NAME", "")
			enabled := true
			exporter := tracetest.NewInMemoryExporter()
			opts := Options{
				Enabled:                &enabled,
				ServiceName:            test.serviceName,
				Attributes:             map[string]string{"service.name": "resource"},
				SampleRate:             1,
				SpanExporter:           exporter,
				UseSimpleSpanProcessor: true,
			}
			var tracer *Tracer
			var err error
			if test.sdk {
				sdkOpts := advancedOptions(opts)
				sdkOpts.Sampler = "always_on"
				tracer, err = NewSDK(context.Background(), sdkOpts)
			} else {
				tracer, err = New(context.Background(), opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := tracer.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			_, span := tracer.StartSpan(context.Background(), "scope-probe", SpanKindInternal)
			span.End()
			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("exported %d spans, want 1", len(spans))
			}
			if got := spans[0].InstrumentationScope.Name; got != test.wantScope {
				t.Errorf("instrumentation scope = %q, want %q", got, test.wantScope)
			}
			service, _ := spans[0].Resource.Set().Value(attribute.Key("service.name"))
			if got := service.AsString(); got != test.wantService {
				t.Errorf("resource service.name = %q, want %q", got, test.wantService)
			}
		})
	}
}
