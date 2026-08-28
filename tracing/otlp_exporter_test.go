// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newOTLPExporter must construct cleanly for both protocols when the endpoint is
// a full URL (scheme+host+port) — the case that broke export with WithEndpoint
// (it produced "http://http://..."). WithEndpointURL parses it correctly.
func TestNewOTLPExporter_URLEndpointBothProtocols(t *testing.T) {
	for _, proto := range []string{"", "http/protobuf", "grpc"} {
		exp, err := newOTLPExporter(context.Background(), Options{
			Endpoint: "http://collector.example.svc.cluster.local:4318",
			Protocol: proto,
		})
		if err != nil {
			t.Fatalf("protocol %q: newOTLPExporter returned error: %v", proto, err)
		}
		_ = exp.Shutdown(context.Background())
	}
}

func TestResolveOTLPProtocolPrecedence(t *testing.T) {
	tests := []struct {
		name      string
		explicit  string
		tracesEnv string
		commonEnv string
		want      string
	}{
		{name: "default", want: "http/protobuf"},
		{name: "common", commonEnv: "grpc", want: "grpc"},
		{name: "traces beats common", tracesEnv: "http/protobuf", commonEnv: "grpc", want: "http/protobuf"},
		{name: "explicit beats env", explicit: "grpc", tracesEnv: "http/protobuf", commonEnv: "http/protobuf", want: "grpc"},
		{name: "unsupported falls back", tracesEnv: "http/json", want: "http/protobuf"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", tt.tracesEnv)
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", tt.commonEnv)
			if got := resolveOTLPProtocol(Options{Protocol: tt.explicit}); got != tt.want {
				t.Fatalf("protocol = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveTraceExporters(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    []string
		wantErr bool
	}{
		{name: "default", want: []string{"otlp"}},
		{name: "none", value: "none", want: []string{"none"}},
		{name: "multiple and deduplicated", value: "console, otlp,console", want: []string{"console", "otlp"}},
		{name: "unknown", value: "zipkin", wantErr: true},
		{name: "none cannot be mixed", value: "none,otlp", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_EXPORTER", "")
			got, err := resolveTraceExporters(Options{Exporters: tt.value})
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveTraceExporters() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(got) != len(tt.want) {
				t.Fatalf("exporters = %#v, want %#v", got, tt.want)
			}
			for index := range got {
				if got[index] != tt.want[index] {
					t.Fatalf("exporters = %#v, want %#v", got, tt.want)
				}
			}
		})
	}
}

func TestDefaultOptionsLeaveBatchProcessorSettingsToOTelEnvironment(t *testing.T) {
	t.Setenv("OTEL_BSP_SCHEDULE_DELAY", "37")
	t.Setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "17")
	opts := DefaultOptions()
	if opts.BatchTimeout != 0 || opts.MaxExportBatch != 0 {
		t.Fatalf("DefaultOptions overrides OTEL_BSP settings: timeout=%s batch=%d", opts.BatchTimeout, opts.MaxExportBatch)
	}
}

func TestNewTraceExporterSelection(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")

	t.Run("none initializes without an exporter", func(t *testing.T) {
		tracer, err := New(context.Background(), Options{ServiceName: "test", Exporters: "none"})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer tracer.Shutdown(context.Background())
		if !tracer.IsEnabled() {
			t.Fatal("tracing should stay enabled with the none exporter")
		}
	})

	t.Run("custom exporter takes precedence", func(t *testing.T) {
		tracer, err := New(context.Background(), Options{
			ServiceName:            "test",
			Exporters:              "invalid",
			SpanExporter:           tracetest.NewInMemoryExporter(),
			UseSimpleSpanProcessor: true,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer tracer.Shutdown(context.Background())
	})

	t.Run("invalid exporter fails initialization", func(t *testing.T) {
		if _, err := New(context.Background(), Options{ServiceName: "test", Exporters: "invalid"}); err == nil {
			t.Fatal("New should reject an unsupported trace exporter")
		}
	})
}

func TestResolveOTLPEndpointPrecedence(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://common:4318/base")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4318/custom")

	endpoint, source := resolveOTLPEndpoint(Options{})
	if endpoint != "http://traces:4318/custom" || source != otlpEndpointTraces {
		t.Fatalf("endpoint/source = %q/%v, want traces-specific", endpoint, source)
	}
	endpoint, source = resolveOTLPEndpoint(Options{Endpoint: "http://explicit:4318/exact"})
	if endpoint != "http://explicit:4318/exact" || source != otlpEndpointExplicit {
		t.Fatalf("explicit endpoint/source = %q/%v", endpoint, source)
	}
	if got := httpTraceEndpoint("http://collector:4318/base/", otlpEndpointCommon); got != "http://collector:4318/base/v1/traces" {
		t.Fatalf("common HTTP endpoint = %q", got)
	}
}

func TestOTLPHTTPEnvironmentEndpointPaths(t *testing.T) {
	tests := []struct {
		name       string
		commonPath string
		tracesPath string
		explicit   string
		wantPath   string
	}{
		{name: "common appends signal path", commonPath: "/collector", wantPath: "/collector/v1/traces"},
		{name: "traces endpoint is exact", commonPath: "/common", tracesPath: "/signal/custom", wantPath: "/signal/custom"},
		{name: "explicit endpoint is exact", commonPath: "/common", tracesPath: "/signal", explicit: "/explicit", wantPath: "/explicit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := make(chan string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r.URL.Path
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL+tt.commonPath)
			if tt.tracesPath != "" {
				t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", server.URL+tt.tracesPath)
			} else {
				t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
			}

			enabled := true
			opts := DefaultOptions()
			opts.Enabled = &enabled
			opts.UseSimpleSpanProcessor = true
			if tt.explicit != "" {
				opts.Endpoint = server.URL + tt.explicit
			}
			tracer, err := New(context.Background(), opts)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer tracer.Shutdown(context.Background())

			_, span := tracer.StartSpan(context.Background(), "export", SpanKindInternal)
			span.End()
			select {
			case got := <-requests:
				if got != tt.wantPath {
					t.Fatalf("request path = %q, want %q", got, tt.wantPath)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for OTLP HTTP export")
			}
		})
	}
}
