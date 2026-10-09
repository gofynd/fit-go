package httpclient

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gofynd/fit-go/tracing"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Use a fresh process: an earlier test's initialized FIT singleton would hide
// unwanted lazy initialization, and changing it here could disturb other tests.
func TestNativeOTelTransportPreservesApplicationTracingGlobals(t *testing.T) {
	const probeEnv = "FIT_HTTP_NATIVE_OWNER_PROBE"
	if mode := os.Getenv(probeEnv); mode != "" {
		testNativeOTelTransportOwnership(t, mode)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"enabled", "disabled", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable,
				"-test.run=^TestNativeOTelTransportPreservesApplicationTracingGlobals$", "-test.count=1")
			command.Env = append(os.Environ(), probeEnv+"="+mode)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("fresh-process ownership probe: %v\n%s", err, output)
			}
		})
	}
}

func testNativeOTelTransportOwnership(t *testing.T, mode string) {
	t.Helper()
	t.Setenv("TRACING_ENABLED", "true")
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	if mode == "disabled" {
		t.Setenv("TRACING_ENABLED", "false")
	} else if mode == "invalid" {
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "%")
	}
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	propagator := &markerPropagator{}
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagator)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	base := &fakeRT{}
	native := otelhttp.NewTransport(base,
		otelhttp.WithTracerProvider(provider), otelhttp.WithPropagators(propagator),
		otelhttp.WithSpanNameFormatter(func(string, *http.Request) string { return "native-client" }))
	var logs bytes.Buffer
	metricCalls := 0
	wrapped := WrapTransport(native,
		WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
		WithMetrics(func(string, string, int, time.Duration) { metricCalls++ }))
	// Rewrapping must retain suppression as well as the caller's OTel options.
	wrapped = WrapTransport(wrapped)
	for range 2 {
		req, err := http.NewRequest(http.MethodGet, "http://service.internal/path", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(requestIDHeader, "caller-id")
		do(t, wrapped, req)
		if otel.GetTracerProvider() != provider || otel.GetTextMapPropagator() != propagator {
			t.Fatal("suppressed FIT tracing replaced application-owned OTel globals")
		}
		if err := tracing.InitError(); err != nil {
			t.Fatalf("suppressed FIT tracing attempted initialization: %v", err)
		}
		if base.got.Header.Get("x-fit-propagator") != "used" || base.got.Header.Get(requestIDHeader) != "caller-id" {
			t.Fatal("caller propagation or request ID was lost")
		}
		if req.Header.Get("x-fit-propagator") != "" {
			t.Fatal("caller-owned request headers were mutated")
		}
	}
	if metricCalls != 2 || strings.Count(logs.String(), "httpclient: request") != 2 {
		t.Fatal("FIT logging/metrics were suppressed along with tracing")
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported %d spans, want exactly two native spans", len(spans))
	}
	for _, span := range spans {
		if span.Name != "native-client" {
			t.Fatalf("unexpected FIT span: %q", span.Name)
		}
	}
}
