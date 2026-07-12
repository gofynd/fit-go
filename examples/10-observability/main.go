// Example 10: Observability (tracing, metrics, profiling)
//
// fit ships three observability pillars that can be used independently:
//   - tracing:   OpenTelemetry spans exported over OTLP
//   - metrics:   Prometheus registry with an HTTP handler
//   - profiling: continuous CPU/heap/wall profiling via Pyroscope
//
// Relevant env:
//
//	TRACING_ENABLED=true
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
//	FIT_PROMETHEUS_ENABLED=true
//	METRICS_DIR=/var/data/metrics
//	PROFILING_ENABLED=true
//	PROFILING_DISTRIBUTOR_ADDRESS=http://localhost:4040
//
// Run:
//
//	go run ./examples/10-observability
//	curl localhost:2112/metrics
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/profiling"
	"github.com/gofynd/fit-go/tracing"
)

func main() {
	ctx := context.Background()

	// --- Tracing ---------------------------------------------------------
	tracer, err := tracing.New(ctx, tracing.Options{
		ServiceName: "checkout",
		Env:         "development",
		SampleRate:  1.0,
	})
	if err != nil {
		log.Fatalf("tracing init: %v", err)
	}
	defer tracer.Shutdown(ctx)

	// Spans nest via the returned context. They are no-ops unless tracing
	// is enabled, so it is always safe to instrument.
	spanCtx, span := tracer.StartSpan(ctx, "process-order", tracing.SpanKindInternal)
	span.SetAttribute("order.id", "o-123")
	span.SetAttribute("order.total", 4299)
	doWork(spanCtx, tracer)
	span.End()

	// --- Metrics ---------------------------------------------------------
	registry, err := metrics.New(metrics.Options{
		MetricsDir:        os.Getenv("METRICS_DIR"),
		ServerEnabled:     true,
		HTTPClientEnabled: true,
	})
	if err != nil {
		log.Fatalf("metrics init: %v", err)
	}
	defer registry.Shutdown()
	restoreMetrics := metrics.SetDefault(registry)
	defer restoreMetrics()

	// --- Profiling -------------------------------------------------------
	// NewFromEnv reads PROFILING_* env vars; Start is a no-op when disabled.
	profiler := profiling.NewFromEnv()
	profiler.Start()
	defer profiler.Stop()
	fmt.Println("profiling running:", profiler.IsRunning())

	// Expose Prometheus metrics and profiler control routes over HTTP.
	mux := http.NewServeMux()
	mux.Handle("/metrics", registry.Handler())
	mux.Handle("/_profiling/", profiler.Routes())
	fmt.Println("metrics on http://localhost:2112/metrics")
	log.Fatal(http.ListenAndServe(":2112", mux))
}

func doWork(ctx context.Context, tracer *tracing.Tracer) {
	_, span := tracer.StartSpan(ctx, "db-query", tracing.SpanKindInternal)
	defer span.End()
	span.SetAttribute("db.statement", "SELECT * FROM orders WHERE id = $1")
}
