package grpc

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	fittracing "github.com/gofynd/fit-go/tracing"
)

type tracedHealthServer struct {
	healthpb.UnimplementedHealthServer
	seen chan oteltrace.SpanContext
}

func (s *tracedHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.seen <- oteltrace.SpanContextFromContext(ctx)
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func TestNewClientCreatesAndPropagatesGRPCSpan(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := fittracing.New(context.Background(), fittracing.Options{
		ServiceName:            "fit-grpc-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := fittracing.SetGlobal(tracer)
	defer restore()
	defer tracer.Shutdown(context.Background())

	listener := bufconn.Listen(1024 * 1024)
	server := gogrpc.NewServer(gogrpc.StatsHandler(otelgrpc.NewServerHandler()))
	health := &tracedHealthServer{seen: make(chan oteltrace.SpanContext, 1)}
	healthpb.RegisterHealthServer(server, health)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := NewClient(
		"passthrough:///fit-grpc-test",
		gogrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	ctx, root := tracer.StartSpan(context.Background(), "caller", fittracing.SpanKindInternal)
	rootSC := oteltrace.SpanContextFromContext(ctx)
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	root.End()

	serverSC := <-health.seen
	if !serverSC.IsValid() || serverSC.TraceID() != rootSC.TraceID() {
		t.Fatalf("server trace = %s, want continued trace %s", serverSC.TraceID(), rootSC.TraceID())
	}

	var clientSpan bool
	for _, span := range exporter.GetSpans() {
		if span.SpanKind == oteltrace.SpanKindClient && span.SpanContext.TraceID() == rootSC.TraceID() {
			clientSpan = true
			break
		}
	}
	if !clientSpan {
		t.Fatal("gRPC client span was not exported on the caller trace")
	}
}

func TestNewClientDoesNotDuplicateCallerOTelGRPCHandler(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := fittracing.New(context.Background(), fittracing.Options{
		ServiceName:            "fit-grpc-dedup-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := fittracing.SetGlobal(tracer)
	defer restore()
	defer tracer.Shutdown(context.Background())

	listener := bufconn.Listen(1024 * 1024)
	server := gogrpc.NewServer()
	healthpb.RegisterHealthServer(server, &tracedHealthServer{seen: make(chan oteltrace.SpanContext, 1)})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := NewClient(
		"passthrough:///fit-grpc-dedup-test",
		gogrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	ctx, root := tracer.StartSpan(context.Background(), "caller", fittracing.SpanKindInternal)
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	root.End()

	clientSpans := 0
	for _, span := range exporter.GetSpans() {
		if span.SpanKind == oteltrace.SpanKindClient {
			clientSpans++
		}
	}
	if clientSpans != 1 {
		t.Fatalf("client spans = %d, want one caller-owned otelgrpc span", clientSpans)
	}
}

func TestNewClientDoesNotDuplicateCallerFITGRPCHandler(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := fittracing.New(context.Background(), fittracing.Options{
		ServiceName:            "fit-grpc-repeat-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := fittracing.SetGlobal(tracer)
	defer restore()
	defer tracer.Shutdown(context.Background())

	listener := bufconn.Listen(1024 * 1024)
	server := gogrpc.NewServer()
	healthpb.RegisterHealthServer(server, &tracedHealthServer{seen: make(chan oteltrace.SpanContext, 1)})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	opts := []gogrpc.DialOption{
		gogrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	opts = append(opts, TracingDialOptions()...)
	conn, err := NewClient("passthrough:///fit-grpc-repeat-test", opts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	ctx, root := tracer.StartSpan(context.Background(), "caller", fittracing.SpanKindInternal)
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	root.End()

	clientSpans := 0
	for _, span := range exporter.GetSpans() {
		if span.SpanKind == oteltrace.SpanKindClient {
			clientSpans++
		}
	}
	if clientSpans != 1 {
		t.Fatalf("client spans = %d, want one FIT-owned span", clientSpans)
	}
}
