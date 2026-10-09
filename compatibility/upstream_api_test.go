package compatibility_test

import (
	"context"
	"reflect"
	"testing"

	fit "github.com/gofynd/fit-go"
	fiterrors "github.com/gofynd/fit-go/errors"
	fitgrpc "github.com/gofynd/fit-go/grpc"
	"github.com/gofynd/fit-go/kafka"
	"github.com/gofynd/fit-go/logging"
	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/postgres"
	"github.com/gofynd/fit-go/profiling"
	"github.com/gofynd/fit-go/redis"
	"github.com/gofynd/fit-go/server"
	"github.com/gofynd/fit-go/tracing"
)

// This package is a compile contract for the public surface present on upstream
// main before the migration-capability merge. Keep it external to prevent tests
// from accidentally relying on unexported implementation details.

var _ func(context.Context, string, string) context.Context = tracing.ContextWithTrace

type producerMock struct{}

func (*producerMock) Connect() error                             { return nil }
func (*producerMock) Produce(string, []kafka.Message, int) error { return nil }
func (*producerMock) ProduceBatch([]kafka.TopicMessages, int) error {
	return nil
}
func (*producerMock) Close() error { return nil }

type consumerMock struct{}

func (*consumerMock) Connect([]kafka.TopicConfig) error { return nil }
func (*consumerMock) Consume(kafka.MessageHandler, kafka.ConsumerOptions) error {
	return nil
}
func (*consumerMock) ConsumeBatch(kafka.BatchHandler, kafka.ConsumerOptions) error {
	return nil
}
func (*consumerMock) Close() error { return nil }

var _ kafka.KafkaProducer = (*producerMock)(nil)
var _ kafka.KafkaConsumer = (*consumerMock)(nil)

func TestUpstreamMainCompatibilitySurface(t *testing.T) {
	ctx := context.Background()
	tracer := &tracing.Tracer{}
	if err := tracer.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush on zero tracer: %v", err)
	}
	_ = tracing.Options{ExportTimeout: 1, MaxQueueSize: 1, BlockOnQueueFull: true}
	_ = server.OTelMiddlewareWithConfig(server.OTelMiddlewareConfig{})
	_ = postgres.ConnectionOptions{
		Context:     ctx,
		QueryTracer: postgres.NewOTelQueryTracer(postgres.OTelQueryTracerOptions{}),
	}

	jwt := server.JWTOptions{ExpectedPayload: map[string]interface{}{"company_id": "1"}}
	var payload map[string]interface{} = jwt.ExpectedPayload
	if payload["company_id"] != "1" {
		t.Fatalf("typed JWT payload = %#v", payload)
	}

	var profilerA, profilerB server.ProfilerState
	_ = profilerA == profilerB
	var logA, logB logging.Options
	var fitA, fitB fit.Fit
	var configA, configB kafka.ConsumerConfig
	var optsA, optsB kafka.ConsumerOptions
	var metricsA, metricsB metrics.Options
	var grpcA, grpcB fitgrpc.Config
	_ = logA == logB && fitA == fitB && configA == configB && optsA == optsB && metricsA == metricsB && grpcA == grpcB
}

type upstreamLoggingOptions logging.Options

func TestUpstreamLoggingPositionalLiteralStillCompiles(t *testing.T) {
	_ = upstreamLoggingOptions{"", "", "", "", nil}
}

func TestUpstreamPositionalStructLayoutsRemainExact(t *testing.T) {
	assertFieldNames(t, fiterrors.SentryConfig{}, "DSN", "Environment", "Release", "Debug", "SampleRate", "TracesSampleRate", "Tags", "ServerName", "Transport")
	assertFieldNames(t, kafka.ProducerConfig{}, "Acks", "IdempotentProducer", "Timeout", "Compression", "MaxRetries", "RetryBackoff")
	assertFieldNames(t, postgres.ConnectionOptions{}, "QueryTracer", "MaxConns", "MinConns", "MaxConnLifetime", "MaxConnIdleTime", "HealthCheckPeriod", "Context", "PerService")
	assertFieldNames(t, postgres.PoolOverrides{}, "MaxConns", "MinConns", "MaxConnLifetime", "MaxConnIdleTime", "HealthCheckPeriod")
	assertFieldNames(t, profiling.Config{}, "Enabled", "Server", "CPUEnabled", "HeapEnabled", "WallEnabled", "TagsJSON", "FlushIntervalMs", "HeapSamplingIntervalBytes", "HeapStackDepth", "WallSamplingDurationMs", "WallSamplingIntervalMicros", "WallCollectCPUTime", "Tags", "ApplicationName")
	assertFieldNames(t, redis.DialOptions{}, "Addr", "Password", "Username", "DB", "ClientName", "TLSConfig", "ConnectTimeout", "SocketTimeout", "KeepAlive", "MaxRetries", "PoolSize", "MinIdleConns", "ReadOnly")
	assertFieldNames(t, redis.ClusterDialOptions{}, "Addrs", "Password", "Username", "ClientName", "TLSConfig", "ConnectTimeout", "SocketTimeout", "KeepAlive", "SlotsRefreshInterval", "ReadOnly", "PoolSize", "MinIdleConns")
	assertFieldNames(t, redis.SentinelDialOptions{}, "MasterName", "SentinelAddrs", "Password", "Username", "SentinelPassword", "SentinelUsername", "DB", "ClientName", "TLSConfig", "EnableTLSForSentinel", "ConnectTimeout", "SocketTimeout", "KeepAlive", "ReadOnly", "PoolSize", "MinIdleConns")
	assertFieldNames(t, redis.ConnectionOptions{}, "Dial", "ClusterDial", "SentinelDial", "DefaultConnectTimeout", "Context")
	assertFieldNames(t, server.Config{}, "Port", "Logger", "ReadTimeout", "WriteTimeout", "IdleTimeout", "MaxPayloadSize", "IncludeHeadersInLog", "MetricsRecorder", "SecureHeaders", "HealthChecker")
	assertFieldNames(t, server.JWTOptions{}, "Secret", "ExpectedPayload")
	assertFieldNames(t, server.LogRequestResponseConfig{}, "Logger", "IncludeHeaders", "MetricsRecorder")
	assertFieldNames(t, server.OTelMiddlewareConfig{}, "Tracer", "CaptureFullURL", "CaptureUserAgent", "CaptureClientIP")
	assertFieldNames(t, tracing.Options{}, "Enabled", "ServiceName", "Env", "Endpoint", "SampleRate", "BatchTimeout", "ExportTimeout", "MaxQueueSize", "MaxExportBatch", "BlockOnQueueFull", "Attributes", "SpanExporter", "UseSimpleSpanProcessor")
}

func assertFieldNames(t *testing.T, value interface{}, expected ...string) {
	t.Helper()
	typeOf := reflect.TypeOf(value)
	actual := make([]string, typeOf.NumField())
	for index := range actual {
		actual[index] = typeOf.Field(index).Name
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("%s fields = %v, want exact upstream layout %v", typeOf, actual, expected)
	}
}
