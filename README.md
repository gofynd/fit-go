# fit-go

A batteries-included Go framework for building scalable microservices. Provides configuration, HTTP/gRPC servers, database clients, messaging, observability, and security modules with sensible defaults and environment-driven configuration.

## Modules

| Module | Description |
|---|---|
| `config` | Type-safe config from env vars, `.env`, JSON, and YAML files |
| `server` | Gin-based HTTP server with multi-type routing, middleware, and payload limits |
| `grpc` | gRPC server plus traced outbound clients via `fitgrpc.NewClient` or `TracingDialOptions`, middleware chains, health checks, and reflection |
| `mongo` | MongoDB connection manager with read/write splitting and pool tuning |
| `postgres` | PostgreSQL client via `lib/pq` with connection pooling |
| `mysql` | MySQL client via `go-sql-driver/mysql` |
| `redis` | Redis client supporting standalone, cluster, and sentinel modes |
| `kafka` | Kafka producer/consumer with SASL/SSL and OpenTelemetry tracing |
| `groupcache` | Distributed in-process caching with Kubernetes peer discovery |
| `logging` | Structured JSON logger with timezone support and trace propagation |
| `tracing` | OpenTelemetry tracing with OTLP export |
| `metrics` | Prometheus metrics for HTTP server and client instrumentation |
| `profiling` | Continuous profiling via Pyroscope (CPU, heap, wall-clock) |
| `errors` | Structured error codes with multilingual messages and Sentry integration |
| `encryption` | AES-256-GCM encryption with Vault and GCP KMS key providers |
| `feature` | Feature flag client with periodic refresh |
| `health` | Health check orchestration across all connections |
| `utils` | HTTP client, string helpers, and input sanitization |

## Quick Start

```go
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go"
	"github.com/gofynd/fit-go/server"
)

func main() {
	ctx := context.Background()

	// Initialize the framework (loads config, logger, tracing, metrics)
	f, err := fit.Init(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Shutdown(ctx)

	// Build your routes on a gin engine
	engine := gin.New()
	engine.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "pong"})
	})

	// Create the HTTP server and mount the engine as the default route handler
	srv := server.New(server.Config{Port: "8080"})
	if err := srv.Init(
		map[server.ServerType]http.Handler{server.ServerTypeDefault: engine},
		nil, // request middlewares
		nil, // response middlewares
	); err != nil {
		log.Fatal(err)
	}

	// Start serving (blocks until the server stops)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
```

See [`examples/`](examples) for complete, runnable programs covering each module.

## Examples

The [`examples/`](examples) directory contains standalone, runnable programs —
each demonstrates one part of the framework. Run any of them from the repo root:

```sh
go run ./examples/01-quickstart
```

Most are configured through environment variables (the header comment in each
`main.go` lists the relevant ones). Examples that talk to external systems
(databases, Kafka, Pyroscope) need those services reachable to do real work,
but all of them compile and start without extra setup.

| Example | Module(s) | What it shows |
|---|---|---|
| [01-quickstart](examples/01-quickstart) | `fit` | Initialize the framework and shut down gracefully on a signal |
| [02-config](examples/02-config) | `config` | Load config, typed getters, validation rules |
| [03-logging](examples/03-logging) | `logging` | Structured JSON logging, derived loggers, trace context |
| [04-http-server](examples/04-http-server) | `server` | Gin-based HTTP server, routing by ServerType, health routes |
| [05-databases](examples/05-databases) | `mongo`, `postgres`, `redis`, `health` | Connect with read/write split and aggregate health checks |
| [06-kafka](examples/06-kafka) | `kafka` | Context-aware produce and consume with the confluent driver |
| [07-caching](examples/07-caching) | `groupcache` | Read-through distributed cache with a single-flight loader |
| [08-encryption](examples/08-encryption) | `encryption` | AES-256-GCM encrypt/decrypt with pluggable key providers |
| [09-errors](examples/09-errors) | `errors` | Structured error codes with localized messages |
| [10-observability](examples/10-observability) | `tracing`, `metrics`, `profiling` | Spans, a Prometheus endpoint, and continuous profiling |

See [`examples/README.md`](examples/README.md) for more detail, including the
smaller modules (`feature`, `utils`, `grpc`) that don't have their own example.

## Configuration

All modules are configured via environment variables, following the [12-factor app](https://12factor.net/config) methodology. The `config` package loads values in this order (last wins):

1. `.env` file (if present)
2. OS environment variables
3. Config files (JSON/YAML) passed to `config.Load()`

Environment variables always take precedence over file values.

```go
cfg, err := config.Load("config.json")

port := cfg.GetInt("PORT", 8080)
debug := cfg.GetBool("DEBUG", false)
name := cfg.GetString("SERVICE_NAME", "my-service")
hosts := cfg.GetStringSlice("ALLOWED_HOSTS", []string{"localhost"})
ttl := cfg.GetDuration("CACHE_TTL", 5 * time.Minute)
```

## Database Clients

### MongoDB

Connections are auto-discovered from environment variables:

```bash
MONGO_CATALOG_READ_WRITE=mongodb://localhost:27017/catalog
MONGO_CATALOG_READ_ONLY=mongodb://localhost:27017/catalog?readPreference=secondaryPreferred
```

```go
client, err := mongo.InitDefault(ctx)
conn := client.Service("catalog")
write := conn.Write // Connection for writes
read := conn.Read   // Connection for reads
```

### PostgreSQL

```bash
POSTGRES_ORDERS_READ_WRITE=postgres://admin:secret@localhost:5432/orders?sslmode=disable
POSTGRES_ORDERS_READ_ONLY=postgres://reader:secret@localhost:5432/orders?sslmode=disable
```

```go
client, err := postgres.InitDefault()
conn := client.Service("orders")
write := conn.Write // *sql.DB for writes
read := conn.Read   // *sql.DB for reads
```

### Redis

Supports standalone, cluster, and sentinel modes:

```bash
REDIS_CACHE_READ_WRITE=redis://localhost:6379/0
REDIS_CACHE_READ_ONLY=redis://localhost:6379/0
```

```go
client, err := redis.InitDefault(ctx)
conn := client.Service("cache")
write := conn.Write // Connection for writes
```

## gRPC

Use the fit-go client helper for every outbound gRPC connection. Calling
`google.golang.org/grpc.NewClient` directly without `TracingDialOptions` does
not install the OpenTelemetry client handler, so it creates no client span and
does not propagate the active trace to the server.

```go
import (
    fitgrpc "github.com/gofynd/fit-go/grpc"
    grpc "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

conn, err := fitgrpc.NewClient(
    "dns:///orders:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
    return err
}
defer conn.Close()

// Pass a context carrying the active span to each RPC.
response, err := orders.NewOrdersClient(conn).GetOrder(ctx, request)
```

Code that must call the upstream `grpc.NewClient` directly should append
`fitgrpc.TracingDialOptions()` to its dial options instead:

```go
opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials)}
opts = append(opts, fitgrpc.TracingDialOptions()...)
conn, err := grpc.NewClient(target, opts...)
```

## Kafka

```go
ctx := context.Background()
client, err := kafka.NewConfluentClient(&kafka.Config{
    Brokers:  []string{"localhost:9092"},
    ClientID: "my-service",
})
defer client.Close()

producer, err := client.Producer(kafka.ProducerConfig{})
defer producer.Close()

// acks: -1 = all in-sync replicas, 1 = leader only, 0 = fire-and-forget.
// ProduceCtx creates one producer span per message and injects the configured
// propagator fields without changing keys, values, partitions or acks.
err = producer.ProduceCtx(ctx, "my-topic", []kafka.Message{
    {Key: []byte("k"), Value: []byte(`{"hello":"world"}`)},
}, -1)
```

Use `ProduceCtx`, `ProduceBatchCtx`, and `ConsumeCtx` for application code. For
batch handlers, call `kafka.ConsumeBatchCtx(consumer, handler, opts)`; it works
with the built-in Confluent consumer and adapts alternate drivers without
changing the base interface. The raw `Produce`, `ProduceBatch`, `Consume`, and
`ConsumeBatch` methods are compatibility escape hatches that intentionally omit
trace propagation.

The Confluent driver honors the `acks` argument on every produce call. Because
librdkafka configures acknowledgements per producer rather than per request,
fit-go caches one otherwise-identical producer per requested acknowledgement
level and closes all of them during shutdown. Set `ProducerConfig.AcksSet=true`
when the producer's default must explicitly be `0`; a zero-value config keeps
the safe `-1` default.

## Observability

### Logging

Structured JSON logging with OpenTelemetry trace context:

```go
logger, _ := logging.New(logging.Options{
    Level:    "info",
    Timezone: "UTC",
    Env:      "production",
})

logger.Info("request processed", "user_id", "u123", "duration_ms", 42)
// {"level":"info","timestamp":"2025-01-15T10:30:00Z","message":"request processed","extra":{"user_id":"u123","duration_ms":42}}
```

The platform envelope remains the default. `FIT_LOG_SCHEMA=traceclue` enables
the legacy OTel-shaped envelope. Its default body policy matches TraceClue
3.1.3 (`debug-only`); set `FIT_TRACECLUE_BODY_TRUNCATION=always` for the
TraceClue 3.0.5/2.1.x behavior. See
[TraceClue compatibility](docs/TRACECLUE_COMPATIBILITY.md).

### Tracing

```bash
TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_SERVICE_NAME=my-service
# Optional: tracecontext,baggage (default), b3, b3multi, jaeger, or none
OTEL_PROPAGATORS=tracecontext,baggage
```

```go
tracer, _ := tracing.New(ctx, tracing.Options{
    ServiceName: "my-service",
})
defer tracer.Shutdown(ctx)

ctx, span := tracer.StartSpan(ctx, "process-order", tracing.SpanKindInternal)
span.SetAttribute("order.id", "o-123")
defer span.End()
```

Trace resource identity precedence is `tracing.Options.ServiceName`,
`OTEL_SERVICE_NAME`, `Options.Attributes["service.name"]`,
`OTEL_RESOURCE_ATTRIBUTES=service.name=...`, `SERVICE_NAME`, then
`unknown_service`. `SERVICE_NAME` remains the case-sensitive application
identity used by FIT configuration and is only a telemetry fallback; setting an
explicit `OTEL_SERVICE_NAME` intentionally moves telemetry to that dashboard
identity. The `SERVICE_NAME` fallback is an explicit fit-go improvement over
legacy TraceClue processes that reported `unknown_service:*` when only the FIT
application identity was configured.

Calling `fit.Init` twice without an intervening `Shutdown` returns an error.
Shutdown restores process-global tracing, propagation, metrics, and slog state,
stops periodic health work, clears lifecycle-owned connections/errors/checks,
and permits a clean reinitialization.

### Metrics

```bash
FIT_PROMETHEUS_ENABLED=true
METRICS_DIR=/var/data/metrics
```

```go
framework, _ := fit.Init(ctx)
defer framework.Shutdown(ctx)

// fit.Init installs the enabled registry for fit servers and both HTTP clients.
client := httpclient.NewHTTPClient()
```

With `METRICS_DIR`, fit creates and atomically refreshes a
node-exporter-compatible `.prom` textfile every four seconds. The deployed Node
`prom-file-client` 0.1.1 also uses four seconds and
`<K8S_POD_NAME>-<pid>.prom`, but waits for the first tick and opens with `wx`, so
later refreshes fail. Immediate creation and atomic replacement are intentional
bug fixes, not byte parity. Direct `metrics.New` users may instead expose
`registry.Handler()` for scraping and call `metrics.SetDefault(registry)` to opt
into automatic server/client recording. Periodic write failures are retained by
`LastFlushError` and reported through `Options.OnFlushError` (or a generic safe
error log). See
[the transport and metrics parity notes](docs/OBSERVABILITY_TRANSPORT_METRICS.md).

### Profiling

```bash
PROFILING_ENABLED=true
PROFILING_DISTRIBUTOR_ADDRESS=http://pyroscope:4040
```

## Encryption

AES-256-GCM encryption with pluggable key providers (HashiCorp Vault, GCP KMS):

```go
mgr := encryption.NewManager()
if err := mgr.Init(); err != nil { ... }

encrypted, _ := mgr.Encrypt("sensitive data")
decrypted, _ := mgr.Decrypt(encrypted)
```

## Feature Flags

```bash
FEATURE_FLAG_ENABLED=true
FEATURE_FLAG_SERVER_URL=http://featurehub:8085
FEATURE_FLAG_API_KEY=your-sdk-key
```

```go
client, _ := feature.Init()
if client.IsEnabled("dark-mode") {
    // feature is on
}
```

## Health Checks

```go
checker := health.NewChecker()
checker.AddCheck(func() string {
    if err := db.Ping(); err != nil {
        return "postgres: " + err.Error()
    }
    return ""
})

errors := checker.Check() // empty slice = healthy

checker.StartPeriodicCheck(30)
defer checker.StopPeriodicCheck()
```

`Fit.Shutdown` calls `Health.Reset` automatically. Direct checker owners can
call `Reset` to stop periodic work, remove registered checks, and clear the
health file before reusing the checker.

## Requirements

- Go 1.25+

## License

[Apache License 2.0](LICENSE)
