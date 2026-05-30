# fit-go

A batteries-included Go framework for building scalable microservices. Provides configuration, HTTP/gRPC servers, database clients, messaging, observability, and security modules with sensible defaults and environment-driven configuration.

## Modules

| Module | Description |
|---|---|
| `config` | Type-safe config from env vars, `.env`, JSON, and YAML files |
| `server` | Gin-based HTTP server with multi-type routing, middleware, and payload limits |
| `grpc` | gRPC server with middleware chains, health checks, and reflection |
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
| [06-kafka](examples/06-kafka) | `kafka` | Produce and consume messages with the confluent driver |
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

## Kafka

```go
client, err := kafka.NewConfluentClient(&kafka.Config{
    Brokers:  []string{"localhost:9092"},
    ClientID: "my-service",
})
defer client.Close()

producer, err := client.Producer(kafka.ProducerConfig{})
defer producer.Close()

// acks: -1 = all in-sync replicas, 1 = leader only, 0 = fire-and-forget.
err = producer.Produce("my-topic", []kafka.Message{
    {Key: []byte("k"), Value: []byte(`{"hello":"world"}`)},
}, -1)
```

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
// {"level":"info","timestamp":"2025-01-15T10:30:00Z","msg":"request processed","user_id":"u123","duration_ms":42}
```

### Tracing

```bash
TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_SERVICE_NAME=my-service
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

### Metrics

```bash
FIT_PROMETHEUS_ENABLED=true
```

```go
registry, _ := metrics.New(metrics.Options{
    ServerEnabled:     true,
    HTTPClientEnabled: true,
})
// Prometheus metrics are automatically collected for HTTP server and client
```

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
```

## Requirements

- Go 1.25+

## License

[Apache License 2.0](LICENSE)
