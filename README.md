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

	// Create an HTTP server
	srv, err := server.New(server.Config{
		Port:   "8080",
		Logger: f.Logger.Slog(),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Register routes
	srv.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "pong"})
	})

	// Start serving
	if err := srv.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
```

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
hosts := cfg.GetStringSlice("ALLOWED_HOSTS", ",", []string{"localhost"})
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
client, err := mongo.Init(cfg)
rwConn := client.GetReadWrite("catalog")
roConn := client.GetReadOnly("catalog")
```

### PostgreSQL

```bash
POSTGRES_ORDERS_READ_WRITE=postgres://admin:secret@localhost:5432/orders?sslmode=disable
POSTGRES_ORDERS_READ_ONLY=postgres://reader:secret@localhost:5432/orders?sslmode=disable
```

```go
client, err := postgres.InitDefault()
rwConn := client.GetReadWrite("orders")
roConn := client.GetReadOnly("orders")
```

### Redis

Supports standalone, cluster, and sentinel modes:

```bash
REDIS_CACHE_READ_WRITE=redis://localhost:6379/0
REDIS_CACHE_READ_ONLY=redis://localhost:6379/0
```

```go
client, err := redis.Init(cfg)
rwConn := client.GetReadWrite("cache")
```

## Kafka

```bash
KAFKA_BROKER_LIST=localhost:9092
```

```go
producer, err := kafka.NewProducer(kafka.ProducerConfig{
    Brokers: cfg.GetString("KAFKA_BROKER_LIST", ""),
})
defer producer.Close()

err = producer.Produce(ctx, "my-topic", key, value)
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

ctx, span := tracer.Start(ctx, "process-order")
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
