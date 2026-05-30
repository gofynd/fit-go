# fit-go examples

Each subdirectory is a standalone, runnable program demonstrating one part of
the framework. Run any example from the repository root:

```sh
go run ./examples/01-quickstart
```

Most examples are configured through environment variables (12-factor style);
the header comment in each `main.go` lists the relevant ones. Examples that
talk to external systems (databases, Kafka, Pyroscope) need those services
reachable to do real work, but all of them compile and start without extra
setup.

| Example | Module(s) | What it shows |
|---|---|---|
| [01-quickstart](01-quickstart) | `fit` | Initialize the framework and shut down gracefully on a signal |
| [02-config](02-config) | `config` | Load config, typed getters, validation rules |
| [03-logging](03-logging) | `logging` | Structured JSON logging, derived loggers, trace context |
| [04-http-server](04-http-server) | `server` | Gin-based HTTP server, routing by ServerType, health routes |
| [05-databases](05-databases) | `mongo`, `postgres`, `redis`, `health` | Connect with read/write split and aggregate health checks |
| [06-kafka](06-kafka) | `kafka` | Produce and consume messages with the confluent driver |
| [07-caching](07-caching) | `groupcache` | Read-through distributed cache with a single-flight loader |
| [08-encryption](08-encryption) | `encryption` | AES-256-GCM encrypt/decrypt with pluggable key providers |
| [09-errors](09-errors) | `errors` | Structured error codes with localized messages |
| [10-observability](10-observability) | `tracing`, `metrics`, `profiling` | Spans, a Prometheus endpoint, and continuous profiling |

## Other modules

A few smaller modules are not given their own example but are easy to use:

- **`feature`** — feature flags: `c, _ := feature.Init(); if c.IsEnabled("dark-mode") { ... }`
- **`utils`** — an instrumented HTTP client (`utils.NewHTTPClient`) plus input
  sanitization helpers (`utils.SanitizeString`, `utils.DetectThreats`).
- **`grpc`** — a gRPC server with JWT auth and health checks
  (`grpc.Init`, `grpc.AuthorizeJWTToken`).

See the [top-level README](../README.md) for the full module overview.
