# fit-go examples

Each subdirectory is a standalone, runnable program demonstrating one part of
the framework. Run any example from the repository root:

```sh
go run ./examples/01-quickstart
```

Most examples are configured through environment variables (12-factor style);
the header comment in each `main.go` lists the relevant ones. Examples that
talk to external systems (databases, Kafka, Pyroscope) need those services
reachable and their credentials/configuration supplied to run successfully.
Use disposable local dependencies: some examples write data or Kafka messages.
Compilation alone does not prove connectivity or runtime behavior.

| Example | Module(s) | What it shows |
|---|---|---|
| [01-quickstart](01-quickstart) | `fit` | Initialize the framework and shut down gracefully on a signal |
| [02-config](02-config) | `config` | Load config, typed getters, validation rules |
| [03-logging](03-logging) | `logging` | Structured JSON logging, derived loggers, trace context |
| [04-http-server](04-http-server) | `server` | Gin-based HTTP server, routing by ServerType, health routes |
| [05-databases](05-databases) | `mongo`, `postgres`, `redis`, `health` | Connect with read/write split and aggregate health checks |
| [06-kafka](06-kafka) | `kafka` | Context-aware produce and consume with the confluent driver |
| [07-caching](07-caching) | `groupcache` | Read-through distributed cache with a single-flight loader |
| [08-encryption](08-encryption) | `encryption` | AES-256-GCM encrypt/decrypt with pluggable key providers |
| [09-errors](09-errors) | `errors` | Structured error codes with localized messages |
| [10-observability](10-observability) | `tracing`, `metrics`, `profiling` | Spans, a Prometheus endpoint, and continuous profiling |

## Other modules

A few smaller modules are not given their own example but are easy to use:

- **`feature`** — `feature.Init` retains synchronous initial fetch/polling;
  `feature.InitStreaming` opts into SSE. Check initialization errors and use
  a bounded `WaitReady`/request-context `Build` before depending on flags.
- **`httpclient`** — explicitly instrument standard `net/http` clients with
  `NewHTTPClientWithTimeout` or `WrapTransport`.
- **`utils`** — FIT-style HTTP helpers, interceptors, and input sanitization
  (`utils.SanitizeString`, `utils.DetectThreats`). `utils.NewHTTPClient` retains
  its original uninstrumented behavior; `utils.NewInstrumentedHTTPClient`
  explicitly adds tracing, request IDs, and process-default metrics.
- **`grpc`** — a gRPC server with JWT auth and health checks
  (`grpc.Init`, `grpc.AuthorizeJWTToken`).

See the [top-level README](../README.md) for the full module overview.

Examples intentionally mix original compatibility constructors and opt-in APIs.
Changing `fit.Init` to `InitManaged` alone does not instrument `server.New` or
`utils.NewHTTPClient`; choose `server.NewRuntime` or the instrumented client
explicitly. The Kafka demo retains original constructors (including configured
producer acknowledgements and continued consumption after handler errors) and
uses an external shutdown timer, not a handler error, to end its consume window.
The observability example's `profiling.NewFromEnv` retains legacy status JSON;
use `profiling.NewRuntime` for requested/effective sample-rate fields. Never
expose profiler control routes on an unrestricted production listener.
