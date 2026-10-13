# fit-go

A batteries-included Go framework for building scalable microservices. Provides configuration, HTTP/gRPC servers, database clients, messaging, observability, and security modules with sensible defaults and environment-driven configuration.

## Modules

| Module | Description |
|---|---|
| `config` | Type-safe config from env vars, `.env`, JSON, and YAML files, with strict schema validation |
| `server` | Gin-based HTTP server with multi-type routing, middleware, and payload limits |
| `grpc` | gRPC server plus traced outbound clients via `fitgrpc.NewClient` or `TracingDialOptions`, middleware chains, health checks, and reflection |
| `fitgraphql` | Privacy-safe gqlgen operation and resolver tracing |
| `mongo` | MongoDB connection manager with read/write splitting and pool tuning |
| `postgres` | PostgreSQL client with native pgx/v5 read/write pools, health checks, tracing, and pool metrics |
| `mysql` | MySQL client via `go-sql-driver/mysql` |
| `redis` | Redis client supporting standalone, cluster, and sentinel modes |
| `kafka` | Kafka producer/consumer with SASL/SSL and OpenTelemetry tracing |
| `groupcache` | Distributed in-process caching with Kubernetes peer discovery |
| `logging` | Structured JSON logger with timezone support and trace propagation |
| `tracing` | OpenTelemetry tracing with OTLP export |
| `metrics` | Prometheus metrics for HTTP server and client instrumentation |
| `otelmetrics` | Generic OpenTelemetry metrics SDK with OTLP and console export |
| `instrumentation` | Statically linked, typed instrumentation extension registry |
| `profiling` | Continuous profiling via Pyroscope (CPU, heap, wall-clock) |
| `errors` | Structured error codes with multilingual messages and Sentry integration |
| `encryption` | AES-256-GCM encryption with Vault and GCP KMS key providers |
| `feature` | FeatureHub SSE client with client/server evaluation contexts |
| `health` | Health check orchestration across all connections |
| `migration` | Compiled, checkpointed migrations with local, GCS, and S3 state stores |
| `protofetch` | Safe `fitproto`-compatible contract fetching and Go generation |
| `utils` | HTTP client, string helpers, and input sanitization |
| `httpclient` | Explicitly instrumented `net/http` transport with trace propagation, request IDs, and optional logs/metrics |
| `redact` | Bounded text, URL, header, and credential redaction helpers; not a universal PII detector |

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

This quick start intentionally uses the original `fit.Init` and `server.New`
contracts. Upgrading the library does not automatically select the managed
lifecycle or instrument the legacy server. For new opt-in capabilities, use
`fit.InitManaged` with the relevant module constructor, such as
`server.NewRuntime`, `kafka.NewProducer`, or `utils.NewInstrumentedHTTPClient`.
See [API names](docs/API_NAMING.md) and the
[compatibility migration map](docs/UPSTREAM_INTEGRATION_MIGRATION.md).

See [`examples/`](examples) for complete programs covering selected modules.
Documentation describes this source tree; an immutable published tag can
precede local fixes. Check the [changelog](CHANGELOG.md) and
[remediation ledger](docs/PR3_COMPATIBILITY_REMEDIATION.md) for publication status,
accepted limitations, and outstanding release/security gates.

## Examples

The [`examples/`](examples) directory contains standalone, runnable programs —
each demonstrates one part of the framework. Run any of them from the repo root:

```sh
go run ./examples/01-quickstart
```

Most are configured through environment variables (the header comment in each
`main.go` lists the relevant ones). Examples that talk to external systems
(databases, Kafka, Pyroscope) need their dependencies and credentials configured
to run successfully. Compiling an example does not prove connectivity or runtime
behavior. Use disposable local systems, not production, for examples that write
data or produce/consume messages.

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

Fork capabilities use descriptive, capability-based names. Former `Advanced`
symbols remain deprecated aliases for source compatibility; see
[`docs/API_NAMING.md`](docs/API_NAMING.md).

All modules are configured via environment variables, following the [12-factor app](https://12factor.net/config) methodology. The compatibility loader `config.Load` preserves the released precedence:

1. `.env` file (if present)
2. OS environment variables
3. Config files (JSON/YAML) passed to `config.Load()`, with the first file defining a key winning

Environment variables always take precedence over file values.

```go
cfg, err := config.Load("config.json")

port := cfg.GetInt("PORT", 8080)
debug := cfg.GetBool("DEBUG", false)
name := cfg.GetString("SERVICE_NAME", "my-service")
hosts := cfg.GetStringSlice("ALLOWED_HOSTS", []string{"localhost"})
ttl := cfg.GetDuration("CACHE_TTL", 5 * time.Minute)
```

Use `config.LoadStrict(...)` for strict dotenv/YAML parsing and documented
last-config-file-wins overlays. `fit.InitManaged` selects that strict loader;
the original `fit.Init` continues to select `config.Load`.

For Convict/Pydantic-style validation, define a strict schema and apply it
before constructing dependencies. Defaults are installed only after the whole
snapshot validates, and sensitive custom-validation failures are redacted.

```go
schema, err := config.NewSchema(
    config.SchemaField{Key: "PORT", Kind: config.IntKind, Required: true},
    config.SchemaField{Key: "CACHE_TTL", Kind: config.DurationKind,
        Default: "5m", HasDefault: true},
)
if err != nil { return err }
resolved, err := cfg.ApplySchema(schema)
if err != nil { return err }
```

Application-specific Convict/Pydantic formats still require an explicit
`SchemaField.Parser`; fit-go does not guess language-specific coercion.

For Google Secret Manager, `config.GetSecretFromGSM(name, version)` retains the
original **base64-encoded** transport payload. Use the additive
`config.GetDecodedSecretFromGSM(name, version)` when you need the decoded secret;
do not decode its result again. Both require access to the configured GCP project
and credentials. Never log either return value.

## Platform Boundaries And Tooling

### GraphQL

Register `fitgraphql.New` on each gqlgen server. It creates operation spans;
resolver spans are an explicit `FieldSpans` opt-in to avoid unbounded span
volume on large selections. The extension deliberately cannot capture GraphQL documents, variables,
arguments, response values, or raw error text. Client-supplied operation names
are omitted by default; `Options.OperationName` may map persisted or allowlisted
names to bounded telemetry identities.

```go
graphqlServer := handler.NewDefaultServer(schema)
resolverSpans := true
graphqlServer.Use(fitgraphql.New(fitgraphql.Options{FieldSpans: &resolverSpans}))
```

### Workers, Crons, And Jobs

Non-HTTP entry points need an explicit boundary. Names and attributes must be
stable operational metadata, never payloads or user identifiers.

```go
err := tracing.RunBoundary(ctx, tracing.BoundaryOptions{
    Type: tracing.BoundaryCron,
    Name: "expire-orders",
}, func(ctx context.Context) error {
    return expireOrders(ctx)
})
```

The wrapper keeps the native OTel span active, bridges fit-go logging and
transport context, marks generic failures, rethrows panics, and preserves the
original result or error.

### Typed Instrumentation Extensions

Go cannot safely reproduce TraceClue's runtime `package:Class` loader. Register
known factories at compile time, then allow the legacy env variables to select
only those names and aliases:

```go
registry := instrumentation.NewRegistry()
err := registry.Register(instrumentation.Registration{
    Name: "custom-client",
    Aliases: []string{"company/client:Instrumentation"},
    Factory: newCustomClientHook,
})
framework, err := fit.InitManaged(ctx, fit.WithInstrumentationRegistry(registry))
```

Unknown names/config fail startup. Hooks start deterministically, roll back on
partial failure, and stop in reverse order. Legacy TraceClue env is interpreted
only when a registry/options or `FIT_INSTRUMENTATION_ENABLED=true` explicitly
activates this facility, so stale legacy variables do not break unrelated boots.

### Migrations

Applications compile migration functions into their own command and construct a
`migration.Runner` with an explicit store and lock. Local state uses atomic
JSON replacement and a sibling advisory lock. `migration/cloudstore` supports
`gs://` and `s3://` buckets, but cloud runs require a renewable `LeaseLocker`, a
monotonic fence token, and an application-owned `FencedWriteFunc` that validates
the token atomically with each state write. A plain object write or expiring
`SET NX` mutex is not sufficient because a stale runner can outlive its lock.

The runner imports legacy Node/pyfit state, normalizes their migration IDs,
checks optional checksums, checkpoints each successful step, and refuses
unknown applied migrations. Every `Up` and `Down` must remain idempotent because
an application mutation can succeed before a state-store checkpoint fails.
Cloud migration functions can read `migration.FenceTokenFromContext`; they must
validate that token in the same datastore transaction or conditional mutation
when stale application writes also need to be prevented.
`migration/migrationcli` supplies `list`, `run`, `revert`, `revert-one`, and
safe Go skeleton creation for an application-linked binary.

### Proto Contracts

`go run github.com/gofynd/fit-go/cmd/fitproto get` and `getall` read the legacy
`fit.config.json` API specification. The tool clones without a shell, rejects
path traversal, unsafe output roots, symlinks, and non-regular generated files;
serializes writers; fsyncs staged output; and transactionally replaces the
output with sibling-renamed backup recovery. Generator or replacement failures
preserve or restore the prior output. It can invoke `buf` with an explicit
template or `protoc`. CI should pin the contract repository branch or revision
policy and review generated diffs.

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
write := conn.Write // *pgxpool.Pool for writes
read := conn.Read   // *pgxpool.Pool for reads
```

PostgreSQL initialization is explicit. To let the top-level framework own the
pools, health check, metrics lifecycle, and shutdown, opt in during `fit.InitManaged`:

```go
framework, err := fit.InitManaged(ctx, fit.WithPostgresPoolOptions(postgres.ConnectionPoolOptions{}))
orders := framework.Postgres.Service("orders")
defer framework.Shutdown(ctx)
```

Calling `fit.InitManaged(ctx)` without `fit.WithPostgresPoolOptions(...)` does not connect to
PostgreSQL. Direct `postgres.InitWithContext` usage remains supported for
applications that manage database lifecycle themselves.

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

`redis.InitDefault` retains the original go-redis implementation. Protocol,
retry, strict TLS, and ioredis compatibility controls are explicit:

```go
client, err := redis.InitWithCompatibility(redis.CompatibilityOptions{
    Context: ctx,
    DialWithSettings: redis.DefaultConfiguredDialFunc(),
    ClusterWithSettings: redis.DefaultConfiguredClusterDialFunc(),
    SentinelWithSettings: redis.DefaultConfiguredSentinelDialFunc(),
    ProtocolByService: map[string]redis.RedisProtocol{
        "cache": redis.RedisProtocolRESP2,
    },
    IORedisCompatibility: map[string]redis.IORedisCompatibilityProfile{
        "cache": redis.IORedisCompatibilityV5,
    },
})
if err != nil { return err }
defer client.Close()
```

Selecting a protocol alone does not enable the owned ioredis transport; the
per-service profile does. This is not a general drop-in ioredis replacement:
transactions, SELECT, subscription modes, unbounded blocking operations, and
cross-node pipelines are rejected. The owned RESP decoder has fixed size/value
limits, and replay of an ambiguous non-idempotent command can duplicate its
side effects. Review the [full compatibility boundary](docs/IOREDIS_COMPATIBILITY.md)
before adoption; the default go-redis path is unaffected.

## HTTP Server And Clients

Use `server.NewRuntime` when you explicitly want automatic request IDs, OTel
server spans/active-context bridging, and adoption of the process-default FIT
metrics registry. Tracing and metrics still require their respective enablement
settings. `server.New` preserves original-main behavior instead.

```go
framework, err := fit.InitManaged(ctx)
if err != nil { return err }
defer framework.Shutdown(ctx)

srv := server.NewRuntime(server.RuntimeConfig{
    Config: server.Config{Port: "8080", HealthChecker: framework.Health},
})
if err := srv.Init(
    map[server.ServerType]http.Handler{server.ServerTypeDefault: engine},
    nil, nil,
); err != nil { return err }
```

Start and stop the HTTP server explicitly; the framework does not own this
separately constructed server. Pass `framework.Health` explicitly to use its
checks; a nil checker selects the server package's separate default. Use a
dedicated bounded shutdown context, not an already cancelled request context.
`RuntimeConfig` also controls CORS, independent
readiness, timeouts, logging, and profiling. Only exact registered health routes
receive the default health exception. Applications needing legacy case/slash/
HEAD/OPTIONS variants can call `UseHealthRouteMiddleware` **before `Init`**;
that application-owned middleware must call `Next` for every request it does not
own. It is not a blanket health-path auth bypass.

For a standard `*http.Client`, choose an explicit instrumented transport:

```go
client := httpclient.NewHTTPClientWithTimeout(10 * time.Second)
// Or instrument an existing transport without changing its client timeout:
existingClient.Transport = httpclient.WrapTransport(existingClient.Transport)
```

`httpclient.NewHTTPClient()` itself has no overall request timeout; choose the
timeout constructor or a request deadline. `WithLogger` opts into safe outbound
logs; `WithMetrics(nil)` explicitly disables process-default metrics for that
transport. Requests and headers are cloned, explicit span contexts remain
authoritative, and caller-supplied propagation fields are replaced by default.
`WithCallerPropagationHeaders` is a trusted-forwarding opt-in, not suitable for
untrusted inbound headers. Logs/spans omit bodies and query values and sanitize
URLs/errors; do not put secrets in arbitrary identifiers or telemetry labels.

For FIT-style request helpers and interceptors, use
`utils.NewInstrumentedHTTPClient(utils.HTTPClientOptions{Timeout: 10 * time.Second})`.
The original `utils.NewHTTPClient` remains uninstrumented and does not
automatically adopt process-default metrics.

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

An RPC context with its own span remains authoritative. When it has no span,
the fit-go client can fill the missing parent from a fit-go consumer or worker
boundary active on the same goroutine while retaining the supplied context's
cancellation, deadline, and values.

Code that must call the upstream `grpc.NewClient` directly should append
`fitgrpc.TracingDialOptions()` to its dial options instead:

```go
opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials)}
opts = append(opts, fitgrpc.TracingDialOptions()...)
conn, err := grpc.NewClient(target, opts...)
```

For FIT-style runtime proto loading, call `AddServiceDefinitions` before
`Start`. fit-go lazily compiles the configured proto at dynamic registration,
registers unary methods on the native server, and executes callback middleware
such as `AuthorizeJWTToken` on the real network path. Generated-only users of
`GRPCServer()` do not require the source proto or its imports at runtime.

Dynamic handler maps preserve FIT/proto-loader semantics: proto field names,
string representations for 64-bit integers and enums, byte slices, collection
defaults, oneof discriminator fields, and JSON-compatible Struct/Value/ListValue
well-known types. Unknown response keys are ignored so additive application data
does not break an older wire contract. Internal, Unknown, and DataLoss status text
is sanitized before it crosses the dynamic RPC network boundary. The original
in-process callback API retains its detailed handler/validation/panic messages;
`InitWithOptions` selects the hardened callback behavior. Generated handlers
registered directly through `GRPCServer()` retain their own error policy.
Companion `.type.json` files are optional validation input rather than a
runtime requirement; descriptor validation remains authoritative.

Use `AddServiceDefinitionsWithOptions` for a custom error mapper. Streaming
services use generated registration through `GRPCServer()`;
Pass `ServerOptions.UnaryInterceptors` and
`ServerOptions.StreamInterceptors` through `InitWithOptions`; the original
`Init(Config)` contract remains unchanged for existing callers.

## Kafka

Fork consumers moving to the integrated upstream module should also read the
[fork-to-upstream migration map](docs/UPSTREAM_INTEGRATION_MIGRATION.md).

```go
ctx := context.Background()
client, err := kafka.NewConfluentClient(&kafka.Config{
    Brokers:  []string{"localhost:9092"},
    ClientID: "my-service",
})
if err != nil { return err }
defer client.Close()

// Explicit options constructor: per-call acknowledgements and raw tracing.
producer, err := kafka.NewProducer(client, kafka.ProducerOptions{Acks: -1})
if err != nil { return err }
defer producer.Close()
if err := producer.Connect(); err != nil { return err }

// acks: -1 = all in-sync replicas, 1 = leader only, 0 = fire-and-forget.
// ProduceCtx creates one producer span per message and injects the configured
// propagator fields without changing keys, values, partitions or acks.
err = kafka.ProduceCtx(producer, ctx, "my-topic", []kafka.Message{
    {Key: []byte("k"), Value: []byte(`{"hello":"world"}`)},
}, -1)
if err != nil { return err }
```

Use `ProduceCtx`, `ProduceBatchCtx`, and `ConsumeCtx` for application code. For
batch handlers, call `kafka.ConsumeBatchCtx(consumer, handler, opts)`; it works
with the built-in Confluent consumer and adapts alternate drivers without
changing the base interface. Tracing is constructor- and driver-dependent:
original Confluent `client.Producer`/`client.Consumer` raw methods remain
untraced. Options-based producers and consumers add raw-call tracing and adopt
the same-goroutine active FIT boundary. Context helpers prefer native driver
extensions; producer helpers fall back to raw methods on older drivers and do
not promise tracing or context support that those drivers lack.

Options-based Confluent producers honor the per-call `acks` argument. Because
librdkafka configures acknowledgements per producer rather than per request,
fit-go caches one otherwise-identical producer per requested acknowledgement
level and closes all of them during shutdown. When the producer's default must
explicitly be `0`, construct it with `NewProducer` and set
`ProducerOptions.AcksSet=true`; a zero-value original `ProducerConfig`
keeps the safe `-1` default.

The original `client.Producer(ProducerConfig)` retains its configured
acknowledgement level, including through `ProduceCtx`; it does not allocate
alternate producers for per-call `acks`. Original Confluent single-message
consumers log and continue after handler errors. Use
`kafka.NewConsumer(client, ConsumerSettings)`
and `ConsumeCtxWithOptions`/`ConsumeBatchCtxWithOptions` for explicit offset,
finalizer, concurrency, and backend controls. Unsupported controls fail rather
than being silently ignored. The KafkaJS-compatible franz backend is opt-in;
rebalance hooks, commit timing, rewind, and shutdown limitations are documented
in the [migration map](docs/UPSTREAM_INTEGRATION_MIGRATION.md).

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
// Upstream platform JSON: {"level":"info","message":"request processed",...}
```

`logging.New(Options)` retains the upstream platform envelope and ignores
runtime-only TraceClue controls. Use `logging.NewRuntime(RuntimeOptions{})`
to opt into the TraceClue envelope; that entry point also honors
`FIT_LOG_SCHEMA`. Its default body policy matches TraceClue
3.1.3 (`debug-only`); use `always` for TraceClue 3.0.5/2.1.x or `non-debug` for
pyfit 1.10 queue formatting. See
[TraceClue compatibility](docs/TRACECLUE_COMPATIBILITY.md).

Ordinary application string fields remain caller-owned: using a logger does not
make arbitrary payloads safe to log. Keep credentials and PII out of log values;
use bounded `redact` helpers at explicit boundaries, not as a universal detector.

### Tracing

```bash
TRACING_ENABLED=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_SERVICE_NAME=my-service
# Optional: tracecontext,baggage (default), b3, b3multi, jaeger, or none
OTEL_PROPAGATORS=tracecontext,baggage
```

The default activation contract requires `TRACING_ENABLED=true`. A pyfit port
that must preserve pyfit's historical activation behavior can opt in with
`FIT_TRACING_ACTIVATION_MODE=pyfit`: tracing is enabled when
`OTEL_SDK_DISABLED` is absent or empty and disabled by every non-empty value,
including the literal value `false`. Root `fit.InitManaged` and direct
`tracing.NewSDK` use the same rule. The original `fit.Init` and
`tracing.New` retain official-main activation behavior.

When `fit.InitManaged` receives `WithConfigPaths`, the OTel tracing, sampler,
propagator, exporter, endpoint/protocol, resource-attribute, and SDK-disable
settings are resolved from the merged config as well as the process environment;
an explicitly set environment variable retains precedence.

```go
tracer, _ := tracing.NewSDK(ctx, tracing.SDKOptions{
    ServiceName: "my-service",
})
defer tracer.Shutdown(ctx)

ctx, span := tracer.StartSpan(ctx, "process-order", tracing.SpanKindInternal)
span.SetAttribute("order.id", "o-123")
defer span.End()
```

Trace resource identity precedence is `tracing.SDKOptions.ServiceName`,
`OTEL_SERVICE_NAME`, `SDKOptions.Attributes["service.name"]`,
`OTEL_RESOURCE_ATTRIBUTES=service.name=...`, `SERVICE_NAME`, then
`unknown_service`. `SERVICE_NAME` remains the case-sensitive application
identity used by FIT configuration and is only a telemetry fallback; setting an
explicit `OTEL_SERVICE_NAME` intentionally moves telemetry to that dashboard
identity. The `SERVICE_NAME` fallback is an explicit fit-go improvement over
legacy TraceClue processes that reported `unknown_service:*` when only the FIT
application identity was configured.

Calling `fit.InitManaged` twice without an intervening `Shutdown` returns an
error. Managed shutdown restores owned tracing-provider, metrics, and slog
state without overwriting independent replacements. The stateless propagator
is deliberately retained during graceful drain; a `tracing.SetGlobal` restore
callback restores its own propagation ownership separately. Managed shutdown
stops periodic health work, clears lifecycle-owned
connections/errors/checks, and permits a clean reinitialization. Repeated
original `fit.Init` calls retain official-main behavior. Switching between
original and managed lifecycle modes requires `Shutdown` first.

### Metrics

Generic OpenTelemetry metrics are separate from the legacy FIT Prometheus
histograms and can be enabled independently:

```bash
OTEL_METRICS_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_METRIC_EXPORT_INTERVAL=60000
OTEL_METRIC_EXPORT_TIMEOUT=30000
```

`fit.InitManaged` installs the resulting meter provider process-wide and shuts it down
with ownership-safe restoration. A stable routing provider rebinds synchronous
instruments and observable callbacks across repeated SDK lifecycles, including
instruments created before initialization; equivalent meter scopes are reused.
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` and
`OTEL_EXPORTER_OTLP_METRICS_PROTOCOL` override their common equivalents.
`console`, comma-separated exporters, injected readers/exporters, views, and
`OTEL_SDK_DISABLED` are supported. Supplying a custom reader/exporter enables
the provider unless `Enabled=false` is explicit. Runtime/exporter errors can be
routed through `Options.ErrorHandler`; root `fit.InitManaged` logs only their type.
Export remains opt-in so upgrading fit-go does not unexpectedly connect a
service to a local collector.

The deployed FIT Prometheus contract remains available separately:

```bash
FIT_PROMETHEUS_ENABLED=true
METRICS_DIR=/var/data/metrics
# Explicitly opt into periodic node-exporter textfile writes:
FIT_PROMETHEUS_TEXTFILE_ENABLED=true
```

```go
framework, err := fit.InitManaged(ctx)
if err != nil { return err }
defer framework.Shutdown(ctx)

// NewRuntime servers and explicitly instrumented HTTP clients adopt the registry.
client := httpclient.NewHTTPClient()
```

Instrumented outbound clients preserve an explicit request context as the
source of truth. If it has no span while running inside a fit-go consumer or
worker boundary, HTTP and the fit-go database/gRPC/Kafka clients adopt the
same-goroutine active span and baggage without replacing the supplied context's
cancellation, deadline, or values. This compatibility bridge does not cross a
new goroutine; detached work must carry an explicit retained context.

With `FIT_PROMETHEUS_TEXTFILE_ENABLED=true` and `METRICS_DIR`, fit creates and
atomically refreshes a
node-exporter-compatible `.prom` textfile every four seconds. The deployed Node
`prom-file-client` 0.1.1 also uses four seconds and
`<K8S_POD_NAME>-<pid>.prom`, but waits for the first tick and opens with `wx`, so
later refreshes fail. Immediate creation and atomic replacement are intentional
bug fixes, not byte parity. Direct `metrics.New` users may instead expose
`registry.Handler()` for scraping and call `metrics.SetDefault(registry)` to opt
into automatic server/client recording. Direct textfile users call
`metrics.NewTextfileRegistry`; periodic write failures are retained by `LastFlushError`
and reported through `TextfileOptions.OnFlushError` (or a generic safe error
log). See
[the transport and metrics parity notes](docs/OBSERVABILITY_TRANSPORT_METRICS.md).

### Profiling

```bash
PROFILING_ENABLED=true
PROFILING_DISTRIBUTOR_ADDRESS=http://pyroscope:4040
PROFILING_SAMPLE_RATE=10
```

`PROFILING_SAMPLE_RATE` is retained as a legacy requested value. The current
`pyroscope-go` runtime samples at a fixed effective 100 Hz and ignores its
deprecated sample-rate field. `profiling.NewRuntime(profiling.DefaultRuntimeConfig())`
and the managed framework profiler report `requested`,
`effective`, and `configurable=false` separately instead of claiming the legacy
value changed collection behavior.

Original `profiling.New`/`NewFromEnv` status retains its historical JSON shape.
Use `GetRuntimeConfig` for the full runtime configuration. Profiling control
routes are not a public API: mount/enable them explicitly behind authentication
and a restricted operational network.

Use `profiling.TagWrapper` to attach scoped Pyroscope and pprof labels to a
work unit without mutating process-global profiling state.

### Error Reporting

`errors.InitSentry` and `errors.InitSentryWithConfig` preserve the released
first-call-wins contract: an initial missing DSN is a no-op and an SDK
initialization failure is returned, but neither path retries on a later legacy
call. Applications that need retryable startup should use
`errors.InitSentryWithHooks`; that additive initializer can be called again
until configuration is available and initialization succeeds. Error events and
explicitly enabled transaction events are sanitized both before and after
optional caller hooks: request
bodies/query strings/cookies and user data are removed, while sensitive keyed
values and secret or PII text are redacted. Bounded reflection also sanitizes
typed nested values and cycles without invoking custom string or marshal code.
Sentry logs and metrics are disabled because fit-go uses OpenTelemetry for
those signals.

Use `errors.WithSentryContext` for request-local tags, extras, correlation IDs,
and breadcrumbs. It clones the request hub so concurrent requests do not share
scope mutation. Capture methods retain the original Go error object for Sentry
grouping and application control flow.

The sanitizer rebuilds SDK serialization caches so pre-serialized fields cannot
bypass sanitization while retaining safe correlation IDs. The post-rc.5
hardening included in release candidate `v0.2.0-rc.6` additionally masks credential-key components such as `pwd`,
`pass`, `pin`, `otp`, `cvv`, `cvc`, `pan`, and compound card-number labels before
examining their values, including numeric values. This additional masking changes
diagnostic output, not application return values; the older immutable rc.5 does
not contain it. An application that
initializes Sentry directly must install its own sanitizer; a library upgrade
cannot attach fit-go hooks to an independently configured SDK client. See the
[publication and privacy ledger](docs/PR3_COMPATIBILITY_REMEDIATION.md).

## Encryption

AES-256-GCM encryption with pluggable key providers (HashiCorp Vault, GCP KMS):

> Compatibility warning: the provider supplies a fixed IV, matching the legacy
> fit.js/pyfit ciphertext format. Reusing an AES-GCM nonce with one key is unsafe
> for new designs. Keep this API for existing ciphertext interoperability and
> migrate new data to a versioned format with a fresh random nonce per value.

```go
mgr := encryption.NewManager()
if err := mgr.Init(); err != nil { ... }

encrypted, _ := mgr.Encrypt("sensitive data")
decrypted, _ := mgr.Decrypt(encrypted)
```

## Feature Flags

Original `feature.Init()` retains synchronous initial fetching and polling.
`feature.InitStreaming()` explicitly selects SSE; unless initial state is
required, it returns before the stream is ready. Handle initialization errors
and use a bounded readiness wait before depending on the values:

```bash
FEATURE_FLAG_ENABLED=true
# A trailing /features or /features/ is also accepted, as in the JS SDK.
FEATURE_FLAG_URL=http://featurehub:8085
# Keys containing * use client-side rollout evaluation. Other keys use
# server-side evaluation and send x-featurehub context as both the legacy
# EventSource query parameter and request header.
FEATURE_FLAG_API_KEY=your-sdk-key
# Optional: make initial FeatureHub state a required startup dependency.
FEATURE_FLAG_REQUIRE_INITIAL_STATE=false
```

```go
client, err := feature.InitStreaming()
if err != nil { return err }
if client == nil { return nil } // Disabled or optional configuration absent.
defer client.Stop()

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
if err := client.WaitReady(ctx); err != nil { return err }
if client.IsEnabled("dark-mode") {
    // feature is on
}

requestFlags := client.NewContext().UserKey("user-123").Attribute("plan", "gold")
if err := requestFlags.Build(ctx); err != nil { return err }
if requestFlags.IsEnabled("dark-mode") {
    // Evaluate this request's isolated context, not the client-wide user.
}
```

Do not evaluate a changed request context until `Build` succeeds. Server-evaluated
`Build` fetches a separate SSE snapshot; it does not reuse another user's values.
The SDK key is part of the URL path, and server-evaluation context is also sent
in the query string for JS compatibility: protect proxy/access logs accordingly.
Permanent 4xx responses, including 404 (but excluding transient statuses such as
408 and 429), terminate the shared stream and notify
waiters; transient failures use bounded backoff. These and the per-Build
connection trade-off are detailed in the
[migration and readiness notes](docs/UPSTREAM_INTEGRATION_MIGRATION.md).

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

Only the managed framework lifecycle calls `Health.ResetContext(ctx)` during
shutdown; original `fit.Init` retains its historical tracer/metrics-only
shutdown. Direct checker owners can use `ResetContext(shutdownCtx)` or
`StopPeriodicCheckContext(shutdownCtx)` to bound the wait for in-flight checks.
`Reset`/`StopPeriodicCheck` use an unbounded background context. Stopping cannot
forcibly terminate a user check or filesystem operation already running; checks
must provide their own bounds. Unbounded `Reset` waits for cleanup and removes
registered checks and the health file. `ResetContext` clears registered checks,
but file cleanup is best-effort after a deadline; handle its error and do not
assume cleanup has finished before reusing the checker.

## Requirements

- Go **1.25.10** or newer (the module minimum).
- A supported CGO/C toolchain for the bundled Confluent/librdkafka integration;
  platform-specific builds may require additional librdkafka setup.

The module minimum is a compatibility contract, not a security certification.
Build releases with a current patched Go toolchain and check the actual module
graph with `govulncheck`. Outstanding dependency advisories and intentionally
deferred upgrades are recorded in the
[remediation ledger](docs/PR3_COMPATIBILITY_REMEDIATION.md).

## License

[Apache License 2.0](LICENSE)
