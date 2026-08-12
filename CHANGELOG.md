# Changelog

All notable changes to this fork of `github.com/gofynd/fit-go` are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/);
this fork follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **KafkaJS-compatible rolling consumer backend**: an explicit
  `ConsumerBackendKafkaJSCompatible` opt-in uses KafkaJS 2.x's literal
  `RoundRobinAssigner` group protocol while retaining fit-go's message, batch,
  tracing, offset-finalizer, TLS, and SASL APIs. The existing
  Confluent/librdkafka producer and default consumer are unchanged.
- **Fail-closed ioredis compatibility state machine**: a separate, explicitly
  constructed compatibility client now models ioredis 5.11.1's connection-wide
  offline FIFO, exact reconnect curve and 20-retry flush boundary, replay-before-
  offline ordering, lost-reply duplicate risk, partial-pipeline abort behavior,
  Promise-like settlement, and quit/disconnect boundaries. An explicitly
  selected owned standalone RESP2 transport now supplies AUTH/SELECT/client
  metadata/INFO readiness, TLS, socket timeout, RESP parsing, and exact
  not-written/partial-write/full-write-lost-reply evidence without using
  go-redis. The owned transport has a private asynchronous write/read ledger:
  multiple direct commands can cross one socket before earlier replies settle,
  and every fully or partially written request in the uncertain suffix replays
  after connection loss. Compatibility submissions also preserve ioredis's
  lowercase command-name wire bytes. It still has no environment or production
  wiring; full live fault differentials, Cluster, and Sentinel remain
  fail-closed gates.
- **Opt-in Redis retry controls**: per-service/read-write environment settings
  now expose go-redis command retry count/backoff separately from dial retry
  count/backoff for standalone, cluster, and Sentinel clients. Existing callers
  retain go-redis defaults. These controls deliberately do not claim ioredis
  offline-queue parity: go-redis retries each command independently with
  jittered exponential backoff and returns `redis.ErrClosed` on close.
- **Deploy-time trace identity without mandatory new envs**: trace resources now
  infer `service.version`, `vcs.ref.head.revision`, and
  `deployment.environment.name` from existing release/Git/deployment variables
  or Go build metadata. Standard `OTEL_RESOURCE_ATTRIBUTES` still has higher
  precedence.
- **Nested Gin route finalization**: `server.OTelRouteMiddleware` lets the Gin
  engine that owns the concrete route update a server span created by an outer
  fit-go engine, preserving one server span while exporting a bounded
  `METHOD /route/:parameter` name and `http.route`.
- **Modern privacy-safe HTTP client attributes**: outbound spans use
  `http.request.method`, `http.response.status_code`, `server.address`, and a
  query/userinfo-free `url.full` while retaining trace propagation.
- **Legacy callback context continuity**: instrumented outbound HTTP, Mongo,
  Redis, MySQL, PostgreSQL, gRPC, and explicit Kafka producer operations now
  fill a missing parent from fit-go's same-goroutine active boundary context.
  Explicit caller spans remain authoritative; caller values, deadlines, and
  cancellation are preserved; baggage, sampling, and tracestate continue with
  the adopted parent. Detached goroutines still require an explicit context.
- **Future platform capability set**: privacy-safe gqlgen operation/resolver
  tracing (`fitgraphql`), generic OTLP/console metrics with ownership-safe
  lifecycle (`otelmetrics`), explicit worker/cron/job/task tracing boundaries,
  a statically linked typed instrumentation registry compatible with TraceClue
  selection/config envs, strict config-schema primitives, compiled checkpointed
  migrations with local/GCS/S3 state, and safe fitproto-compatible contract
  fetching/generation. Dynamic plugin loading and telemetry payload capture are
  deliberately excluded. GraphQL operation names require an explicit bounded
  mapper. Metrics use a stable routing provider so pre-initialized synchronous
  and observable instruments survive repeated SDK lifecycles. Cloud migrations
  require renewable lease-loss context, monotonic fencing, and an atomic fenced
  state writer; proto output uses locked, fsynced, crash-recoverable staged
  replacement.
- **Profiler sample-rate truthfulness**: `PROFILING_SAMPLE_RATE` is retained as
  the requested legacy value while status also reports pyroscope-go's fixed
  effective 100 Hz rate and `configurable=false`.
- **FeatureHub SDK compatibility**: SSE now distinguishes client-evaluated keys
  (API keys containing `*`) from server-evaluated keys, sends the JavaScript
  SDK-compatible `x-featurehub` header/query context, accepts edge URLs with or
  without a trailing `/features`, reconnects when server context changes, honors
  numeric `edge.stale` delays, exposes request-scoped client-evaluated contexts,
  and preserves the installed SDK's numeric matcher semantics.
  FeatureHub is non-blocking/optional at startup by default, matching current
  FIT.js; `FEATURE_FLAG_REQUIRE_INITIAL_STATE=true` provides pyfit-style bounded
  fail-fast readiness when the application requires initial flag state.
- **`server.DynamicCORS` + `Config.CORS`**: a callback-based, credentialed CORS
  middleware installed **engine-level** in `Server.Init` (after logging, before the
  payload/user-data parse middlewares), so it answers a preflight `OPTIONS` on gin's
  no-route path — a preflight to a GET/POST-only route is handled, not 404/405'd. The
  caller supplies the per-Origin allow decision (`CORSOptions.AllowOrigin`) — e.g. a
  dynamic allowlist of `*.fynd.com` / `*.addsale.com` + registered app domains — while
  fit-go owns the mechanism (reflect Origin + `Allow-Credentials` + `Vary`, preflight
  204 with headers/methods/max-age, and a configurable skip header such as
  `X-Skip-Cors`). Coexists with the pre-existing static `CORS(CORSConfig)`; `nil`
  `Config.CORS` mounts nothing.
- **`international`** package: `AddressFormParser` / `AddressDisplayParser`, the
  byte-compatible Go port of Node `fit/international`, so services with
  country-specific address layouts can migrate unchanged. (Closes the last module
  breadth gap vs fit.js.)
- **Request logging — full fit.js/pyfit parity**: the server access logger logs
  `request_url` (path), `query_params` (**full values**), `path_params`, and
  opted-in header values **verbatim**, at a single **info** level — matching the
  Node/Python request-log contract byte-for-byte (fit.js `log-request-response-details`
  / pyfit tracing middleware). No redaction on the inbound access log. `path_params`
  skips gin catch-all (`/*wildcard`) params, which only duplicate `request_url`
  (e.g. a wildcard-mount + internal-dispatch service), keeping named params.
- **`redact` package** (reusable primitive): `SafeURL`, `QueryMap`/`Query`,
  `HeaderValue`. Used by the outbound `httpclient`/`utils` clients, which log
  `scheme://host/path` only (outbound URLs routinely carry `?api_key=`/`?token=`),
  and available for any service that wants opt-in inbound redaction.
- **Sampling honors the OTel env**: `tracing` now reads `OTEL_TRACES_SAMPLER` and
  `OTEL_TRACES_SAMPLER_ARG` (via `Options.Sampler` / `Options.SampleRate`) instead
  of hardcoding sample-all — so a configured `parentbased_traceidratio` at `0.25`
  actually samples 25% rather than 100%.
- **slog compatibility / unified log stream**: `logging.NewSlogHandler` + an
  `slog.Handler` that routes all `log/slog` output (service code AND third-party
  libraries) through the fit logger — same OTel-JSON, same sink, same implicit
  trace context. `fit.Init` now patches `slog.SetDefault` automatically; or call
  `logging.SetAsDefaultSlog(logger)`. `Logger.WithContext` also reads the OTel
  span context now (not just the fit-go logging keys).

### Added (earlier)
- **OpenTelemetry tracing** restored across the platform surface, gated on
  `TRACING_ENABLED` (zero overhead when off):
  - `server`: `OTelMiddleware` via official **otelgin** (PII-safe: `http.route`,
    no query string); `/_healthz`,`/_readyz` filtered.
  - `redis`: official **redisotel** with `WithDBStatement(false)` (no PII).
  - `postgres`: **otelpgx** with SQL statement suppressed and keyword-only span
    names.
  - `mongo` (driver v2): hand-rolled `CommandMonitor` (no otelmongo for v2),
    with a bounded in-flight span map (size cap + stale sweep).
  - `kafka` (confluent v2): `ConsumeCtx`/`ProduceCtx` traceparent inject/extract.
  - `grpc`: per-RPC server spans via official **otelgrpc** StatsHandler.
  - `httpclient`: PII-safe client span + traceparent (not otelhttp, which leaks
    the query string).
  - `tracing`: global W3C propagator; `ContextWithTrace` installs a remote span
    context (cross-boundary continuation); `Span.IsSampled()`/`Status()`.
- **Implicit trace-in-logs**: `logging` falls back to the goroutine-local active
  context (`internal/goroutinectx`), so `logging.*` calls in HTTP handlers and
  Kafka consumers carry the trace without explicit context threading.
- **Encryption** (`encryption`): AES-256-GCM variable-length nonce, byte-compatible
  with Node `fit/encryption` and `pyfit`; cross-language known-answer test.
- **Kafka** consumer: **opt-in** topic auto-create (`ConsumerConfig.AutoCreateTopics`,
  default **false** = legacy fit.js subscribe-only) before subscribe; rebalance
  callback with partition-assignment visibility + optional `OnPartitionsAssigned`
  / `OnPartitionsRevoked` hooks.
- **Redis** standalone/cluster/sentinel: `CLIENT SETNAME`/`SETINFO` probe +
  rebuild fallback for proxies that reject the CLIENT command.
- **Metrics** (`metrics`): `httpclient.WithMetrics` outbound recorder; registry
  adapters `ServerRecorderFunc()` / `HTTPClientRecorderFunc()` for one-line wiring
  of in/out HTTP RED metrics; `/metrics` handler.

### Changed
- The server no longer echoes a `traceparent` **response** header (non-standard;
  otelgin does not). Real propagation (inbound extract → span → outbound inject)
  is unchanged via the global W3C propagator.
- Removed stale package documentation claiming New Relic compatibility and
  `NEWRELIC_*` configuration. fit-go remains an OpenTelemetry/OTLP client;
  deployments select trace storage such as Grafana Tempo behind their collector.

### Notes / follow-ups
- Testcontainers-based integration tests for redis/mongo/kafka/postgres are a
  planned follow-up; current coverage is unit tests plus live-infra runtime
  validation.
- Differentiators kept vs the canonical `CommonLibraries/go-fit`: encryption,
  PII-safe redis, Postgres/MySQL, the bounded mongo map, and the redis
  CLIENT-rejection fallback — candidates to upstream.
