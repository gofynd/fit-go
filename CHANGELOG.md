# Changelog

All notable changes to `github.com/gofynd/fit-go` are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/);
the module follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Default-path log and Sentry output is redacted for existing main users with
  no API or wire change: `logging` error values, the mandatory Sentry
  `BeforeSend`/`BeforeSendTransaction` sanitizer on `InitSentry`/
  `InitSentryWithConfig`, `server.New` access-log query (always) and opted-in
  headers, `DecryptionMiddleware` failures, `utils.HTTPClient` `[EXT]` logs,
  the legacy `grpc.Init` panic log, Kafka health and Confluent diagnostic logs,
  `fit.Init` startup warnings, and span status messages when tracing is
  enabled. See `docs/UPSTREAM_INTEGRATION_MIGRATION.md` ("Behaviour
  differences for existing main users"), which also lists the remaining
  runtime-semantic and dependency differences; pinned by upstream-default
  guard tests.
- Document the inherited compatibility boundaries that remain intentionally
  unchanged: `server.New` logs the decoded URL path as main did, and the
  fit.js/pyfit-compatible encryption manager uses a fixed provider IV. New
  encrypted formats must use a versioned random nonce per ciphertext; the
  legacy wire format is retained only for interoperability.
- Correct the Sentry initialization contract documentation: legacy
  `InitSentry`/`InitSentryWithConfig` are first-call-wins, while the additive
  `InitSentryWithHooks` path supports retryable startup.

### Fixed
- Allow `Stop` or a deadline-based `ShutdownContext` to force an in-progress
  legacy gRPC graceful drain while leaving plain legacy `Shutdown` unbounded.
- Match main's repeated released-tracer shutdown result (first exporter error,
  then nil) and retain failed-global-init diagnostics through a no-op package
  shutdown.
- Serialize FeatureHub context revisions with full/incremental/delete event
  application so buffered old-stream events cannot mutate or mark a newer
  server-evaluated context ready.
- Let advanced Confluent and Franz message handlers, batch handlers, and offset
  finalizers call their own consumer's `Close` without self-deadlocking while
  preserving synchronous external close, including callbacks wrapped with
  public tracing adapters or an explicitly replaced active tracing context.
  Guard advanced Confluent dispatch by
  assignment generation after revoke/reassignment, and reject mixed per-topic
  `FromBeginning` settings on additive paths while retaining legacy
  Confluent's first-topic behavior.
- Sanitize ioredis bootstrap AUTH/SELECT/INFO failures and route keyed
  `OBJECT`, `MEMORY USAGE`, `XGROUP`, and `XINFO` cluster forms by their actual
  key argument rather than the subcommand.
- Render JSON-shaped values in `international.AddressDisplayParser` with
  JavaScript-like string coercion while preserving exact native Go integers.
- Restore upstream-main tracing lifecycle for the released constructors
  (`New`, `Init`, `InitWithOptions`, lazy `Global`): a failed global
  initialization is cached (`Global()` returns nil without re-running SDK
  init; later `Init` calls return `(nil, nil)`), a failed `New` stays enabled
  with no OTel tracer, `Init` after `Shutdown` returns the shut-down tracer,
  and only main's resource/exporter/sampler/propagator are built (no leaked
  advanced exporters, no extra `OTEL_*` reads). `Shutdown` keeps the global
  propagator installed on every path, including `InitSDK` tracers, so
  propagation continues during graceful drain; released-constructor tracers
  also leave their provider installed. With no OTel tracer, `StartSpan` no
  longer stamps in-memory IDs on the logging context and decorators skip the
  goroutine-local store.
- `server.AuthorizeJWTToken` again uses upstream main's HS256 verifier byte
  for byte (numeric-only `exp`/`nbf`, `now == exp` accepted, empty secret used
  as-is, padding, payload shapes, context values). The golang-jwt verifier is
  used only by `AuthorizeJWTTokenWithOptions`/`AuthorizeJWTTokenAdvanced`.
- Legacy Confluent `ConsumeBatch` returns the handler's own error value again
  (not wrapped), also when `Close` races the failure, and delivers an already
  collected batch when `Close` races collection; legacy `Consume` keeps main's
  log-and-skip and `Close`-race handling. Advanced consumers keep wrapped
  rewind errors.
- `kafka.InjectTraceHeaders`/`InjectTraceHeadersToMessages` again append only
  a `traceparent` (sampled flag `01`) from the fit-go span in ctx, in place,
  leaving existing headers untouched. The strip-and-propagate behaviour via
  the global propagator is used only by `kafka.NewProducer`'s
  `ProducerTraceHeadersInject` policy.
- Preserve official-main runtime behavior for original APIs: GSM results stay
  base64-encoded, legacy datastore initializers retain best-effort TLS behavior,
  Redis retains its historical TLS verification default, and original gRPC
  initialization retains post-start registration behavior. Explicit decoded,
  strict, and managed entry points remain available for new integrations.
- Prevent process crashes and hangs in Sentry breadcrumb sanitization, disabled
  feature clients, oversized/deep RESP input, unroutable Redis cluster commands,
  unsupported Redis reply-count modes, and Kafka consumer/producer lifecycle
  paths. Redis request retry budgets now survive reconnects, protocol-limit
  failures settle once, MOVED/ASK are handled for single commands, bounded
  blocking commands are never replayed after ambiguous execution, and
  cross-node pipelines fail before any write rather than risk duplicate
  mutations or mis-associated replies.
- Apply advanced Kafka consume options instead of silently discarding them,
  flush franz manual commit marks when per-run auto-commit is requested, retain
  the original producer's configured acknowledgements, bound legacy close,
  deep-clone traced messages, preserve original nil/empty Kafka-key semantics,
  rewind advanced consumers to failed records after handler failure, and keep
  lost-partition callbacks separate from the safe revoke-commit boundary.
- Keep FeatureHub polling evaluation functional, retry transient edge responses,
  isolate request-scoped server evaluation, ignore non-positive `edge.stale`
  hints, accept the JavaScript SDK's positive boolean/numeric-string stale
  hints, stop retrying permanent HTTP failures, and report a terminal failure to
  waiters even when it occurs after readiness. Reject stale in-flight context
  snapshots, preserve legacy boolean coercion/cancellation, and remove API
  keys/user context from returned errors. Unbuilt or failed request contexts no
  longer fall back to values evaluated for the parent client. Streaming retries
  now use bounded exponential backoff with equal jitter (a configured interval
  above 30 seconds is honoured), honor bounded `Retry-After` guidance, reset
  only after feature-state events, floor/cap `edge.stale` delays and ignore
  non-finite values, expire a retained stale snapshot at most 30 seconds after
  the first stale notice of a window, allow at most one context-change
  interrupt per stale window while applying the latest context when it ends,
  and cannot arm a stale-expiry timer after `Stop`. Legacy evaluation-context
  snapshots are bounded by `FEATURE_FLAG_INIT_TIMEOUT`.
- Retire the global tracer after shutdown (`Global()` and the released `Init`
  return the shut-down tracer; only `InitSDK` starts a fresh lifecycle),
  prefer an explicitly bound logger context, preserve slog groups and caller
  PCs, deduplicate routed metric instruments, and respect an explicit resource
  `service.name`. Legacy tracing constructors retain explicit-context-only log
  enrichment; goroutine-local lookup remains opt-in through the managed SDK.
- Restrict health middleware bypass to routes the server actually owns, append
  rather than replace `Vary: Origin`, validate propagated request IDs on the
  advanced path, and make GraphQL resolver spans opt-in.
- Bound proto fetching and copy size, validate repository schemes, make
  international rendering deterministic and JSON-string aware, prevent
  migration identifier collisions/overflow, and improve secret redaction for
  whole-token Luhn-valid payment cards, labelled and supported international
  phone numbers, short password aliases, and validated JWTs. The scanner keeps
  hostnames, Kafka topics, UUIDs, IP addresses, explicitly labelled timestamps,
  event-path epoch milliseconds, and explicitly labelled order/invoice IDs.
- Preserve the original health check's additive multi-call behavior and numeric
  environment prefix parsing while allowing managed shutdown to stop every
  tracked periodic loop within the caller's shutdown context.
- Rewind advanced Franz and Confluent consumers to the exact failed record after
  a handler error without changing a fresh group's configured latest/earliest
  fallback; collect every failed partition in a poll wave, rewind finalizer and
  post-handler commit failures, and do not replay a record whose
  `CommitBeforeHandler` commit already succeeded while rewinding later fetched
  records of its group. Confluent seeks only still-owned partitions, drops
  rewinds for revoked partitions, and keeps a failed exact seek pending: later
  `Consume` calls return an error without reading until it succeeds (no
  rebuild from `auto.offset.reset=latest`; callers should back off between
  retries). Confluent again rejects `OnPartitionsLost*` hooks. Preserve the revoke-hook
  notification fallback for lost partitions without committing them, and
  reject unsupported options on additive context-aware legacy-consumer entry
  points instead of dropping them.
- Reject cluster-wide `SCAN`/`KEYS`/`FLUSHDB`/`FLUSHALL`/`RANDOMKEY` in the
  ioredis Cluster path instead of routing them to one node, and keep the
  Cluster transport after an authoritative same-node pipeline redirect.
- Preserve completed same-node pipeline replies when Redis Cluster reports a
  redirect, close nodes whose connection races with cluster shutdown, process
  queued retry failures linearly, and allocate large RESP payloads incrementally
  instead of trusting a length header up front. Bootstrap, PING, topology,
  redirect, and malformed-protocol errors omit raw server values and command
  arguments while retaining stable causes such as `io.EOF` for `errors.Is`.
- Preserve retired global-tracer state across temporary `SetGlobal` ownership,
  bound pre-initialization health cleanup by the initialization context, and
  restore the legacy gRPC `Init` contract (no `next(err)` log line, raw panic
  value including `panic("")`, raw response-validation text; only the panic
  log line is redacted) while managed servers stay sanitized. Redact compound password keys, compact
  JWE/JWT tokens, grouped cards, loose numeric codes, and supported
  international phone forms while retaining known operational numeric lists,
  durations, decimals, labelled timestamps, coordinates, versions and labelled
  IDs. Unlabelled Luhn-valid 13-digit values are redacted unless
  timestamp-labelled or in an event path, and bare 10-digit values starting
  6–9 are redacted unless ID-labelled. Function-call arguments, `card-`/`card.`
  labels, `PWD=`/`pass:` aliases (only `retry pass: <n>` is kept), merged
  phone/ID runs, and `(415) 555-1234` are now handled; look-backs are bounded
  and the card scanner is allocation-free (about 1.4 ms per 256 KiB).
- Update gRPC to 1.83.2 for GO-2026-6443 and the OpenTelemetry core/exporter
  and matching contrib families to 1.45.0/0.70.0 for GO-2026-6505 (exporter
  configuration log leakage).

### Added
- `redis.IORedisRedirectError` exposes redirect-side-effect ambiguity and write
  disposition through `errors.As`, without requiring callers to parse error
  strings or exposing Redis-authored reply text.
- `config.GetDecodedSecretFromGSM`, `mongo.InitWithTLSValidation`, and
  `mysql.InitWithTLSValidation` provide explicit decoded/strict behavior without
  changing existing consumers.
- `server.Server.UseHealthRouteMiddleware` lets an application explicitly own
  legacy health-route variants without giving unrelated routes an auth bypass.
- Kafka advanced consumer settings expose callback-scoped
  `RebalanceLifecycle` hooks for deadlock-free shutdown without weakening the
  synchronous external `Close` contract. Original-signature hooks may also call
  `Close` without deadlocking, an external `Close` does not block behind a slow
  hook, and the revoke hook still runs on external `Close`; a hook that never
  returns still retains its goroutine.
- `health.Checker.ResetContext` and `StopPeriodicCheckContext` let managed
  shutdown honor its deadline while the original blocking methods remain
  source- and behavior-compatible wrappers.

## [0.2.0-rc.1] - 2026-10-07

### Compatibility
- The public API from upstream main is retained. Context-aware Kafka operations
  are optional extension interfaces and package helpers, so existing producer
  and consumer implementations still satisfy `KafkaProducer` and
  `KafkaConsumer`.
- Additional controls use descriptive, capability-based entry points where extending an
  old value type or interface would change equality, positional literals, or
  mock conformance: `logging.NewRuntime`, `metrics.NewTextfileRegistry`,
  `grpc.InitWithOptions`, `kafka.NewConsumer`, and the
  `Consume*WithOptions` helpers. The original entry points retain their upstream
  defaults; former `Advanced` names remain deprecated aliases/wrappers.
- `RecordMetadata` retains the upstream `{Topic, Partition, Offset}` contract.
  KafkaJS-shaped HTTP responses must convert explicitly with
  `ToKafkaJSRecordMetadata`; this prevents an internal migration requirement
  from changing every library consumer's JSON.
- Release this integration under a new immutable upstream tag. Do not move or
  overwrite an existing tag, and remove downstream replacements to the retired
  fork after the upstream tag is available.

### Added
- **KafkaJS-compatible rolling consumer backend**: an explicit
  `ConsumerBackendFranzKafkaJS2Compat` opt-in uses franz-go while reproducing
  KafkaJS 2.x's literal `RoundRobinAssigner` group protocol and retaining
  fit-go's message, batch, tracing, offset-finalizer, TLS, and SASL APIs. The
  former symbol remains available as a deprecated source-compatible alias. The
  existing Confluent/librdkafka producer and default consumer are unchanged.
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
- **Opt-in caller propagation fallback**: `httpclient.WithCallerPropagationHeaders`
  always removes the caller's `traceparent`, lets an active trace-aware propagator
  supply its replacement, replaces other fields emitted by the active context,
  and otherwise retains trusted caller propagation fields. The default strict
  clear-before-inject policy is unchanged.
- **Opt-in Kafka producer lifecycle bounds**: `BrokerCheckTimeout` can make
  `Connect` validate broker metadata, `CloseTimeout` overrides the existing
  15-second close budget, and context cancellation now releases keyless metadata
  callers promptly while the timeout-only driver operation drains safely.
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
- **Opt-in request logging for fit.js/pyfit structural parity**: TraceClue access-log mode logs
  `request_url` (path), redacted `query_params`, `path_params`, and
  allowlisted header names with privacy-safe values, at a single **info** level.
  Sensitive query/header values remain outside the compatibility target even
  where Node/Python emitted them verbatim. `path_params`
  skips gin catch-all (`/*wildcard`) params, which only duplicate `request_url`
  (e.g. a wildcard-mount + internal-dispatch service), keeping named params.
- **`redact` package** (reusable primitive): `SafeURL`, `QueryMap`/`Query`,
  `HeaderValue`. Used by the outbound `httpclient`/`utils` clients, which log
  `scheme://host/path` only (outbound URLs routinely carry `?api_key=`/`?token=`),
  and available for any service that wants opt-in inbound redaction.
- **Sampling honors the OTel env**: `tracing` now reads `OTEL_TRACES_SAMPLER` and
  `OTEL_TRACES_SAMPLER_ARG` (via `SDKOptions.Sampler` /
  `SDKOptions.SampleRate`) instead
  of hardcoding sample-all — so a configured `parentbased_traceidratio` at `0.25`
  actually samples 25% rather than 100%.
- **slog compatibility / unified log stream**: `logging.NewSlogHandler` + an
  `slog.Handler` that routes all `log/slog` output (service code AND third-party
  libraries) through the fit logger — same OTel-JSON, same sink, same implicit
  trace context. `fit.InitManaged` patches `slog.SetDefault` automatically; or call
  `logging.SetAsDefaultSlog(logger)`. `Logger.WithContext` also reads the OTel
  span context now (not just the fit-go logging keys).

### Added (earlier)
- **OpenTelemetry tracing** restored across the platform surface, gated on
  `TRACING_ENABLED` (zero overhead when off):
  - `server`: a privacy-safe Gin middleware records bounded route, method,
    scheme, host, status, and request ID attributes by default; query strings,
    user agents, client addresses, raw errors, and route values remain opt-in;
    `/_healthz`,`/_readyz` are filtered.
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
- **Kafka** consumer: **opt-in** topic auto-create (`ConsumerSettings.AutoCreateTopics`,
  default **false** = legacy fit.js subscribe-only) before subscribe; rebalance
  callback with partition-assignment visibility + optional `OnPartitionsAssigned`
  / `OnPartitionsRevoked` hooks.
- **Redis** standalone/cluster/sentinel: `CLIENT SETNAME`/`SETINFO` probe +
  rebuild fallback for proxies that reject the CLIENT command.
- **Metrics** (`metrics`): `httpclient.WithMetrics` outbound recorder; registry
  adapters `ServerRecorderFunc()` / `HTTPClientRecorderFunc()` for one-line wiring
  of in/out HTTP RED metrics; `/metrics` handler.

### Changed
- The server still emits active configured propagation fields on the response
  by default, preserving official-main behavior. Fork consumers that adopted
  its later no-response-header behavior can set
  `OTelMiddlewareOptions.PropagateResponseHeaders` to `false` and use
  `OTelMiddlewareWithOptions`; request extraction and downstream
  injection remain unchanged.
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
