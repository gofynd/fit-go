# Fork-to-upstream integration migration

This integration keeps the public API and default behavior of upstream
`github.com/gofynd/fit-go` main while carrying the capabilities developed on
the observability fork. The standard Go API compatibility check reports no
incompatible changes from upstream commit `df96a2884f700d0ebf71ad9afa79727978f73869`.

Normal code that used only upstream main does not need a source migration.
Code that reflects over or serializes the root `Fit` value must account for its
additive managed-runtime lifecycle fields; `Fit` already contained an unexported mutex,
so external positional literals were never valid. Code that already imported
fork-only APIs must use the explicit compatibility entry points below. This
separation keeps all-exported option/config structs, interfaces, and function
signatures compatible with upstream equality, positional-literal, mock, and
JSON contracts.

The former `Advanced` names remain available as deprecated aliases and wrappers
for fork source compatibility. New code should use the descriptive names below;
removing deprecated names requires a future major release.

| Fork-only use | Integrated upstream use |
|---|---|
| Fork root lifecycle (`fit.Init` owning process globals, FeatureHub, PostgreSQL, OTel metrics, profiler, or textfile metrics) | `fit.InitManaged`; original `fit.Init` retains official-main defaults and shutdown semantics |
| Strict dotenv/YAML parsing and last-config-file-wins precedence | `config.LoadStrict`; original `config.Load` retains best-effort dotenv and first-file-wins behavior |
| FeatureHub SSE, optional startup readiness, typed values, and local strategy evaluation | `feature.InitStreaming` or `feature.InitWithOptions`; original `feature.Init` retains synchronous polling and context refresh |
| `tracing.ContextWithTrace(ctx, traceID, spanID, sampled)` | `tracing.ContextWithTraceSampled(ctx, traceID, spanID, sampled)` |
| `JWTOptions` scalar payload, RSA, algorithm, clock-skew, issuer, audience, or subject fields | `server.JWTVerificationOptions` and `server.AuthorizeJWTTokenWithOptions` |
| `errors.SentryConfig` caller hooks | `errors.SentryHooksConfig` and `errors.InitSentryWithHooks` |
| `tracing.Options` exporter, protocol, propagator, activation, sampler, resource, or SDK-disable fields | `tracing.SDKOptions` and `tracing.NewSDK` / `tracing.InitSDK` |
| `ConsumerConfig` backend, assignor, auto-create, rebalance hooks, or shutdown fields | `ConsumerSettings` and `kafka.NewConsumer` |
| `ConsumerOptions` commit/finalizer fields | `ConsumeOptions` and the matching `kafka.Consume*WithOptions` helper |
| `ProducerConfig` explicit-zero, delivery/metadata/reconnect/partition/trace/close controls | `ProducerOptions` and `kafka.NewProducer` |
| `producer.ProduceCtx(...)` through a `KafkaProducer` interface | `kafka.ProduceCtx(producer, ...)` |
| `producer.ProduceBatchCtx(...)` through a `KafkaProducer` interface | `kafka.ProduceBatchCtx(producer, ...)` |
| `consumer.ConsumeCtx(...)` through a `KafkaConsumer` interface | `kafka.ConsumeCtx(consumer, ...)` |
| KafkaJS fields on `RecordMetadata` | `kafka.ToKafkaJSRecordMetadata(metadata)` at the KafkaJS wire boundary |
| Fork gRPC lifecycle (deferred source-proto validation, generated-only registration, OTel server instrumentation, live dependency health, bounded shutdown, or interceptors) | `grpc.ServerOptions` and `grpc.InitWithOptions`; original `grpc.Init` retains official-main eager proto validation, health, and registration defaults |
| TraceClue fields on `logging.Options` | `logging.RuntimeOptions` and `logging.NewRuntime` |
| Textfile lifecycle fields on `metrics.Options` | `metrics.TextfileOptions` and `metrics.NewTextfileRegistry` |
| Fork instrumentation in `utils.NewHTTPClient` (OTel propagation, generated wire request IDs, or process-default metrics) | `utils.NewInstrumentedHTTPClient`; original `utils.NewHTTPClient` retains official-main wire behavior |
| Post-main PostgreSQL connection/pool controls | `postgres.ConnectionPoolOptions`, `postgres.PoolOptions`, and `postgres.InitWithPoolOptions` |
| Decoded Google Secret Manager payloads | `config.GetDecodedSecretFromGSM`; original `config.GetSecretFromGSM` retains its encoded transport-payload result |
| Fail-closed datastore TLS validation | `mongo.InitWithTLSValidation`, `mysql.InitWithTLSValidation`, `postgres.InitWithPoolOptions`, or `redis.InitWithCompatibility`; original initializers retain upstream-main fallback and Redis verification behavior |
| Automatic MongoDB command tracing and an explicitly selected newer driver's retry policy | `mongo.DefaultInstrumentedDialFunc`; the original default dialer remains untraced and disables v2.6+ adaptive retries when that optional driver control is available |
| Automatic MySQL OpenTelemetry instrumentation | `mysql.InstrumentedConnectionOptions` and `mysql.InitInstrumented`; the original initializer keeps `database/sql` behavior |
| fit.js/pyfit non-standard AES-GCM nonce sizes | `encryption.NewManagerWithOptions(encryption.ManagerOptions{AllowedNonceSizes: []int{9, 12}})`; the original constructor retains the fixed 12-byte requirement. This is wire compatibility, not a safe nonce-generation design |
| Profiler sample-rate status fields | `profiling.RuntimeConfig`, `profiling.NewRuntime`, and `GetRuntimeConfig`; original `Status`/`Routes` keep their prior JSON shape |
| Redis protocol/retry/ioredis controls | `redis.CompatibilityOptions`, configured dial option/function types, and `redis.InitWithCompatibility` |
| Automatic request IDs, OTel server middleware, process-default metrics, health-middleware bypass, or process profiler routes | `server.RuntimeConfig` and `server.NewRuntime`; use `server.RequestIDWithHeaderPropagation` in a manually assembled runtime chain. Original `server.New`/`server.RequestID` retain official-main behavior |
| Application-owned Express health variants | `Server.UseHealthRouteMiddleware`; only explicitly supplied middleware runs before caller auth/parsers, while generic health-like paths do not receive a broad bypass |
| Post-main access-log controls | `server.AccessLogConfig` and `server.AccessLog` |
| Fork's no-`traceparent` response behavior | Set `server.OTelMiddlewareOptions.PropagateResponseHeaders` to `false` and use `server.OTelMiddlewareWithOptions`; nil/default retains official-main response propagation |

The helper functions prefer a native extension implementation and safely adapt
an original upstream implementation where semantics can be preserved. They
return an explicit error when advanced controls are requested from a driver
that cannot implement them; controls are never silently discarded.

Advanced Kafka rebalance hooks with the original callback signatures
(`OnPartitionsAssigned/Revoked/Lost`) may call the consumer's `Close` without
deadlocking, and an external `Close` does not block behind a slow hook; the
revoke hook still runs on external `Close`. `OnPartitions*WithLifecycle` and
`RebalanceLifecycle.Close` remain available for callback-scoped shutdown. A
hook that never returns still retains its goroutine. Confluent consumers reject
`OnPartitionsLost*` (Franz-only).

## Intentional security hardening

The post-rc.5 correction included in release candidate `v0.2.0-rc.6` additionally masks structured credential
aliases such as `pwd`, `pass`, `passphrase`, `otp`, `pin`, `cvv`, `cvc`, `pan` and
card-number labels, including typed numeric values and request headers. Short
aliases match complete delimiter/camelCase components, not arbitrary substrings
in operational keys such as `shipping` or `span_id`. Original constructors still
use the mandatory export boundary; no caller API migration is needed. These
changes are **not in the older immutable rc.5**; select rc.6 or a subsequent
revision containing the correction and verify its remote publication/checksum.
Metroplex's directly initialized SDK needs its separate application sanitizer
fix; replacing the library alone does not apply that policy to its SDK client.

Privacy/security output hardening is deliberately not reverted. Sentry now
applies mandatory export sanitization even through the original
`InitSentryWithConfig` path: user PII, request query/body/cookies, sensitive
extras/tags, attachments, logs, and metrics are removed or masked. Callers
should correlate with an opaque allowlisted tag rather than email, username, or
raw user data. Access logs keep credential/header/query redaction in the
original structural log schema. HTTP-client logging redacts credentials, query
values, sensitive path segments, and raw transport errors, and root startup
warnings do not serialize raw tracing or metrics initialization errors. See
[Behaviour differences for existing main users](#behaviour-differences-for-existing-main-users)
for every affected default path.

The local follow-up to `11f29d2` also covers SDK-frozen Sentry envelope dynamic
sampling context. Unsafe sampling metadata is omitted because the SDK exposes
no safe setter; safe metadata, transaction delivery and opaque body trace/span
IDs are retained. This privacy-only envelope change is not an API or sampling
rate change. See the local follow-up table in
[the remediation ledger](PR3_COMPATIBILITY_REMEDIATION.md#local-review-follow-up-to-11f29d2-2026-10-08-not-published)
for the focused Kafka/Redis/FeatureHub/lifecycle corrections and boundaries.

The 2026-10-09 local follow-up also rebuilds the SDK's private serialization
snapshots after sanitizing, so pre-serialized events and hook-produced cached
fields cannot bypass redaction. Safe sampling metadata is restored through
public SDK APIs without changing sanitized trace contexts or sampling rates.

Two inherited compatibility boundaries are deliberately not presented as
security hardening. `server.New` still records the decoded URL path unchanged;
applications must not put credentials or PII in path segments. The encryption
manager still uses the fixed provider IV required by existing fit.js/pyfit
ciphertext. AES-GCM nonce reuse with one key is unsafe, so new encrypted data
needs a separately versioned random-nonce format rather than a silent change to
the legacy ciphertext contract.

FIT-style gRPC handler errors and recovered panics are converted to a generic
Internal response on the explicit advanced/managed server. The original `Init`
entry point retains upstream-main's wire behavior: no log line for
`next(err)`, the raw panic value (including `panic("")`), and raw
response-validation text. Only its panic log line is redacted. Services needing
the hardened boundary must migrate to `InitWithOptions`.

Fail-closed datastore TLS handling is available only through the explicit
strict/runtime entry points in the table above. The original MongoDB,
PostgreSQL, MySQL, and Redis initializers retain upstream-main behavior so an
upgrade cannot turn partial legacy TLS configuration into a startup failure.
Redis's original initializer also retains its historical peer-verification
setting. New deployments should select the strict entry points and prove their
complete TLS material before rollout.

`config.GetSecretFromGSM` continues to return the base64 transport payload,
matching upstream main and callers that already decode it. Consumers that need
the usable secret value, including datastore resolver wiring, must explicitly
use `config.GetDecodedSecretFromGSM`.

## Behaviour differences for existing main users

This is the authoritative list of what an existing consumer of upstream main
(`df96a28`) observes after upgrading **without code changes**. Every item was
checked against the code on this branch; additive entry points (`InitSDK`,
`NewProducer`, `NewConsumer`, `InitManaged`, `NewRuntime`, `*WithOptions`, …)
are excluded unless an original entry point now reaches them. The redaction
items are deliberate and pinned by guard tests
(`logging/upstream_redaction_guard_test.go`,
`server/upstream_redaction_guard_test.go`,
`errors/upstream_redaction_guard_test.go`,
`kafka/legacy_consume_upstream_guard_test.go`).

### (a) Runtime-semantic differences

| Area (file) | Main | Now |
|---|---|---|
| Sentry `InitSentry` / `InitSentryWithConfig` options (`errors/sentry.go`) | `ClientOptions` without `EnableTracing`, so transactions were dropped even with `SENTRY_TRACES_SAMPLE_RATE > 0`; SDK defaults for logs/metrics; no `BeforeSend` | `EnableTracing = TracesSampleRate > 0` (transactions are sent when a rate is set), `EnableLogs: false`, `DisableMetrics: true`. The first-call-owns-init (`sync.Once`) contract is unchanged |
| Sentry export sanitizer (`errors/sentry.go`, `sanitizeSentryEvent`) | Events sent as captured | Mandatory `BeforeSend`/`BeforeSendTransaction` sanitizer: `event.User` cleared; request query, body and cookies replaced by the mask, request URL/headers/env redacted; message, exceptions (incl. mechanism), breadcrumbs, tags (sensitive keys masked), extras, contexts, spans, threads and stack frames redacted; `Fingerprint` and `Transaction` passed through `redact.Text`, so issue grouping and transaction names can change; attachments, logs and metrics dropped |
| Tracer after `Shutdown` (`tracing/tracing.go`, `IsEnabled`) | `IsEnabled()` stayed `true` | `IsEnabled()` is `false` once `Shutdown` starts, so decorators, `kafka.TracedMessageHandler` and other instrumentation stop creating spans during the drain. The global propagator (and, for `Init`/`New` tracers, the shut-down provider) stays installed, so propagation continues |
| Tracer re-init (`tracing/tracing.go`, `initWithOptions`) | `sync.Once`: `Init` → `Shutdown` → `Init` returned the shut-down tracer; a failed init was never retried | Same (`9b8754c`). Only the additive `InitSDK` creates a fresh tracer after shutdown or retries |
| Released tracer instrumentation scope (`tracing/tracing.go`) | `fit.go/<Options.ServiceName>`, independent of resource `service.name` | Same, including when an attribute overrides resource identity. Resolved-resource scope naming is limited to opt-in SDK constructors |
| Span lookup helpers (`tracing/tracing.go`, `SpanFromContext`, `TraceIDFromContext`, `SpanIDFromContext`) | fit-go context keys only | Also fall back to (and prefer) a valid native OTel span context, e.g. one created by otelgin/otelgrpc. `ContextWithTrace` values keep their precedence |
| Trace decorators (`tracing.Decorators.Trace`, `TraceWithResult`) | Did not publish goroutine-local context | When a real OTel tracer is available, publish the child context for the duration of the operation and restore the prior context; this changes implicit correlation and adds goroutine-ID lookup overhead. Failed-SDK/in-memory paths still skip that store |
| `kafka.TracedMessageHandler` with tracing enabled (`kafka/tracing.go`) | Span `kafka.consume <topic>`; parent from a hand-parsed `traceparent`; four `messaging.*` attributes; raw status message | Span `process <topic>`; parent extracted through the installed global propagator (`traceparent`, `tracestate`, baggage; remote parent); extra `messaging.destination.name`, `messaging.operation.*`, `messaging.destination.partition.id`, `messaging.kafka.offset` attributes; span context published as the goroutine-local active context for the handler; status message redacted |
| Legacy `ConfluentProducer.Close` (`kafka/confluent.go`, `closeLegacy`) | Synchronous `Flush(15s)` then `Close` under the producer lock | Total wait capped at 15 s; if exceeded it logs a warning and returns `nil` while flush/close continue in the background. Concurrent `Close` callers wait for the same completion |
| `utils.HTTPClient.Do` (`utils/http.go`) | Always generated a new request ID for its log line; `http.DefaultTransport.(*http.Transport)` assertion panicked if the default transport was replaced | Reuses a caller-supplied `x-request-id` in the log line (the wire header is not added on this path); a replaced `http.DefaultTransport` is used as-is with the `ProxyURL`/proxy-list setting ignored instead of panicking |
| `SERVER_TYPE` (`server/server_type.go`, `server/server.go`) | `central` rejected as an unknown server type | `central` is a recognised type (`ServerTypeCentral`); `ServerType(12).String()` is `"central"`. Mounting still fails if no router is supplied for it |
| Health (`health/health.go`) | `HEALTH_CHECK_INTERVAL_SECONDS <= 0` panicked in `time.NewTicker`; `Check` held the read lock while running checks | Non-positive or unparsable values are ignored (argument or 30 s used); `Check` snapshots the checks and runs them outside the lock |
| Legacy gRPC server (`grpc/server.go`) | Health `Check`/`Watch` updated only service `""`; `Shutdown` held the lock during `GracefulStop` | Standard health status for `cfg.FileName` is also updated; `Shutdown` releases the lock before its still-unbounded `GracefulStop`. `Stop` can force an active drain and `ShutdownContext` can enforce a deadline started by another caller |
| Legacy profiler routes (`server/profiler_route.go`) | CPU profile marked running even if `pprof.StartCPUProfile` failed | Marked running only on success (affects `/status`/`/config` and a later stop) |
| `DecodedTokenFromContext` (`server/middleware.go`) | Fell back to the request context when the gin key held a non-map | Returns `nil` in that case; `AuthorizeJWTToken` always stores a map, so normal use is unaffected |
| `postgres.InitDefault` / `InitWithContext` (`postgres/client.go`) | Pools already built for earlier services leaked when a later one failed | Those pools are closed before the (unchanged) error is returned |
| Profiler application name (`profiling/profiling.go`, `buildAppName`) | `DEPLOYMENT_TYPE` used untrimmed | Trimmed; a blank value falls back to `server`. Changes the Pyroscope application name only when the variable has surrounding whitespace |
| Legacy Confluent consumer config (`kafka/confluent.go`) | Producer-only `acks`/`compression.type` passed to the consumer instance (librdkafka ignored them with a `CONFWARN` log) | Removed from the consumer map; every consumer key librdkafka honours is unchanged |
| `mongo.DefaultDialFunc` (`mongo/driver.go`) | Driver retry defaults | Disables adaptive retries only when the application selects driver v2.6+ (no effect with the pinned v2.5.0) |
| Removed panics | Panicked | No panic: nil `req.Header` with default headers in `utils.HTTPClient.Do`; `Logger.WithContext(nil)`; `tracing.SpanFromContext(nil)` (and `kafka.InjectTraceHeaders` with a nil ctx/msg); legacy producer delivery report sent on the closed delivery channel after a partial `Produce` failure; profiler wall interval `<= 0` (`PROFILING_WALL_SAMPLING_INTERVAL_MICROS`) |

### (b) Log and Sentry text only

No API or wire change; only diagnostic text differs.

| Caller (file) | Main | Now |
|---|---|---|
| `logging.New` logger, `formatValue` (`logging/logger.go`) | `error` values logged as `err.Error()` | `redact.ErrorMessage` (`redact/redact.go`): an error wrapping `context.Canceled`/`DeadlineExceeded` becomes exactly `operation canceled`/`operation timed out` (the rest of the message is dropped); a `*url.Error` becomes `METHOD <safe URL>: <category>` (`network timeout`, `network error`, `operation failed`, …) with the cause dropped; an empty message becomes `operation failed`; otherwise `redact.Text`. Non-error values (including plain strings) are unchanged |
| `server.LogRequestResponse` via `server.New` (`server/middleware.go`) | `request_url` = `URL.RequestURI()` (escaped path + raw query); opted-in headers raw | `request_url` = decoded `URL.Path` plus a key-sorted query in which only `redact.DefaultQueryAllowlist` keys (`limit`, `page`, `sort`, `order`, …) keep their values; every other value, including `order_id`, is masked; multi-values joined with `,`; an unparsable query is masked whole. `INCLUDE_HEADERS_IN_LOG` values pass `redact.HeaderValue` |
| `DecryptionMiddleware` failure log (`server/middleware.go`) | raw error | `redact.ErrorMessage` |
| `utils.HTTPClient.Do` `[EXT]` logs (`utils/http.go`) | full URL incl. query; raw error | `redact.SafeURL` (no query/fragment/userinfo, credential path segments masked) in the message and `request_url`; `redact.ErrorMessage` for `error` |
| Legacy `grpc.Init` panic log (`grpc/server.go`) | raw panic value | `redact.Text`; the wire response is unchanged |
| Legacy Confluent consumer (`kafka/confluent.go`, `logMessageFailure`, `logBatchFailure`) | `offset` logged as a `kafka.Offset` string; per-record commit failure `kafka/confluent: commit failed`; batch commit failure `kafka/confluent: batch commit failed` with only `error`; raw errors | `offset` is numeric; commit failures are logged as `kafka/confluent: post-handler offset resolution failed` / `post-handler batch offset resolution failed` with `topic`, `partition`, (`firstOffset`, `lastOffset`) fields; errors redacted. Producer close, topic auto-create and partition assign/unassign warnings are also redacted |
| Legacy producer close (`kafka/confluent.go`, `closeLegacy`) | none | New warning `kafka/confluent: legacy producer close timed out; safe cleanup continues in background` with `pendingReports` when the 15 s cap is hit |
| Kafka health (`kafka/health.go`) | check message embedded in the log message; file-write failure logged `path` and raw error | fixed message `kafka healthz failed (file write skipped)` with the redacted check message in `error`; file-write failure logs a fixed message without path or cause |
| Root `fit.Init` warnings (`fit.go`) | raw tracing/metrics init error | `redact.ErrorMessage` |
| Span status (`tracing.Span.SetStatus`, `Trace`/`TraceWithResult`, `kafka/tracing.go`) | raw message | `redact.Text` / `redact.ErrorMessage` (also what `Span.Status()` reports) |
| Sentry `log` lines (`errors/sentry.go`) | init failure printed the error; debug init line printed a truncated DSN; not-initialized lines printed raw error/message | init failure without error text; `initialized (debug)`; not-initialized lines redacted |

Redaction coverage: password/secret values, emails, phones, payment cards,
Bearer/JWT tokens and other recognised credentials are masked. Kept: IP
addresses and ports, ID-labelled numbers (`order_id=9876543210` in free text),
labelled timestamps (`ts=1791364800006`), UUIDs. Accepted trade-offs: bare
10-digit values starting 6–9 are redacted unless ID-labelled; unlabelled
Luhn-valid 13-digit values are redacted unless timestamp-labelled or in an
event path; `PWD=` is redacted even when path-like; only `retry pass: <n>`
survives as a `pass` counter. The logger scans `error` values only; plain
string fields are logged as supplied.

### (c) Dependencies

Selecting this release raises, through minimum version selection, these
modules in every consumer's build (`go.mod`):

- OpenTelemetry `otel`, `sdk`, `trace`, `metric`, `otlptracehttp` 1.43.0 →
  1.45.0; gRPC 1.80.0 → 1.83.2; contrib 0.70.0 (`otelgrpc` and `otelhttp`
  direct); `golang.org/x/net`, `x/crypto`, `x/sys`, `x/text`, `x/sync`;
  `google.golang.org/genproto/googleapis/{api,rpc}`; gin's transitive
  dependencies (`bytedance/sonic`, `gin-contrib/sse`, `validator`, `go-json`,
  `quic-go`, …). Gin itself stays 1.12.0.
- New module requirements: `gqlgen`/`gqlparser`, `franz-go`/`kmsg`,
  `protocompile`, `otelsql`, `gopkg.in/yaml.v3` (now direct), `godotenv`,
  `gofrs/flock`, `prometheus/common` (now direct), b3/jaeger propagators, OTLP
  metric/trace gRPC+HTTP and stdout exporters, `sdk/metric`, and
  `gocloud.dev` with its AWS SDK v2 and Google Cloud Storage graph (in
  `go.mod`/`go.sum`).
- The root `fit` package now links `pgx`/`otelpgx`, `pyroscope-go`, the OTLP
  metric and trace exporters (gRPC and HTTP), `sdk/metric`, the stdout
  exporters, the b3/jaeger propagators, `godotenv` and `yaml.v3`; on main it
  linked only the OTLP/HTTP trace exporter.
- The `go` directive is unchanged at `1.25.10`.

### Unchanged (verified)

- `config.Load` (best-effort dotenv, first-file-wins) and
  `config.GetSecretFromGSM` (base64 transport payload).
- `server.New` routes and middleware order (`SecureHeaders` → access log →
  request middlewares → payload/user/application parsers → response
  middlewares → health → profiling when enabled → `SERVER_TYPE` mounts → 404),
  `CORS`, `RequestID`.
- `grpc.Init` wire contract: eager proto validation, health, reflection, no
  extra interceptors or stats handler, raw `next(err)`/panic/validation text.
- `AuthorizeJWTToken` accept/reject decisions and context values (`59dac4c`;
  593-case test against verbatim main).
- Legacy Kafka consumer configuration and semantics: log-and-skip in
  `Consume`, handler error returned unchanged from `ConsumeBatch`, mixed
  batches, commit/auto-commit behaviour, `Close` races (`4d45fdf`).
- `kafka.InjectTraceHeaders`/`InjectTraceHeadersToMessages`: appends one
  `traceparent` from the fit-go span (`4d45fdf`).
- `redis.Init`/`InitDefault`, `mongo.Init`, `mysql.Init`,
  `postgres.InitDefault`/`InitWithContext` (except the leak fix above),
  `metrics.New`, `feature.Init` (polling), `encryption.NewManager`, profiling
  (`New`/`NewFromEnv`/`Start`/`Stop`/`Routes`/`Status` keys and
  `server.RegisterProfileRoutes` responses; except the application-name trim
  above), the legacy Kafka producer configuration and empty-key semantics, and
  the original address parsers (now `server/international_compat.go`).
- `fit.Init` steps, defaults and `Shutdown` scope (apart from the redacted
  warnings above).

## Compatibility and safety corrections after rc.1 review

The local post-rc.1 review keeps the public API and upstream-main defaults while
repairing runtime-only faults in additive paths: Sentry breadcrumb copying,
nil-safe feature contexts, bounded RESP parsing, finite Redis retry budgets,
single-command MOVED/ASK handling, fail-fast cross-node Redis pipelines, Kafka
option/commit/producer lifecycle handling, exact health-route bypass,
FeatureHub retry/context isolation, tracing shutdown retirement, explicit
logger-context precedence, CORS `Vary` preservation, bounded request IDs, and
privacy-safe redaction. Legacy tracing constructors do not enable the new
goroutine-local log lookup. GraphQL resolver spans are explicit opt-in;
operation spans remain the default when the extension is registered. The
additive `international.AddressDisplayParser` uses JavaScript-like `String`
coercion for JSON-shaped non-scalars while keeping exact native Go integers.

Request-scoped FeatureHub contexts fail closed until `Build` succeeds and do
not read the parent client's server-evaluated or legacy values. A context
mutation during `Build` invalidates the fetched snapshot rather than publishing
values for stale attributes. Non-positive `edge.stale` hints are ignored, while
permanent HTTP failures stop the shared reconnect loop after readiness instead
of creating an unbounded authorization-error loop; terminal waiters are woken
with the sanitized HTTP status instead of hanging after a previously healthy
stream. Transient failures use bounded exponential backoff and bounded
`Retry-After`; only feature-state events reset that backoff. Outage retries use
equal jitter, and a configured reconnect interval above 30 seconds is honoured.
Positive stale delays are floored by the reconnect interval, capped at 30
seconds, and not jittered; non-finite `edge.stale` values are ignored. The
stale-retention deadline is fixed by the first stale notice, so a cached ready
snapshot expires at most 30 seconds later even if stale replies continue. Each
stale window allows at most one context-change interrupt; later changes are
coalesced and the latest context is sent when the window ends. Stopping a
client cancels its one owned expiry timer before another can be armed. Legacy
`feature.Init` evaluation-context snapshot fetches are bounded by
`FEATURE_FLAG_INIT_TIMEOUT`; `feature.Init` itself remains a polling client and
never opens an SSE stream. Context revision changes and full/incremental/delete
event application are serialized: a buffered event from an older
server-evaluated stream cannot mutate the newer context or publish readiness.
The post-rc.6 correction included in rc.7 also scopes cached versions to the evaluated
context and known flag UUID: a new context's first accepted event cannot retain
the prior user's values, and a recreated flag can restart its version. Missing
UUIDs retain historical key-based version ordering; client-evaluated attribute
changes keep the shared repository. This correction is not backported into rc.6.

The rc.7 Sentry follow-up sanitizes detached diagnostic payloads, checks both
Go field names and JSON aliases, and preserves acyclic shared references under
an 8192-node traversal budget. Public span fields are retained, but before-send
span snapshots are not live SDK lifecycle objects: private context/parent/
recorder/lock state is not copied. SDK preprocessing before the hooks is unchanged.
Native OTel HTTP wrapping no longer initializes FIT tracing when FIT spans are
suppressed; native tracing ownership and FIT logging/metrics/request IDs remain.
These follow-ups are packaged in the new immutable rc.7 candidate. Verify its
remote publication and checksum before consumer adoption; rc.6 remains unchanged.

KafkaJS-compatible processing recovery rewinds the active Franz or Confluent
group member to the earliest unprocessed record in every partition that failed
in the same poll wave. This covers handler, finalizer, and post-handler commit
failures without letting one partition hide a sibling failure. With
`CommitBeforeHandler`, a record whose pre-handler commit succeeded is not
replayed when its handler fails, but later fetched records of that partition
group are rewound rather than skipped. Recovery does not replace
`FromBeginning=false` with an earliest-retention fallback. Franz remembers
known unresolved positions across real ownership loss and driver recreation;
after broker offset lookup it applies them only to assigned partitions with no
committed offset. Broker commits remain authoritative. This recovery state is
local to the consumer object, not durable across process crashes or shared with
another group member. Successful processing/pre-handler commits clear the
relevant remembered position. Confluent seeks only
partitions the member still owns and drops rewinds for revoked partitions; if
an exact seek fails, the rewind stays pending and every later `Consume` call
returns an error without reading until the seek succeeds. The consumer is never
rebuilt from `auto.offset.reset=latest`, so callers that retry `Consume` should
back off between attempts. Lost partitions fall back to the revoke notification
only when no distinct lost hook is configured, and never use the safe-revoke
final commit boundary. The legacy `client.Consumer` path keeps main's
semantics: `Consume` logs and skips a failed record, and `ConsumeBatch` returns
the handler's own error value unchanged (not wrapped), including when `Close`
races the failure. In advanced consumers, a message handler, batch handler, or
offset finalizer can call its own `Close` without self-deadlocking; only that
callback-local call completes asynchronously, while external `Close` remains
synchronous. Advanced Confluent dispatch rejects a group gathered under a
revoked assignment generation before calling application code. Advanced
Confluent and Franz also reject mixed per-topic `FromBeginning` values; legacy
Confluent retains main's first-topic behavior.

The owned RESP2 compatibility transport deliberately rejects connection-state
operations that cannot be made reconnect-safe (`MULTI`/`EXEC`, every
`SELECT`, subscriptions, monitor mode, and indefinite blocking commands). In
Cluster mode it also rejects cluster-wide `SCAN`, `KEYS`, `FLUSHDB`,
`FLUSHALL`, and `RANDOMKEY` rather than running them on one node; an
authoritative same-node pipeline redirect returns per-command replies without
retiring the Cluster transport.
Finite-timeout blocking commands and exact `HELLO 2` are supported. Consumers
needing transactions or database selection should use the established
go-redis client APIs, where those states are owned by the driver, rather than
expecting unsafe transparent replay from the compatibility queue.

Bootstrap, PING, Sentinel/Cluster discovery, redirects, and malformed-protocol
errors omit raw reply values, keys, and command arguments. Stable causes such as
`io.EOF`, context cancellation, and deadline expiry remain available through
`errors.Is`. Ordinary Redis command-error replies are intentionally still
returned to the caller because they are part of the command API; callers must
use the platform redactor before logging them. `WriteDisposition` describes the
socket write, whereas `MayHaveExecuted` describes possible side effects: a
fully written command can still be known not to have executed when Redis
returned an authoritative redirect, including at the redirect limit.
Keyed `OBJECT`, `MEMORY USAGE`, `XGROUP`, and `XINFO` forms have explicit
argument-2 routing; unknown subcommands are rejected without echoing arguments
instead of being misrouted by hashing the subcommand.

MongoDB remains pinned to official main's v2.5 driver because a v2.6 upgrade
would also change BSON validation and error classification outside fit-go's
constructor boundary. Go's module selection still permits an application such
as Metroplex to require v2.6 explicitly. In that case the original dialer
disables adaptive retries while `DefaultInstrumentedDialFunc` retains the selected
driver's retry default; the application owns the remaining dependency-level
BSON/error-classification rollout gate.

## Release and downstream order

1. Publish and validate the pending reviewed source under a new immutable
   candidate; resolve the dependency-security decision before sign-off. Run an
   authenticated organization-wide reverse-dependency search and compile
   every consumer. The integration now keeps every pre-existing all-exported
   struct layout exact and locks those layouts with a compatibility test because
   Go's standard API checker alone does not detect positional-literal breakage.
2. After these gates and review approval, merge and tag the integration in the
   official `gofynd/fit-go` repository as a new immutable release (recommended:
   `v0.2.0`). Do not reuse a fork tag.
3. Update fork consumers using the table above and test them against the local
   integration worktree.
4. Replace local paths and `github.com/swapnilfynd/fit-go` replacements with the
   official immutable version, run `go mod tidy`, and verify the checksum.
5. Deploy dependents after the library release. Do not deploy a consumer whose
   `go.mod` still contains a local filesystem replacement.

Metroplex's local `internal/app.go` uses `GetDecodedSecretFromGSM` and
`UseHealthRouteMiddleware`. Its pre-rc.6 replacement is published rc.5 at
`2ab9e42`; all older tags remain immutable. The post-rc.5 Sentry and HTTP
corrections are packaged in release candidate `v0.2.0-rc.6`; Metroplex also has
separate application-owned Sentry fixes that the library tag does not supply.
After verifying the remote rc.6 publication, repin and repeat Metroplex
`GOWORK=off` checks while preserving existing application integration. The
exact current local pin and evidence belong in Metroplex's library document.
The original workfile must not be
used as proof of a remote pin. Record both candidate-workspace and published-
pin evidence separately in Metroplex's library document. Neither closes
the security and deployment/reverse-consumer gates listed in the remediation
ledger's publication status. Its dependencies and Go minimum remain unchanged
by explicit user decision.
An official
release after upstream merge is a separate pin migration; the fork replacement
must remain until that official immutable tag is available and validated.
