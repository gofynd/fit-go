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
| fit.js/pyfit non-standard AES-GCM nonce sizes | `encryption.NewManagerWithOptions(encryption.ManagerOptions{AllowedNonceSizes: []int{9, 12}})`; the original constructor retains the 12-byte requirement |
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

Privacy/security output hardening is deliberately not reverted. Sentry now
applies mandatory export sanitization even through the original
`InitSentryWithConfig` path: user PII, request query/body/cookies, sensitive
extras/tags, attachments, logs, and metrics are removed or masked. Callers
should correlate with an opaque allowlisted tag rather than email, username, or
raw user data. Access logs keep credential/header/query redaction in the
original structural log schema. HTTP-client logging redacts credentials, query
values, sensitive path segments, and raw transport errors, and root startup
warnings do not serialize raw tracing or metrics initialization errors. See
[Log and Sentry output changes](#log-and-sentry-output-changes-for-existing-main-users)
for every affected default path.

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

## Log and Sentry output changes for existing main users

The original entry points keep their APIs, field names, and wire contracts,
but the following default-path diagnostics now pass through `redact`. This is
a deliberate, pinned change (guard tests:
`logging/upstream_redaction_guard_test.go`,
`server/upstream_redaction_guard_test.go`,
`errors/upstream_redaction_guard_test.go`,
`kafka/legacy_consume_upstream_guard_test.go`).

| Caller (file) | Main behavior | Now |
|---|---|---|
| `logging.New` logger, `formatValue` (`logging/logger.go`) | `error` values logged as `err.Error()` | `error` values rendered with `redact.ErrorMessage`; non-error values unchanged |
| `errors.InitSentry` / `InitSentryWithConfig` (`errors/sentry.go`) | No sanitizer | Mandatory `BeforeSend`/`BeforeSendTransaction` sanitizer: message, exceptions, breadcrumbs, tags, extras, contexts, spans and request URL/headers redacted; request query/body/cookies masked; user cleared; attachments/logs/metrics dropped; `EnableTracing` only when `TracesSampleRate > 0`, `EnableLogs: false`, `DisableMetrics: true`; init-failure and not-initialized debug logs omit or redact error text |
| `LogRequestResponse` via `server.New` (`server/middleware.go`) | `request_url` = raw request URI; opted-in headers raw | Query always redacted (values kept only for `redact.DefaultQueryAllowlist` keys such as `limit`, `sort`); URL path unchanged; headers from `INCLUDE_HEADERS_IN_LOG` pass `redact.HeaderValue` (credential headers masked) |
| `DecryptionMiddleware` failure log (`server/middleware.go`) | raw error | `redact.ErrorMessage` |
| `utils.HTTPClient.Do` `[EXT]` logs (`utils/http.go`) | full URL incl. query; raw error | `redact.SafeURL` (no query/fragment/userinfo, credential path segments masked); `redact.ErrorMessage` |
| Legacy `grpc.Init` panic log (`grpc/server.go`) | raw panic value | `redact.Text`; wire response unchanged |
| Kafka health failure log (`kafka/health.go`) | raw check message in the log message; file-write error with path | check message in a redacted `error` field; file-write failure logs a fixed message |
| Legacy Confluent consumer handler-failure and post-handler logs, plus producer close, topic auto-create, and partition assign/unassign warnings (`kafka/confluent.go`) | raw error | `redact.ErrorMessage` |
| Root `fit.Init` tracing/metrics init warnings (`fit.go`) | raw error | `redact.ErrorMessage` |
| Span status when tracing is enabled (`tracing.Span.SetStatus`, `Trace`/`TraceWithResult`, `kafka/tracing.go`) | raw message | `redact.Text` / `redact.ErrorMessage` |

Masked: password/secret values, emails, phones, payment cards, Bearer/JWT
tokens and other recognised credentials. Kept: IP addresses and ports,
ID-labelled numbers (`order_id=9876543210`), labelled timestamps
(`ts=1791364800006`), UUIDs. Accepted trade-offs: bare 10-digit values starting
6–9 are redacted unless ID-labelled; unlabelled Luhn-valid 13-digit values are
redacted unless timestamp-labelled or in an event path; `PWD=` is redacted even
when path-like; only `retry pass: <n>` survives as a `pass` counter. The logger scans
`error` values only; plain string fields are logged as supplied.

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
operation spans remain the default when the extension is registered.

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
never opens an SSE stream.

KafkaJS-compatible processing recovery rewinds the active Franz or Confluent
group member to the earliest unprocessed record in every partition that failed
in the same poll wave. This covers handler, finalizer, and post-handler commit
failures without letting one partition hide a sibling failure. With
`CommitBeforeHandler`, a record whose pre-handler commit succeeded is not
replayed when its handler fails, but later fetched records of that partition
group are rewound rather than skipped. Recovery does not replace
`FromBeginning=false` with an earliest-retention fallback. Confluent seeks only
partitions the member still owns and drops rewinds for revoked partitions; if
an exact seek fails, the rewind stays pending and every later `Consume` call
returns an error without reading until the seek succeeds. The consumer is never
rebuilt from `auto.offset.reset=latest`, so callers that retry `Consume` should
back off between attempts. Lost partitions fall back to the revoke notification
only when no distinct lost hook is configured, and never use the safe-revoke
final commit boundary. The legacy `client.Consumer`/`Consume` path keeps main's
log-and-skip semantics.

The owned RESP2 compatibility transport deliberately rejects connection-state
operations that cannot be made reconnect-safe (`MULTI`/`EXEC`, arbitrary
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

MongoDB remains pinned to official main's v2.5 driver because a v2.6 upgrade
would also change BSON validation and error classification outside fit-go's
constructor boundary. Go's module selection still permits an application such
as Metroplex to require v2.6 explicitly. In that case the original dialer
disables adaptive retries while `DefaultInstrumentedDialFunc` retains the selected
driver's retry default; the application owns the remaining dependency-level
BSON/error-classification rollout gate.

## Release and downstream order

1. Merge and tag this integration in the official `gofynd/fit-go` repository
   as a new immutable release (recommended: `v0.2.0`). Do not reuse a fork tag.
2. Run an authenticated organization-wide reverse-dependency search and compile
   every consumer. The integration now keeps every pre-existing all-exported
   struct layout exact and locks those layouts with a compatibility test because
   Go's standard API checker alone does not detect positional-literal breakage.
3. Update fork consumers using the table above and test them against the local
   integration worktree.
4. Replace local paths and `github.com/swapnilfynd/fit-go` replacements with the
   official immutable version, run `go mod tidy`, and verify the checksum.
5. Deploy dependents after the library release. Do not deploy a consumer whose
   `go.mod` still contains a local filesystem replacement.

Metroplex's local `internal/app.go` uses `GetDecodedSecretFromGSM` and
`UseHealthRouteMiddleware`, which are not in `v0.2.0-rc.1`. Its pin test
`cmd/promotions_dependency_pin_test.go` requires the replacement
`github.com/swapnilfynd/fit-go v0.2.0-rc.1` with its recorded checksum, or an
absolute local replacement together with `FIT_GO_LOCAL_INTEGRATION=1`.
Pre-release validation uses the local replacement. At release, repin to the new
official immutable tag and update the pin test.
