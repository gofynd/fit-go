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

Advanced Kafka rebalance callbacks that may request shutdown must use the
`OnPartitions*WithLifecycle` form and call the supplied
`RebalanceLifecycle.Close`. The original callback signatures remain source
compatible, but synchronously calling the consumer's ordinary `Close` from
inside those callbacks is not safe; external and signal-handler calls to
`Close` remain synchronous and wait for the active run. A legacy callback that
never returns necessarily retains its callback goroutine and can keep an
external close waiting; use the lifecycle callback plus a bounded callback.

## Intentional security hardening

Two privacy/security behaviors are deliberately not restored. Sentry now
applies mandatory export sanitization even through the original
`InitSentryWithConfig` path: user PII, request query/body/cookies, sensitive
extras/tags, attachments, logs, and metrics are removed or masked. Callers
should correlate with an opaque allowlisted tag rather than email, username, or
raw user data. Access logs also keep credential/header/query redaction in the
original structural log schema; raw secrets are not a compatibility target.

FIT-style gRPC handler errors and recovered panics are converted to a generic
Internal response on the explicit advanced/managed server. The original `Init`
entry point retains upstream-main's raw message behavior because changing that
wire contract would break existing consumers; services needing the hardened
boundary must migrate to `InitWithOptions`. HTTP-client logging likewise
continues to redact credentials, query values, sensitive path segments, and raw
transport errors, and root startup warnings do not serialize raw tracing or
metrics initialization errors.

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
`Retry-After`; only feature-state events reset that backoff. Positive stale
delays are floored by the reconnect interval and capped at 30 seconds. A cached
ready snapshot expires after 30 seconds of stale-only responses, rapid context
changes are rate-limited while stale, and stopping a client cancels its one
owned expiry timer before another can be armed.

KafkaJS-compatible processing recovery rewinds the active Franz or Confluent
group member to the earliest unprocessed record in every partition that failed
in the same poll wave. This covers handler, finalizer, and post-handler commit
failures without letting one partition hide a sibling failure. A successful
`CommitBeforeHandler` boundary is not rewound when the subsequent handler
fails. Recovery does not replace `FromBeginning=false` with an
earliest-retention fallback and therefore cannot replay a fresh group's
historical retention merely because its first handled record failed. Transport
recovery continues from broker-committed offsets. Lost partitions fall back to
the revoke notification only when no distinct lost hook is configured, and
never use the safe-revoke final commit boundary.

The owned RESP2 compatibility transport deliberately rejects connection-state
operations that cannot be made reconnect-safe (`MULTI`/`EXEC`, arbitrary
`SELECT`, subscriptions, monitor mode, and indefinite blocking commands).
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

Metroplex's current local source uses `GetDecodedSecretFromGSM` and
`UseHealthRouteMiddleware`, so it requires this integration tree (or a future
official tag containing those APIs). Its checked-in module replacement still
points at `github.com/swapnilfynd/fit-go v0.2.0-rc.1`, which does not provide the
complete integration surface. Pre-release validation therefore uses an
explicit local workspace/replacement; the fork pin must be replaced with the
new official immutable tag before deployment.
