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
| Automatic MongoDB command tracing and an explicitly selected newer driver's retry policy | `mongo.DefaultInstrumentedDialFunc`; the original default dialer remains untraced and disables v2.6+ adaptive retries when that optional driver control is available |
| Automatic MySQL OpenTelemetry instrumentation | `mysql.InstrumentedConnectionOptions` and `mysql.InitInstrumented`; the original initializer keeps `database/sql` behavior |
| fit.js/pyfit non-standard AES-GCM nonce sizes | `encryption.NewManagerWithOptions(encryption.ManagerOptions{AllowedNonceSizes: []int{9, 12}})`; the original constructor retains the 12-byte requirement |
| Profiler sample-rate status fields | `profiling.RuntimeConfig`, `profiling.NewRuntime`, and `GetRuntimeConfig`; original `Status`/`Routes` keep their prior JSON shape |
| Redis protocol/retry/ioredis controls | `redis.CompatibilityOptions`, configured dial option/function types, and `redis.InitWithCompatibility` |
| Automatic request IDs, OTel server middleware, process-default metrics, health-middleware bypass, or process profiler routes | `server.RuntimeConfig` and `server.NewRuntime`; use `server.RequestIDWithHeaderPropagation` in a manually assembled runtime chain. Original `server.New`/`server.RequestID` retain official-main behavior |
| Post-main access-log controls | `server.AccessLogConfig` and `server.AccessLog` |
| Fork's no-`traceparent` response behavior | Set `server.OTelMiddlewareOptions.PropagateResponseHeaders` to `false` and use `server.OTelMiddlewareWithOptions`; nil/default retains official-main response propagation |

The helper functions prefer a native extension implementation and safely adapt
an original upstream implementation where semantics can be preserved. They
return an explicit error when advanced controls are requested from a driver
that cannot implement them; controls are never silently discarded.

## Intentional security hardening

Two privacy/security behaviors are deliberately not restored. Sentry now
applies mandatory export sanitization even through the original
`InitSentryWithConfig` path: user PII, request query/body/cookies, sensitive
extras/tags, attachments, logs, and metrics are removed or masked. Callers
should correlate with an opaque allowlisted tag rather than email, username, or
raw user data. Access logs also keep credential/header/query redaction in the
original structural log schema; raw secrets are not a compatibility target.

FIT-style gRPC handler
errors and recovered panics are still converted to a generic Internal response
instead of exposing the raw server error to a remote caller. The original
status code is retained where it is safe, and the detailed error remains
server-side. HTTP-client logging likewise continues to redact credentials,
query values, sensitive path segments, and raw transport errors, and root
startup warnings do not serialize raw tracing or metrics initialization errors.
These changes do not alter successful request/response payloads, but code or
tests that depended on receiving sensitive internal error text must be
corrected rather than migrated to an unsafe compatibility mode.

Database TLS handling is also intentionally fail-closed. Redis, PostgreSQL,
MongoDB, and MySQL now reject incomplete, unreadable, or invalid TLS material;
`rediss://` cannot silently fall back to plaintext. This is an observable
startup change and requires a pre-release environment audit, but restoring the
old insecure fallback is not an acceptable compatibility mode.

One correctness change remains an explicit rollout gate:

- `config.GetSecretFromGSM` returns the decoded Secret Manager payload rather
  than its base64 transport representation. Consumers that manually decoded the
  old result must remove that second decode.

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

Metroplex has completed step 3 locally. Its repository guard rejects the
retired fork and permits a local filesystem replacement only when
`FIT_GO_LOCAL_INTEGRATION=1` is explicitly set for pre-release validation.
