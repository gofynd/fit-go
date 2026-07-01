# Changelog

All notable changes to this fork of `github.com/gofynd/fit-go` are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/);
this fork follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> This is the GoFynd-Commerce working fork of the public `fit-go` snapshot,
> carrying the observability + platform-parity work used by the `metroplex`
> service while it is upstreamed. See `docs/` for the design notes.

## [Unreleased]

### Added
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

### Notes / follow-ups
- Testcontainers-based integration tests for redis/mongo/kafka/postgres are a
  planned follow-up; current coverage is unit tests plus live-infra runtime
  validation.
- Differentiators kept vs the canonical `CommonLibraries/go-fit`: encryption,
  PII-safe redis, Postgres/MySQL, the bounded mongo map, and the redis
  CLIENT-rejection fallback — candidates to upstream.
