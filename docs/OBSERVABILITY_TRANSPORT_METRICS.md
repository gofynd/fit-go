# Observability Transport and Metrics Parity

This tracker covers the fit-go transport, metrics, database telemetry privacy,
and test-race lane of the Commerce legacy FIT migration. Legacy runtime behavior
remains the reference unless the platform privacy contract requires a stricter
boundary.

## Tracker

| ID | Requirement | Status | Local proof |
|---|---|---|---|
| FG-02 | HTTP propagation and OTel client status parity | Implemented | `go test ./httpclient` |
| FG-03 | Instrument legacy `utils.HTTPClient` | Implemented | `go test ./utils` |
| FG-10 | PII-safe MySQL OpenTelemetry spans | Implemented | `go test ./mysql` |
| FG-11 | Automatic metrics wiring and usable output | Implemented with named textfile fixes | `go test . ./metrics ./server ./httpclient ./utils` |
| FG-15 | Remove Mongo/Redis test-mock races | Implemented | `go test -race ./mongo ./redis` |
| FG-26 | PII-safe PostgreSQL operation spans | Implemented | `go test ./postgres` |
| FG-27 | PII-safe Redis operation spans | Implemented | `go test ./redis` |
| FG-28 | PII-safe HTTP access/client errors | Implemented | `go test ./server ./httpclient ./utils ./redact` |
| FG-29 | Kafka delivery/commit/runtime-option correctness | Implemented | `go test -race ./kafka` |
| FG-30 | Jaeger `uberctx-*` baggage and stale cleanup | Implemented | `go test ./tracing ./httpclient ./kafka` |
| FG-31 | Non-LIFO process-global ownership and external replacement | Implemented | `go test -race ./tracing ./metrics ./logging` |
| FG-32 | TraceClue UTF-16 truncation | Implemented | `go test ./logging` |
| FG-33 | Public span-status description redaction | Implemented | `go test ./tracing -run PublicSpanStatus` |
| FG-34 | FIT/health shutdown and clean reinitialization | Implemented | `go test -race . ./health` |
| FG-35 | TriggerHappy object-message log mapping | Implemented with privacy improvement | `go test ./logging -run TriggerHappyObjectMessageGolden` |
| FG-36 | Privacy-safe gqlgen operation/resolver tracing | Implemented with payload-capture prohibition | `go test ./fitgraphql` |
| FG-37 | Generic OTel meter provider, OTLP/console exporters, and restart-safe instrument routing | Implemented, opt-in | `go test -race . ./otelmetrics` |
| FG-38 | Typed TraceClue extension selection and lifecycle | Implemented with static-link divergence | `go test -race . ./instrumentation` |
| FG-39 | Worker/cron/job/task entry boundaries | Implemented, explicit adoption | `go test -race ./tracing` |
| FG-40 | Strict Convict/Pydantic-style config schema primitives | Implemented with explicit schema porting | `go test -race ./config` |
| FG-41 | Compiled migrations and local/GCS/S3 state | Implemented; cloud state requires application-owned renewable fencing | `go test -race ./migration/...` |
| FG-42 | Safe fitproto-compatible fetch/generation | Implemented with locked transactional replacement and crash recovery | `go test -race ./protofetch/... ./cmd/fitproto` |

The tracker is source-complete in the local worktree. On 2026-07-13,
`go mod tidy -diff`, `go vet ./...`, `go test -count=1 ./...`, and
`go test -race -count=1 ./...` all passed. Metroplex also passed
`go test -count=1 ./...` with a temporary module-file replacement pointing at
this worktree; the real Metroplex dependency files were not modified. Publishing
an immutable fit-go revision, pinning it, adopting opt-in capabilities, and
collecting deployed collector/dashboard evidence remain separate release steps.

## HTTP

`httpclient.NewHTTPClient` clones `http.DefaultTransport`, preserving Go's dial,
pool, HTTP/2, TLS, and timeout defaults, then replaces only the proxy callback.
`WrapTransport` remains available for custom transports.

When fit tracing is enabled, the transport starts a client span and invokes the
process-global OpenTelemetry propagator. This carries `traceparent`, `tracestate`,
and baggage instead of manually constructing one header. Client 4xx and 5xx
responses are marked as span errors, matching the legacy Node OTel HTTP client;
1xx through 3xx leave status unset.

Before injection, the cloned request drops every format fit-go can emit:
`traceparent`, `tracestate`, `baggage`, B3 single/multi headers, Jaeger's
`uber-trace-id`, and any custom fields declared by the configured propagator.
Dynamic Jaeger baggage fields (`uberctx-*`) are also removed even though the
Jaeger propagator cannot enumerate them through `Fields()`.
Matching is case-insensitive. This prevents a propagator change from forwarding
two competing parents. A no-op propagator (`OTEL_PROPAGATORS=none`) is therefore
a real propagation opt-out; unrelated headers remain unchanged.

`utils.NewHTTPClient` retains its public API, timeout, proxy, logging,
interceptors, default headers, and optional custom metric recorder. Its existing
transport is now wrapped by `httpclient.WrapTransport`, so old callers receive
the same tracing and propagation. Supplying a custom metric recorder disables
the process-default recorder for that client to prevent duplicate observations.
Access/client logs retain method, safe scheme/host/path, status, request ID, and
duration. Query values, URL userinfo, opted-in sensitive header values, and raw
transport/provider errors are redacted; callers still receive the original error.

## Kafka

Context-aware producer and consumer APIs preserve the configured OTel wire
format, including Jaeger `uberctx-*` baggage. Produce operations drain every
delivery report accepted by librdkafka, never close a channel still owned by the
driver, coordinate in-flight sends with shutdown, and fail shutdown when
`Flush` reports outstanding records. Per-call `acks` selects a compatible
cached producer rather than silently ignoring the call contract.

Consumer options are validated before polling. `PollTimeout` reaches the driver;
unsupported partition concurrency and an auto-commit mode that conflicts with
the constructed consumer fail explicitly. Manual pre/post-handler commit errors
are returned, while handler errors retain the documented consume-loop behavior.
Telemetry receives generic failure classifications; the caller still receives
the original broker or handler error.

## GraphQL And Process Boundaries

`fitgraphql` is a gqlgen handler extension that creates an internal operation
span and optional resolver spans under the surrounding HTTP/WebSocket span. It
exports operation type, explicitly mapped persisted/allowlisted operation
identity, resolver object/name, and generic error state/count only. Raw
client-supplied operation names, query text, variables, aliases/paths,
arguments, results, panic values, and raw GraphQL errors cannot be enabled,
which is an intentional privacy improvement over the legacy automatic
instrumentation.

`tracing.RunBoundary`, `RunBoundaryWithResult`, and `WrapBoundary` cover worker,
cron, job, and task entry points. They preserve a native or remote parent,
create stable boundary attributes, bridge active context to fit logging and
same-goroutine transports, mark generic error/panic state, and clean up before
return/rethrow. Applications still have to pass the resulting context across
detached goroutines; no library can infer that ownership safely.

## Metrics

The `otelmetrics` package supplies a generic OTel SDK meter provider in addition
to FIT's fixed Prometheus metrics. It supports `otlp`, `console`, `none`, and
comma-separated exporter selection; OTLP gRPC and HTTP/protobuf; common and
metrics-specific endpoint/protocol precedence; export interval/timeout; custom
resources, readers, exporters, and views; and the same resource identity used
by tracing. `fit.Init` enables it only when `OTEL_METRICS_EXPORTER` is present
and not `none`, or when the compatibility switch `OTEL_METRICS_ENABLED=true` is
set. This opt-in prevents an upgrade from creating an unexpected localhost
exporter.

Process-global meter providers use a stable routing provider plus non-LIFO owner
stack. Synchronous instruments and observable callback registrations created
before initialization or under an earlier provider are rebound to each active
SDK lifecycle. Equivalent meter scopes are reused. Shutdown first detaches the
SDK from recording, keeps the privacy-safe OTel error handler active while the
SDK/exporters shut down, then restores the handler predecessor; a closed owner
is never revived. Injected readers/exporters enable the provider unless an
explicit `Enabled=false` overrides them.

The legacy FIT Prometheus path remains separately environment-driven:

The framework boot path is intentionally environment-driven:

```text
FIT_PROMETHEUS_ENABLED=true
METRICS_DIR=/var/data/metrics
FIT_PROMETHEUS_SERVER_ENABLED=true
FIT_PROMETHEUS_AXIOS_ENABLED=true
```

`fit.Init` creates and installs a process-default registry. A fit server with no
explicit recorder and both fit HTTP client implementations then record into that
registry automatically. Explicit recorders continue to take precedence. When
the master switch is off, or `METRICS_DIR` is missing on the `fit.Init` path,
the prior disabled behavior is retained.

With `METRICS_DIR`, the registry atomically rewrites
`<K8S_POD_NAME>-<pid>.prom` every four seconds and performs a final flush during
shutdown. Direct `metrics.New` callers can use the same file output, or omit
`MetricsDir` and expose `Registry.Handler()` as a scrape endpoint. They must call
`metrics.SetDefault` only when automatic server/client recording is desired.
Periodic output errors are stored in `LastFlushError`; callers can install
`Options.OnFlushError` for readiness/alert integration. Without a callback,
fit-go emits a generic error log that deliberately omits the file path and raw
OS error.

Process-default registries are tracked as owners rather than raw previous
pointers. If owners A and B overlap and A shuts down first, B remains installed
and later restores the baseline instead of the already-closed A registry.

Metric names, labels, units, and defaults match deployed fit.js:

- `fit_http_request_duration_ms`, buckets `10,50,100,250,500,1000,2500,5000,10000`
- `fit_http_client_request_duration_ms`, buckets `100,250,500,1000,2500,5000,10000,30000`
- matched server routes use the registered template (`req.baseUrl + req.route.path`
  in Express and `c.FullPath()` in Gin); requests without a useful template use
  the same UUID-then-numeric normalization fallback
- server duration is an integer number of milliseconds (`Date.now() - startTime`
  in fit.js and `time.Duration.Milliseconds()` in fit-go)
- health and readiness probes are not recorded
- remaining labels include method, hostname, status code, and deployment name

### File Output Divergence

The deployed lockfile resolves `prom-file-client` commit `05a55f1`, whose
installed package reports version 0.1.1 even though the package manifests request
tag 0.1.2. Its behavior is:

- flush on a 4-second interval, with no initial write
- `<K8S_POD_NAME>-<pid>.prom` in Kubernetes; otherwise
  `metrics-<random-number>-<pid>.prom`
- a forked writer process that opens with `wx`; the first write can succeed, but
  every later write to that filename fails because the file already exists

fit-go preserves the deployed Kubernetes contract: the 4-second interval and
`<K8S_POD_NAME>-<pid>.prom` filename. It intentionally writes once during
initialization and uses Prometheus' `WriteToTextfile`, which writes a temporary
file and atomically renames it over the process file. This fixes both the empty
first interval and the `wx` refresh defect. The local/non-Kubernetes fallback is
the deterministic `metrics-<pid>.prom`, and no child writer is forked. These are
named behavioral fixes/divergences, not byte-for-byte parity; tests lock both the
preserved Kubernetes contract and the intentional differences.

## Global Lifecycle And Compatibility

Tracer providers, propagators, OTel meter providers, Prometheus registries, and default slog loggers use
ownership chains that tolerate out-of-order shutdown. `New`, `SetGlobal`, its
restore callback, and tracer shutdown can overlap without reviving a closed
predecessor. Provider and propagator ownership are checked independently, so an
unrelated component that replaces either process global remains installed.

`fit.Shutdown` stops and waits for periodic health work, removes registered
checks and the health file, clears connections and the error-registry handle,
and installs a fresh checker. A later `fit.Init` therefore cannot inherit state
from the prior lifecycle.

TraceClue-compatible body and metadata limits count JavaScript UTF-16 code units
instead of Go runes. Astral Unicode therefore has the same length accounting and
truncation boundary used by the installed JavaScript formatter.

TriggerHappy's object-first Winston calls are available through the explicit
`DebugObject`/`InfoObject`/`WarnObject`/`ErrorObject` APIs. Golden tests pin its
empty-body, nested-object, string-message, and array-message behavior. See
`TRACECLUE_COMPATIBILITY.md` for the exact fit.js/TraceClue commits and the
intentional error-value privacy difference.

## Migration And Proto Tooling

`migration.Runner` checkpoints every successful step, imports Node/pyfit state,
normalizes aliases, detects checksum/registry drift, preflights irreversible
reverts, and requires an explicit lock. Local file state is fsynced and atomically
renamed. Cloud stores fail construction unless paired with a renewable
`LeaseLocker` and an application-owned fenced writer that atomically validates a
monotonic token. The same token is exposed to `Up`/`Down` through
`FenceTokenFromContext`; idempotency remains required because no generic runner
can atomically combine arbitrary application mutations with its state object.

`protofetch` requires output to be a strict descendant of an explicit root,
rejects traversal/symlinks/non-regular files and source/output overlap, suppresses
child-process output, and serializes writers. It fsyncs the generated tree and
uses sibling renames plus a backup to transactionally replace output. Startup
recovers an interrupted pre-install rollback or finishes a post-install commit;
directory replacement is not claimed to be one atomic filesystem operation.

## MySQL

MySQL uses `github.com/XSAM/otelsql`, the maintained `database/sql`
instrumentation derived from the OpenTelemetry Go contrib implementation. The
wrapper is installed only when fit tracing is enabled; disabled clients continue
through raw `sql.Open`.

`DisableQuery` is always enabled. Spans contain the static MySQL system identity
and operation lifecycle but never SQL statements, parameters, DSNs, usernames,
passwords, SQLCommenter data, or raw driver errors. Driver failures are returned
to the caller but their potentially sensitive text is not recorded as a span
event or status description.

## PostgreSQL

PostgreSQL retains `otelpgx` operation spans and duration/error metrics behind a
privacy wrapper. SQL is reduced to an allow-listed operation before it reaches
the upstream tracer; arguments are removed, connection objects are withheld,
and SQL/connection attributes are disabled. This keeps query, batch, copy,
prepare, connect, and pool-acquire lifecycle visibility without exporting SQL,
bind values, statement names, table names, usernames, database names, server
addresses, ports, or passwords.

Backend errors are still returned unchanged to the application. The tracer sees
only `postgresql operation failed` (except the fixed no-rows sentinel), so a
provider error that echoes SQL or values cannot enter an exception event or span
status description.

## Redis

Redis uses a package-owned go-redis hook instead of `redisotel`. The upstream
hook suppresses command statements when configured but still calls
`RecordError(err)` and copies `err.Error()` into span status. The fit-go hook
emits fixed `redis.command`, `redis.pipeline`, and `redis.dial` client spans,
passes the child context to the underlying driver, and records only static
database/operation attributes plus pipeline size.

Keys, values, scripts, command text, credentials, endpoints, and raw backend
errors are never read into telemetry. Failures remain unchanged for the caller
and receive only the generic span status `redis operation failed`; Redis's
expected missing-key sentinel remains non-error.
