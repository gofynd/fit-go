# TraceClue Log Compatibility

`SchemaTraceClue` / `FIT_LOG_SCHEMA=traceclue` is the global wire format
default. The platform schema remains available through an explicit
`SchemaPlatform` or `FIT_LOG_SCHEMA=platform` selection.

## Installed Legacy Profiles

| Legacy runtime | Formatter behavior | fit-go setting |
|---|---|---|
| Sentinel and Pointblank, TraceClue 3.1.3 | 500-character body limit only when configured `LOG_LEVEL=debug` | `debug-only` (default) |
| Highbrow, TraceClue 3.0.5 | 500-character body limit at every level | `always` |
| TriggerHappy, TraceClue commit `c61fd045` (package 2.1.2) / `winston-opentelemetry-format` 0.0.4 | 500-character body limit at every level | `always` |
| pyfit 1.10 queue formatter | 500-character body limit outside debug | `non-debug` |

Select a profile with `FIT_TRACECLUE_BODY_TRUNCATION=debug-only|non-debug|always|never`,
or set `Options.TraceClueBodyTruncation` directly.

When a body is truncated, fit-go emits `_body_too_large=true` and
`_body_original_length`. `TraceClueRestrictAttributesTo` enables the legacy
structured mode: unlisted fields are encoded under `_meta`, limited to 1,000
characters by default, with `_meta_too_large` and `_meta_original_length`.
`TraceClueDiscardAttributesFrom` removes named fields before packing.

The compatibility envelope preserves the deployed keys, severity-number quirk
(`warn` and `fatal` omit it), timestamp shape, resource object, and active native
OTel trace/span IDs.

## HTTP Access Attributes

`server.LogRequestResponse` follows the selected log schema. With the default
platform schema it keeps the operational `query_params` and `path_params`
fields and includes the existing response `duration` field. With
`FIT_LOG_SCHEMA=traceclue`, it emits the legacy access-log shape: a redacted
`request_url` that may include the query string, route values under
`request_params`, and no middleware duration attribute. Query values and
configured headers remain redacted in both modes; selecting TraceClue never
disables the Commerce privacy boundary.

## TriggerHappy Object Messages

TriggerHappy locks fit.js commit `ff7ff1ff` (package 0.4.4). That lock requests
TraceClue v2.1.3 but resolves commit `c61fd045`, whose package metadata is 2.1.2,
and `winston-opentelemetry-format` 0.0.4. The installed formatter has these
observable mappings for `logger.info({...})` / `logger.error({...})`:

| Input object shape | TraceClue body | TraceClue attributes |
|---|---|---|
| no `message` property | empty string | all enumerable object properties |
| string `message` | that string | empty; sibling properties are dropped |
| object `message` | empty string | enumerable properties of `message`; siblings are dropped |
| array `message` | that array | empty; sibling properties are dropped |

Use `DebugObject`, `InfoObject`, `WarnObject`, or `ErrorObject` when a migration
must preserve that mapping. The normal string methods retain their existing Go
interfaces. Error values in attributes are sanitized by fit-go instead of
serializing provider text; this is a deliberate platform privacy improvement.

## Service Identity

Trace and TraceClue-log resources use the same precedence: explicit option,
`OTEL_SERVICE_NAME`, explicit `service.name` resource attribute,
`OTEL_RESOURCE_ATTRIBUTES` (traces), `SERVICE_NAME`, then `unknown_service`.
Legacy TraceClue did not consult `SERVICE_NAME`, so TriggerHappy could report an
unknown telemetry service even while FIT routing and Kafka used `triggerhappy`.
Using `SERVICE_NAME` only as the final telemetry fallback is an explicit identity
improvement; an explicit `OTEL_SERVICE_NAME` still wins and represents a named
dashboard migration.

## Tracing Activation

The normal fit-go contract is explicit: `TRACING_ENABLED=true` enables the
tracer. `FIT_TRACING_ACTIVATION_MODE=pyfit` is available for ports whose
deployment contract relied on pyfit import-time activation. In that mode,
`OTEL_SDK_DISABLED` being absent or empty enables tracing; every non-empty
value disables it, including `false`. Root `fit.Init` and direct
`tracing.New` resolve this identically.

This unusual truthiness rule is compatibility behavior, not a recommended new
deployment convention. New Go services should use the explicit mode.

## Instrumentation Extensions

TraceClue accepts `TRACECLUE_EXTRA_INSTRUMENTATIONS=package:Class,...` and a
JSON `TRACECLUE_INSTRUMENTATION_CONFIGS` map, then loads JavaScript/Python code
at runtime. fit-go preserves the selection/config contract without reproducing
unsafe dynamic loading: applications register typed factories in
`instrumentation.Registry`, including any legacy `package:Class` aliases, and
pass that registry to `fit.Init`.

Configured but unregistered names fail startup. Factories receive only their
JSON config, start in deterministic order, roll back on partial startup, and
shut down once in reverse order. Configuration alone does not silently enable a
hook; it must be enabled by default or selected in the extra-instrumentation
list. Legacy extension env is ignored unless the typed facility is explicitly
activated by a registry/options or `FIT_INSTRUMENTATION_ENABLED=true`; this lets
deployments retain stale TraceClue variables during migration. This is the
statically linked Go equivalent, not plugin byte parity.

## GraphQL And Non-HTTP Entry Points

Legacy GraphQL instrumentation automatically patched supported runtimes and
could capture documents, variables, arguments, results, and errors. The
`fitgraphql` gqlgen extension must be registered explicitly and intentionally
exports operation type, allowlisted operation identity, field identity, and
generic error state. Raw client operation names are omitted unless an explicit
`OperationNameMapper` maps them to a bounded persisted/allowlisted identity.
Payload capture has no opt-in because application data does not belong in
telemetry.

Workers, crons, jobs, and detached tasks should run through
`tracing.RunBoundary` (or `RunBoundaryWithResult`). This creates a stable entry
span, preserves remote/native parent context, and bridges active-span logging
without recording payloads or raw errors.

## Metrics And Profiling

`otelmetrics` supplies the generic OTel meter-provider path that TraceClue
configured implicitly. It follows the signal-specific/common endpoint and
protocol precedence, standard exporter/interval/timeout variables, shared
resource identity, and ownership-safe shutdown. A stable process-global router
rebinds pre-existing synchronous instruments and observable callback
registrations across repeated provider lifecycles. Runtime exporter errors use
an explicit handler; root fit-go emits only the error type. It is independent of
the FIT Prometheus textfile metrics and remains opt-in.

pyfit's `PROFILING_SAMPLE_RATE` is parsed and reported for compatibility. The
current Go profiler has a fixed effective 100 Hz rate, so fit-go reports both
the requested and effective values and marks the setting non-configurable.
`profiling.TagWrapper` adds scoped Pyroscope/pprof labels without mutating
process-global tags.

The wrapper intentionally applies labels when tags are present. Some pyfit
releases contained an inverted decorator condition that skipped the labelled
path when tags were supplied; fit-go treats that as a legacy defect rather
than reproducing it.
