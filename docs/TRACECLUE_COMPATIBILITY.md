# TraceClue Log Compatibility

`SchemaTraceClue` / `FIT_LOG_SCHEMA=traceclue` is an opt-in wire-compatibility
mode. The normal fit-go platform schema remains the default.

## Installed Legacy Profiles

| Legacy runtime | Formatter behavior | fit-go setting |
|---|---|---|
| Sentinel and Pointblank, TraceClue 3.1.3 | 500-character body limit only when configured `LOG_LEVEL=debug` | `debug-only` (default) |
| Highbrow, TraceClue 3.0.5 | 500-character body limit at every level | `always` |
| TriggerHappy, TraceClue commit `c61fd045` (package 2.1.2) / `winston-opentelemetry-format` 0.0.4 | 500-character body limit at every level | `always` |

Select the older profile with `FIT_TRACECLUE_BODY_TRUNCATION=always`, or set
`Options.TraceClueBodyTruncation` directly. `never` is available only as an
explicit Go policy override.

When a body is truncated, fit-go emits `_body_too_large=true` and
`_body_original_length`. `TraceClueRestrictAttributesTo` enables the legacy
structured mode: unlisted fields are encoded under `_meta`, limited to 1,000
characters by default, with `_meta_too_large` and `_meta_original_length`.
`TraceClueDiscardAttributesFrom` removes named fields before packing.

The compatibility envelope preserves the deployed keys, severity-number quirk
(`warn` and `fatal` omit it), timestamp shape, resource object, and active native
OTel trace/span IDs.

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
