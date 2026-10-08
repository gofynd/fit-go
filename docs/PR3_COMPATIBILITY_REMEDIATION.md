# PR #3 compatibility remediation ledger

## Status and scope

This document records the local, uncommitted remediation on top of commit
`98619a6156e0cd2854b71e03e8149bab9cac69d5`. It distinguishes verified behavior
from accepted limitations so a release description does not turn a best-effort
compatibility path into a stronger promise than the code provides.

The compatibility objective is:

- retain the public API and defaults from official `gofynd/fit-go:main`;
- put migration-specific runtime behavior behind additive entry points;
- fail closed when a compatibility transport cannot preserve ordering or
  side-effect guarantees;
- keep errors/logs free of secrets and PII; and
- keep Metroplex buildable with a small integration-only change after an
  official immutable fit-go tag exists.

## Changes made

### FeatureHub streaming

- Disabled/nil clients and evaluation contexts are nil-safe.
- A permanent HTTP response records terminal failure and wakes readiness
  waiters instead of silently ending the loop.
- Transient failures use bounded exponential backoff. `Retry-After` seconds and
  HTTP-date values are honored and capped at 30 seconds; malformed oversized
  numeric values also cap at 30 seconds.
- Backoff resets only after `features`, `feature`, or `delete_feature`, never
  after `ack` or another non-state event.
- Positive `edge.stale` values are floored by the configured reconnect interval
  and capped at 30 seconds. A previously ready snapshot is retained for at most
  30 seconds, after which readiness is cleared with an explicit snapshot-expiry
  error. A real feature event clears stale retention before publishing ready.
- A server-evaluated context change can interrupt a stale wait, but interrupts
  are rate-limited by the reconnect interval. Context changes during an outage
  do not bypass failure backoff.
- One replaceable timer owns stale expiry; replacing state stops the old timer,
  and the stopped-client check is made while holding the timer lock so `Stop`
  cannot race with a newly armed expiry timer.

### Redaction

- Payment cards are matched as complete 13–19 digit Luhn-valid tokens. A
  Luhn-valid prefix of a longer number is not redacted by itself.
- Contiguous and grouped PANs are covered, including double/mixed separators,
  8-8 and Diners-style grouping, compound/suffix labels (`customer_card`,
  `giftcard`, `cardpan`, `pan.number`, and `card number`), and a punctuation or
  decimal suffix after a complete PAN.
- IPv4 addresses are not extended into labelled card matches. Explicit
  timestamp labels (`timestamp_ms`, `time`, `sent_at`, and related forms) and
  plausible epoch-millisecond values in `/event(s)/...` paths are preserved.
- Explicit country-code phones override operational labels: values such as
  `id=+919876543210` and `version: +1 415 555 1234` are redacted. Labelled phone
  fields are normalized across snake_case, camelCase, quoted JSON keys, and
  common `phone`/`mobile`/`msisdn`/`whatsapp`/`telephone` forms.
- Indian compact and 5-5 mobile forms, supported dotted forms, 0091 forms,
  North-American forms, and common international/local groupings are redacted,
  including multiple phones in one candidate and URL path segments.
- ID-like exemptions require a real delimiter/camel boundary. All-cap words
  such as `PAID`, `VALID`, `UID`, and `HOTEL` do not accidentally become
  `id`/`tel` labels.
- Protected-span lookup and bounded label look-back are linear; tests retain
  the existing at-most-one-allocation guard for a large numeric-list input.

### Kafka failed-wave recovery

- Advanced Franz and Confluent consumers collect the earliest unprocessed
  offset for every partition that fails in one poll wave. A finalizer or commit
  error on one partition can no longer be hidden by an already-rewound sibling.
- Confluent partition workers use the run context rather than cancelling the
  whole wave on the first error, so successful sibling commits are not turned
  into redeliveries.
- Post-handler commit failures carry an exact rewind boundary. Franz applies
  all offsets in one `SetOffsets` call; Confluent seeks all affected partitions
  and rebuilds from committed offsets only if that exact seek fails.
- A handler failure after a successful `CommitBeforeHandler` commit carries an
  authoritative empty rewind plan. It is surfaced to the caller without
  replaying an offset that was intentionally committed before user code ran.

### Redis ioredis compatibility path

- RESP length/depth/value limits prevent process panics and unbounded recursive
  decoding. Malformed integer/length/prefix errors omit the raw protocol bytes.
- PING, Sentinel/Cluster bootstrap, slot refresh, redirects, and routing errors
  omit raw server values, topology reply data, keys, and command arguments.
- Sanitized errors preserve stable transport identities such as `io.EOF`,
  `io.ErrUnexpectedEOF`, context cancellation/deadline, and the compatibility
  closed error for programmatic `errors.Is`/`errors.As` checks.
- `WriteDisposition` records socket progress and is intentionally independent
  of `MayHaveExecuted`; a fully written request can be proven not executed by
  an authoritative redirect. `IORedisRedirectError` exposes both fields through
  `errors.As`, and authoritative redirect chains stop after 16 attempts without
  retiring the whole Cluster transport.

## Deliberate non-changes and accepted limits

| Area | Deliberate boundary |
|---|---|
| FeatureHub protocol | The API key remains in FeatureHub's required URL path, server-evaluated context remains in the query/header, a permanent 4xx ends the stream, and each request-scoped `Build` owns its SSE request. |
| Ambiguous card values | An unlabelled plausible 13-digit value that passes Luhn is redacted unless timestamp/path context makes it unambiguous. This favors privacy over perfect numeric-ID precision. |
| Ambiguous phones | Bare Indian mobile-shaped values are redacted. A compact value may be retained only behind an explicit ID-like label; adding `+CC` or a phone label makes it fail closed. |
| Redis command replies | Ordinary Redis command errors remain caller-visible per-reply API results and can contain Redis-authored text; callers must apply the platform redactor before logging. |
| Redis connection state | MULTI/EXEC/WATCH, every SELECT, Pub/Sub/monitor, unsafe CLIENT modes, raw QUIT, and unbounded/invalid blocking operations remain rejected. |
| Redis pipelines | Cross-node pipelines are rejected before writing. Multi-key operations are routed by the first key; this path is not a cluster-wide fan-out API. |
| Redis replay | A fully/partially written request with no authoritative reply can be replayed within the fixed retry budget, including a non-idempotent command. This is the documented ioredis compatibility risk. |
| RESP limits | The fixed decoder ceilings remain 512 MiB per bulk string and 1,048,575 scalar members in a top-level array. A limit failure affects the request and other already-sent requests on that connection. |
| Kafka callback lifecycle | Legacy rebalance callbacks must not call ordinary `Close` synchronously and must return. Use `*WithLifecycle`; a callback that never returns can retain its goroutine and hold an external close. |
| Kafka payload copying | Produce paths keep defensive deep copies to prevent caller mutation/races. |
| gRPC legacy errors | The original initializer retains upstream response text; managed/advanced initialization uses the hardened generic boundary. Panic logs are redacted. |
| Server/logging/tracing | TraceClue remains the default schema, profiler response-key changes remain, and the legacy tracing initializer retains its owned propagator wrapper. |
| Toolchain | The module directive remains Go 1.25.10. Selecting a patched release toolchain is a release-engineering gate. |

## Metroplex integration state

Metroplex source is not unchanged: its local `internal/app.go` uses
`GetDecodedSecretFromGSM` and `UseHealthRouteMiddleware`. The checked-in module
replacement still targets `github.com/swapnilfynd/fit-go v0.2.0-rc.1`, which
does not contain the complete local integration API. Until fit-go is merged and
tagged upstream, Metroplex must be validated through the explicit local
workspace/replacement. Before deployment, repin it to the new official immutable
tag and remove the retired-fork replacement.

## Validation evidence and remaining gates

The following checks ran on 2026-10-08 against the cumulative local worktree
containing every change described in this ledger, based on
`98619a6156e0cd2854b71e03e8149bab9cac69d5`:

- all fit-go packages compiled in normal and race modes with `go test ./...
  -run '^$'`;
- `go vet ./...`, `go mod tidy -diff`, the compatibility and redaction packages
  in normal/race modes, focused FeatureHub/Redis remediation tests in
  normal/race modes, and every Kafka test except the two loopback mock-broker
  fixtures in normal/race modes passed;
- every Metroplex package compiled and `go vet ./...` passed against this exact
  local fit-go worktree through an explicit workspace file; and
- `git diff --check` passed.

This sandbox does not allow loopback listeners. The full Redis suite therefore
reached its expected socket-permission failures, and the full Kafka package
reached the librdkafka mock-broker socket restriction; focused non-socket tests
passed. Socket-backed FeatureHub, Redis, TLS, gRPC, and mock-broker tests must
be rerun in a normal development/CI host. `apidiff`, live Kafka
broker tests, vulnerability scanning, and an authenticated organization-wide
reverse-dependency build remain release gates.

Before tagging:

1. Run `go build ./...`, `go vet ./...`, `go test ./...`, and the full race suite
   on the final tree in CI with loopback sockets enabled.
2. Run live-broker Kafka recovery/rebalance tests, including multi-partition
   failures and all commit modes.
3. Run `apidiff` against current official main and compile every known reverse
   dependency.
4. Build/test Metroplex against the exact candidate tag, then remove its fork
   replacement.
5. Build with the release-selected patched Go toolchain and rerun
   `govulncheck`.
