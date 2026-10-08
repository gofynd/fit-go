# PR #3 compatibility remediation ledger

## Status and scope

This ledger records the remediation on top of PR head
`98619a6156e0cd2854b71e03e8149bab9cac69d5`, compared with official main
`df96a2884f700d0ebf71ad9afa79727978f73869`:

| Commit | Area |
|---|---|
| `1e3a776` | Baseline v4 remediation patch |
| `534a759` | Redis ioredis Cluster command rejection and same-node redirects |
| `ef2cd9d` | FeatureHub stale-window retention and interrupts |
| `0522954` | gRPC legacy `Init` error contract |
| `57e2b31` | Redaction leaks and scanner cost |
| `69129e4` | Kafka recovery and close/hook lifecycle |
| (this change) | Upstream-default guard tests and documentation |

It separates verified behavior from accepted limitations so a release
description does not promise more than the code and tests provide.

The compatibility objective is:

- retain the public API and defaults from official `gofynd/fit-go:main`;
- put migration-specific runtime behavior behind additive entry points;
- in the opt-in compatibility transports (ioredis, Kafka advanced recovery),
  reject or pause operations whose ordering or side-effect guarantees cannot be
  preserved instead of silently degrading them;
- keep the default log/Sentry paths free of the secret and PII classes the
  redactor recognises (a deliberate output change, listed in
  [Log and Sentry output changes](UPSTREAM_INTEGRATION_MIGRATION.md#log-and-sentry-output-changes-for-existing-main-users)); and
- keep Metroplex buildable with a small integration-only change after an
  official immutable fit-go tag exists.

## Changes made

### Kafka (advanced Confluent and Franz consumers; `69129e4`)

The legacy `client.Consumer`/`Consume` path keeps main's semantics: a handler
error is logged (redacted) and skipped, and a later successful record commits
past it.

- Failed-wave recovery collects the earliest unprocessed offset for every
  partition that failed in one poll wave, so a finalizer or commit error on one
  partition cannot be hidden by an already-rewound sibling. Confluent partition
  workers use the run context, so successful sibling commits are not turned
  into redeliveries.
- `CommitBeforeHandler` in message mode: a handler failure after a successful
  pre-handler commit does not replay the failed record (at-most-once), but
  rewinds to the next record of the group, so later fetched records are no
  longer skipped (both backends).
- Confluent recovery seeks only partitions this member still owns, drops
  pending rewinds for revoked partitions, and keeps a failed exact seek as a
  pending rewind. The next `Consume` call must apply it before reading and
  returns an error without reading while it cannot. The consumer is never
  rebuilt from `auto.offset.reset=latest`; callers that retry `Consume` after
  an error should back off between attempts.
- Legacy-signature `OnPartitionsAssigned/Revoked/Lost` hooks may call `Close`
  without deadlocking; an external `Close` no longer blocks behind a slow hook;
  the legacy revoke hook still runs on external `Close`. A hook that never
  returns still retains its goroutine.
- Confluent rejects `OnPartitionsLost*` hooks (Franz-only feature).

### Redis ioredis compatibility path (`534a759`)

- In Cluster mode, `SCAN`, `KEYS`, `FLUSHDB`, `FLUSHALL`, and `RANDOMKEY` are
  rejected (the error names only the verb) instead of being routed by the hash
  of their first argument.
- An authoritative same-node pipeline redirect returns the per-command replies
  and keeps the Cluster transport; only ambiguous outcomes retire it.
- RESP length/depth/value limits, sanitized bootstrap/PING/topology/redirect
  errors, stable `errors.Is` causes, and the `WriteDisposition` versus
  `MayHaveExecuted` split are unchanged from the baseline.
- Guard: `redis.Init`/`InitDefault` never construct an ioredis transport.

### FeatureHub streaming (`ef2cd9d`; `feature.Init` remains polling)

- The stale-retention deadline is fixed at the first stale notice of a window;
  later stale replies cannot keep a ready snapshot beyond 30 seconds.
- At most one context-change interrupt per stale window; later changes are
  coalesced and the latest context is sent when the window ends.
- Outage retries use equal jitter; a configured reconnect interval above
  30 seconds is honoured as the lower bound. `edge.stale` delays are not
  jittered and stay capped at 30 seconds.
- Non-finite `edge.stale` strings (`NaN`, `Inf`) are not used as delays.
- Legacy `EvaluationContext` snapshot fetches are bounded by
  `FEATURE_FLAG_INIT_TIMEOUT`; the polling refresh path is unchanged.
- Baseline behavior retained: nil-safe disabled clients, terminal-failure
  wakeups, bounded `Retry-After`, backoff reset only after feature-state
  events, one owned stale-expiry timer that cannot be re-armed after `Stop`.

### gRPC legacy parity (`0522954`)

The original `grpc.Init` server emits no log line for `next(err)`, returns the
raw panic value (including `panic("")`), and returns raw response-validation
text. Its panic *log* line is redacted (deliberate). Advanced/managed servers
keep the generic message and redacted diagnostics.

### Redaction (`57e2b31`)

Baseline retained: whole-token Luhn-valid 13–19 digit cards (contiguous and
grouped), labelled and supported international phones, compound password keys,
validated JWT/JWE tokens; hostnames, Kafka topics, UUIDs, IPs, labelled
timestamps, event-path epoch milliseconds and labelled order/invoice IDs are
kept. Fixes in this commit:

- Function-call exception applies only to non-phone arguments:
  `Name(9876543210)` and `call(98765 43210)` are redacted, `f(1234567890)` kept.
- `-`/`.` after a card label (`my_card-4111…`, `card.4111…`) is accepted.
- `PWD=` and `pass:` values are redacted; only an explicit `retry pass: <n>`
  counter is kept.
- Merged >15-digit phone candidates are split so an adjacent ID cannot hide a
  phone; `(415) 555-1234` is recognised.
- IP-guard and timestamp-path look-backs are bounded; a constant-time
  card-label pre-filter keeps the card scanner at about 1.4 ms per 256 KiB with
  0 allocations; a growth-ratio test pins linear scaling.
- `ts`/`timestamp` suffix, `identifier`/`seq`/`size`/`build` operational
  labels and `tel`/`whatsapp`/`msisdn` phone labels are restored; all-caps
  words stay inert.
- A review corpus pins must-redact and must-keep rows.

## Upstream-default guard tests

| Contract | Test |
|---|---|
| Public API / struct layouts | `compatibility/upstream_api_test.go`, `compatibility/descriptive_api_test.go` |
| Root `fit.Init` defaults | `fit_upstream_defaults_test.go` |
| Redis legacy entry points | `redis/legacy_entrypoint_guard_test.go` |
| gRPC legacy vs advanced errors | `grpc/server_test.go` (`TestMiddleware*LegacyContract`, `*LegacyVsAdvanced`) |
| Kafka legacy consumer skip/commit | `kafka/legacy_consume_upstream_guard_test.go`, `TestLegacyConfluent*` |
| Kafka legacy option rejection, acks, empty key | `TestLegacyConfluentConsumerRejectsCompleteOptionsInsteadOfDroppingThem`, `TestLegacyConfluentProducerIgnoresPerCallAcksAndCloseIsBestEffort`, `TestConfluentProducerConstructorSelectsEmptyKeyWireSemantics` |
| `feature.Init` polling, no SSE | `feature/init_upstream_guard_test.go`, `TestLegacy*` in `feature/flags_test.go` |
| `config.GetSecretFromGSM` encoded payload | `TestGetSecretFromGSMPreservesEncodedRESTPayload` |
| Default logger redaction | `logging/upstream_redaction_guard_test.go` |
| `server.New` access-log redaction | `server/upstream_redaction_guard_test.go` |
| `InitSentryWithConfig` sanitizer | `errors/upstream_redaction_guard_test.go` |

The three redaction guards pin that a password value, email, bare phone
`9876543210`, card `4111111111111111`, and Bearer token are masked, while
`10.1.2.3:5432`, `order_id=9876543210`, `ts=1791364800006`, and a UUID stay
verbatim. In the access log, query values are masked unless their key is in
`redact.DefaultQueryAllowlist` (`limit`, `sort`, …); the URL path is logged
unchanged.

## Deliberate non-changes and accepted limits

| Area | Deliberate boundary |
|---|---|
| Default-path output | Log and Sentry output on the original entry points is redacted. There is no API or wire change; see the migration guide section. |
| FeatureHub protocol | The API key remains in FeatureHub's required URL path, server-evaluated context remains in the query/header, a permanent 4xx ends the stream, and each request-scoped `Build` owns its SSE request. |
| Ambiguous phones | Bare 10-digit values starting 6–9 are redacted even when they are order IDs, unless an ID-like label (`order_id=`, `id:`) precedes them. A `+CC` prefix or phone label overrides the ID label. |
| Ambiguous cards | An unlabelled Luhn-valid 13-digit value is redacted unless a timestamp label or `/event(s)/…` path context marks it as epoch milliseconds. |
| Password aliases | `PWD=` is redacted even when the value looks like a path (`PWD=/app`). Only `retry pass: <n>` is kept. |
| Redis command replies | Ordinary Redis command errors remain caller-visible per-reply results and can contain Redis-authored text; callers must redact before logging. |
| Redis connection state | MULTI/EXEC/WATCH, every SELECT, Pub/Sub/monitor, unsafe CLIENT modes, raw QUIT, unbounded/invalid blocking operations, and Cluster-wide commands remain rejected. |
| Redis pipelines | Cross-node pipelines are rejected before writing. Multi-key operations are routed by the first key; this path is not a cluster-wide fan-out API. |
| Redis replay | A fully/partially written request with no authoritative reply can be replayed within the fixed retry budget, including a non-idempotent command. This is the documented ioredis compatibility risk. |
| RESP limits | 512 MiB per bulk string and 1,048,575 scalar members in a top-level array. A limit failure affects the request and other already-sent requests on that connection. |
| Kafka hooks | A rebalance hook that never returns retains its goroutine. Confluent does not support `OnPartitionsLost*`. |
| Kafka pending rewinds | While a Confluent exact seek keeps failing, every `Consume` call returns an error without reading; callers must back off between retries. |
| Kafka payload copying | Produce paths keep defensive deep copies to prevent caller mutation/races. |
| gRPC legacy errors | Original `Init` returns raw panic/validation/handler text on the wire; its panic log is redacted. Managed/advanced servers use the generic boundary. |
| Server/logging/tracing | TraceClue remains the default schema, profiler response-key changes remain, and the legacy tracing initializer retains its owned propagator wrapper. |
| Toolchain | The module directive remains Go 1.25.10. Selecting a patched release toolchain is a release-engineering gate. |

## Metroplex integration state

Metroplex's local `internal/app.go` uses `GetDecodedSecretFromGSM` and
`UseHealthRouteMiddleware`, which are not in `v0.2.0-rc.1`. Its pin test
`cmd/promotions_dependency_pin_test.go` accepts either the replacement
`github.com/swapnilfynd/fit-go v0.2.0-rc.1` with its recorded checksum, or an
absolute local replacement only when `FIT_GO_LOCAL_INTEGRATION=1`. Pre-release
validation uses the local replacement. At release, repin Metroplex to the new
official immutable fit-go tag and update the pin test accordingly.

## Validation

On this host loopback listeners are allowed, so the full `go test ./...` and
`go test -race ./...` suites run, including miniredis, `httptest`, gRPC, and
librdkafka mock-broker tests. Only 8 live-infrastructure tests skip: the 6
`*Live` Kafka broker tests and 2 Redis Sentinel/Cluster live subtests.

Final verification on 2026-10-08 (Go 1.26.4, linux/amd64), on the tree that
contains every change in this ledger:

- `gofmt -l`, `git diff --check`, `go build ./...`, and `go vet ./...` are clean.
- `go test -count=1 ./...` and `go test -race -count=1 ./...` pass with only the
  8 live-infrastructure skips listed above.
- `go mod tidy -diff` is clean. The only module changes from the PR head are
  the OpenTelemetry 1.45.0/contrib 0.70.0 and gRPC 1.83.2 security updates.
- `apidiff -m -incompatible` against official main `df96a28` reports no
  incompatible changes.
- `govulncheck ./...` reports no reachable third-party advisory; 8 reachable
  Go 1.26.4 standard-library advisories are fixed by Go 1.26.6.
- Metroplex (`go build ./...`, `go vet ./...`, and `go test ./...`) passes
  against this tree through an uncommitted local workspace file.

Remaining release gates:

1. Run build, vet, full and race suites on the final tree in CI.
2. Run live-broker Kafka recovery/rebalance tests, including multi-partition
   failures and all commit modes.
3. Compile every known reverse dependency of fit-go against the release
   candidate.
4. Build/test Metroplex against the exact candidate tag, then remove its fork
   replacement.
5. Build with the release-selected patched Go toolchain and rerun
   `govulncheck`.
