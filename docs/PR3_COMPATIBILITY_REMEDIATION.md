# PR #3 compatibility remediation ledger

## Status and scope

### 2026-10-09 release candidate `v0.2.0-rc.6`

This candidate packages the post-rc.5 Sentry credential-key and HTTP abnormal-
exit fixes, regression tests, Kafka interface documentation and README/example
corrections detailed below. It is prepared as a new immutable tag on the PR #3
source branch, not a rewrite of rc.5. Publication must be verified against the
remote tag and its downloaded checksum before a consumer pin is accepted.
Metroplex's separate SDK sanitizer changes remain local to Commerce and are not
contained in the library release.

Original-main APIs, constructor defaults, Kafka offsets/acknowledgements, Redis
support, dependencies and the Go 1.25.10 minimum are unchanged by this candidate.
The deliberately retained `x/net` v0.58.0 security decision and outstanding
live/reverse-consumer/deployment gates remain in force. Publication is not a
merge-readiness or security-clean certification.

The dated local status and evidence below describe the preparation stages;
their references to uncommitted/unpublished source are historical after rc.6
publication. Current remote-module Metroplex validation must be recorded in its
canonical library document, independently of earlier workspace results.

Fresh candidate validation used `GOWORK=off`: full build, vet and tests on Go
1.25.10, and the full race suite on Go 1.26.9, passed. Both test runs covered
33 test-bearing packages with zero failures and ten conditional infrastructure
skips (eight live Kafka tests and Redis live Sentinel/Cluster subtests).
`go mod tidy -diff` was clean; `go mod verify` passed. Module API comparison
against official main `df96a28` found zero incompatible exported changes.
Artifacts are retained in `/tmp/fitgo-rc6-publication.ymCQ4R/` as
`fitgo-tests.json`, `fitgo-race.json` and `candidate.exp`. These checks do not
replace live-broker/topology or organization-wide reverse-consumer testing.

### 2026-10-09 post-rc.5 review follow-up (historical local preparation)

The published PR head remains `2ab9e42cd9b08121194dde3271bdc535679cb970`
and the immutable fork tag remains `v0.2.0-rc.5`. The corrections in this
section are pending local changes, **not included in that tag or remote PR
diff**. Updating the PR description does not publish these source changes.

| Finding | Local correction | Blast-radius boundary |
|---|---|---|
| Structured Sentry fields lost credential-key context, allowing strings and numeric CVV/OTP/PIN/card values through | Mask complete credential components (`pwd`, `pass`, `passphrase`, `pin`, `otp`, `cvv`, `cvc`, `pan`) and compound card/account/verification labels before inspecting the value; apply the same classification to request headers | Preserve existing long-name rules; no short-substring matching of `compass`, `shipping`, `span_id`, `cardinality` or `card_count`; actual SDK event/transaction tests cover 12 map surfaces and opaque trace IDs |
| Metroplex initializes Sentry independently, and some string-map/frame fields and pre-serialized SDK caches bypassed key classification | Mirror the private credential-key policy, sanitize cloned tags/user metadata/request/frame maps, and project a fresh SDK event to rebuild cached JSON | Application-owned change, not supplied by the library tag; no new library API requirement, so Metroplex still builds with `GOWORK=off` against rc.5; preserve public SDK fields, safe sampling metadata, trace IDs and immutable caller maps |
| An instrumented HTTP transport left its span recording after panic or `runtime.Goexit` | Install deferred span cleanup immediately after creation; use fixed safe error status on abnormal exit | Do not recover, change panic identity, return a synthetic error or expose panic values; normal response/error identity and span-end ordering before logs/metrics are retained |
| Kafka interface comments overstated automatic tracing on released raw constructors | Describe constructor-dependent tracing and explicit-context helpers | Documentation-only; no Kafka runtime, offset, acknowledgement or subscription change |

No public API, constructor default, HTTP/event/data schema, datastore operation,
deployment manifest, dependency or Go-minimum change is part of this follow-up.
The intentional output change is additional credential masking in Sentry; tags
that previously contained such credentials can change, but operational values
and trace correlation controls are tested separately. Ambiguous complete labels
such as `pin` are treated as sensitive, regardless of their value type. This is
not a claim that arbitrary unlabeled data or every possible credential spelling
is identifiable. Generic text redaction and previously accepted transport
limitations are unchanged.

Final-source validation artifacts for this follow-up are retained in
`/tmp/fitgo-rc5-remediation.wZm9hA/`. Minimum-Go ordinary tests/build/vet and the
Go 1.26.9 full race suite pass all 32 test-bearing fit-go packages, with ten
expected live-infrastructure test/subtest skips. Export comparison against
official main `df96a28` reports zero incompatible changes. Focused repeated
race suites and independent transport/SDK export probes pass. Coverage is
84.9% for `errors` (both key classifiers 100%), 91.6% for `httpclient`, and
88.4% for Metroplex shared observability. Metroplex full published-pin and
candidate-workspace tests/build/vet pass: 270 test-bearing packages and 106
conditional test/subtest skips per graph. Targeted candidate HTTP/Sentry/
scheduler race checks pass nine packages with zero skips. Exact evidence is
recorded separately in its library document; the existing workfile is unchanged.
Existing no-skip broker/topology evidence below predates this follow-up; live
fixtures, UAT/prod and organization-wide reverse consumers were not rerun here.

A fresh Go 1.26.9 `govulncheck` on this local source still reports the five
reachable `x/net` advisories listed below. The user-requested v0.58.0 dependency
and Go 1.25.10 minimum are unchanged. Security-clean/merge-ready certification
is not claimed. Publication of these local fixes requires a new commit and
immutable tag followed by a fresh Metroplex pin test; rc.5 must not be moved.

The subsequent local README/example follow-up corrects constructor-dependent
Kafka tracing/acknowledgements and connection setup, adds explicit runtime HTTP
and Redis adoption, and documents FeatureHub readiness, GSM decoding, health
ownership and profiler status. The directly linked transport/metrics notes are
aligned with those constructor boundaries. Only documentation and the Kafka
example/helper tests change in this follow-up; library behavior, dependencies,
Metroplex source/pin and published tags remain untouched.

Validation for this documentation/example follow-up: Go 1.25.10
`GOWORK=off go test ./examples/...` and `go vet ./examples/...` pass; the four
Kafka example shutdown tests pass under Go 1.26.9 `-race -count=20`, covering idle
topics, early failure, shutdown errors and timer races. New Redis/server/HTTP/
Kafka/FeatureHub README API snippets compile on Go 1.25.10; snippet compilation
does not execute their external dependencies. Local Markdown link-target and
`git diff --check` checks pass. The full-suite counts above predate the new
example test file; no new full-suite or live-broker run is claimed here.

### 2026-10-09 cumulative runtime remediation (`v0.2.0-rc.5`)

Release candidate `v0.2.0-rc.5` packages this repair pass on top of published
`9269ce6` / immutable `v0.2.0-rc.4`, compared with official main `df96a28`.
Existing tags are not rewritten. Metroplex must select the new remote tag and
verify its checksum and tests with `GOWORK=off`; the earlier temporary-workspace
results below are pre-publication evidence, not proof of a remote-module pin.
Metroplex's application-owned Sentry repair remains a separate local change.
Publishing this candidate does not deploy either repository or certify the PR
as merge-ready. No Kubernetes operation or remote PR-description edit is included.

| Review finding | Scoped correction | Required regression boundary |
|---|---|---|
| Kafka partition worker exits without a result | Private worker checkpoints report abnormal termination and retain the first unresolved offset; already successful pre-handler commits are not undone | Message/batch, both backends, Goexit in handler/finalizer/commit, multi-partition waves, all commit modes, repeated retry and legacy synchronous behavior |
| Shutdown hides real errors | Suppress cancellation only if every joined error branch is cancellation-related; advanced Confluent no longer discards arbitrary handler failures | Pure cancellation, joined application/cancellation errors, independent sibling failures, message/batch, legacy error identity |
| Advanced Confluent partial batch poll loses fetched records | Rewind owned collected records before returning a subsequent poll error | Retry on the same consumer, exact fetched offsets and ownership; original-main legacy collection behavior remains unchanged |
| Cluster public Quit never completes | Graceful control operation drains accepted commands and settles the lifecycle across success, failure and cancellation | Ready/offline, cluster/standalone, repeated Quit, queued commands and real Redis topology tests |
| FeatureHub event limit only bounds lines | Shared accumulator bounds aggregate event fields in both streaming and Build snapshots | Multiline/empty/metadata fields, exact bounds, oversized events, reset, CR/LF/BOM framing and unfinished EOF |
| DATE rollout timezone mismatch | Convert supplied DATE values to UTC before comparison, as the pinned JS SDK does | Positive/negative timezone crossings and DATE/DATETIME controls; original polling unchanged |
| Managed slog restoration leaves stdlib logs behind | Snapshot and restore the standard writer/flags with the slog ownership chain, preserving independent replacements | All owner restore orders, repeat cycles, external logger/writer/flags, prefix and minimum Go version |
| Metroplex owns a separate breadcrumb race | Copy event fields that its sanitizer mutates before applying the existing privacy rules | Shared breadcrumbs, actual SDK cloned hubs, immutable inputs and concurrent capture under race |
| Health stop waits behind filesystem I/O | Atomic stop signalling and context-bounded cleanup waiting; legacy background-context cleanup remains synchronous | Concurrent stops, pending writes/checks, reset/replacement, deadline and unbounded controls |
| Non-HTTP boundary reports Goexit as successful | Only mark successful after the operation actually returns | Original returned-error/panic identity, ended span, restored active context and Goexit status |

Compatibility constraints: exported APIs and official-main constructor defaults
remain unchanged; no HTTP/event/data/Redis-key schema change or new consumer
adoption is introduced. New Kafka recovery applies only to additive consumers.
The inherited **legacy** Confluent partial-batch polling limitation stays intact.
The runtime fixes do not approve a module minimum-Go upgrade, relax privacy,
change commit-before-handler into at-least-once delivery, or expand unsupported
ioredis command modes.

An OS filesystem syscall cannot be cancelled by a Go context. Deadline-bound
health callers now stop waiting without acquiring the filesystem writer's I/O
lock; framework initialization/shutdown still hold their lifecycle mutex around
the bounded reset. An already admitted write/remove can finish after the error
return. Graceful background
stops still join writes. A permanently blocked OS cleanup can retain its one
cleanup goroutine; callers must not treat a timed-out reset as completed file
cleanup. This is an explicit operating-system limit, not a cancellation guarantee.

Validation of this final runtime tree is recorded below after implementation and
independent cross-review. The Go/dependency security decision remains separate:
preserve Go 1.25.10 unless a Go 1.26 minimum is explicitly approved. Live UAT/prod
and organization-wide reverse-dependency validation remain distinct release gates.

#### Final-source library validation

The runtime source was frozen before the following whole-tree checks. Reports
are retained in `/tmp/fitgo-final-validation.9flRuD/`; these checks include the
uncommitted source and new regression files, not just commit `9269ce6`.

| Check | Result / evidence |
|---|---|
| Minimum supported toolchain | Go 1.25.10, `GOWORK=off`: full tests, build and vet pass |
| Full race suite | Go 1.26.9, `GOWORK=off`: all 32 test-bearing packages pass, no test skips; `fitgo-final-race.json` |
| Full ordinary suite | All 32 test-bearing packages pass, no test skips; `fitgo-final-tests.json` |
| Real Kafka and mixed membership | All eight live Kafka test families run inside both full suites, using isolated Kafka 7.3.2 and the installed KafkaJS 2.2.4; includes transactions/control records, offset metadata, multi-partition transient failures, real ownership loss, drain and Node/Go mixed groups |
| Real Redis topologies | Sentinel and three-node Cluster tests run inside both full suites, including accepted-command drain and repeated public Quit |
| Adversarial review probes | The pre-fix aggregate SSE, UTC DATE, Cluster Quit, stdlib-log restoration and Metroplex shared-breadcrumb probes pass unchanged under race |
| Official-main exported API | `apidiff -m -incompatible` against main `df96a28`: zero incompatible changes; `candidate.exp`. Positional-layout/default-contract guard tests also pass |
| Module hygiene | Minimum-toolchain `go mod tidy -diff`, `go mod verify`, gofmt and `git diff --check` pass; actual `go.mod`/`go.sum` remain unchanged |
| Metroplex final-source integration | Isolated workspace: full suite passes all 270 test-bearing packages, with 106 conditional test/subtest skips; `metroplex-final-tests.json`. Build/vet and targeted composition-root, consumer, repository, scheduler, Redis and observability race suites pass |
| Metroplex published-pin control | `GOWORK=off`, Go 1.26.9: full suite passes the same 270 packages with 106 conditional skips, then build/vet pass; `metroplex-published-pin-tests.json`. Application-owned Sentry race tests also pass with the published dependency |

Coverage is measured for the affected packages, not claimed for every package
in Commerce: FeatureHub 80.6%, health 88.8%, Kafka 83.4%, logging 89.2%, Redis
86.3%, tracing 90.5%, and Metroplex shared observability 87.3%. Profiles are
`fitgo-affected-coverage.out`, `feature-final-coverage.out` and
`metroplex-sentry-coverage.out`. This measurement omits the live fixture env;
the earlier full live suites supply that separate evidence. Coverage is a
discovery signal, not a claim of universal parity.

After the live runs, a test-only fixture added 31 DATE/DATETIME positive,
negative, invalid-input, comparison and fallback cases. Every expected result
was checked against `MatcherRegistry` in installed FeatureHub JavaScript SDK
1.4.0, using `feature/testdata/date_strategy_fixture.json`; the public Go context
evaluation is also asserted. No runtime source or live harness changed.
FeatureHub's normal/coverage and race reruns pass, including the new fixture.
Final whole-tree offline reruns are recorded separately below and must not be
confused with the no-skip live-fixture runs above.

After that test-only addition, Go 1.25.10 full ordinary tests and Go 1.26.9 full
race tests pass all 32 test-bearing packages, with 10 expected live-infrastructure
test/subtest skips because the isolated fixtures have been removed. Reports:
`fitgo-final-offline-tests.json` and `fitgo-final-offline-race.json`. Final vet and
diff checks also pass. The same unchanged runtime source passed those ten live
paths in both full suites before fixture cleanup; the new pure date fixture
passes against the isolated security-upgrade copy as well.

The fixtures use only newly created, uniquely named Docker resources. No
existing local service, shared broker, cluster or datastore is used or altered.
This is runtime/library regression evidence, not universal FIT.js parity or
live UAT/prod certification. Local socket/mock tests and real dependency tests
are distinguished from deployment and reverse-consumer gates.

Command ledger (run from the fit-go worktree; live fixtures must be provisioned
before reuse, because this pass removes its isolated containers after testing):

```sh
export FIT_GO_KAFKA_RUNTIME_BROKER=127.0.0.1:29097
export FIT_GO_KAFKAJS_NODE_MODULE=/home/user/brunt/Commerce/services/sentinel/node_modules/kafkajs
export FIT_GO_KAFKAJS_EXPECTED_VERSION=2.2.4
export FIT_GO_REDIS_SENTINEL_LIVE_URI='redis-sentinel://127.0.0.1:27315/0?master=primary'
export FIT_GO_REDIS_CLUSTER_LIVE_URI='redis://127.0.0.1:27311/0?sharded_db=true'
GOWORK=off GOTOOLCHAIN=go1.25.10 GOMAXPROCS=2 go test -p=2 -count=1 -json ./...
GOWORK=off GOTOOLCHAIN=go1.25.10 GOMAXPROCS=2 go build -p=2 ./...
GOWORK=off GOTOOLCHAIN=go1.25.10 GOMAXPROCS=2 go vet -p=2 ./...
GOWORK=off GOTOOLCHAIN=go1.26.9 GOMAXPROCS=2 go test -p=2 -race -count=1 -json ./...
```

The Kafka broker is a newly created plaintext Kafka 7.3.2/ZooKeeper pair. Redis
7 fixtures use Cluster ports 27311–27313, primary 27314 and Sentinel 27315;
all bind only to localhost. Never substitute a shared or production endpoint
for these write-producing tests. Metroplex candidate tests use the explicit
temporary workspace described above, Go 1.26.9 and `go test -p=2 -count=1 -json
./...`; published-pin controls use `GOWORK=off` instead. The original consumer
workspace is never rewritten.

#### Security compatibility decision (not silently applied)

With the patched Go 1.26.9 build toolchain, the unchanged dependency graph still
has five reachable `golang.org/x/net` advisories: GO-2026-6603, GO-2026-6610,
GO-2026-6611, GO-2026-6612 and GO-2026-6617. There are no reachable standard-
library findings on that toolchain. The JSON report
`vulnerabilities-compatible.json` contains findings despite its successful tool
exit; that exit must not be interpreted as a clean scan.

A disposable copy with minimum Go 1.26.0, `x/net` v0.60.0, `x/crypto` v0.57.0,
`x/sync` v0.23.0, `x/sys` v0.48.0 and `x/text` v0.42.0 scans with **zero reachable
findings** using Go 1.26.9 (`vulnerabilities-upgraded.json`). It is not the actual
library dependency graph. Its full fit-go tests (including the live fixtures),
build/vet and Metroplex build/vet pass; `security-candidate-tests.json` records 32
test-bearing packages with no skips. Applying it requires an explicit minimum-Go decision;
retaining Go 1.25.10 does not make the current graph security-clean. On 2026-10-09
the user explicitly chose to retain the existing dependencies and Go minimum and
publish a new candidate; the security upgrade is deferred, not fixed or waived
as a merge-readiness claim. Do not call this PR security-clean or merge-ready
while that gate is unresolved. The current primary
advisory is [GO-2026-6617](https://pkg.go.dev/vuln/GO-2026-6617); release engineering
must use a patched build toolchain, independently of the module's Go directive.

#### Candidate publication checks

The rc.5 publication reruns use the unchanged runtime and regression files above,
with `GOWORK=off`. Go 1.25.10 full tests, build/vet, `go mod tidy -diff` and
`go mod verify` pass. The Go 1.26.9 full race rerun also passes. Both test suites
pass all 32 test-bearing packages and report ten expected live-infrastructure
test/subtest skips; the earlier isolated live evidence is recorded separately.
An additional API export comparison against official main `df96a28` reports zero
incompatible changes. Reports are in `/tmp/fitgo-rc5-publication.8CRMsk/`.
The actual `go.mod` and `go.sum` are unchanged. Published-tag download/checksum
and Metroplex `GOWORK=off` results belong in Metroplex's local library document
after the new remote tag is available; they are not asserted in advance here.

### Published baseline: `v0.2.0-rc.4` (historical findings before this local pass)

This release candidate packages the follow-ups to `11f29d2` below. Their
"local only"/"not published" headings and validation entries are historical
snapshots, not the current publication target. Existing immutable tags must
not move. Metroplex must select the new remote tag and rerun its checks with
`GOWORK=off`; its local repin is not a deployment.

This candidate is **not merge-ready certification**. The latest review left
these issues open: abnormal Kafka worker exit (`runtime.Goexit`) can bypass
recovery; advanced Confluent can suppress an unrelated handler error during
shutdown; SSE limits bound lines rather than aggregate events; and stalled
health-file I/O can defeat a stop deadline. Metroplex's separate, application-
owned Sentry breadcrumb race is not repaired by this library publication.
The dependency/toolchain security upgrade and live Kafka/Redis validation
remain separate release gates. No new runtime remediation or dependency-floor
change is included in this publication step.

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
| `462dc5a` | Upstream-default guard tests and documentation |
| `9b8754c` | Tracing legacy lifecycle parity |
| `59dac4c` | `AuthorizeJWTToken` restored to main's HS256 verifier |
| `4d45fdf` | Legacy `ConsumeBatch` error identity and `InjectTraceHeaders` parity |
| `15f1940` (`v0.2.0-rc.2`) | Documentation refresh for the three parity fixes |
| `v0.2.0-rc.3` follow-up | Final correctness fixes on top of immutable `v0.2.0-rc.2` |
| Local, unpublished follow-up to `11f29d2` | Mixed Kafka poll recovery, Sentry envelope privacy/trace IDs, Redis routing, SSE framing, and lifecycle cleanup |

It separates verified behavior from accepted limitations so a release
description does not promise more than the code and tests provide.

The compatibility objective is:

- retain the public API and defaults from official `gofynd/fit-go:main`;
- put migration-specific runtime behavior behind additive entry points;
- in the opt-in compatibility transports (ioredis, Kafka advanced recovery),
  reject or pause operations whose ordering or side-effect guarantees cannot be
  preserved instead of silently degrading them;
- apply the documented redactor to default log/Sentry text and structured
  values (a deliberate output change), while explicitly retaining and
  documenting main's raw access-log URL-path field; every difference is listed
  in [Behaviour differences for existing main users](UPSTREAM_INTEGRATION_MIGRATION.md#behaviour-differences-for-existing-main-users); and
- keep Metroplex buildable with a small integration-only change after an
  official immutable fit-go tag exists.

## Changes made

### Local review follow-up to `11f29d2` (2026-10-08; not published)

The target branch remains official main `df96a28`. This follow-up changes no
exported API, module dependency, topic/schema, datastore TLS default, tracing
initializer, JWT verifier, or Metroplex source/pin. It uses additive-path
regressions plus original-main default guards to bound the blast radius.

| Confirmed defect | Correction and compatibility boundary |
|---|---|
| Franz poll returns records and a transient error | Preserve the earliest fetched position for each topic/partition, rewind under the existing rebalance gate, and retain the member. No record is handled or committed from the failed poll. No-record transient failures keep the existing rebuild path; `FromBeginning=false` does not become earliest |
| Empty Franz values/headers become nil | Use the existing nullable deep-copy helper, preserving tombstone/null versus empty distinctions and caller isolation |
| Sentry envelope retains raw transaction sampling text | Check the SDK's private dynamic sampling context before both mandatory sanitizer passes. If unsafe, project all public event fields to a fresh event without that private envelope metadata. Safe metadata is retained; transaction delivery, body trace identity, hooks, transport selection, and first-call initialization remain intact |
| Sentry trace/span ID byte arrays are masked | Preserve known SDK ID types and correctly sized hexadecimal strings under `trace_id`, `span_id`, and `parent_span_id`. Sensitive keys still win, and ordinary byte arrays remain opaque/masked |
| Redis counted-key arithmetic can overflow and panic | Compare declared counts against remaining arguments before addition/slicing; retain existing zero-key and cross-slot policies |
| XREADGROUP group/consumer names can be mistaken for STREAMS/BLOCK | Share grammar-aware prefix parsing between routing and blocking checks, skipping group/consumer names and option values as data |
| Redis discovered IPv6 addresses are invalid | Use `net.JoinHostPort` for Sentinel and Cluster discovery, preserving IPv4/hostname/fallback behavior |
| FeatureHub ignores valid CR-only or BOM-prefixed SSE | Share a bounded line splitter across continuous streaming and isolated Build snapshots. Accept LF/CRLF/CR and one leading BOM, including split reads; retain existing limits and dispatch behavior. Legacy polling is unchanged |
| A background health stop defeats another caller's deadline | Signal under the bookkeeping lock, wait outside it, and retain unfinished done channels for concurrent callers. An intervening stop/reset supersedes a pending managed replacement. Original public multi-start behavior and stopped-check file-write guards remain |
| Migration lease cleanup captures the initial no-op | Resolve the cancellation closure at return time, releasing the execution context and caller cancellation registration on success, operation/unlock error, and panic; fencing/unlock semantics remain |

The Sentry metadata projection is a deliberate privacy trade-off: for an
unsafe sampling context only, envelope-level dynamic sampling fields are
omitted rather than mutated through unsafe/private SDK access. Safe sampling
metadata and all public event fields remain, with an SDK-field coverage test
to catch future dependency additions. This is not a change to sampling rates.

Regression evidence includes both actual Sentry HTTP export transports and
six real Franz client/Kafka-protocol-broker cases (message/batch; uncommitted,
committed, automatic marks), normal and race. The protocol-broker harness
uses a temporary modfile/overlay, not a new library dependency. Real external
Kafka brokers and live Redis Sentinel/Cluster remain release gates.

### Ownership-loss, serialization-cache and option-order follow-up (2026-10-09; local only)

The next review found two inherited gaps in the protections above and one new
Redis parsing regression. The fixes stay local on `11f29d2`; no published tag,
Metroplex pin/source, public signature, dependency or legacy default is changed.

| Confirmed issue | Narrow correction | Regression boundary |
|---|---|---|
| Franz `SetOffsets` recovery is discarded after genuine ownership loss | Remember exact unresolved positions across assignments and driver recreation. After broker offset lookup, substitute a remembered position only for a currently assigned partition with no committed offset. Clear boundaries after successful processing/pre-handler commit; broker commits remain authoritative | Real broker-triggered member loss, new latest-start group, committed-offset and from-beginning controls; assignment/commit/pre-handler-commit unit tests |
| Sentry private serialized fields override sanitized public fields | Project every sanitized event to a cache-free public event, restore vetted safe DSC through an empty SDK scope, retain the original sanitized contexts, then rebuild serialization snapshots | HTTP export through sync/async transports, errors/transactions, pre-capture/hook serialization, safe/unsafe sampling metadata, cleared fields and public-field coverage |
| XREADGROUP requires GROUP at argument 1 locally | Recognize GROUP anywhere in the option prefix and skip its two data arguments, including repeated clauses | COUNT/BLOCK/NOACK before GROUP, keyword-valued names, duplicate clauses, malformed prefixes and unchanged unsafe-BLOCK rejection |

Kafka recovery state is in-memory and belongs to this consumer object; it is
not a substitute for durable broker commits across process crashes, nor does it
force another group member to replay an uncommitted boundary. It never resets
the entire topic to earliest or processes an unowned partition. Original
`client.Consumer` behavior and Confluent recovery are unchanged in this round.

The Sentry correction deliberately prevents cached PII from being exported;
it does not change sampling rates, drop transactions, alter hook ordering or
remove vetted safe sampling metadata. The public-field projection and empty
scope behavior are covered by tests and must be revalidated on SDK upgrades.
Nested pointer/collection forms beyond the existing canonical trace-context ID
tests are not a new guarantee of this serialization-cache fix.

Redis's default go-redis path is untouched. Malformed prefixes and every
non-positive/invalid BLOCK clause remain rejected on ioredis paths, including
a zero BLOCK followed by a positive duplicate. Full ioredis parity for the
previously documented rejected modes is not claimed.

### SSE, idle-RESP, batch-unwind and scope follow-up (2026-10-09; local only)

Four independently reproduced defects were repaired without changing public
signatures, dependencies, the Go 1.25.10 directive, Metroplex source/pin, or
Kafka offset policy. Official main remains `df96a28`.

| Confirmed issue | Correction | Compatibility boundary |
|---|---|---|
| Continuous FeatureHub SSE applied an unfinished event at clean EOF | Dispatch only after a blank line; discard pending fields on EOF | Completed full/incremental/delete events are retained; isolated Build already uses this boundary; legacy polling is unchanged |
| Idle owned RESP close left readiness stale until a command arrived | Observe transport closure only with no writer, reader or in-flight request, then use the existing reconnect loop | Buffered replies, partial pipelines and ambiguous replay reconciliation remain authoritative; empty offline Quit settles; main's go-redis is untouched |
| Kafka batch panic/Goexit leaked receive/process spans | Defer span cleanup without recovering or serializing a panic, leaving abnormal status unset | Normal success/error status and exact returned error identity are unchanged; no handler retry/commit change |
| Legacy tracer scope followed resource identity instead of Options | Limit resolved-resource scope naming to opt-in SDK constructors | Released constructors again use `fit.go/<Options.ServiceName>` as main does; SDK resource identity remains unchanged |

Tests cover 116 SSE boundary cases, a pinned EventSource 2.0.2 oracle, fresh
net.Pipe RESP reconnects, buffered reply/EOF and partial pipeline boundaries,
in-flight replay, empty offline Quit and Disconnect, panic(nil)/Goexit and
normal Kafka errors, and legacy versus SDK resource/scope identity. The scope
probe also passes against a clean official-main copy.

Two existing initialization/retry test fixtures closed their successful SSE
connections immediately, making readiness transient under the Go 1.25.10
single-threaded scheduler. Their successful responses now flush and remain
open until cancellation, like a healthy SSE edge; error responses and
intentional disconnect tests are unchanged. Both fixtures pass 50 repetitions,
and full FeatureHub tests pass 10 normal and 3 race repetitions. Production
readiness was not relaxed. Existing wall-clock redaction growth and short Redis
dial tests were also sensitive to concurrent compiler/test load; the growth
guard and production redactor were not weakened or changed in this round.

The security upgrade is a separate compatibility decision: `x/net` v0.60.0
declares Go 1.26.0 and upgrades related `x/*` modules. Applying it directly
would raise the minimum version for existing main consumers. These fixes keep
Go 1.25.10 and actual dependency files unchanged; a disposable Go 1.26.9
dependency candidate is validated separately, not published or adopted by
Metroplex. This is not a clean security-release claim.

The disposable candidate uses Go 1.26.9, `x/net` v0.60.0, `x/crypto` v0.57.0,
`x/sync` v0.23.0, `x/sys` v0.48.0 and `x/text` v0.42.0. Its full fit-go
tests/vet pass, `govulncheck ./...` reports zero reachable vulnerabilities
(one imported-package and one required-module advisory are uncalled), and
Metroplex builds/passes vet using a separate temporary workspace. These checks
do not establish compatibility for consumers still building with Go 1.25;
adopting the higher floor requires a separate release decision and downstream
validation. The real `go.mod`, `go.sum` and Metroplex pin are unchanged.

### Tracing legacy lifecycle parity (`9b8754c`)

Applies to the released constructors (`New`, `Init`, `InitWithOptions`, lazy
`Global`); `NewSDK`/`InitSDK` keep their explicit lifecycle.

- A failed global initialization is cached, as main's `sync.Once` did: later
  `Init`/`InitWithOptions` calls return `(nil, nil)` and `Global()` returns nil
  without re-running SDK initialization on hot paths. A failed `New` returns
  its tracer still enabled with no OTel tracer (in-memory spans), as on main.
- `Init` → `Shutdown` → `Init` returns the shut-down tracer, as on main. Only
  `InitSDK` creates a fresh tracer after shutdown.
- The released path builds only main's resource, OTLP/HTTP exporter, sampler,
  and TraceContext+Baggage propagator; no advanced exporter is constructed or
  leaked and no extra `OTEL_*` environment is read.
- `Shutdown` leaves the global text-map propagator installed on every path
  (including `InitSDK` tracers), so outbound propagation continues during a
  graceful drain. Released-constructor tracers also leave their (shut-down)
  provider installed, as on main. `SetGlobal` restore functions still restore
  both globals.
- With no real OTel tracer, `StartSpan` no longer stamps the random in-memory
  IDs onto the logging context, and the decorators skip the goroutine-local
  store.
- A released-constructor tracer returns its exporter shutdown error on the
  first `Shutdown` only and nil on later calls, matching main. Advanced tracers
  retain their cached-result contract. A failed global-init diagnostic remains
  visible through `GlobalWithError` after a no-op package shutdown.

### `AuthorizeJWTToken` (`59dac4c`)

`AuthorizeJWTToken` again runs main's hand-rolled HS256 verifier verbatim
(`server/middleware.go`): `exp`/`nbf` enforced only when numeric, `now == exp`
accepted, `exp: 0` handled as before, an empty secret (after the
`JWT_SECRET_DELETE_ENTITY` fallback) used as-is, padded segments accepted, and
the same payload comparison and context values. The golang-jwt verifier
(algorithm allow-lists, RSA, leeway, `iss`/`aud`/`sub`, fail-closed empty
secret) is used only by `AuthorizeJWTTokenWithOptions` and
`AuthorizeJWTTokenAdvanced`. A 593-case test compares the middleware with a
verbatim copy of main.

### Legacy Kafka consume and trace-header parity (`4d45fdf`)

- Legacy `ConsumeBatch` (consumer from `client.Consumer`) returns the handler's
  own error value unchanged (identity preserved, not wrapped), also when
  `Close` races the failure. It again delivers an already collected batch when
  `Close` races collection, and the batch collector does not observe `Close`
  while filling, as on main.
- Legacy `Consume` logs and skips a failed record before any `Close` check and
  handles a record it has already read even when `Close` races the read.
- `InjectTraceHeaders`/`InjectTraceHeadersToMessages` again append a single
  `traceparent` (sampled flag forced to `01`) from the fit-go span stored by
  `tracing.StartSpan`, in place, leaving existing headers untouched; no global
  propagator, no native-span or goroutine-local adoption, no stripping. The
  strip-and-propagate behaviour moved to an internal helper used only by the
  producer's automatic `ProducerTraceHeadersInject` policy (`kafka.NewProducer`).
- Advanced consumers keep wrapped rewind errors.

### Kafka (advanced Confluent and Franz consumers; `69129e4`)

The legacy `client.Consumer`/`Consume` path keeps main's semantics: a handler
error is logged (redacted) and skipped, and a later successful record commits
past it. Legacy `ConsumeBatch` returns the handler's own error (`4d45fdf`).

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
- An advanced message handler, batch handler, or offset finalizer may call its
  own consumer's `Close` without waiting on itself. Only that callback-local
  call completes asynchronously; external `Close` remains synchronous. Callback
  ownership is stored independently of tracing context, so public tracing
  wrappers and explicit active-context replacement cannot hide it.
- Advanced Confluent dispatch holds an assignment lease through the handler and
  offset boundary. A record gathered before revoke/reassignment is discarded
  before application code; the new owner remains responsible for it.
- Advanced Confluent and Franz subscriptions reject mixed per-topic
  `FromBeginning` values. Legacy Confluent retains main's first-topic behavior.

### Redis ioredis compatibility path (`534a759`)

- In Cluster mode, `SCAN`, `KEYS`, `FLUSHDB`, `FLUSHALL`, and `RANDOMKEY` are
  rejected (the error names only the verb) instead of being routed by the hash
  of their first argument.
- An authoritative same-node pipeline redirect returns the per-command replies
  and keeps the Cluster transport; only ambiguous outcomes retire it.
- RESP length/depth/value limits, sanitized bootstrap/PING/topology/redirect
  errors, stable `errors.Is` causes, and the `WriteDisposition` versus
  `MayHaveExecuted` split are unchanged from the baseline.
- AUTH, SELECT, INFO-readiness, and transport bootstrap failures now return a
  fixed boundary message and preserve only safe causes. Cluster routing uses
  explicit argument-2 key rules for keyed `OBJECT`, `MEMORY USAGE`, `XGROUP`,
  and `XINFO` forms instead of hashing the subcommand.
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
- Context mutation, revision advancement, readiness invalidation, feature
  application, and readiness publication now share one synchronization
  boundary. Buffered full/incremental/delete events from an older stream cannot
  mutate the newer server-evaluated context or mark it ready.

### gRPC legacy parity (`0522954`)

The original `grpc.Init` server emits no log line for `next(err)`, returns the
raw panic value (including `panic("")`), and returns raw response-validation
text. Its panic *log* line is redacted (deliberate). Advanced/managed servers
keep the generic message and redacted diagnostics.

Legacy `Shutdown()` remains an unbounded graceful drain, as on main. A
concurrent `Stop()` can now force an in-progress drain, and
`ShutdownContext()` can enforce its deadline even when another caller started
that drain.

### International rendering

`AddressDisplayParser` now uses JavaScript-like `String` coercion for
JSON-shaped nulls, arrays, objects, booleans, floating-point values, NaN, and
infinities. Native Go integer kinds retain exact decimal formatting for
existing callers.

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
| Kafka legacy handler-error identity and Close races | `kafka/legacy_handler_error_identity_test.go` |
| `InjectTraceHeaders` main contract | `kafka/inject_trace_headers_upstream_parity_test.go` |
| `AuthorizeJWTToken` decisions (593 cases vs verbatim main) | `server/jwt_upstream_parity_test.go` |
| Tracing released-constructor lifecycle | `tracing/upstream_parity_test.go`, `tracing/lifecycle_test.go` |
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
| Default-path output | Log and Sentry output on the original entry points is redacted. There is no API change; see [Behaviour differences for existing main users](UPSTREAM_INTEGRATION_MIGRATION.md#behaviour-differences-for-existing-main-users) for this and every remaining runtime and dependency difference. |
| Legacy access-log path | `server.New` keeps main's raw URL-path field for compatibility. Query values and opted-in headers are redacted, but applications must not put credentials or PII in path segments; a stricter default needs a separately reviewed migration. |
| Legacy AES-GCM wire format | `encryption.NewManager` and its compatibility options keep the provider-supplied fixed IV so existing fit.js/pyfit ciphertext remains decryptable. Reusing a nonce with one AES-GCM key is unsafe; new data needs a separately versioned random-nonce format rather than a silent wire-format change. |
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
| gRPC shutdown | Forced shutdown cancels RPCs, but handlers must honor cancellation. gRPC's internal handler wait can retain an ongoing graceful stop and its concurrent force-stop call if a handler never returns. |
| Server/logging/tracing | TraceClue remains the default schema, profiler response-key changes remain, and the legacy tracing initializer retains its owned propagator wrapper. |
| Toolchain | The module directive remains Go 1.25.10. Selecting a patched release toolchain is a release-engineering gate. |

## Metroplex integration state

Effect of the parity fixes on Metroplex:

- `AuthorizeJWTToken(JWTOptions{})` (leads and shortlinks routes): accept and
  reject decisions are main's again (`59dac4c`); Metroplex sees no change from
  main.
- `tracing.InitSDK` (`internal/app.go`): the SDK path keeps its fresh
  lifecycle, and `tracing.Shutdown` now leaves the global propagator installed,
  so outbound propagation continues while the service drains (`9b8754c`).
- `kafka.NewProducer` with the default `ProducerTraceHeadersInject` policy:
  produced records get the strip-and-propagate behaviour (stale propagation
  fields removed, all fields of the global propagator injected). This is the
  new behaviour selected by using `NewProducer`; the exported
  `InjectTraceHeaders` keeps main's append-only contract (`4d45fdf`).
- `CommitBeforeHandler` consumers: a handler failure after a successful
  pre-handler commit no longer replays the failed record but rewinds to the
  next record of its group, so later fetched records are not skipped
  (`69129e4`).

Metroplex's existing local `internal/app.go` uses `GetDecodedSecretFromGSM` and
`UseHealthRouteMiddleware`, and its module replacement already uses the
published immutable fork tag `github.com/swapnilfynd/fit-go v0.2.0-rc.3` at
`11f29d2`. The follow-up described above is unpublished and is validated with
a temporary workspace only. All six pre-existing Metroplex dirty files are
preserved; this follow-up does not repin or change them. Publish a new
immutable candidate before repinning; never move `rc.2` or `rc.3`. Workspace
validation is not evidence that the published tag contains these fixes.

## Validation

On this host loopback listeners are allowed, so the full `go test ./...` and
`go test -race ./...` suites run, including miniredis, `httptest`, gRPC, and
librdkafka mock-broker tests. The local follow-ups add two disposable-broker
Kafka tests: 10 live-infrastructure cases skip without external configuration
(8 Kafka tests and 2 Redis Sentinel/Cluster live subtests). The added Kafka
fixtures were separately executed against a local protocol broker as noted below.

Historical candidate-tree verification on 2026-10-08 (published `11f29d2`;
the separate local follow-up rerun is recorded below)

- `gofmt -l`, `git diff --check`, `go build ./...`, and `go vet ./...` are clean.
- `go test -count=1 ./...` and `go test -race -count=1 ./...` pass with only the
  8 live-infrastructure skips at that published head (6 Kafka tests and
  2 Redis Sentinel/Cluster live subtests).
- `GOTOOLCHAIN=go1.25.10 go test -count=1 ./...` and `go vet ./...` pass, so
  the module's declared toolchain remains supported.
- `go mod tidy -diff` is clean. The only module changes from the PR head are
  the OpenTelemetry 1.45.0/contrib 0.70.0 and gRPC 1.83.2 security updates.
- `apidiff -m -incompatible` against official main `df96a28` reports no
  incompatible changes.
- `GOTOOLCHAIN=go1.26.7 govulncheck ./...` reports zero reachable
  vulnerabilities. The 8 standard-library findings seen with Go 1.26.4 are
  absent.
- Metroplex (`go build ./...`, `go vet ./...`, and `go test ./...`) passes
  against this exact local tree through an uncommitted workspace file;
  targeted race tests for its Kafka/Redis/tracing/shutdown boundaries and the
  Promotions, Extensions, observability, and scheduler packages also pass.
  That workspace run preceded the published `rc.3` repin; it used the existing
  local `rc.2` replacement and checksum without modifying them.

### Local follow-up validation

Local follow-up rerun on 2026-10-08 (working branch
`fix/pr3-final-remediation`, uncommitted on `11f29d2`):

- fit-go format/diff, build, vet, full tests, full race suite, `go mod
  tidy -diff`, and `go mod verify` pass. No dependency/directive change from
  `11f29d2`; full tests/vet also pass with `GOTOOLCHAIN=go1.25.10`.
- `apidiff -m -incompatible` against official main `df96a28` is empty.
- `GOTOOLCHAIN=go1.26.7 govulncheck ./...` reports zero reachable
  vulnerabilities; one imported-package and three required-module findings
  remain uncalled. This does not clear standard-library advisories when
  building with an older toolchain.
- The original Redis/FeatureHub and Sentry privacy/trace-ID probes pass, with
  focused race coverage. Sentry fixtures exercise sync and default async
  HTTP transports, unsafe inbound sampling keys/values, safe sampling
  metadata, and unchanged source maps. Health tests cover concurrent stop,
  an in-flight replacement, and an already queued replacement versus reset.
- The six Franz protocol-broker cases pass normally and under race with a
  temporary kfake modfile/overlay. New committed/uncommitted/automatic cases
  preserve historical latest-start behavior and the exact fetched records.
- Metroplex build/vet/full tests pass through
  `/tmp/fitgo-local-remediation-validation.WjFz8q/go.work`; targeted race
  tests pass for Promotions/Extensions consumers, shared Redis/observability,
  scheduler, and internal Kafka/Redis/tracing/shutdown tests. Its published
  `GOWORK=off` `rc.3` build/vet and dependency-pin checks pass as a separate
  control, not as evidence of these unpublished fixes.
- Metroplex's six pre-existing modified files retain their initial SHA-256
  checksums. No cluster activity, commit, push, tag, PR update or repin.

### Final local follow-up validation (2026-10-09)

Tree: `fix/pr3-final-remediation`, uncommitted on `11f29d2`, including the
ownership-loss/cache/option-order changes above; official main remains
`df96a28`. This evidence supersedes the older validation for this local tree.

- `GOWORK=off go build ./...`, `go vet ./...`, `go test -count=1 ./...`
  and `go test -race -count=1 ./...` pass. Full tests/vet also pass with
  `GOTOOLCHAIN=go1.25.10`. The 10 external-infrastructure skips above are
  explicitly enumerated by the JSON test output, not counted as proof.
- `go mod tidy -diff`, `go mod verify`, formatting and diff checks pass.
  `go.mod`/`go.sum` are unchanged from `11f29d2`. Fresh module export data
  compared with official main reports zero incompatible exported API changes.
- Independently reproduced regressions pass under race: the original real
  Heartbeat/UnknownMemberID ownership-loss probe (uncommitted/latest,
  committed/latest and beginning), the six mixed-poll broker cases, cached
  Sentry HTTP/direct-user probes and the 24 Redis option permutations. The
  checked-in Sentry HTTP suite covers 16 error/transaction, sync/async,
  pre-cache/hook and safe/unsafe-DSC cases.
- The checked-in real LeaveGroup ownership-loss broker fixture passes all
  12 message/batch cases normally and under race using the temporary protocol
  broker: uncommitted/latest, committed/latest, uncommitted/beginning,
  automatic, pre-handler commit and a newer authoritative broker commit.
  It skips historical latest-start records and retries the failed offset
  only when that offset is not already durably committed.
- Metroplex build/vet/full tests pass through the existing temporary
  `/tmp/fitgo-local-remediation-validation.WjFz8q/go.work`. Targeted race tests
  pass for internal Kafka/health/GSM/shutdown/Sentry/Redis, all four affected
  consumer registries, shared observability and shared Redis. Its published
  `GOWORK=off` rc.3 pin test passes as a separate control, not as evidence
  that rc.3 contains these unpublished fixes. All six original dirty-file
  checksums are unchanged.
- **Security release gate is not clean.** A fresh
  `GOTOOLCHAIN=go1.26.7 govulncheck ./...` exits 3 and identifies 11 reachable
  advisory IDs: GO-2026-6599, 6600, 6603, 6605, 6607, 6608, 6610, 6611, 6612,
  6613 and 6617. The database now lists Go 1.26.9 and `golang.org/x/net`
  v0.60.0 as the relevant patched versions; GO-2026-6617 affects both.
  See the [official advisory](https://pkg.go.dev/vuln/GO-2026-6617).
  The earlier zero-reachable report is historical and must not be used for
  publication today. No dependency, module directive or release toolchain
  upgrade is bundled into these three compatibility fixes; assess that
  separate security patch against upstream-main and reverse dependencies.
- No commit, push, tag move, PR update, Metroplex repin or cluster activity.

### Latest four-fix validation (2026-10-09)

Tree: uncommitted `fix/pr3-final-remediation` on `11f29d2`, including the four
SSE/idle-RESP/batch-unwind/scope fixes and the two test-fixture corrections.
This evidence supersedes the preceding local validation for this tree.

- Full Go 1.25.10 tests pass with `GOMAXPROCS=1`, `-p=1`, `-count=1`:
  all 32 test-bearing packages pass. Go 1.25.10 vet also passes.
- Full host-toolchain race tests pass with `-p=1`, `-count=1`: all 32
  test-bearing packages pass. Build, vet, formatting, diff checks, module
  verification and `go mod tidy -diff` pass.
- Both JSON runs enumerate the same 10 external-infrastructure skips:
  eight Kafka live tests and the live Redis Sentinel/Cluster subtests. No
  skipped workload is counted as verified. Initial timing/fixture failures
  and their controlled reruns are described above.
- Fresh `apidiff -m -incompatible` against official main `df96a28` is empty.
  The independent incomplete-SSE and batch-panic probes pass under race;
  the independent legacy-scope probe passes on both candidate and clean main.
  Redis idle lifecycle tests pass 20 race repetitions with fresh sockets,
  buffered/partial replies, in-flight replay and shutdown controls.
- Metroplex full tests/build/vet pass against the actual local fit-go candidate
  through the existing temporary workspace. Targeted race tests pass for its
  internal integration, all four affected consumer registries, shared Redis
  and shared observability. Its `GOWORK=off` published rc.3 pin checks pass
  separately; rc.3 does not contain these unpublished repairs.
- All six original Metroplex dirty-file checksums and fit-go dependency
  checksums are unchanged. No commit, push, tag, PR update, repin or cluster
  activity. The security gate remains open in the actual dependency graph;
  the separately validated Go 1.26.9 dependency candidate above is not adopted.

### Release gates (unchanged by local-only validation)

1. Repeat format/diff, build, vet, full and race suites in CI after commit.
2. Run live-broker Kafka recovery/rebalance tests, including multi-partition
   failures and all commit modes.
3. Compile every known reverse dependency of fit-go against the release
   candidate.
4. Build/test Metroplex against the exact candidate tag with `GOWORK=off`.
   After upstream merge and an official release, migrate its fork replacement
   to the official immutable tag and repeat the consuming-module validation.
5. Build with the release-selected patched Go toolchain and rerun
   `govulncheck`.
6. Keep PR #3's remote description aligned with the published commit/tag,
   validation evidence, and deliberate compatibility/security boundaries.
