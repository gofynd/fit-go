# Slingshot ioredis 5.11.1 compatibility boundary

`redis.SlingshotIORedisCompatClient` is a strictly opt-in state machine for the
Redis outage behavior used by legacy Slingshot. It is not part of
`redis.InitDefault`, is not selected by an environment variable, and changes no
existing service default.

## Source contract

The legacy pins used for this implementation are:

- Slingshot `package-lock.json`: ioredis `5.11.1` and FIT.js `4.0.1` at commit
  `a2b5f344a89c5452a1fc67162bf1de6c51e6575a`.
- FIT.js `dist/redis/index.js`: ordinary standalone Slingshot connections pass
  FIT options into `new Redis(connectionString, commonConnectionOptions)`.
- ioredis `built/redis/RedisOptions.js`: `enableOfflineQueue: true`,
  `maxRetriesPerRequest: 20`, and `retryStrategy(times) = min(times * 50, 2000)`.
- ioredis `built/redis/event_handler.js`: one retry counter belongs to the
  connection, both command and offline queues are flushed on every 21st failed
  reconnect, ready resets the counter, unacknowledged commands replay before the
  later offline queue, and a partial pipeline reply aborts the unread suffix.
- ioredis `built/Redis.js`: offline commands share one FIFO; `disconnect()`
  flushes it with `Connection is closed.` and an offline `QUIT` with an empty
  queue resolves locally.
- ioredis runtime capture (Node 22.22.0 executing the pinned 5.11.1 package) proves
  the actual startup wire order is `auth`, `select`, `client setname`, `client
  SETINFO LIB-NAME`, `client SETINFO LIB-VER`, `info`. Command names are lowercase
  while command arguments retain their input bytes. The asynchronous package
  metadata lookup makes the two SETINFO commands appear in the opposite order
  from a superficial reading of the promise array in `event_handler.js`.

These semantics are not equivalent to go-redis command retries. go-redis owns
retry state per command and does not expose whether a failed operation was not
written, partially written, or fully written with its reply lost.

## Implemented and tested

The compatibility client and explicitly selected owned RESP transport provide:

- eager, connection-owned reconnect lifecycle;
- exact retry delays and the connection-global 21st-failure flush boundary;
- a shared accepted-command FIFO;
- ordered asynchronous writes and reads, allowing multiple direct commands to
  reach one connection before earlier replies settle;
- replay of the complete fully/partially-written, zero-reply uncertain set
  before later offline work;
- explicit `ReplayCount` and `AmbiguousReplays` when a write may have executed;
- no replay of a pipeline suffix after one or more replies were received;
- per-reply server errors for pipelines and rejected direct-command futures;
- waiter cancellation that does not cancel an accepted command;
- ordered `QUIT` drain, immediate offline empty-queue `QUIT`, immediate
  `disconnect()`, and exact legacy error strings.
- exact RESP command framing and a dedicated reader that detects idle remote
  close without competing with reply parsing;
- `auth` (password and Redis 6 ACL forms), non-zero `select`, `client setname`,
  ioredis 5.11.1 `CLIENT SETINFO`, and `INFO` loading readiness in the captured
  runtime order;
- ioredis command-name normalization without mutating command argument bytes;
- ioredis's tolerated AUTH warning cases, ignored CLIENT metadata errors,
  `INFO NOPERM` readiness exception, and fail-closed FIT startup errors;
- a 10-second default TCP/TLS connect timeout, optional mutual TLS through a
  caller-owned `tls.Config`, TCP no-delay/keepalive, and optional socket
  inactivity timeout reset on every partial data read;
- RESP2 simple strings, errors, integers, bulk strings/nulls, and nested
  arrays/nulls;
- explicit `NotWritten`, `PartiallyWritten`, and `FullyWritten` dispositions,
  RESP plaintext byte counts, underlying socket byte counts (ciphertext under
  TLS), prefix replies, and conservative duplicate-execution exposure.

Deterministic tests use real loopback TCP, `net.Pipe`, and a generated test TLS
certificate for startup command order, AUTH/SELECT/INFO boundaries, INFO loading,
TLS, socket inactivity and partial-data reset, not-written and partial-write
faults, full-write/lost-reply, exact two-direct-command lost-reply replay,
prior-reply preservation when a later concurrent write fails, partial pipeline
replies, outage recovery, FIFO ordering, server errors, idle close, and graceful
drain. A controlled factory
closes the 21-reconnect exhaustion case without spending the legacy 10.5-second
delay budget. The delay function itself is pinned for all first 20 values and
its 2-second cap.

`NewSlingshotIORedisRESPCompatClient` is additive and explicit. It does not read
FIT Redis environment variables, resolve GSM references, modify `InitDefault`,
or replace any existing fit-go connection.

The checked-in runtime oracle can be replayed against an installed pinned copy:

```sh
IOREDIS_MODULE=/path/to/node_modules/ioredis \
  node redis/testdata/slingshot_ioredis_wire_probe.cjs startup
IOREDIS_MODULE=/path/to/node_modules/ioredis \
  node redis/testdata/slingshot_ioredis_wire_probe.cjs lost-reply
```

The script refuses any version other than `5.11.1`. Both outputs are positive
oracles for the owned transport. The deterministic Go lost-reply test pins the
same connection-indexed startup and `incr first`, `incr second`, reconnect,
replay sequence and the same integer results, while also asserting one
ambiguous replay on each Go future.

## Deliberate fail-closed limits

The following remain registration-blocking for Slingshot:

- live Node 22.22.0/ioredis 5.11.1 versus Go record/replay for connect loss before
  write, partial write, full write/lost reply, partial pipeline reply, recovery,
  retry exhaustion, quit during outage, and process shutdown;
- exact post-ready SELECT-error behavior on reconnect (FIT rejects an initial
  SELECT error, while ioredis logs a later SELECT error and can still become
  ready); the transport currently fails closed on every SELECT error;
- command-level ioredis transformations and modes not used by the raw transport
  contract, including Buffer-returning commands, HGETALL object transforms,
  transactions, blocking commands, Pub/Sub, monitor mode, RESP3, and Unix
  sockets, must be inventoried against actual Slingshot call sites before the
  client can be treated as a general ioredis replacement;
- TLS fault injection must verify partial TLS-record and lost-reply disposition
  against the deployed proxy/Redis stack; the transport exposes underlying
  ciphertext progress but deterministic tests currently cover successful TLS;
- actual Slingshot module boot wiring for both `slingshot.write` and
  `slingshot_domain.write`, URI/GSM/env precedence, client names, health,
  tracing, logging, Sentry behavior, and FIT's shutdown log/error sequence;
- Redis Cluster and Sentinel queue/failover behavior.

Until those gates close, use of this API is an implementation aid, not a parity
claim, and the Slingshot production role must remain unregistered.
