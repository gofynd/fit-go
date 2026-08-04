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

These semantics are not equivalent to go-redis command retries. go-redis owns
retry state per command and does not expose whether a failed operation was not
written, partially written, or fully written with its reply lost.

## Implemented and tested

The compatibility client provides:

- eager, connection-owned reconnect lifecycle;
- exact retry delays and the connection-global 21st-failure flush boundary;
- a shared accepted-command FIFO;
- replay of a direct command or zero-reply pipeline before later queued work;
- explicit `ReplayCount` and `AmbiguousReplays` when a write may have executed;
- no replay of a pipeline suffix after one or more replies were received;
- per-reply server errors for pipelines and rejected direct-command futures;
- waiter cancellation that does not cancel an accepted command;
- ordered `QUIT` drain, immediate offline empty-queue `QUIT`, immediate
  `disconnect()`, and exact legacy error strings.

Deterministic tests use real loopback TCP connections for outage recovery,
lost-reply replay, FIFO ordering, partial pipeline replies, server errors, and
graceful drain. A controlled factory closes the 21-reconnect exhaustion case
without spending the legacy 10.5-second delay budget. The delay function itself
is pinned for all first 20 values and its 2-second cap. Tests are source-derived
regression evidence; they do not by themselves certify a production transport.

## Deliberate fail-closed limits

fit-go does not provide a built-in transport factory for this state machine.
The caller must supply a `SlingshotIORedisTransportFactory` whose transport
returns explicit `MayHaveExecuted` and partial replies. An automatic adapter over
`*redis.Client` would have to guess those values and is therefore intentionally
absent.

The following remain registration-blocking for Slingshot:

- an owned standalone RESP transport with authentication, database selection,
  TLS, INFO ready-check/loading behavior, socket timeout, and the exact FIT.js
  startup error boundary;
- live Node 22.22.0/ioredis 5.11.1 versus Go record/replay for connect loss before
  write, partial write, full write/lost reply, partial pipeline reply, recovery,
  retry exhaustion, quit during outage, and process shutdown;
- actual Slingshot module boot wiring for both `slingshot.write` and
  `slingshot_domain.write`, plus health, tracing, logging, and Sentry behavior;
- Redis Cluster and Sentinel queue/failover behavior.

Until those gates close, use of this API is an implementation aid, not a parity
claim, and the Slingshot production role must remain unregistered.
