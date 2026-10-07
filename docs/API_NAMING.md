# Descriptive API names

fit-go keeps the official-main API unchanged and exposes fork capabilities
through descriptive, capability-based names. The former `Advanced` symbols are
deprecated aliases or wrappers; they remain source-compatible and may only be
removed in a future major release.

| Capability | Preferred API |
|---|---|
| Framework-owned lifecycle | `fit.InitManaged` |
| PostgreSQL pool controls | `fit.WithPostgresPoolOptions`, `postgres.ConnectionPoolOptions`, `postgres.InitWithPoolOptions` |
| OpenTelemetry SDK lifecycle | `tracing.SDKOptions`, `tracing.NewSDK`, `tracing.InitSDK` |
| TraceClue/runtime logging | `logging.RuntimeOptions`, `logging.NewRuntime` |
| Prometheus textfile lifecycle | `metrics.TextfileOptions`, `metrics.NewTextfileRegistry` |
| Profiler runtime status | `profiling.RuntimeConfig`, `profiling.NewRuntime`, `GetRuntimeConfig` |
| FeatureHub SSE transport | `feature.InitStreaming` |
| MongoDB command tracing | `mongo.DefaultInstrumentedDialFunc` |
| MySQL command tracing | `mysql.InstrumentedConnectionOptions`, `mysql.InitInstrumented` |
| Redis protocol/ioredis compatibility | `redis.CompatibilityOptions`, `redis.InitWithCompatibility`, `DefaultConfigured*DialFunc` |
| Kafka construction and run controls | `kafka.ProducerOptions`, `kafka.ConsumerSettings`, `kafka.ConsumeOptions`, `NewProducer`, `NewConsumer`, `Consume*WithOptions` |
| HTTP server runtime | `server.RuntimeConfig`, `server.NewRuntime` |
| Redacted access logs | `server.AccessLogConfig`, `server.AccessLog`, `server.GinAccessLog` |
| Request-ID request-header propagation | `server.RequestIDWithHeaderPropagation` |
| HTTP OpenTelemetry options | `server.OTelMiddlewareOptions`, `server.OTelMiddlewareWithOptions` |
| Extended JWT verification | `server.JWTVerificationOptions`, `server.AuthorizeJWTTokenWithOptions` |
| Cross-language encryption nonces | `encryption.ManagerOptions`, `encryption.NewManagerWithOptions` |
| Sanitized Sentry hooks | `errors.SentryHooksConfig`, `errors.InitSentryWithHooks` |
| Instrumented compatibility HTTP client | `utils.NewInstrumentedHTTPClient` |

Original official-main entry points retain their released defaults. Choosing a
preferred API is explicit and does not silently alter an existing caller.
