// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fit is a batteries-included framework for building scalable Go
// microservices. It provides configuration, server, database, messaging,
// observability, and utility modules with sensible, environment-driven defaults.
package fit

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gofynd/fit-go/config"
	"github.com/gofynd/fit-go/errors"
	"github.com/gofynd/fit-go/feature"
	"github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/instrumentation"
	"github.com/gofynd/fit-go/logging"
	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/otelmetrics"
	"github.com/gofynd/fit-go/postgres"
	"github.com/gofynd/fit-go/profiling"
	"github.com/gofynd/fit-go/redact"
	"github.com/gofynd/fit-go/tracing"
	"go.opentelemetry.io/otel"
)

// Connections holds initialized database and service connections.
type Connections struct {
	Mongo       interface{} // MongoDB connections (read/write per service)
	MySQL       interface{} // MySQL connections via database/sql
	Postgres    interface{} // PostgreSQL read/write pools via pgxpool
	Redis       interface{} // Redis connections (standalone or cluster)
	FeatureFlag interface{} // Feature flag client
	Kafka       interface{} // Kafka client
	GroupCache  interface{} // GroupCache distributed in-process cache
}

// Fit is the main framework singleton that holds global state.
type Fit struct {
	mu               sync.RWMutex
	Config           *config.Config
	Connections      Connections
	Logger           *logging.Logger
	Tracer           *tracing.Tracer
	Metrics          *metrics.Registry
	OTelMetrics      *otelmetrics.Provider
	Postgres         *postgres.Client
	Instrumentations *instrumentation.Manager
	Health           *health.Checker
	Profiler         *profiling.Profiler
	Errors           *errors.ErrorRegistry
	metricsUndo      func()
	otelMetricsUndo  func()
	profilingUndo    func()
	tracingUndo      func()
	loggingUndo      func()
	initialized      bool
}

// instance is the global Fit singleton.
var (
	instance *Fit
	once     sync.Once
)

// Instance returns the global Fit instance, initializing it if needed.
func Instance() *Fit {
	once.Do(func() {
		instance = &Fit{
			Health: health.NewChecker(),
		}
	})
	return instance
}

// Init initializes the framework with the given options.
func Init(ctx context.Context, opts ...Option) (*Fit, error) {
	f := Instance()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.initialized {
		return nil, fmt.Errorf("fit: framework is already initialized; call Shutdown before reinitializing")
	}
	// Every Init owns a fresh set of mutable framework state. This also cleans up
	// work started through Instance().Health before the first initialization.
	if f.Health != nil {
		f.Health.Reset()
	}
	f.Connections = Connections{}
	f.Postgres = nil
	f.Health = health.NewChecker()
	f.Errors = nil
	f.Profiler = nil
	f.OTelMetrics = nil
	f.Instrumentations = nil

	// Apply options
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	// 1. Load configuration
	cfg, err := config.Load(o.ConfigPaths...)
	if err != nil {
		return nil, fmt.Errorf("fit: failed to load config: %w", err)
	}
	f.Config = cfg

	// Initialize FeatureHub before process-global logging/tracing state is
	// installed so a required initial-state failure leaves no partial framework
	// ownership behind.
	if cfg.GetBool("FEATURE_FLAG_ENABLED", false) {
		featureOptions := feature.Options{
			Enabled:             true,
			URL:                 cfg.GetString("FEATURE_FLAG_URL", ""),
			APIKey:              cfg.GetString("FEATURE_FLAG_API_KEY", ""),
			RequireInitialState: cfg.GetBool("FEATURE_FLAG_REQUIRE_INITIAL_STATE", false),
		}
		if serviceName := strings.TrimSpace(cfg.GetString("SERVICE_NAME", "")); serviceName != "" {
			featureOptions.DefaultAttributes = map[string][]string{"service_name": {serviceName}}
		}
		featureClient, err := feature.InitWithOptions(featureOptions)
		if err != nil {
			f.Config = nil
			return nil, fmt.Errorf("fit: failed to init feature flags: %w", err)
		}
		if featureClient != nil {
			f.Connections.FeatureFlag = featureClient
		}
	}

	// 2. Initialize logging
	logger, err := logging.New(logging.Options{
		Level:    cfg.GetString("LOG_LEVEL", "info"),
		Timezone: cfg.GetString("LOG_TIMEZONE", "UTC"),
		Env:      cfg.GetString("NODE_ENV", "development"),
		Service:  cfg.GetString("SERVICE_NAME", "unknown"),
	})
	if err != nil {
		f.stopFeatureFlags()
		f.Config = nil
		return nil, fmt.Errorf("fit: failed to init logger: %w", err)
	}
	f.Logger = logger

	// Route the standard library log/slog through the fit logger, so plain
	// slog.* calls (service code AND third-party libraries) land in the single
	// OTel-shaped JSON stream with implicit trace context. (Node fit patches
	// Winston's default similarly.)
	f.loggingUndo = logging.SetAsDefaultSlog(logger)

	// 3. Initialize error registry if service name code is set
	if code := strings.TrimSpace(cfg.GetString("SERVICE_NAME_CODE", "")); code != "" {
		f.Errors = errors.DefaultRegistry
		if err := f.Errors.InitServiceCode(code); err != nil {
			if f.loggingUndo != nil {
				f.loggingUndo()
				f.loggingUndo = nil
			}
			f.Logger = nil
			f.Config = nil
			f.Errors = nil
			f.stopFeatureFlags()
			return nil, fmt.Errorf("fit: failed to init error registry: %w", err)
		}
	}

	// 4. Resolve tracing through the tracing package's single activation gate.
	// Explicit mode follows TRACING_ENABLED; pyfit compatibility mode follows
	// pyfit's OTEL_SDK_DISABLED truthiness contract.
	{
		opts := tracingOptionsFromConfig(cfg)
		tracer, err := tracing.New(ctx, opts)
		if err != nil {
			logger.Warn("fit: tracing init failed, continuing without tracing", "error", redact.ErrorMessage(err))
		} else if tracer.IsEnabled() {
			f.Tracer = tracer
			// Install as the process-global tracer so tracing.Global() (used by the
			// server/kafka/db instrumentation) resolves to THIS tracer rather than a
			// separately lazy-initialized one. Makes fit.Init a complete boot path.
			f.tracingUndo = tracing.SetGlobal(tracer)
		}
	}

	// 5. Initialize the generic OTel metrics pipeline when explicitly enabled.
	// The OTel specification's exporter environment variables are supported, but
	// fit-go keeps export opt-in so an upgrade cannot unexpectedly connect every
	// service to localhost:4317.
	if otelMetricsRequested(cfg) {
		enabled := true
		metricOptions := otelmetrics.DefaultOptions()
		metricOptions.Enabled = &enabled
		if serviceName := strings.TrimSpace(cfg.GetString("OTEL_SERVICE_NAME", "")); serviceName != "" {
			metricOptions.ServiceName = serviceName
		}
		metricOptions.FallbackServiceName = strings.TrimSpace(cfg.GetString("SERVICE_NAME", metricOptions.FallbackServiceName))
		metricOptions.Env = cfg.GetString("NODE_ENV", metricOptions.Env)
		metricOptions.Exporters = cfg.GetString("OTEL_METRICS_EXPORTER", "otlp")
		if endpoint := strings.TrimSpace(cfg.GetString("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")); endpoint != "" {
			metricOptions.Endpoint = endpoint
			metricOptions.EndpointIsCommon = false
		} else if endpoint := strings.TrimSpace(cfg.GetString("OTEL_EXPORTER_OTLP_ENDPOINT", "")); endpoint != "" {
			metricOptions.Endpoint = endpoint
			metricOptions.EndpointIsCommon = true
		}
		metricOptions.Protocol = cfg.GetString("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
			cfg.GetString("OTEL_EXPORTER_OTLP_PROTOCOL", metricOptions.Protocol))
		metricOptions.ExportInterval = cfg.GetDuration("OTEL_METRIC_EXPORT_INTERVAL", metricOptions.ExportInterval)
		metricOptions.ExportTimeout = cfg.GetDuration("OTEL_METRIC_EXPORT_TIMEOUT", metricOptions.ExportTimeout)
		metricOptions.ErrorHandler = otel.ErrorHandlerFunc(func(err error) {
			logger.Warn("fit: OpenTelemetry metrics runtime error", "error_type", fmt.Sprintf("%T", err))
		})

		provider, err := otelmetrics.New(ctx, metricOptions)
		if err != nil {
			logger.Warn("fit: OpenTelemetry metrics init failed, continuing without OTel metrics", "error_type", fmt.Sprintf("%T", err))
		} else if provider.IsEnabled() {
			f.OTelMetrics = provider
			f.otelMetricsUndo = otelmetrics.InstallGlobal(provider)
		}
	}

	// Legacy TraceClue variables are interpreted only after typed
	// instrumentation is explicitly activated. This lets existing deployments
	// carry now-ignored legacy variables through a non-breaking fit-go upgrade.
	if instrumentationRequested(cfg, o) {
		configuredInstrumentations, err := instrumentation.ParseOptions(
			cfg.GetString(instrumentation.ExtraEnv, ""),
			cfg.GetString(instrumentation.ConfigEnv, ""),
		)
		if err != nil {
			cleanupErr := f.cleanupFailedInit(ctx)
			return nil, stderrors.Join(fmt.Errorf("fit: invalid instrumentation config: %w", err), cleanupErr)
		}
		configuredInstrumentations.Extra = append(configuredInstrumentations.Extra, o.InstrumentationOptions.Extra...)
		for name, value := range o.InstrumentationOptions.Config {
			configuredInstrumentations.Config[name] = value
		}
		if o.InstrumentationRegistry == nil {
			cleanupErr := f.cleanupFailedInit(ctx)
			return nil, stderrors.Join(stderrors.New("fit: typed instrumentation is enabled but no registry was supplied"), cleanupErr)
		}
		manager, startErr := o.InstrumentationRegistry.Start(ctx, configuredInstrumentations)
		if startErr != nil {
			cleanupErr := f.cleanupFailedInit(ctx)
			return nil, stderrors.Join(fmt.Errorf("fit: start instrumentation: %w", startErr), cleanupErr)
		}
		f.Instrumentations = manager
	}

	// PostgreSQL is opt-in because discovering connection variables must not make
	// an otherwise optional dependency mandatory. Initialization happens after
	// tracing and OTel metrics so pgx pools bind to the active providers.
	if o.PostgresOptions != nil {
		postgresClient, postgresErr := postgres.InitWithContext(ctx, *o.PostgresOptions)
		if postgresErr != nil {
			cleanupErr := f.cleanupFailedInit(ctx)
			return nil, stderrors.Join(fmt.Errorf("fit: failed to init PostgreSQL: %w", postgresErr), cleanupErr)
		}
		f.Connections.Postgres = postgresClient
		f.Postgres = postgresClient
		f.Health.AddCheck(postgresClient.HealthCheck())
	}

	// 6. Initialize legacy FIT Prometheus metrics if enabled.
	if cfg.GetBool("FIT_PROMETHEUS_ENABLED", false) {
		metricsDir := cfg.GetString("METRICS_DIR", "")
		if metricsDir == "" {
			logger.Warn("fit: METRICS_DIR not set, prometheus metrics disabled")
		} else {
			registry, err := metrics.New(metrics.Options{
				MetricsDir:        metricsDir,
				ServerEnabled:     cfg.GetBool("FIT_PROMETHEUS_SERVER_ENABLED", true),
				HTTPClientEnabled: cfg.GetBool("FIT_PROMETHEUS_AXIOS_ENABLED", true),
				ServerBuckets:     cfg.GetString("FIT_PROMETHEUS_SERVER_BUCKETS", ""),
				HTTPClientBuckets: cfg.GetString("FIT_PROMETHEUS_AXIOS_BUCKETS", ""),
				DeploymentName:    cfg.GetString("DEPLOYMENT_NAME", ""),
			})
			if err != nil {
				logger.Warn("fit: metrics init failed, continuing without metrics", "error", redact.ErrorMessage(err))
			} else {
				if f.metricsUndo != nil {
					f.metricsUndo()
				}
				f.Metrics = registry
				f.metricsUndo = metrics.SetDefault(registry)
			}
		}
	}

	// 7. Install one profiler instance for framework routes and shutdown. Legacy
	// profiling is on-demand: initialization exposes the control surface but does
	// not begin collection until /_profiling/start or profiling.Start is called.
	if cfg.GetBool("PROFILING_ENABLED", false) {
		profilerConfig := profiling.DefaultConfig()
		profilerConfig.Enabled = true
		profilerConfig.Server = cfg.GetString("PROFILING_DISTRIBUTOR_ADDRESS", profilerConfig.Server)
		profilerConfig.CPUEnabled = cfg.GetBool("PROFILING_CPU_ENABLED", profilerConfig.CPUEnabled)
		profilerConfig.HeapEnabled = cfg.GetBool("PROFILING_HEAP_ENABLED", profilerConfig.HeapEnabled)
		profilerConfig.WallEnabled = cfg.GetBool("PROFILING_CPU_WALL_ENABLED", profilerConfig.WallEnabled)
		profilerConfig.TagsJSON = cfg.GetString("PROFILING_TAGS_JSON", profilerConfig.TagsJSON)
		profilerConfig.FlushIntervalMs = cfg.GetInt("PROFILING_FLUSH_INTERVAL_MS", profilerConfig.FlushIntervalMs)
		profilerConfig.SampleRate = cfg.GetInt("PROFILING_SAMPLE_RATE", profilerConfig.SampleRate)
		profilerConfig.HeapSamplingIntervalBytes = cfg.GetInt("PROFILING_HEAP_SAMPLING_INTERVAL_BYTES", profilerConfig.HeapSamplingIntervalBytes)
		profilerConfig.HeapStackDepth = cfg.GetInt("PROFILING_HEAP_STACK_DEPTH", profilerConfig.HeapStackDepth)
		profilerConfig.WallSamplingDurationMs = cfg.GetInt("PROFILING_WALL_SAMPLING_DURATION_MS", profilerConfig.WallSamplingDurationMs)
		profilerConfig.WallSamplingIntervalMicros = cfg.GetInt("PROFILING_WALL_SAMPLING_INTERVAL_MICROS", profilerConfig.WallSamplingIntervalMicros)
		profilerConfig.WallCollectCPUTime = cfg.GetBool("PROFILING_WALL_COLLECT_CPU_TIME", profilerConfig.WallCollectCPUTime)
		f.Profiler = profiling.New(profilerConfig)
		f.profilingUndo = profiling.SetDefault(f.Profiler)
	}

	logger.Info("fit: framework initialized",
		"service", cfg.GetString("SERVICE_NAME", "unknown"),
		"env", cfg.GetString("NODE_ENV", "development"),
	)
	f.initialized = true

	return f, nil
}

func tracingOptionsFromConfig(cfg *config.Config) tracing.Options {
	opts := tracing.DefaultOptions()
	if cfg == nil {
		return opts
	}

	// OTel's standard service variable wins over FIT's legacy fallback.
	opts.ServiceName = strings.TrimSpace(cfg.GetString("OTEL_SERVICE_NAME", opts.ServiceName))
	opts.FallbackServiceName = strings.TrimSpace(cfg.GetString("SERVICE_NAME", opts.FallbackServiceName))
	opts.Env = cfg.GetString("NODE_ENV", opts.Env)
	opts.ActivationMode = cfg.GetString("FIT_TRACING_ACTIVATION_MODE", opts.ActivationMode)
	opts.Exporters = configValueUnlessEnvironmentSet(cfg, "OTEL_TRACES_EXPORTER", opts.Exporters)
	opts.Propagators = configValueUnlessEnvironmentSet(cfg, "OTEL_PROPAGATORS", opts.Propagators)
	opts.Sampler = configValueUnlessEnvironmentSet(cfg, "OTEL_TRACES_SAMPLER", opts.Sampler)
	if !environmentSet("OTEL_TRACES_SAMPLER_ARG") {
		opts.SampleRate = cfg.GetFloat("OTEL_TRACES_SAMPLER_ARG", opts.SampleRate)
	}

	// Signal-specific variables have precedence over common variables, while an
	// explicitly set environment variable always wins over config-file values.
	if !environmentSet("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") && !environmentSet("OTEL_EXPORTER_OTLP_ENDPOINT") {
		if endpoint := strings.TrimSpace(cfg.GetString("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")); endpoint != "" {
			opts.Endpoint = endpoint
		} else {
			opts.Endpoint = cfg.GetString("OTEL_EXPORTER_OTLP_ENDPOINT", opts.Endpoint)
		}
	}
	if !environmentSet("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL") && !environmentSet("OTEL_EXPORTER_OTLP_PROTOCOL") {
		if protocol := strings.TrimSpace(cfg.GetString("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "")); protocol != "" {
			opts.Protocol = protocol
		} else {
			opts.Protocol = cfg.GetString("OTEL_EXPORTER_OTLP_PROTOCOL", opts.Protocol)
		}
	}
	if !environmentSet("OTEL_RESOURCE_ATTRIBUTES") {
		opts.ResourceAttributes = cfg.GetString("OTEL_RESOURCE_ATTRIBUTES", "")
	}

	if raw := strings.TrimSpace(cfg.GetString("TRACING_ENABLED", "")); raw != "" &&
		!strings.EqualFold(strings.TrimSpace(opts.ActivationMode), "pyfit") {
		enabled := cfg.GetBool("TRACING_ENABLED", false)
		opts.Enabled = &enabled
	}
	if raw := strings.TrimSpace(cfg.GetString("OTEL_SDK_DISABLED", "")); raw != "" {
		disabled := strings.EqualFold(raw, "true")
		if strings.EqualFold(strings.TrimSpace(opts.ActivationMode), "pyfit") {
			// pyfit treats any non-empty value as disabled, including "false".
			disabled = true
		}
		opts.SDKDisabled = &disabled
	}
	return opts
}

func environmentSet(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

func configValueUnlessEnvironmentSet(cfg *config.Config, key, fallback string) string {
	if environmentSet(key) {
		return fallback
	}
	return cfg.GetString(key, fallback)
}

// Shutdown gracefully shuts down all framework components.
func (f *Fit) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.Logger != nil {
		f.Logger.Info("fit: shutting down framework")
	}

	var errs []error
	if f.Health != nil {
		f.Health.Reset()
	}
	if f.Instrumentations != nil {
		if err := f.Instrumentations.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		f.Instrumentations = nil
	}
	if f.Postgres != nil {
		if err := f.Postgres.Close(); err != nil {
			errs = append(errs, err)
		}
		f.Postgres = nil
		f.Connections.Postgres = nil
	}
	f.stopFeatureFlags()
	if f.Profiler != nil {
		f.Profiler.Stop()
		f.Profiler = nil
	}
	if f.profilingUndo != nil {
		f.profilingUndo()
		f.profilingUndo = nil
	}

	if f.tracingUndo != nil {
		f.tracingUndo()
		f.tracingUndo = nil
	}
	if f.OTelMetrics != nil {
		if err := f.OTelMetrics.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("OpenTelemetry metrics shutdown: %w", err))
		}
		f.OTelMetrics = nil
	}
	if f.otelMetricsUndo != nil {
		f.otelMetricsUndo()
		f.otelMetricsUndo = nil
	}
	if f.Tracer != nil {
		if err := f.Tracer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("tracer shutdown: %w", err))
		}
		f.Tracer = nil
	}

	if f.Metrics != nil {
		if f.metricsUndo != nil {
			f.metricsUndo()
			f.metricsUndo = nil
		}
		if err := f.Metrics.Shutdown(); err != nil {
			errs = append(errs, fmt.Errorf("metrics shutdown: %w", err))
		}
		f.Metrics = nil
	}

	if f.loggingUndo != nil {
		f.loggingUndo()
		f.loggingUndo = nil
	}
	f.Logger = nil
	f.Config = nil
	f.Connections = Connections{}
	f.Health = health.NewChecker()
	if f.Errors != nil {
		f.Errors.Reset()
	}
	f.Errors = nil
	f.initialized = false

	if len(errs) > 0 {
		return fmt.Errorf("fit: shutdown errors: %v", errs)
	}
	return nil
}

func otelMetricsRequested(cfg *config.Config) bool {
	if cfg == nil || cfg.GetBool("OTEL_SDK_DISABLED", false) {
		return false
	}
	if exporter, exists := cfg.Raw("OTEL_METRICS_EXPORTER"); exists {
		exporter = strings.TrimSpace(exporter)
		if exporter == "" || strings.EqualFold(exporter, "none") {
			return false
		}
		return true
	}
	return cfg.GetBool("OTEL_METRICS_ENABLED", false)
}

func instrumentationRequested(cfg *config.Config, options *options) bool {
	if options == nil {
		return false
	}
	return options.InstrumentationRegistry != nil ||
		len(options.InstrumentationOptions.Extra) > 0 ||
		len(options.InstrumentationOptions.Config) > 0 ||
		(cfg != nil && cfg.GetBool("FIT_INSTRUMENTATION_ENABLED", false))
}

func (f *Fit) stopFeatureFlags() {
	if featureClient, ok := f.Connections.FeatureFlag.(interface{ Stop() }); ok {
		featureClient.Stop()
	}
	f.Connections.FeatureFlag = nil
}

func (f *Fit) cleanupFailedInit(ctx context.Context) error {
	var cleanupErrors []error
	if f.Postgres != nil {
		cleanupErrors = append(cleanupErrors, f.Postgres.Close())
		f.Postgres = nil
		f.Connections.Postgres = nil
	}
	if f.Instrumentations != nil {
		cleanupErrors = append(cleanupErrors, f.Instrumentations.Shutdown(ctx))
		f.Instrumentations = nil
	}
	if f.OTelMetrics != nil {
		cleanupErrors = append(cleanupErrors, f.OTelMetrics.Shutdown(ctx))
		f.OTelMetrics = nil
	}
	if f.otelMetricsUndo != nil {
		f.otelMetricsUndo()
		f.otelMetricsUndo = nil
	}
	if f.tracingUndo != nil {
		f.tracingUndo()
		f.tracingUndo = nil
	}
	if f.Tracer != nil {
		cleanupErrors = append(cleanupErrors, f.Tracer.Shutdown(ctx))
		f.Tracer = nil
	}
	if f.loggingUndo != nil {
		f.loggingUndo()
		f.loggingUndo = nil
	}
	f.Logger = nil
	f.Config = nil
	if f.Errors != nil {
		f.Errors.Reset()
	}
	f.Errors = nil
	f.stopFeatureFlags()
	return stderrors.Join(cleanupErrors...)
}

// Option configures the Fit framework initialization.
type Option func(*options)

type options struct {
	ConfigPaths             []string
	InstrumentationRegistry *instrumentation.Registry
	InstrumentationOptions  instrumentation.Options
	PostgresOptions         *postgres.ConnectionOptions
}

// WithInstrumentationRegistry installs statically linked instrumentation
// factories used by TraceClue-compatible extension configuration.
func WithInstrumentationRegistry(registry *instrumentation.Registry) Option {
	return func(options *options) {
		options.InstrumentationRegistry = registry
	}
}

// WithInstrumentations selects additional registered instrumentations and
// provides typed factory JSON. Explicit values override environment config.
func WithInstrumentations(configuration instrumentation.Options) Option {
	return func(target *options) {
		target.InstrumentationOptions = configuration
	}
}

// WithPostgres enables explicit PostgreSQL initialization during Init. It
// discovers POSTGRES_*_READ_WRITE and POSTGRES_*_READ_ONLY connections, adds
// their health check, and closes all pools during Shutdown. PostgreSQL remains
// disabled unless this option is supplied.
func WithPostgres(configuration postgres.ConnectionOptions) Option {
	return func(target *options) {
		copy := configuration
		target.PostgresOptions = &copy
	}
}

func defaultOptions() *options {
	return &options{}
}

// WithConfigPaths sets additional configuration file paths.
func WithConfigPaths(paths ...string) Option {
	return func(o *options) {
		o.ConfigPaths = append(o.ConfigPaths, paths...)
	}
}
