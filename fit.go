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
	"fmt"
	"strings"
	"sync"

	"github.com/gofynd/fit-go/config"
	"github.com/gofynd/fit-go/errors"
	"github.com/gofynd/fit-go/feature"
	"github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/logging"
	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/profiling"
	"github.com/gofynd/fit-go/redact"
	"github.com/gofynd/fit-go/tracing"
)

// Connections holds all initialized database and service connections.
// interface.
type Connections struct {
	Mongo       interface{} // MongoDB connections (read/write per service)
	MySQL       interface{} // MySQL connections via database/sql
	Postgres    interface{} // PostgreSQL connections via database/sql
	Redis       interface{} // Redis connections (standalone or cluster)
	FeatureFlag interface{} // Feature flag client
	Kafka       interface{} // Kafka client
	GroupCache  interface{} // GroupCache distributed in-process cache
}

// Fit is the main framework singleton that holds global state.
type Fit struct {
	mu            sync.RWMutex
	Config        *config.Config
	Connections   Connections
	Logger        *logging.Logger
	Tracer        *tracing.Tracer
	Metrics       *metrics.Registry
	Health        *health.Checker
	Profiler      *profiling.Profiler
	Errors        *errors.ErrorRegistry
	metricsUndo   func()
	profilingUndo func()
	tracingUndo   func()
	loggingUndo   func()
	initialized   bool
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

// Init initializes the Fit framework with the given options.
// This is the primary entry point -
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
	f.Health = health.NewChecker()
	f.Errors = nil
	f.Profiler = nil

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
		featureClient, err := feature.InitWithOptions(feature.Options{
			Enabled:             true,
			URL:                 cfg.GetString("FEATURE_FLAG_URL", ""),
			APIKey:              cfg.GetString("FEATURE_FLAG_API_KEY", ""),
			RequireInitialState: cfg.GetBool("FEATURE_FLAG_REQUIRE_INITIAL_STATE", false),
		})
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

	// 4. Initialize tracing if enabled
	if cfg.GetBool("TRACING_ENABLED", false) {
		enabled := true
		// Start from DefaultOptions and OVERRIDE only what config supplies. Building a
		// bare Options{} here silently dropped the sampler, sample rate, exporter
		// endpoint/protocol and batching defaults — and a zero SampleRate collapses to
		// NeverSample() in buildSampler, so a service booting through fit.Init produced
		// NO locally-rooted traces at all. Everything the platform configures via the
		// OTel-standard env (OTEL_TRACES_SAMPLER, OTEL_TRACES_SAMPLER_ARG,
		// OTEL_EXPORTER_OTLP_ENDPOINT, …) must survive this path.
		opts := tracing.DefaultOptions()
		// OTel's standard service variable wins over FIT's legacy fallback. Do
		// not replace an OTEL_SERVICE_NAME already resolved by DefaultOptions
		// merely because SERVICE_NAME also exists in the merged config.
		if serviceName := strings.TrimSpace(cfg.GetString("OTEL_SERVICE_NAME", "")); serviceName != "" {
			opts.ServiceName = serviceName
		}
		// SERVICE_NAME is an application/Kafka/log identity fallback. It must not
		// override a service.name supplied through OTEL_RESOURCE_ATTRIBUTES.
		opts.FallbackServiceName = strings.TrimSpace(cfg.GetString("SERVICE_NAME", opts.FallbackServiceName))
		opts.Env = cfg.GetString("NODE_ENV", opts.Env)
		// Set explicitly: tracing.New's own gate reads only the env var, which is
		// empty when TRACING_ENABLED came from a config file.
		opts.Enabled = &enabled
		tracer, err := tracing.New(ctx, opts)
		if err != nil {
			logger.Warn("fit: tracing init failed, continuing without tracing", "error", redact.ErrorMessage(err))
		} else {
			f.Tracer = tracer
			// Install as the process-global tracer so tracing.Global() (used by the
			// server/kafka/db instrumentation) resolves to THIS tracer rather than a
			// separately lazy-initialized one. Makes fit.Init a complete boot path.
			f.tracingUndo = tracing.SetGlobal(tracer)
		}
	}

	// 5. Initialize metrics if enabled
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

	// 6. Install one profiler instance for framework routes and shutdown. Legacy
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

func (f *Fit) stopFeatureFlags() {
	if featureClient, ok := f.Connections.FeatureFlag.(interface{ Stop() }); ok {
		featureClient.Stop()
	}
	f.Connections.FeatureFlag = nil
}

// Option configures the Fit framework initialization.
type Option func(*options)

type options struct {
	ConfigPaths []string
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
