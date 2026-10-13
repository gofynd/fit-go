// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package compatibility_test

import (
	"context"
	"testing"

	fit "github.com/gofynd/fit-go"
	"github.com/gofynd/fit-go/encryption"
	fiterrors "github.com/gofynd/fit-go/errors"
	"github.com/gofynd/fit-go/feature"
	"github.com/gofynd/fit-go/kafka"
	"github.com/gofynd/fit-go/logging"
	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/mongo"
	"github.com/gofynd/fit-go/mysql"
	"github.com/gofynd/fit-go/postgres"
	"github.com/gofynd/fit-go/profiling"
	"github.com/gofynd/fit-go/redis"
	"github.com/gofynd/fit-go/server"
	"github.com/gofynd/fit-go/tracing"
	"github.com/gofynd/fit-go/utils"
)

// This test is primarily a compile contract for the descriptive API names. The
// migration-era Advanced names continue to compile throughout the package test
// suite and remain wrappers/aliases for source compatibility.
func TestDescriptiveAPISurfaceCompiles(t *testing.T) {
	var _ func(context.Context, ...fit.Option) (*fit.Fit, error) = fit.InitManaged
	var _ func(postgres.ConnectionPoolOptions) fit.Option = fit.WithPostgresPoolOptions
	var _ func(logging.RuntimeOptions) (*logging.Logger, error) = logging.NewRuntime
	var _ func(metrics.TextfileOptions) (*metrics.Registry, error) = metrics.NewTextfileRegistry
	var _ func(context.Context, tracing.SDKOptions) (*tracing.Tracer, error) = tracing.NewSDK
	var _ func(tracing.SDKOptions) (*tracing.Tracer, error) = tracing.InitSDK
	var _ func() profiling.RuntimeConfig = profiling.DefaultRuntimeConfig
	var _ func(profiling.RuntimeConfig) *profiling.Profiler = profiling.NewRuntime
	var _ func() (*feature.Client, error) = feature.InitStreaming
	var _ func() mongo.DialFunc = mongo.DefaultInstrumentedDialFunc
	var _ func(mysql.InstrumentedConnectionOptions) (*mysql.Client, error) = mysql.InitInstrumented
	var _ func(context.Context, postgres.ConnectionPoolOptions) (*postgres.Client, error) = postgres.InitWithPoolOptions
	var _ func(encryption.ManagerOptions) *encryption.Manager = encryption.NewManagerWithOptions
	var _ func(fiterrors.SentryHooksConfig) error = fiterrors.InitSentryWithHooks
	var _ func(utils.HTTPClientOptions) *utils.HTTPClient = utils.NewInstrumentedHTTPClient
	var _ func(redis.CompatibilityOptions) (*redis.Client, error) = redis.InitWithCompatibility
	var _ func(kafka.KafkaClient, kafka.ProducerOptions) (kafka.KafkaProducer, error) = kafka.NewProducer
	var _ func(kafka.KafkaClient, kafka.ConsumerSettings) (kafka.KafkaConsumer, error) = kafka.NewConsumer
	var _ func(kafka.KafkaConsumer, kafka.MessageHandler, kafka.ConsumeOptions) error = kafka.ConsumeWithOptions
	var _ func(server.RuntimeConfig) *server.Server = server.NewRuntime

	_ = server.AccessLogConfig{}
	_ = server.JWTVerificationOptions{}
	_ = server.OTelMiddlewareOptions{}
	_ = redis.CompatibilityOptions{
		DialWithSettings:     redis.DefaultConfiguredDialFunc(),
		ClusterWithSettings:  redis.DefaultConfiguredClusterDialFunc(),
		SentinelWithSettings: redis.DefaultConfiguredSentinelDialFunc(),
	}
}

func TestDeprecatedForkAPISurfaceStillCompiles(t *testing.T) {
	var _ func(context.Context, ...fit.Option) (*fit.Fit, error) = fit.InitAdvanced
	var _ func(postgres.ConnectionAdvancedOptions) fit.Option = fit.WithPostgresAdvanced
	var _ func(logging.AdvancedOptions) (*logging.Logger, error) = logging.NewAdvanced
	var _ func(metrics.AdvancedOptions) (*metrics.Registry, error) = metrics.NewAdvanced
	var _ func(context.Context, tracing.AdvancedOptions) (*tracing.Tracer, error) = tracing.NewAdvanced
	var _ func(tracing.AdvancedOptions) (*tracing.Tracer, error) = tracing.InitWithAdvancedOptions
	var _ func() profiling.AdvancedConfig = profiling.DefaultAdvancedConfig
	var _ func(profiling.AdvancedConfig) *profiling.Profiler = profiling.NewAdvanced
	var _ func() (*feature.Client, error) = feature.InitAdvanced
	var _ func() mongo.DialFunc = mongo.DefaultAdvancedDialFunc
	var _ func(mysql.ConnectionAdvancedOptions) (*mysql.Client, error) = mysql.InitAdvanced
	var _ func(context.Context, postgres.ConnectionAdvancedOptions) (*postgres.Client, error) = postgres.InitWithAdvancedContext
	var _ func(encryption.ManagerAdvancedOptions) *encryption.Manager = encryption.NewManagerAdvanced
	var _ func(fiterrors.SentryAdvancedConfig) error = fiterrors.InitSentryWithAdvancedConfig
	var _ func(utils.HTTPClientOptions) *utils.HTTPClient = utils.NewHTTPClientAdvanced
	var _ func(redis.AdvancedConnectionOptions) (*redis.Client, error) = redis.InitAdvanced
	var _ func(kafka.KafkaClient, kafka.ProducerAdvancedConfig) (kafka.KafkaProducer, error) = kafka.NewProducerAdvanced
	var _ func(kafka.KafkaClient, kafka.ConsumerAdvancedConfig) (kafka.KafkaConsumer, error) = kafka.NewConsumerAdvanced
	var _ func(kafka.KafkaConsumer, kafka.MessageHandler, kafka.ConsumerAdvancedOptions) error = kafka.ConsumeAdvanced
	var _ func(server.AdvancedConfig) *server.Server = server.NewAdvanced

	_ = server.LogRequestResponseAdvancedConfig{}
	_ = server.JWTAdvancedOptions{}
	_ = server.OTelMiddlewareAdvancedConfig{}
	_ = redis.AdvancedConnectionOptions{
		AdvancedDial:         redis.DefaultAdvancedDialFunc(),
		AdvancedClusterDial:  redis.DefaultAdvancedClusterDialFunc(),
		AdvancedSentinelDial: redis.DefaultAdvancedSentinelDialFunc(),
	}
}
