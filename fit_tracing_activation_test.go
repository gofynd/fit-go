// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofynd/fit-go/config"
)

func TestInitHonorsPyfitTracingActivationMode(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("FIT_TRACING_ACTIVATION_MODE", "pyfit")
	t.Setenv("TRACING_ENABLED", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("SERVICE_NAME", "fit-pyfit-activation")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if framework.Tracer == nil || !framework.Tracer.IsEnabled() {
		t.Fatal("fit.Init did not honor pyfit tracing activation")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestInitPyfitTracingModeHonorsNonEmptyDisableValue(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("FIT_TRACING_ACTIVATION_MODE", "pyfit")
	t.Setenv("TRACING_ENABLED", "true")
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("SERVICE_NAME", "fit-pyfit-disabled")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if framework.Tracer != nil {
		t.Fatal("fit.Init ignored pyfit's non-empty OTEL_SDK_DISABLED contract")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestTracingOptionsLoadOTelValuesFromConfigFile(t *testing.T) {
	keys := []string{
		"OTEL_SERVICE_NAME", "SERVICE_NAME", "NODE_ENV", "TRACING_ENABLED",
		"OTEL_SDK_DISABLED", "OTEL_TRACES_EXPORTER", "OTEL_PROPAGATORS",
		"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "OTEL_EXPORTER_OTLP_PROTOCOL",
		"OTEL_RESOURCE_ATTRIBUTES",
	}
	previous := make(map[string]string, len(keys))
	present := make(map[string]bool, len(keys))
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			previous[key] = value
			present[key] = true
		}
		_ = os.Unsetenv(key)
	}
	t.Cleanup(func() {
		for _, key := range keys {
			if present[key] {
				_ = os.Setenv(key, previous[key])
			} else {
				_ = os.Unsetenv(key)
			}
		}
	})

	path := filepath.Join(t.TempDir(), "tracing.json")
	if err := os.WriteFile(path, []byte(`{
		"OTEL_SERVICE_NAME": "config-service",
		"SERVICE_NAME": "legacy-service",
		"NODE_ENV": "config-test",
		"TRACING_ENABLED": true,
		"OTEL_SDK_DISABLED": false,
		"OTEL_TRACES_EXPORTER": "none",
		"OTEL_PROPAGATORS": "baggage",
		"OTEL_TRACES_SAMPLER": "parentbased_traceidratio",
		"OTEL_TRACES_SAMPLER_ARG": 0.25,
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector.example/v1/traces",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
		"OTEL_RESOURCE_ATTRIBUTES": "deployment.environment.name=config%20test,custom.key=config"
	}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	opts := tracingOptionsFromConfig(cfg)
	if opts.ServiceName != "config-service" || opts.FallbackServiceName != "legacy-service" || opts.Env != "config-test" {
		t.Fatalf("service config not carried into tracing options: %+v", opts)
	}
	if opts.Exporters != "none" || opts.Propagators != "baggage" || opts.Sampler != "parentbased_traceidratio" || opts.SampleRate != 0.25 {
		t.Fatalf("OTel sampling/export config not carried into tracing options: %+v", opts)
	}
	if opts.Endpoint != "http://collector.example/v1/traces" || opts.Protocol != "grpc" {
		t.Fatalf("OTLP config not carried into tracing options: %+v", opts)
	}
	if opts.ResourceAttributes != "deployment.environment.name=config%20test,custom.key=config" {
		t.Fatalf("resource attributes not carried into tracing options: %q", opts.ResourceAttributes)
	}
	if opts.Enabled == nil || !*opts.Enabled || opts.SDKDisabled == nil || *opts.SDKDisabled {
		t.Fatalf("activation config not carried into tracing options: %+v", opts)
	}
}
