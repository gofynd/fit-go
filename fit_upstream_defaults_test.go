// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"
	"log/slog"
	"testing"

	"github.com/gofynd/fit-go/metrics"
)

func TestOriginalInitRetainsOfficialMainLifecycle(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("PROFILING_ENABLED", "true")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")

	beforeSlog := slog.Default()
	beforeHealth := Instance().Health
	first, err := Init(context.Background())
	if err != nil {
		t.Fatalf("first Init: %v", err)
	}
	second, err := Init(context.Background())
	if err != nil {
		t.Fatalf("second Init: %v", err)
	}
	if first != second {
		t.Fatal("Init replaced the Fit singleton")
	}
	if second.initialized {
		t.Fatal("original Init opted into advanced lifecycle ownership")
	}
	if second.Health != beforeHealth {
		t.Fatal("original Init replaced the process health checker")
	}
	if second.Connections.FeatureFlag != nil || second.Profiler != nil || second.OTelMetrics != nil {
		t.Fatal("original Init enabled post-main framework components")
	}
	if slog.Default() != beforeSlog {
		t.Fatal("original Init replaced the process slog default")
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestOriginalInitCreatesScrapeMetricsWithoutMetricsDir(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FIT_PROMETHEUS_ENABLED", "true")
	t.Setenv("FIT_PROMETHEUS_SERVER_ENABLED", "true")
	t.Setenv("FIT_PROMETHEUS_AXIOS_ENABLED", "true")
	t.Setenv("METRICS_DIR", "")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")

	beforeDefault := metrics.Default()
	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if framework.Metrics == nil {
		t.Fatal("original Init did not create an enabled scrape registry")
	}
	if framework.Metrics.MetricsFile() != "" {
		t.Fatalf("original Init enabled post-main textfile output: %q", framework.Metrics.MetricsFile())
	}
	if metrics.Default() != beforeDefault {
		t.Fatal("original Init installed a post-main process-default registry")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
