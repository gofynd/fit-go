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

package fit

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/gofynd/fit-go/metrics"
)

func TestInit_WiresMetricsDefaultAndTextfile(t *testing.T) {
	resetFitMetricsTestState(t)

	t.Setenv("FIT_PROMETHEUS_ENABLED", "true")
	t.Setenv("FIT_PROMETHEUS_SERVER_ENABLED", "true")
	t.Setenv("FIT_PROMETHEUS_AXIOS_ENABLED", "true")
	t.Setenv("METRICS_DIR", t.TempDir())
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("SERVICE_NAME", "fit-metrics-test")
	t.Setenv("NODE_ENV", "test")

	f, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if f.Metrics == nil {
		t.Fatal("Init did not create enabled metrics registry")
	}
	registry := f.Metrics
	if metrics.Default() != registry {
		t.Fatal("Init did not install process-default metrics registry")
	}
	if registry.MetricsFile() == "" {
		t.Fatal("Init did not configure textfile output")
	}
	if _, err := os.Stat(registry.MetricsFile()); err != nil {
		t.Fatalf("metrics textfile was not initialized: %v", err)
	}

	if err := f.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if metrics.Default() == registry {
		t.Fatal("Shutdown did not restore process-default metrics registry")
	}
}

func TestInit_MetricsRemainDisabledWithoutCompleteEnablement(t *testing.T) {
	tests := []struct {
		name       string
		enabled    string
		metricsDir string
	}{
		{name: "master switch off", enabled: "false", metricsDir: t.TempDir()},
		{name: "missing metrics directory", enabled: "true", metricsDir: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetFitMetricsTestState(t)
			previous := metrics.Default()
			t.Setenv("FIT_PROMETHEUS_ENABLED", tt.enabled)
			t.Setenv("METRICS_DIR", tt.metricsDir)
			t.Setenv("TRACING_ENABLED", "false")
			t.Setenv("SERVICE_NAME", "fit-metrics-disabled-test")
			t.Setenv("NODE_ENV", "test")

			f, err := Init(context.Background())
			if err != nil {
				t.Fatalf("Init: %v", err)
			}
			if f.Metrics != nil {
				t.Fatal("metrics should remain disabled")
			}
			if metrics.Default() != previous {
				t.Fatal("disabled init changed process-default metrics")
			}
			if err := f.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown: %v", err)
			}
		})
	}
}

func TestInit_RejectsDuplicateAndCanReinitializeAfterShutdown(t *testing.T) {
	resetFitMetricsTestState(t)

	t.Setenv("FIT_PROMETHEUS_ENABLED", "true")
	t.Setenv("METRICS_DIR", t.TempDir())
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("SERVICE_NAME", "fit-lifecycle-test")
	t.Setenv("NODE_ENV", "test")

	previousSlog := slog.Default()
	first, err := Init(context.Background())
	if err != nil {
		t.Fatalf("first Init: %v", err)
	}
	firstMetrics := first.Metrics
	if _, err := Init(context.Background()); err == nil {
		t.Fatal("duplicate Init succeeded; it would leak global telemetry resources")
	}
	if metrics.Default() != firstMetrics {
		t.Fatal("rejected duplicate Init changed the process metrics registry")
	}

	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if slog.Default() != previousSlog {
		t.Fatal("Shutdown did not restore the previous process slog logger")
	}
	if first.initialized {
		t.Fatal("Shutdown left Fit marked initialized")
	}

	second, err := Init(context.Background())
	if err != nil {
		t.Fatalf("reinitialize after Shutdown: %v", err)
	}
	if second.Metrics == nil || second.Metrics == firstMetrics {
		t.Fatal("reinitialize did not create a fresh metrics lifecycle")
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func resetFitMetricsTestState(t *testing.T) {
	t.Helper()
	instance = nil
	once = sync.Once{}
	t.Cleanup(func() {
		if instance != nil {
			_ = instance.Shutdown(context.Background())
		}
		instance = nil
		once = sync.Once{}
	})
}
