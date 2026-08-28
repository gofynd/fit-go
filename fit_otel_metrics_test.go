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
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestInitOwnsOptInOTelMetricsLifecycle(t *testing.T) {
	resetFitOTelMetricsState(t)
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "1h")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("SERVICE_NAME", "fit-otel-metrics-test")
	t.Setenv("NODE_ENV", "test")

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if framework.OTelMetrics == nil || !framework.OTelMetrics.IsEnabled() {
		t.Fatal("Init did not create the requested OTel metrics provider")
	}
	if framework.Metrics != nil {
		t.Fatal("generic OTel metrics unexpectedly enabled legacy Prometheus metrics")
	}

	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if _, err := otel.GetMeterProvider().Meter("post-shutdown").Int64Counter("fit.post.shutdown"); err != nil {
		t.Fatalf("global routing provider was unusable after Shutdown: %v", err)
	}
}

func TestInitLeavesOTelMetricsDisabledByDefaultAndForNone(t *testing.T) {
	for _, exporter := range []string{"", "none"} {
		t.Run(exporter, func(t *testing.T) {
			resetFitOTelMetricsState(t)
			t.Setenv("OTEL_METRICS_EXPORTER", exporter)
			t.Setenv("TRACING_ENABLED", "false")
			t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
			t.Setenv("SERVICE_NAME", "fit-otel-metrics-disabled")
			framework, err := Init(context.Background())
			if err != nil {
				t.Fatalf("Init: %v", err)
			}
			if framework.OTelMetrics != nil {
				t.Fatal("OTel metrics should remain disabled")
			}
			if err := framework.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown: %v", err)
			}
		})
	}
}

func resetFitOTelMetricsState(t *testing.T) {
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
