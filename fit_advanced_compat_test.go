// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/postgres"
	"github.com/gofynd/fit-go/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type fitShutdownErrorExporter struct{ err error }

func (*fitShutdownErrorExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return nil
}
func (e *fitShutdownErrorExporter) Shutdown(context.Context) error { return e.err }

func TestInitAdvancedBoundsPreviousHealthResetByCallerContext(t *testing.T) {
	resetFitMetricsTestState(t)
	setAdvancedCompatibilityTestEnvironment(t)

	framework := Instance()
	started := make(chan struct{})
	release := make(chan struct{})
	framework.Health.AddCheck(func() string {
		close(started)
		<-release
		return ""
	})
	framework.Health.StartPeriodicCheck(1)
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := InitAdvanced(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("InitAdvanced error = %v; want caller deadline", err)
	}
	close(release)
	framework.Health.Reset()
}

func TestInitManagedUsesStrictConfigOverlays(t *testing.T) {
	resetFitMetricsTestState(t)
	setAdvancedCompatibilityTestEnvironment(t)

	dir := t.TempDir()
	base := filepath.Join(dir, "base.json")
	override := filepath.Join(dir, "override.json")
	if err := os.WriteFile(base, []byte(`{"FIT_GO_OVERLAY_REGRESSION":"base"}`), 0o600); err != nil {
		t.Fatalf("write base config: %v", err)
	}
	if err := os.WriteFile(override, []byte(`{"FIT_GO_OVERLAY_REGRESSION":"override"}`), 0o600); err != nil {
		t.Fatalf("write override config: %v", err)
	}

	framework, err := InitManaged(context.Background(), WithConfigPaths(base, override))
	if err != nil {
		t.Fatalf("InitManaged: %v", err)
	}
	if got := framework.Config.GetString("FIT_GO_OVERLAY_REGRESSION", ""); got != "override" {
		t.Fatalf("overlay value = %q, want last-file value %q", got, "override")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestOriginalAndManagedLifecyclesCannotOverlap(t *testing.T) {
	resetFitMetricsTestState(t)
	setAdvancedCompatibilityTestEnvironment(t)

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := InitManaged(context.Background()); err == nil {
		t.Fatal("InitManaged accepted an active original lifecycle")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("original Shutdown: %v", err)
	}

	// Original Shutdown intentionally retains public fields for compatibility.
	// Managed initialization must discard those stale, already-shut-down handles.
	framework.Tracer = &tracing.Tracer{}
	framework.Metrics = &metrics.Registry{}
	managed, err := InitManaged(context.Background())
	if err != nil {
		t.Fatalf("InitManaged after original Shutdown: %v", err)
	}
	if managed.Tracer != nil || managed.Metrics != nil {
		t.Fatal("InitManaged retained stale original telemetry handles")
	}
	if _, err := Init(context.Background()); err == nil {
		t.Fatal("Init accepted an active managed lifecycle")
	}
	if err := managed.Shutdown(context.Background()); err != nil {
		t.Fatalf("managed Shutdown: %v", err)
	}
}

func TestOriginalShutdownErrorDoesNotPermanentlyBlockManagedInit(t *testing.T) {
	resetFitMetricsTestState(t)
	setAdvancedCompatibilityTestEnvironment(t)
	enabled := true
	wantErr := errors.New("exporter shutdown failed")
	tracer, err := tracing.NewSDK(context.Background(), tracing.SDKOptions{
		ServiceName:            "shutdown-error",
		Enabled:                &enabled,
		SpanExporter:           &fitShutdownErrorExporter{err: wantErr},
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("NewSDK: %v", err)
	}
	framework := Instance()
	framework.Tracer = tracer
	framework.legacyInitialized = true
	if err := framework.Shutdown(context.Background()); err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("Shutdown error = %v, want %v", err, wantErr)
	}
	if framework.legacyInitialized {
		t.Fatal("failed legacy shutdown retained the lifecycle latch")
	}

	managed, err := InitManaged(context.Background())
	if err != nil {
		t.Fatalf("InitManaged after failed legacy shutdown: %v", err)
	}
	if err := managed.Shutdown(context.Background()); err != nil {
		t.Fatalf("managed Shutdown: %v", err)
	}
}

func TestLegacyPostgresOptionsArePromotedWithoutLosingValues(t *testing.T) {
	legacy := postgres.ConnectionOptions{
		MaxConns:          13,
		MinConns:          2,
		MaxConnLifetime:   3 * time.Hour,
		MaxConnIdleTime:   4 * time.Minute,
		HealthCheckPeriod: 5 * time.Second,
		PerService: map[string]postgres.ServicePoolOverrides{
			"orders": {
				Read:  &postgres.PoolOverrides{MaxConns: 7},
				Write: &postgres.PoolOverrides{MinConns: 3},
			},
		},
	}
	advanced := postgresAdvancedOptionsFrom(legacy)
	if advanced.MaxConns != legacy.MaxConns || advanced.MinConns != legacy.MinConns ||
		advanced.MaxConnLifetime != legacy.MaxConnLifetime || advanced.MaxConnIdleTime != legacy.MaxConnIdleTime ||
		advanced.HealthCheckPeriod != legacy.HealthCheckPeriod {
		t.Fatalf("promoted options lost top-level values: %+v", advanced)
	}
	orders := advanced.PerService["orders"]
	if orders.Read == nil || orders.Read.MaxConns != 7 || orders.Write == nil || orders.Write.MinConns != 3 {
		t.Fatalf("promoted options lost per-service values: %+v", orders)
	}

	opts := defaultOptions()
	WithPostgresPoolOptions(postgres.ConnectionPoolOptions{MinIdleConns: 4})(opts)
	if opts.PostgresAdvancedOptions == nil || opts.PostgresAdvancedOptions.MinIdleConns != 4 || opts.PostgresOptions != nil {
		t.Fatalf("WithPostgresPoolOptions selected the wrong option set: %+v", opts)
	}
}

func setAdvancedCompatibilityTestEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("DOTENV_PATH", filepath.Join(t.TempDir(), "missing.env"))
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("FEATURE_FLAG_ENABLED", "false")
	t.Setenv("PROFILING_ENABLED", "false")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")
}
