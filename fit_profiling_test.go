// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"
	"testing"

	"github.com/gofynd/fit-go/profiling"
)

func TestInitOwnsAndRestoresProcessProfiler(t *testing.T) {
	baseline := profiling.Default()
	t.Setenv("SERVICE_NAME", "profiling-lifecycle-test")
	t.Setenv("PROFILING_ENABLED", "true")
	t.Setenv("PROFILING_SAMPLE_RATE", "20")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if framework.Profiler == nil || profiling.Default() != framework.Profiler {
		t.Fatal("Init did not install its profiler as the process default")
	}
	if framework.Profiler.IsRunning() {
		t.Fatal("Init should preserve legacy on-demand profiling behavior")
	}
	profilerConfig := framework.Profiler.GetConfig()
	if profilerConfig.SampleRate != 20 || profilerConfig.EffectiveSampleRate != 100 || profilerConfig.SampleRateConfigurable {
		t.Fatalf("unexpected profiler sample-rate config: %+v", profilerConfig)
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if profiling.Default() != baseline {
		t.Fatal("Shutdown did not restore the previous process profiler")
	}
}
