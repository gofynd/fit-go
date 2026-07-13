// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package fit

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/gofynd/fit-go/instrumentation"
)

func TestInitOwnsTypedInstrumentationLifecycle(t *testing.T) {
	resetFitInstrumentationState(t)
	setBaseInstrumentationEnv(t)
	t.Setenv(instrumentation.ExtraEnv, "legacy:Hook")
	t.Setenv(instrumentation.ConfigEnv, `{"legacy:Hook":{"enabled":true}}`)
	registry := instrumentation.NewRegistry()
	started, stopped := false, false
	if err := registry.Register(instrumentation.Registration{
		Name: "typed", Aliases: []string{"legacy:Hook"},
		Factory: func(_ context.Context, raw json.RawMessage) (instrumentation.Hook, error) {
			if string(raw) != `{"enabled":true}` {
				t.Fatalf("factory config = %s", raw)
			}
			return instrumentation.HookFuncs{
				StartFunc:    func(context.Context) error { started = true; return nil },
				ShutdownFunc: func(context.Context) error { stopped = true; return nil },
			}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	framework, err := Init(context.Background(), WithInstrumentationRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	if !started || framework.Instrumentations == nil {
		t.Fatal("typed instrumentation did not start")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !stopped || framework.Instrumentations != nil {
		t.Fatal("typed instrumentation did not stop")
	}
}

func TestInitFailsClosedWhenExtensionIsNotLinked(t *testing.T) {
	resetFitInstrumentationState(t)
	setBaseInstrumentationEnv(t)
	t.Setenv("FIT_INSTRUMENTATION_ENABLED", "true")
	t.Setenv(instrumentation.ExtraEnv, "legacy:Missing")

	framework, err := Init(context.Background())
	if framework != nil || err == nil || !strings.Contains(err.Error(), "no registry") {
		t.Fatalf("Init() = %v, %v", framework, err)
	}
	if Instance().Config != nil || Instance().Logger != nil || Instance().initialized {
		t.Fatal("failed Init retained partial framework state")
	}
}

func TestInitIgnoresLegacyInstrumentationVariablesUntilActivated(t *testing.T) {
	resetFitInstrumentationState(t)
	setBaseInstrumentationEnv(t)
	t.Setenv(instrumentation.ExtraEnv, "legacy:Missing")
	t.Setenv(instrumentation.ConfigEnv, `{invalid`)

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("inactive legacy instrumentation broke startup: %v", err)
	}
	if framework.Instrumentations != nil {
		t.Fatal("inactive legacy instrumentation unexpectedly started")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownKeepsFrameworkConnectionsAvailableToInstrumentation(t *testing.T) {
	resetFitInstrumentationState(t)
	setBaseInstrumentationEnv(t)
	registry := instrumentation.NewRegistry()
	connection := &lifecycleFeatureConnection{}
	connectionAvailable := false
	if err := registry.Register(instrumentation.Registration{
		Name: "typed", EnabledByDefault: true,
		Factory: func(context.Context, json.RawMessage) (instrumentation.Hook, error) {
			return instrumentation.HookFuncs{
				ShutdownFunc: func(context.Context) error {
					connectionAvailable = Instance().Connections.FeatureFlag == connection && !connection.stopped
					return nil
				},
			}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	framework, err := Init(context.Background(), WithInstrumentationRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	framework.Connections.FeatureFlag = connection
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !connectionAvailable {
		t.Fatal("framework connection stopped before instrumentation")
	}
	if !connection.stopped {
		t.Fatal("framework connection was not stopped")
	}
}

func setBaseInstrumentationEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("PROFILING_ENABLED", "false")
	t.Setenv("FEATURE_FLAG_ENABLED", "false")
	t.Setenv("SERVICE_NAME", "fit-instrumentation-test")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")
	t.Setenv(instrumentation.ExtraEnv, "")
	t.Setenv(instrumentation.ConfigEnv, "")
	t.Setenv("FIT_INSTRUMENTATION_ENABLED", "false")
}

func resetFitInstrumentationState(t *testing.T) {
	t.Helper()
	instance = nil
	once = sync.Once{}
	t.Cleanup(func() {
		if instance != nil && instance.initialized {
			_ = instance.Shutdown(context.Background())
		}
		instance = nil
		once = sync.Once{}
	})
}

type lifecycleFeatureConnection struct {
	stopped bool
}

func (connection *lifecycleFeatureConnection) Stop() { connection.stopped = true }
