// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"
	"testing"
	"time"

	fiterrors "github.com/gofynd/fit-go/errors"
)

func TestShutdownResetsLifecycleStateAndStopsHealthWork(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("SERVICE_NAME", "fit-lifecycle-reset")
	t.Setenv("SERVICE_NAME_CODE", "")
	t.Setenv("NODE_ENV", "test")

	f, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	oldHealth := f.Health
	f.Connections = Connections{
		Mongo:      "mongo",
		Redis:      "redis",
		Kafka:      "kafka",
		GroupCache: "groupcache",
	}
	f.Errors = fiterrors.DefaultRegistry

	started := make(chan struct{})
	release := make(chan struct{})
	oldHealth.AddCheck(func() string {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return ""
	})
	oldHealth.StartPeriodicCheck(30)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("periodic health check did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- f.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before the in-flight health check stopped: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after health check release")
	}

	if f.Config != nil || f.Logger != nil || f.Tracer != nil || f.Metrics != nil || f.Errors != nil {
		t.Fatalf("Shutdown retained lifecycle components: %+v", f)
	}
	if f.Connections != (Connections{}) {
		t.Fatalf("Shutdown retained connections: %+v", f.Connections)
	}
	if f.Health == nil || f.Health == oldHealth {
		t.Fatal("Shutdown did not install a fresh health checker")
	}
	if errs := f.Health.Check(); len(errs) != 0 {
		t.Fatalf("fresh health checker retained checks: %v", errs)
	}

	second, err := Init(context.Background())
	if err != nil {
		t.Fatalf("reinitialize: %v", err)
	}
	if second != f {
		t.Fatal("reinitialize unexpectedly replaced the Fit singleton")
	}
	if second.Health == oldHealth || second.Errors != nil || second.Connections != (Connections{}) {
		t.Fatal("reinitialize inherited state from the prior lifecycle")
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestInitAppliesAndResetsServiceErrorIdentity(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("SERVICE_NAME", "fit-error-identity")
	t.Setenv("SERVICE_NAME_CODE", "ONE")
	t.Setenv("NODE_ENV", "test")

	first, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := fiterrors.New(nil, 7).Code; got != "ONE0007" {
		t.Fatalf("first lifecycle error code = %q, want ONE0007", got)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := fiterrors.New(nil, 7).Code; got != "0007" {
		t.Fatalf("shutdown retained service error identity: %q", got)
	}

	t.Setenv("SERVICE_NAME_CODE", "TWO")
	second, err := Init(context.Background())
	if err != nil {
		t.Fatalf("reinitialize: %v", err)
	}
	if got := fiterrors.New(nil, 7).Code; got != "TWO0007" {
		t.Fatalf("second lifecycle error code = %q, want TWO0007", got)
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}
