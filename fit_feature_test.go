// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInitStopsFeatureHubWhenLaterInitializationFails(t *testing.T) {
	resetFitMetricsTestState(t)
	disconnected := make(chan struct{})
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "event: features\ndata: []\n\n")
		w.(http.Flusher).Flush()
		<-request.Context().Done()
		close(disconnected)
	}))
	defer edge.Close()

	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("FEATURE_FLAG_URL", edge.URL)
	t.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	t.Setenv("FEATURE_FLAG_REQUIRE_INITIAL_STATE", "true")
	t.Setenv("FEATURE_FLAG_INIT_TIMEOUT", "1s")
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("TRACING_ENABLED", "false")
	t.Setenv("LOG_TIMEZONE", "Invalid/Timezone")

	framework, err := Init(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed to init logger") {
		t.Fatalf("Init error = %v; want logger initialization failure", err)
	}
	if framework != nil {
		t.Fatal("Init returned a framework after partial initialization failure")
	}

	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("FeatureHub stream remained active after initialization rollback")
	}
	state := Instance()
	if state.Config != nil || state.Logger != nil || state.Connections.FeatureFlag != nil || state.initialized {
		t.Fatalf("partial initialization state was retained: %+v", state)
	}
}

func TestInitTreatsFeatureHubAsOptionalByDefault(t *testing.T) {
	resetFitMetricsTestState(t)
	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("FEATURE_FLAG_URL", "")
	t.Setenv("FEATURE_FLAG_API_KEY", "")
	t.Setenv("FEATURE_FLAG_REQUIRE_INITIAL_STATE", "false")
	t.Setenv("FIT_PROMETHEUS_ENABLED", "false")
	t.Setenv("TRACING_ENABLED", "false")

	framework, err := Init(context.Background())
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if framework.Connections.FeatureFlag != nil {
		t.Fatal("incomplete optional FeatureHub configuration should remain disabled")
	}
	if err := framework.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
