// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Upstream main's feature.Init (FEATURE_FLAG_ENABLED=true) is a synchronous
// polling client. It must never select the SSE streaming transport.
func TestUpstreamInitUsesPollingLegacyClientNotStreaming(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path+" accept="+r.Header.Get("Accept"))
		mu.Unlock()
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			w.Header().Set("Content-Type", "text/event-stream")
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"poll-flag": true})
	}))
	defer server.Close()

	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("FEATURE_FLAG_URL", server.URL)
	t.Setenv("FEATURE_FLAG_API_KEY", "guard-key")
	client, err := Init()
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer client.Stop()

	if client.legacy == nil {
		t.Fatal("Init did not select the legacy polling client")
	}
	if client.done != nil || client.cancel != nil || client.features != nil {
		t.Fatal("Init started streaming client state")
	}
	if !client.IsEnabled("poll-flag") {
		t.Fatal("initial synchronous poll did not populate flags")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) == 0 {
		t.Fatal("Init made no initial fetch")
	}
	for _, request := range requests {
		if strings.Contains(request, "text/event-stream") || strings.Contains(request, "/features/guard-key") {
			t.Fatalf("Init issued an SSE request: %q (all: %v)", request, requests)
		}
		if !strings.HasPrefix(request, "GET /features ") {
			t.Fatalf("Init request = %q, want GET /features polling fetch", request)
		}
	}
}
