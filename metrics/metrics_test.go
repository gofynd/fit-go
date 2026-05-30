// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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

package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// TestRegistryNew
// ---------------------------------------------------------------------------

func TestRegistryNew(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		registry, err := New(Options{})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if registry == nil {
			t.Fatal("New() returned nil")
		}
	})

	t.Run("server metrics enabled", func(t *testing.T) {
		registry, err := New(Options{ServerEnabled: true})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if !registry.ShouldRecordServerMetrics() {
			t.Error("Server metrics should be enabled")
		}
		if registry.serverHist == nil {
			t.Error("Server histogram should be initialized")
		}
	})

	t.Run("HTTP client metrics enabled", func(t *testing.T) {
		registry, err := New(Options{HTTPClientEnabled: true})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if !registry.ShouldRecordHTTPClientMetrics() {
			t.Error("HTTP client metrics should be enabled")
		}
		if registry.clientHist == nil {
			t.Error("Client histogram should be initialized")
		}
	})

	t.Run("with deployment name", func(t *testing.T) {
		registry, _ := New(Options{
			DeploymentName: "test-deployment",
			ServerEnabled:  true,
		})
		if registry.deploymentName != "test-deployment" {
			t.Errorf("DeploymentName = %q, want test-deployment", registry.deploymentName)
		}
	})

	t.Run("deployment name from env", func(t *testing.T) {
		os.Setenv("DEPLOYMENT_NAME", "env-deployment")
		defer os.Unsetenv("DEPLOYMENT_NAME")

		registry, _ := New(Options{ServerEnabled: true})
		if registry.deploymentName != "env-deployment" {
			t.Errorf("DeploymentName = %q, want env-deployment", registry.deploymentName)
		}
	})

	t.Run("custom server buckets", func(t *testing.T) {
		registry, _ := New(Options{
			ServerEnabled: true,
			ServerBuckets: "1,5,10,50,100",
		})
		// Verify the histogram was created (we can't inspect buckets directly,
		// but we can verify the histogram works by observing).
		registry.RecordServerMetrics(ServerMetrics{
			Method:     "GET",
			Route:      "/test",
			StatusCode: 200,
			Duration:   10 * time.Millisecond,
		})
		output := registry.GetMetricsOutput()
		if !strings.Contains(output, "fit_http_request_duration_ms") {
			t.Error("Output should contain server metric after recording")
		}
	})

	t.Run("custom prometheus registry", func(t *testing.T) {
		promReg := prometheus.NewRegistry()
		registry, err := New(Options{
			ServerEnabled:      true,
			PrometheusRegistry: promReg,
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if registry.promRegistry != promReg {
			t.Error("Should use the provided prometheus registry")
		}
	})
}

// ---------------------------------------------------------------------------
// TestRecordServerMetrics
// ---------------------------------------------------------------------------

func TestRecordServerMetrics(t *testing.T) {
	registry, _ := New(Options{
		ServerEnabled:  true,
		DeploymentName: "test",
	})

	registry.RecordServerMetrics(ServerMetrics{
		Method:     "GET",
		Route:      "/api/users",
		StatusCode: 200,
		Duration:   100 * time.Millisecond,
	})

	output := registry.GetMetricsOutput()
	if !strings.Contains(output, "fit_http_request_duration_ms") {
		t.Error("Output should contain server metric name")
	}
	if !strings.Contains(output, `method="GET"`) {
		t.Error("Output should contain method label")
	}
	if !strings.Contains(output, `route="/api/users"`) {
		t.Error("Output should contain route label")
	}
	if !strings.Contains(output, `status_code="200"`) {
		t.Error("Output should contain status_code label")
	}
	if !strings.Contains(output, "_count") {
		t.Error("Output should contain _count metric")
	}
	if !strings.Contains(output, "_sum") {
		t.Error("Output should contain _sum metric")
	}
}

func TestRecordServerMetrics_Disabled(t *testing.T) {
	registry, _ := New(Options{ServerEnabled: false})
	// Should not panic when disabled.
	registry.RecordServerMetrics(ServerMetrics{
		Method:     "GET",
		Route:      "/api/users",
		StatusCode: 200,
		Duration:   100 * time.Millisecond,
	})
}

func TestRecordServerMetrics_NilRegistry(t *testing.T) {
	var registry *Registry
	// Should not panic on nil registry.
	registry.RecordServerMetrics(ServerMetrics{})
}

// ---------------------------------------------------------------------------
// TestRecordHTTPClientMetrics
// ---------------------------------------------------------------------------

func TestRecordHTTPClientMetrics(t *testing.T) {
	registry, _ := New(Options{
		HTTPClientEnabled: true,
		DeploymentName:    "test",
	})

	registry.RecordHTTPClientMetrics(HTTPClientMetrics{
		Method:     "POST",
		Host:       "api.example.com",
		StatusCode: 201,
		Duration:   250 * time.Millisecond,
	})

	output := registry.GetMetricsOutput()
	if !strings.Contains(output, "fit_http_client_request_duration_ms") {
		t.Error("Output should contain client metric name")
	}
	if !strings.Contains(output, `host="api.example.com"`) {
		t.Error("Output should contain host label")
	}
	if !strings.Contains(output, `status_code="201"`) {
		t.Error("Output should contain status_code label")
	}
}

func TestRecordHTTPClientMetrics_Disabled(t *testing.T) {
	registry, _ := New(Options{HTTPClientEnabled: false})
	// Should not panic when disabled.
	registry.RecordHTTPClientMetrics(HTTPClientMetrics{
		Method:     "GET",
		Host:       "api.example.com",
		StatusCode: 200,
		Duration:   100 * time.Millisecond,
	})
}

func TestRecordHTTPClientMetrics_NilRegistry(t *testing.T) {
	var registry *Registry
	// Should not panic on nil registry.
	registry.RecordHTTPClientMetrics(HTTPClientMetrics{})
}

// ---------------------------------------------------------------------------
// TestMetricsOutput
// ---------------------------------------------------------------------------

func TestMetricsOutput(t *testing.T) {
	t.Run("both enabled with observations", func(t *testing.T) {
		registry, _ := New(Options{
			ServerEnabled:     true,
			HTTPClientEnabled: true,
			DeploymentName:    "test",
		})

		registry.RecordServerMetrics(ServerMetrics{
			Method:     "GET",
			Route:      "/api",
			StatusCode: 200,
			Duration:   50 * time.Millisecond,
		})
		registry.RecordHTTPClientMetrics(HTTPClientMetrics{
			Method:     "POST",
			Host:       "example.com",
			StatusCode: 200,
			Duration:   100 * time.Millisecond,
		})

		output := registry.GetMetricsOutput()
		if !strings.Contains(output, "fit_http_request_duration_ms") {
			t.Error("Output should contain server metric")
		}
		if !strings.Contains(output, "fit_http_client_request_duration_ms") {
			t.Error("Output should contain client metric")
		}
		if !strings.Contains(output, "# HELP") {
			t.Error("Output should contain HELP comments")
		}
		if !strings.Contains(output, "# TYPE") {
			t.Error("Output should contain TYPE comments")
		}
	})

	t.Run("empty registry", func(t *testing.T) {
		registry, _ := New(Options{})
		output := registry.GetMetricsOutput()
		if output != "" {
			t.Errorf("Empty registry should return empty output, got %q", output)
		}
	})

	t.Run("nil registry", func(t *testing.T) {
		var registry *Registry
		output := registry.GetMetricsOutput()
		if output != "" {
			t.Errorf("Nil registry should return empty output, got %q", output)
		}
	})
}

// ---------------------------------------------------------------------------
// TestHandler
// ---------------------------------------------------------------------------

func TestHandler(t *testing.T) {
	registry, _ := New(Options{
		ServerEnabled:  true,
		DeploymentName: "test",
	})

	registry.RecordServerMetrics(ServerMetrics{
		Method:     "GET",
		Route:      "/health",
		StatusCode: 200,
		Duration:   5 * time.Millisecond,
	})

	handler := registry.Handler()
	if handler == nil {
		t.Fatal("Handler() returned nil")
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Handler status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "fit_http_request_duration_ms") {
		t.Error("Handler response should contain metrics")
	}
}

func TestHandler_NilRegistry(t *testing.T) {
	var registry *Registry
	handler := registry.Handler()
	if handler == nil {
		t.Fatal("Handler() on nil registry should not return nil")
	}
}

// ---------------------------------------------------------------------------
// TestParseBuckets
// ---------------------------------------------------------------------------

func TestParseBuckets(t *testing.T) {
	tests := []struct {
		input    string
		expected []float64
	}{
		{"1,5,10", []float64{1, 5, 10}},
		{"1.5, 2.5, 5.0", []float64{1.5, 2.5, 5.0}},
		{"100", []float64{100}},
		{"", defaultServerBuckets},
		{"invalid,bucket,values", defaultServerBuckets},
		{"1,invalid,10", []float64{1, 10}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseBuckets(tt.input)
			if len(got) != len(tt.expected) {
				t.Errorf("parseBuckets(%q) length = %d, want %d", tt.input, len(got), len(tt.expected))
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("parseBuckets(%q)[%d] = %f, want %f", tt.input, i, got[i], tt.expected[i])
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestShutdown
// ---------------------------------------------------------------------------

func TestShutdown(t *testing.T) {
	registry, _ := New(Options{
		ServerEnabled:     true,
		HTTPClientEnabled: true,
	})

	err := registry.Shutdown()
	if err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}

	// After shutdown, collectors should be unregistered.
	if len(registry.collectors) != 0 {
		t.Errorf("collectors length = %d, want 0 after Shutdown", len(registry.collectors))
	}
}

func TestShutdown_NilRegistry(t *testing.T) {
	var registry *Registry
	err := registry.Shutdown()
	if err != nil {
		t.Errorf("Shutdown() on nil registry error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestShouldRecord
// ---------------------------------------------------------------------------

func TestShouldRecord(t *testing.T) {
	t.Run("server enabled", func(t *testing.T) {
		r, _ := New(Options{ServerEnabled: true})
		if !r.ShouldRecordServerMetrics() {
			t.Error("ShouldRecordServerMetrics() = false, want true")
		}
	})

	t.Run("server disabled", func(t *testing.T) {
		r, _ := New(Options{ServerEnabled: false})
		if r.ShouldRecordServerMetrics() {
			t.Error("ShouldRecordServerMetrics() = true, want false")
		}
	})

	t.Run("client enabled", func(t *testing.T) {
		r, _ := New(Options{HTTPClientEnabled: true})
		if !r.ShouldRecordHTTPClientMetrics() {
			t.Error("ShouldRecordHTTPClientMetrics() = false, want true")
		}
	})

	t.Run("client disabled", func(t *testing.T) {
		r, _ := New(Options{HTTPClientEnabled: false})
		if r.ShouldRecordHTTPClientMetrics() {
			t.Error("ShouldRecordHTTPClientMetrics() = true, want false")
		}
	})

	t.Run("nil registry", func(t *testing.T) {
		var r *Registry
		if r.ShouldRecordServerMetrics() {
			t.Error("Nil registry should return false")
		}
		if r.ShouldRecordHTTPClientMetrics() {
			t.Error("Nil registry should return false")
		}
	})
}

// ---------------------------------------------------------------------------
// TestMultipleObservations
// ---------------------------------------------------------------------------

func TestMultipleObservations(t *testing.T) {
	registry, _ := New(Options{
		ServerEnabled:  true,
		DeploymentName: "test",
	})

	for i := 0; i < 10; i++ {
		registry.RecordServerMetrics(ServerMetrics{
			Method:     "GET",
			Route:      "/api",
			StatusCode: 200,
			Duration:   time.Duration(i*10) * time.Millisecond,
		})
	}

	output := registry.GetMetricsOutput()
	// The count should be 10.
	if !strings.Contains(output, "_count") {
		t.Error("Output should contain _count")
	}
	if !strings.Contains(output, "10") {
		t.Error("Count should be 10")
	}
}
