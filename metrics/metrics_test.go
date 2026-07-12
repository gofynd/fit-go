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

package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	families, err := registry.promRegistry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != "fit_http_request_duration_ms" {
			continue
		}
		if family.GetHelp() != "HTTP request duration in milliseconds" {
			t.Fatalf("metric help = %q, want fit.js help text", family.GetHelp())
		}
		if got := family.GetMetric()[0].GetHistogram().GetSampleSum(); got != 100 {
			t.Fatalf("histogram sum = %v, want 100 milliseconds", got)
		}
		return
	}
	t.Fatal("server histogram family not found")
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
		{"10,1,-5,1,0", []float64{1, 10}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseBuckets(tt.input, defaultServerBuckets)
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

func TestParseBuckets_UsesCallerDefaults(t *testing.T) {
	got := parseBuckets("invalid", defaultClientBuckets)
	if len(got) != len(defaultClientBuckets) || got[0] != defaultClientBuckets[0] {
		t.Fatalf("client fallback buckets = %v, want %v", got, defaultClientBuckets)
	}
}

func TestFileOutput_ImmediateAndAtomicRefresh(t *testing.T) {
	dir := t.TempDir()
	registry, err := New(Options{
		MetricsDir:         dir,
		MetricsFile:        "fit-test.prom",
		FlushInterval:      time.Hour,
		ServerEnabled:      true,
		DeploymentName:     "test-deployment",
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer registry.Shutdown()

	path := filepath.Join(dir, "fit-test.prom")
	if registry.MetricsFile() != path {
		t.Fatalf("MetricsFile() = %q, want %q", registry.MetricsFile(), path)
	}
	// prom-file-client 0.1.1 waits for its first 4s tick. Creating the file
	// immediately is an intentional fit-go startup-readiness fix.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("initial textfile was not created: %v", err)
	}

	registry.RecordServerMetrics(ServerMetrics{
		Method:     http.MethodGet,
		Route:      "/orders/:id",
		StatusCode: http.StatusOK,
		Duration:   25 * time.Millisecond,
	})
	if err := registry.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	output := string(body)
	if !strings.Contains(output, "fit_http_request_duration_ms") ||
		!strings.Contains(output, `deployment_name="test-deployment"`) {
		t.Fatalf("textfile output missing metric labels:\n%s", output)
	}
	if err := registry.LastFlushError(); err != nil {
		t.Fatalf("LastFlushError() = %v", err)
	}

	registry.RecordServerMetrics(ServerMetrics{
		Method:     http.MethodPost,
		Route:      "/orders",
		StatusCode: http.StatusCreated,
		Duration:   30 * time.Millisecond,
	})
	if err := registry.Flush(); err != nil {
		t.Fatalf("second Flush() error = %v", err)
	}
	body, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() after second flush error = %v", err)
	}
	// The deployed writer opens with wx, so this refresh fails after its first
	// successful write. Atomic replacement intentionally fixes that defect.
	if !strings.Contains(string(body), `status_code="201"`) {
		t.Fatalf("second atomic refresh did not replace textfile:\n%s", body)
	}
}

func TestFileOutput_DeployedKubernetesContract(t *testing.T) {
	t.Setenv("K8S_POD_NAME", "metroplex-communications-abc123")
	dir := t.TempDir()
	registry, err := New(Options{
		MetricsDir:         dir,
		ServerEnabled:      true,
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer registry.Shutdown()

	wantFile := filepath.Join(dir, fmt.Sprintf("metroplex-communications-abc123-%d.prom", os.Getpid()))
	if registry.MetricsFile() != wantFile {
		t.Fatalf("MetricsFile() = %q, want deployed podname-pid contract %q", registry.MetricsFile(), wantFile)
	}
	if registry.flushInterval != 4*time.Second {
		t.Fatalf("flush interval = %v, want deployed 4s contract", registry.flushInterval)
	}
}

func TestFileOutput_NonKubernetesFallbackIsDocumentedDivergence(t *testing.T) {
	t.Setenv("K8S_POD_NAME", "")
	dir := t.TempDir()
	registry, err := New(Options{
		MetricsDir:         dir,
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer registry.Shutdown()

	wantBase := fmt.Sprintf("metrics-%d.prom", os.Getpid())
	if got := filepath.Base(registry.MetricsFile()); got != wantBase {
		t.Fatalf("fallback filename = %q, want deterministic fit-go fallback %q", got, wantBase)
	}
}

func TestFileOutput_InvalidDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := New(Options{MetricsDir: path, ServerEnabled: true}); err == nil {
		t.Fatal("New() should reject a non-directory MetricsDir")
	}
}

func TestFileOutput_PeriodicAndFinalFlush(t *testing.T) {
	dir := t.TempDir()
	registry, err := New(Options{
		MetricsDir:         dir,
		MetricsFile:        "periodic.prom",
		FlushInterval:      5 * time.Millisecond,
		HTTPClientEnabled:  true,
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	registry.RecordHTTPClientMetrics(HTTPClientMetrics{
		Method:     http.MethodGet,
		Host:       "api.example.com",
		StatusCode: http.StatusOK,
		Duration:   15 * time.Millisecond,
	})

	deadline := time.Now().Add(time.Second)
	for {
		body, readErr := os.ReadFile(registry.MetricsFile())
		if readErr == nil && strings.Contains(string(body), `status_code="200"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic output was not refreshed: read error=%v body=%q", readErr, body)
		}
		time.Sleep(5 * time.Millisecond)
	}

	registry.RecordHTTPClientMetrics(HTTPClientMetrics{
		Method:     http.MethodPost,
		Host:       "api.example.com",
		StatusCode: http.StatusCreated,
		Duration:   20 * time.Millisecond,
	})
	if err := registry.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	body, err := os.ReadFile(registry.MetricsFile())
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(body), `status_code="201"`) {
		t.Fatalf("shutdown did not perform final flush:\n%s", body)
	}
}

func TestSetDefault_RestoresPreviousRegistry(t *testing.T) {
	previous := Default()
	first, _ := New(Options{ServerEnabled: true})
	restore := SetDefault(first)
	if Default() != first {
		t.Fatal("SetDefault did not install registry")
	}
	restore()
	restore()
	if Default() != previous {
		t.Fatal("default registry was not restored")
	}
}

func TestSetDefault_RestoresBaselineAfterOutOfOrderOwners(t *testing.T) {
	baseline := Default()
	first, _ := New(Options{ServerEnabled: true})
	second, _ := New(Options{HTTPClientEnabled: true})
	restoreFirst := SetDefault(first)
	restoreSecond := SetDefault(second)

	restoreFirst()
	if Default() != second {
		t.Fatal("restoring the older owner clobbered the newer registry")
	}
	restoreSecond()
	if Default() != baseline {
		t.Fatal("newer owner restored the inactive older registry instead of the baseline")
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
	if err := registry.Shutdown(); err != nil {
		t.Errorf("second Shutdown() error = %v", err)
	}
}

func TestShutdown_NilRegistry(t *testing.T) {
	var registry *Registry
	err := registry.Shutdown()
	if err != nil {
		t.Errorf("Shutdown() on nil registry error = %v", err)
	}
}

func TestRegister_CustomCollectorIndependentOfHTTPFlags(t *testing.T) {
	registry, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "legacy_direct_event_total",
		Help: "legacy direct counter",
	}, []string{"type"})
	if err := registry.Register(counter); err != nil {
		t.Fatalf("Register: %v", err)
	}
	counter.WithLabelValues("email").Inc()

	output := registry.GetMetricsOutput()
	if !strings.Contains(output, `legacy_direct_event_total{type="email"} 1`) {
		t.Fatalf("custom counter missing from output:\n%s", output)
	}
	if registry.ShouldRecordServerMetrics() || registry.ShouldRecordHTTPClientMetrics() {
		t.Fatal("custom collector must not enable FIT HTTP histograms")
	}
}

func TestRegister_RollsBackAndRejectsAfterShutdown(t *testing.T) {
	registry, err := New(Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	first := prometheus.NewCounter(prometheus.CounterOpts{Name: "register_rollback_total", Help: "first"})
	duplicate := prometheus.NewCounter(prometheus.CounterOpts{Name: "register_rollback_total", Help: "duplicate"})
	if err := registry.Register(first, duplicate); err == nil {
		t.Fatal("Register duplicate: expected error")
	}
	if strings.Contains(registry.GetMetricsOutput(), "register_rollback_total") {
		t.Fatal("failed multi-collector registration was not rolled back")
	}
	if err := registry.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := registry.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: "after_shutdown_total", Help: "closed"})); err == nil {
		t.Fatal("Register after shutdown: expected error")
	}
}

func TestPeriodicFlushFailureIsVisible(t *testing.T) {
	dir := t.TempDir()
	errCh := make(chan error, 1)
	registry, err := New(Options{
		MetricsDir:    dir,
		MetricsFile:   "periodic.prom",
		FlushInterval: 10 * time.Millisecond,
		OnFlushError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := os.Remove(registry.MetricsFile()); err != nil {
		t.Fatalf("remove metrics file: %v", err)
	}
	if err := os.Mkdir(registry.MetricsFile(), 0o755); err != nil {
		t.Fatalf("replace metrics file with directory: %v", err)
	}

	select {
	case flushErr := <-errCh:
		if flushErr == nil || registry.LastFlushError() == nil {
			t.Fatal("periodic flush failure was not retained")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("periodic flush failure was not reported")
	}
	if err := registry.Shutdown(); err == nil {
		t.Fatal("Shutdown should report the final textfile flush failure")
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
