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

package profiling

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
)

func TestTagWrapperScopesProfilingLabels(t *testing.T) {
	var observed map[string]string
	TagWrapper(context.Background(), map[string]string{"region": "us-east-1", "vehicle": "car"}, func(ctx context.Context) {
		observed = map[string]string{}
		pprof.ForLabels(ctx, func(key, value string) bool {
			observed[key] = value
			return true
		})
	})
	if observed["region"] != "us-east-1" || observed["vehicle"] != "car" {
		t.Fatalf("profiling labels = %#v", observed)
	}
}

func TestTagWrapperNilCallbackIsNoop(t *testing.T) {
	TagWrapper(nil, map[string]string{"key": "value"}, nil)
}

// ---------------------------------------------------------------------------
// Config tests
// ---------------------------------------------------------------------------

func TestProfilerConfig(t *testing.T) {
	t.Run("defaults from clean env", func(t *testing.T) {
		clearProfilingEnv(t)

		cfg := DefaultConfig()

		if cfg.Enabled {
			t.Error("Expected Enabled to default to false")
		}
		if cfg.Server != "http://utility-pyroscope-distributor.utility.svc.cluster.local:4040" {
			t.Errorf("Unexpected default server: %s", cfg.Server)
		}
		if !cfg.CPUEnabled {
			t.Error("Expected CPUEnabled to default to true")
		}
		if !cfg.HeapEnabled {
			t.Error("Expected HeapEnabled to default to true")
		}
		if !cfg.WallEnabled {
			t.Error("Expected WallEnabled to default to true")
		}
		if cfg.FlushIntervalMs != 10000 {
			t.Errorf("Expected FlushIntervalMs=10000, got %d", cfg.FlushIntervalMs)
		}
		if cfg.SampleRate != 10 || cfg.EffectiveSampleRate != 100 || cfg.SampleRateConfigurable {
			t.Errorf("unexpected sample-rate compatibility config: %+v", cfg)
		}
		if cfg.HeapSamplingIntervalBytes != 524288 {
			t.Errorf("Expected HeapSamplingIntervalBytes=524288, got %d", cfg.HeapSamplingIntervalBytes)
		}
		if cfg.HeapStackDepth != 64 {
			t.Errorf("Expected HeapStackDepth=64, got %d", cfg.HeapStackDepth)
		}
		if cfg.WallSamplingDurationMs != 60000 {
			t.Errorf("Expected WallSamplingDurationMs=60000, got %d", cfg.WallSamplingDurationMs)
		}
		if cfg.WallSamplingIntervalMicros != 10000 {
			t.Errorf("Expected WallSamplingIntervalMicros=10000, got %d", cfg.WallSamplingIntervalMicros)
		}
		if cfg.WallCollectCPUTime {
			t.Error("Expected WallCollectCPUTime to default to false")
		}
	})

	t.Run("reads values from env", func(t *testing.T) {
		t.Setenv("PROFILING_ENABLED", "true")
		t.Setenv("PROFILING_DISTRIBUTOR_ADDRESS", "http://test:4040")
		t.Setenv("PROFILING_CPU_ENABLED", "false")
		t.Setenv("PROFILING_FLUSH_INTERVAL_MS", "5000")
		t.Setenv("PROFILING_SAMPLE_RATE", "25")
		t.Setenv("PROFILING_HEAP_SAMPLING_INTERVAL_BYTES", "1048576")
		t.Setenv("PROFILING_WALL_COLLECT_CPU_TIME", "true")

		cfg := DefaultConfig()

		if !cfg.Enabled {
			t.Error("Expected Enabled=true from env")
		}
		if cfg.Server != "http://test:4040" {
			t.Errorf("Expected server from env, got %s", cfg.Server)
		}
		if cfg.CPUEnabled {
			t.Error("Expected CPUEnabled=false from env")
		}
		if cfg.FlushIntervalMs != 5000 {
			t.Errorf("Expected FlushIntervalMs=5000, got %d", cfg.FlushIntervalMs)
		}
		if cfg.SampleRate != 25 || cfg.EffectiveSampleRate != 100 || cfg.SampleRateConfigurable {
			t.Errorf("unexpected sample-rate compatibility config: %+v", cfg)
		}
		if cfg.HeapSamplingIntervalBytes != 1048576 {
			t.Errorf("Expected HeapSamplingIntervalBytes=1048576, got %d", cfg.HeapSamplingIntervalBytes)
		}
		if !cfg.WallCollectCPUTime {
			t.Error("Expected WallCollectCPUTime=true from env")
		}
	})

	t.Run("application name derivation", func(t *testing.T) {
		t.Setenv("K8S_POD_NAME", "my-service-abc123-xyz")
		t.Setenv("PROJECT_NAME", "fynd")
		t.Setenv("DEPLOYMENT_TYPE", "worker")

		p := New(Config{
			Enabled:                    true,
			CPUEnabled:                 true,
			WallSamplingIntervalMicros: 100000,
		})
		p.Start()
		defer p.Stop()

		appName := p.GetApplicationName()
		if !strings.HasPrefix(appName, "fynd-") {
			t.Errorf("Expected app name to start with 'fynd-', got %s", appName)
		}
		if !strings.HasSuffix(appName, "Worker") {
			t.Errorf("Expected app name to end with 'Worker', got %s", appName)
		}
	})

	t.Run("application name with explicit deployment name", func(t *testing.T) {
		t.Setenv("PROJECT_NAME", "commerce")
		t.Setenv("DEPLOYMENT_NAME", "api-server")
		t.Setenv("DEPLOYMENT_TYPE", "server")

		p := New(Config{
			Enabled:                    true,
			WallSamplingIntervalMicros: 100000,
		})
		p.Start()
		defer p.Stop()

		if p.GetApplicationName() != "commerce-api-server-Server" {
			t.Errorf("Expected 'commerce-api-server-Server', got %s", p.GetApplicationName())
		}
	})

	t.Run("tags parsing", func(t *testing.T) {
		t.Setenv("PLATFORM_VERSION", "v1.2.3")
		t.Setenv("K8S_POD_NAME", "test-pod")
		t.Setenv("PROJECT_NAME", "fynd")

		p := New(Config{
			Enabled:                    true,
			TagsJSON:                   `{"custom_tag":"value","env":"staging"}`,
			WallSamplingIntervalMicros: 100000,
		})
		p.Start()
		defer p.Stop()

		tags := p.GetTags()
		if tags["fynd_platform_version"] != "v1.2.3" {
			t.Errorf("Expected platform version tag, got %v", tags)
		}
		if tags["custom_tag"] != "value" {
			t.Errorf("Expected custom_tag=value, got %v", tags["custom_tag"])
		}
		if tags["env"] != "staging" {
			t.Errorf("Expected env=staging, got %v", tags["env"])
		}
		if tags["project_name"] != "fynd" {
			t.Errorf("Expected project_name=fynd, got %v", tags["project_name"])
		}
		if tags["pod_name"] != "test-pod" {
			t.Errorf("Expected pod_name=test-pod, got %v", tags["pod_name"])
		}
	})

	t.Run("GetConfig returns current config", func(t *testing.T) {
		cfg := Config{
			Enabled:    true,
			Server:     "http://test:4040",
			CPUEnabled: true,
			TagsJSON:   `{"env":"test"}`,
		}
		p := New(cfg)
		result := p.GetConfig()

		if result.Server != cfg.Server {
			t.Errorf("Expected server %s, got %s", cfg.Server, result.Server)
		}
		if result.TagsJSON != cfg.TagsJSON {
			t.Errorf("Expected TagsJSON %s, got %s", cfg.TagsJSON, result.TagsJSON)
		}
	})
}

// ---------------------------------------------------------------------------
// Start / Stop tests
// ---------------------------------------------------------------------------

func TestProfilerStartStop(t *testing.T) {
	t.Run("disabled profiler does not start", func(t *testing.T) {
		p := New(Config{Enabled: false})
		p.Start()

		if !p.IsProfilingDisabled() {
			t.Error("Expected profiling to be disabled")
		}
		if p.IsRunning() {
			t.Error("Expected profiler not running when disabled")
		}
	})

	t.Run("start and stop all", func(t *testing.T) {
		p := New(Config{
			Enabled:                    true,
			CPUEnabled:                 true,
			HeapEnabled:                true,
			WallEnabled:                true,
			WallSamplingIntervalMicros: 100000,
			HeapSamplingIntervalBytes:  524288,
		})

		p.Start()
		time.Sleep(10 * time.Millisecond)

		if !p.IsRunning() {
			t.Error("Expected profiler to be running")
		}
		if !p.IsCPUProfilingRunning() {
			t.Error("Expected CPU profiling running")
		}
		if !p.IsHeapProfilingRunning() {
			t.Error("Expected heap profiling running")
		}
		if !p.IsWallProfilingRunning() {
			t.Error("Expected wall profiling running")
		}

		p.Stop()

		if p.IsRunning() {
			t.Error("Expected profiler stopped")
		}
		if p.IsCPUProfilingRunning() {
			t.Error("Expected CPU profiling stopped")
		}
		if p.IsHeapProfilingRunning() {
			t.Error("Expected heap profiling stopped")
		}
		if p.IsWallProfilingRunning() {
			t.Error("Expected wall profiling stopped")
		}
	})

	t.Run("CPU profiling start/stop", func(t *testing.T) {
		p := New(Config{Enabled: true, CPUEnabled: true})
		p.enabled.Store(true)

		p.StartCPUProfiling()
		if !p.IsCPUProfilingRunning() {
			t.Error("Expected CPU profiling running")
		}

		p.StopCPUProfiling()
		if p.IsCPUProfilingRunning() {
			t.Error("Expected CPU profiling stopped")
		}
	})

	t.Run("heap profiling start/stop", func(t *testing.T) {
		oldRate := runtime.MemProfileRate
		defer func() { runtime.MemProfileRate = oldRate }()

		p := New(Config{
			Enabled:                   true,
			HeapEnabled:               true,
			HeapSamplingIntervalBytes: 262144,
		})
		p.enabled.Store(true)

		p.StartHeapProfiling()
		if !p.IsHeapProfilingRunning() {
			t.Error("Expected heap profiling running")
		}
		if runtime.MemProfileRate != 262144 {
			t.Errorf("Expected MemProfileRate=262144, got %d", runtime.MemProfileRate)
		}

		p.StopHeapProfiling()
		if p.IsHeapProfilingRunning() {
			t.Error("Expected heap profiling stopped")
		}
	})

	t.Run("wall profiling start/stop", func(t *testing.T) {
		p := New(Config{
			Enabled:                    true,
			WallEnabled:                true,
			WallSamplingIntervalMicros: 100000,
		})
		p.enabled.Store(true)

		p.StartWallProfiling()
		if !p.IsWallProfilingRunning() {
			t.Error("Expected wall profiling running")
		}

		time.Sleep(50 * time.Millisecond)

		p.StopWallProfiling()
		if p.IsWallProfilingRunning() {
			t.Error("Expected wall profiling stopped")
		}
	})

	t.Run("idempotent start/stop", func(t *testing.T) {
		p := New(Config{
			Enabled:                    true,
			CPUEnabled:                 true,
			WallSamplingIntervalMicros: 100000,
		})

		p.Start()
		p.StartCPUProfiling()
		p.StartCPUProfiling()

		if !p.IsCPUProfilingRunning() {
			t.Error("Expected CPU profiling running")
		}

		p.StopCPUProfiling()
		p.StopCPUProfiling()

		if p.IsCPUProfilingRunning() {
			t.Error("Expected CPU profiling stopped")
		}

		p.Stop()
		p.Stop()
	})

	t.Run("capture heap profile", func(t *testing.T) {
		p := New(Config{Enabled: true, HeapEnabled: true})
		p.StartHeapProfiling()
		defer p.StopHeapProfiling()

		_ = make([]byte, 1024*1024)

		buf, err := p.CaptureHeapProfile()
		if err != nil {
			t.Fatalf("CaptureHeapProfile() error: %v", err)
		}
		if len(buf) == 0 {
			t.Error("Expected non-empty heap profile")
		}
	})

	t.Run("capture goroutine profile", func(t *testing.T) {
		p := New(Config{Enabled: true})

		done := make(chan bool)
		go func() { <-done }()
		defer close(done)

		buf, err := p.CaptureGoroutineProfile()
		if err != nil {
			t.Fatalf("CaptureGoroutineProfile() error: %v", err)
		}
		if len(buf) == 0 {
			t.Error("Expected non-empty goroutine profile")
		}
	})
}

// ---------------------------------------------------------------------------
// Status tests
// ---------------------------------------------------------------------------

func TestProfilerStatus(t *testing.T) {
	t.Run("GetDetailedStatus reflects running state", func(t *testing.T) {
		p := New(Config{
			Enabled:                    true,
			CPUEnabled:                 true,
			HeapEnabled:                false,
			WallEnabled:                true,
			WallSamplingIntervalMicros: 100000,
		})
		p.Start()
		defer p.Stop()

		status := p.GetDetailedStatus()
		if !status.Overall {
			t.Error("Expected overall=true")
		}
		if !status.CPU.Enabled || !status.CPU.Running {
			t.Error("Expected CPU enabled and running")
		}
		if status.Heap.Enabled || status.Heap.Running {
			t.Error("Expected heap disabled and not running")
		}
		if !status.Wall.Enabled || !status.Wall.Running {
			t.Error("Expected wall enabled and running")
		}
	})

	t.Run("Status returns map", func(t *testing.T) {
		p := New(Config{
			Enabled:                    true,
			CPUEnabled:                 true,
			WallSamplingIntervalMicros: 100000,
		})
		p.Start()
		defer p.Stop()

		s := p.Status()
		if s["running"] != true {
			t.Error("Expected running=true in status map")
		}
		cpuStatus, ok := s["cpu"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected cpu status map")
		}
		if cpuStatus["running"] != true {
			t.Error("Expected cpu.running=true")
		}
		sampleRate, ok := s["sampleRate"].(map[string]interface{})
		if !ok || sampleRate["requested"] != 10 || sampleRate["effective"] != 100 || sampleRate["configurable"] != false {
			t.Errorf("unexpected sample-rate status: %#v", s["sampleRate"])
		}
	})

	t.Run("default profiler exists", func(t *testing.T) {
		p := Default()
		if p == nil {
			t.Error("Expected default profiler to exist")
		}
	})
}

func TestSetDefaultRestoresPreviousProfiler(t *testing.T) {
	baseline := Default()
	first := New(Config{Enabled: true})
	second := New(Config{Enabled: true})
	restoreFirst := SetDefault(first)
	restoreSecond := SetDefault(second)

	restoreFirst()
	if Default() != second {
		t.Fatal("restoring an older owner clobbered the active profiler")
	}
	restoreSecond()
	if Default() != baseline {
		t.Fatal("restoring the active owner did not restore the baseline profiler")
	}
}

// ---------------------------------------------------------------------------
// HTTP routes tests
// ---------------------------------------------------------------------------

func TestProfilerRoutes(t *testing.T) {
	p := New(Config{
		Enabled:                    true,
		CPUEnabled:                 false,
		HeapEnabled:                true,
		WallEnabled:                false,
		WallSamplingIntervalMicros: 100000,
		HeapSamplingIntervalBytes:  524288,
	})

	handler := p.Routes()

	t.Run("GET /_profiling/start", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/_profiling/start", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if body["message"] != "profiling started" {
			t.Errorf("Unexpected message: %v", body["message"])
		}
	})

	t.Run("GET /_profiling/status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/_profiling/status", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Expected application/json, got %s", ct)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if body["running"] != true {
			t.Error("Expected running=true after start")
		}
	})

	t.Run("GET /_profiling/config", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/_profiling/config", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}
		var cfg Config
		if err := json.NewDecoder(rec.Body).Decode(&cfg); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if !cfg.Enabled {
			t.Error("Expected enabled=true in config")
		}
		if !cfg.HeapEnabled {
			t.Error("Expected heapEnabled=true in config")
		}
	})

	t.Run("GET /_profiling/start_heap and stop_heap", func(t *testing.T) {
		// Start heap
		req := httptest.NewRequest(http.MethodGet, "/_profiling/start_heap", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}

		// Stop heap
		req = httptest.NewRequest(http.MethodGet, "/_profiling/stop_heap", nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if body["running"] != false {
			t.Error("Expected running=false after stop_heap")
		}
	})

	t.Run("GET /_profiling/start_wall and stop_wall", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/_profiling/start_wall", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}

		req = httptest.NewRequest(http.MethodGet, "/_profiling/stop_wall", nil)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}
	})

	t.Run("GET /_profiling/stop", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/_profiling/stop", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", rec.Code)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if body["message"] != "profiling stopped" {
			t.Errorf("Unexpected message: %v", body["message"])
		}
	})
}

// ---------------------------------------------------------------------------
// Env helper tests
// ---------------------------------------------------------------------------

func TestEnvHelpers(t *testing.T) {
	t.Run("envBool", func(t *testing.T) {
		tests := []struct {
			key      string
			setValue string
			def      bool
			expected bool
		}{
			{"TEST_BOOL_TRUE", "true", false, true},
			{"TEST_BOOL_ONE", "1", false, true},
			{"TEST_BOOL_FALSE", "false", true, false},
			{"TEST_BOOL_UNSET", "", true, true},
		}
		for _, tt := range tests {
			if tt.setValue != "" {
				t.Setenv(tt.key, tt.setValue)
			}
			if got := envBool(tt.key, tt.def); got != tt.expected {
				t.Errorf("envBool(%q, %v) = %v, want %v", tt.key, tt.def, got, tt.expected)
			}
		}
	})

	t.Run("envInt", func(t *testing.T) {
		t.Setenv("TEST_INT", "42")
		if envInt("TEST_INT", 0) != 42 {
			t.Error("Expected 42")
		}
		if envInt("TEST_INT_UNSET", 100) != 100 {
			t.Error("Expected default 100")
		}
		t.Setenv("TEST_INT_INVALID", "notanint")
		if envInt("TEST_INT_INVALID", 50) != 50 {
			t.Error("Expected default on invalid")
		}
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func clearProfilingEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"PROFILING_ENABLED", "PROFILING_DISTRIBUTOR_ADDRESS", "PROFILING_CPU_ENABLED",
		"PROFILING_HEAP_ENABLED", "PROFILING_CPU_WALL_ENABLED", "PROFILING_TAGS_JSON",
		"PROFILING_FLUSH_INTERVAL_MS", "PROFILING_HEAP_SAMPLING_INTERVAL_BYTES",
		"PROFILING_SAMPLE_RATE",
		"PROFILING_HEAP_STACK_DEPTH", "PROFILING_WALL_SAMPLING_DURATION_MS",
		"PROFILING_WALL_SAMPLING_INTERVAL_MICROS", "PROFILING_WALL_COLLECT_CPU_TIME",
	} {
		oldVal := os.Getenv(key)
		os.Unsetenv(key)
		t.Cleanup(func() {
			if oldVal != "" {
				os.Setenv(key, oldVal)
			}
		})
	}
}
