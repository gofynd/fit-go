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

// Package profiling provides Pyroscope continuous profiling integration for the fit.go framework.
//
// This module supports:
// - CPU profiling via pprof
// - Heap/memory profiling via pprof
// - Wall-clock profiling via goroutine sampling
// - Pyroscope server push via grafana/pyroscope-go
// - On-demand start/stop control
// - HTTP routes for manual profiling control
//
// Environment variables:
// - PROFILING_ENABLED: Enable profiling (default: false)
// - PROFILING_DISTRIBUTOR_ADDRESS: Pyroscope server URL
// - PROFILING_CPU_ENABLED: Enable CPU profiling (default: true)
// - PROFILING_HEAP_ENABLED: Enable heap profiling (default: true)
// - PROFILING_CPU_WALL_ENABLED: Enable wall profiling (default: true)
// - PROFILING_TAGS_JSON: Additional tags as JSON object
// - PROFILING_FLUSH_INTERVAL_MS: Flush interval (default: 10000)
// - PROFILING_SAMPLE_RATE: Legacy requested CPU sample rate (default: 10)
// - PROFILING_HEAP_SAMPLING_INTERVAL_BYTES: Heap sample rate (default: 524288)
// - PROFILING_HEAP_STACK_DEPTH: Heap stack depth (default: 64)
// - PROFILING_WALL_SAMPLING_DURATION_MS: Wall profile duration (default: 60000)
// - PROFILING_WALL_SAMPLING_INTERVAL_MICROS: Wall sample interval (default: 10000)
// - PROFILING_WALL_COLLECT_CPU_TIME: Include CPU time in wall profile (default: false)
// - K8S_POD_NAME: Kubernetes pod name for tag derivation
// - PROJECT_NAME: Project name for app identification
// - DEPLOYMENT_NAME: Deployment name override
// - DEPLOYMENT_TYPE: Deployment type (server, worker, etc.)
// - PLATFORM_VERSION: Platform version tag
package profiling

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pyroscope "github.com/grafana/pyroscope-go"
)

const pyroscopeEffectiveSampleRate = 100

// Config holds the profiler configuration.
type Config struct {
	Enabled                    bool              `json:"enabled"`
	Server                     string            `json:"server"`
	CPUEnabled                 bool              `json:"cpuEnabled"`
	HeapEnabled                bool              `json:"heapEnabled"`
	WallEnabled                bool              `json:"cpuWallEnabled"`
	TagsJSON                   string            `json:"tagsJson"`
	FlushIntervalMs            int               `json:"flushIntervalMs"`
	SampleRate                 int               `json:"sampleRate"`
	EffectiveSampleRate        int               `json:"effectiveSampleRate"`
	SampleRateConfigurable     bool              `json:"sampleRateConfigurable"`
	HeapSamplingIntervalBytes  int               `json:"heapSamplingIntervalBytes"`
	HeapStackDepth             int               `json:"heapStackDepth"`
	WallSamplingDurationMs     int               `json:"wallSamplingDurationMs"`
	WallSamplingIntervalMicros int               `json:"wallSamplingIntervalMicros"`
	WallCollectCPUTime         bool              `json:"wallCollectCpuTime"`
	Tags                       map[string]string `json:"tags,omitempty"`
	ApplicationName            string            `json:"applicationName,omitempty"`
}

// DetailedStatus represents the detailed profiling status.
type DetailedStatus struct {
	Overall bool        `json:"overall"`
	CPU     ProfileType `json:"cpu"`
	Heap    ProfileType `json:"heap"`
	Wall    ProfileType `json:"wall"`
}

// ProfileType represents the status of a single profile type.
type ProfileType struct {
	Enabled bool `json:"enabled"`
	Running bool `json:"running"`
}

// Profiler manages continuous profiling.
type Profiler struct {
	mu             sync.Mutex
	config         Config
	cpuRunning     atomic.Bool
	heapRunning    atomic.Bool
	wallRunning    atomic.Bool
	overallRunning atomic.Bool
	enabled        atomic.Bool

	// Internal state
	cpuBuffer  *profileBuffer
	stopWallCh chan struct{}
	wallDone   chan struct{}

	// Pyroscope profiler instance (nil when using pprof fallback).
	pyroscope *pyroscope.Profiler
}

// profileBuffer holds pprof output in memory.
type profileBuffer struct {
	buf []byte
}

func (pb *profileBuffer) Write(p []byte) (int, error) {
	pb.buf = append(pb.buf, p...)
	return len(p), nil
}

// DefaultConfig returns the default profiler configuration from environment variables.
func DefaultConfig() Config {
	return Config{
		Enabled:                    envBool("PROFILING_ENABLED", false),
		Server:                     envString("PROFILING_DISTRIBUTOR_ADDRESS", "http://utility-pyroscope-distributor.utility.svc.cluster.local:4040"),
		CPUEnabled:                 envBool("PROFILING_CPU_ENABLED", true),
		HeapEnabled:                envBool("PROFILING_HEAP_ENABLED", true),
		WallEnabled:                envBool("PROFILING_CPU_WALL_ENABLED", true),
		TagsJSON:                   envString("PROFILING_TAGS_JSON", "{}"),
		FlushIntervalMs:            envInt("PROFILING_FLUSH_INTERVAL_MS", 10000),
		SampleRate:                 envInt("PROFILING_SAMPLE_RATE", 10),
		EffectiveSampleRate:        pyroscopeEffectiveSampleRate,
		SampleRateConfigurable:     false,
		HeapSamplingIntervalBytes:  envInt("PROFILING_HEAP_SAMPLING_INTERVAL_BYTES", 524288),
		HeapStackDepth:             envInt("PROFILING_HEAP_STACK_DEPTH", 64),
		WallSamplingDurationMs:     envInt("PROFILING_WALL_SAMPLING_DURATION_MS", 60000),
		WallSamplingIntervalMicros: envInt("PROFILING_WALL_SAMPLING_INTERVAL_MICROS", 10000),
		WallCollectCPUTime:         envBool("PROFILING_WALL_COLLECT_CPU_TIME", false),
	}
}

// New creates a new Profiler with the given configuration.
func New(cfg Config) *Profiler {
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 10
	}
	// pyroscope-go exposes a deprecated SampleRate field but fixes collection
	// at 100 Hz internally. Report the requested legacy value and the actual Go
	// value separately rather than pretending the request changes collection.
	cfg.EffectiveSampleRate = pyroscopeEffectiveSampleRate
	cfg.SampleRateConfigurable = false
	profiler := &Profiler{
		config: cfg,
	}
	profiler.enabled.Store(cfg.Enabled)
	return profiler
}

// NewFromEnv creates a new Profiler using environment variable configuration.
func NewFromEnv() *Profiler {
	return New(DefaultConfig())
}

// buildAppName constructs the application name from environment variables.
func (p *Profiler) buildAppName() string {
	podName := envString("K8S_POD_NAME", "unknown-pod")
	projectName := envString("PROJECT_NAME", "DefaultProject")
	deploymentName := envString("DEPLOYMENT_NAME", "")
	deploymentType := strings.ToLower(strings.TrimSpace(envString("DEPLOYMENT_TYPE", "server")))
	if deploymentType == "" {
		deploymentType = "server"
	}

	// Derive deployment name from pod name if not provided.
	if deploymentName == "" {
		parts := strings.Split(podName, "-")
		if len(parts) > 2 {
			deploymentName = strings.Join(parts[:len(parts)-2], "-")
		} else {
			deploymentName = "default-server"
		}
	}

	deploymentTypeCapitalized := strings.ToUpper(deploymentType[:1]) + deploymentType[1:]
	return fmt.Sprintf("%s-%s-%s", projectName, deploymentName, deploymentTypeCapitalized)
}

// buildTags constructs the profiling tags from environment and config.
func (p *Profiler) buildTags() map[string]string {
	podName := envString("K8S_POD_NAME", "unknown-pod")
	projectName := envString("PROJECT_NAME", "DefaultProject")

	tags := map[string]string{
		"project_name": projectName,
		"pod_name":     podName,
	}

	// Add platform version if available.
	if pv := envString("PLATFORM_VERSION", ""); pv != "" {
		tags["fynd_platform_version"] = pv
	}

	// Parse extra tags from JSON.
	if p.config.TagsJSON != "" && p.config.TagsJSON != "{}" {
		var extraTags map[string]string
		if err := json.Unmarshal([]byte(p.config.TagsJSON), &extraTags); err == nil {
			for k, v := range extraTags {
				tags[k] = v
			}
		}
	}

	return tags
}

// pyroscopeProfileTypes builds the list of Pyroscope profile types based on config.
func (p *Profiler) pyroscopeProfileTypes() []pyroscope.ProfileType {
	var types []pyroscope.ProfileType
	if p.config.CPUEnabled {
		types = append(types, pyroscope.ProfileCPU)
	}
	if p.config.HeapEnabled {
		types = append(types, pyroscope.ProfileAllocObjects, pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects, pyroscope.ProfileInuseSpace)
	}
	if p.config.WallEnabled {
		types = append(types, pyroscope.ProfileGoroutines)
	}
	return types
}

// Start initializes and starts all enabled profiling types.
// When PROFILING_ENABLED is true and a server address is configured, Pyroscope
// push mode is used. Otherwise, local pprof profiling serves as a fallback.
func (p *Profiler) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.config.Enabled {
		p.enabled.Store(false)
		return
	}
	p.enabled.Store(true)

	p.config.ApplicationName = p.buildAppName()
	p.config.Tags = p.buildTags()

	// Configure heap sampling rate.
	runtime.MemProfileRate = p.config.HeapSamplingIntervalBytes

	// Attempt Pyroscope push mode.
	if p.config.Server != "" {
		profiler, err := pyroscope.Start(pyroscope.Config{
			ApplicationName: p.config.ApplicationName,
			ServerAddress:   p.config.Server,
			Tags:            p.config.Tags,
			ProfileTypes:    p.pyroscopeProfileTypes(),
			UploadRate:      time.Duration(p.config.FlushIntervalMs) * time.Millisecond,
		})
		if err == nil {
			p.pyroscope = profiler
			// Mark all configured types as running.
			if p.config.CPUEnabled {
				p.cpuRunning.Store(true)
			}
			if p.config.HeapEnabled {
				p.heapRunning.Store(true)
			}
			if p.config.WallEnabled {
				p.wallRunning.Store(true)
			}
			if p.cpuRunning.Load() || p.heapRunning.Load() || p.wallRunning.Load() {
				p.overallRunning.Store(true)
			}
			return
		}
		// Pyroscope failed to start; fall back to local pprof.
	}

	// Fallback: local pprof-based profiling.
	if p.config.CPUEnabled {
		p.startCPULocked()
	}
	if p.config.WallEnabled {
		p.startWallLocked()
	}
	if p.config.HeapEnabled {
		p.heapRunning.Store(true)
	}

	if p.cpuRunning.Load() || p.heapRunning.Load() || p.wallRunning.Load() {
		p.overallRunning.Store(true)
	}
}

// Stop stops all profiling types.
func (p *Profiler) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.overallRunning.Load() {
		return
	}

	// Stop Pyroscope if running.
	if p.pyroscope != nil {
		_ = p.pyroscope.Stop()
		p.pyroscope = nil
		p.cpuRunning.Store(false)
		p.heapRunning.Store(false)
		p.wallRunning.Store(false)
		p.overallRunning.Store(false)
		return
	}

	// Fallback pprof stop.
	p.stopCPULocked()
	p.stopWallLocked()
	p.heapRunning.Store(false)
	p.overallRunning.Store(false)
}

// StartCPUProfiling starts CPU profiling.
func (p *Profiler) StartCPUProfiling() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.startCPULocked()
}

func (p *Profiler) startCPULocked() {
	if p.cpuRunning.Load() {
		return
	}
	p.cpuBuffer = &profileBuffer{}
	if err := pprof.StartCPUProfile(p.cpuBuffer); err == nil {
		p.cpuRunning.Store(true)
		p.overallRunning.Store(true)
	}
}

// StopCPUProfiling stops CPU profiling.
func (p *Profiler) StopCPUProfiling() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopCPULocked()
}

func (p *Profiler) stopCPULocked() {
	if !p.cpuRunning.Load() {
		return
	}
	pprof.StopCPUProfile()
	p.cpuRunning.Store(false)
	p.cpuBuffer = nil
	p.updateOverallStatus()
}

// StartHeapProfiling starts heap profiling.
func (p *Profiler) StartHeapProfiling() {
	if p.heapRunning.Load() {
		return
	}
	runtime.MemProfileRate = p.config.HeapSamplingIntervalBytes
	p.heapRunning.Store(true)
	p.overallRunning.Store(true)
}

// StopHeapProfiling stops heap profiling.
func (p *Profiler) StopHeapProfiling() {
	if !p.heapRunning.Load() {
		return
	}
	runtime.MemProfileRate = 0
	p.heapRunning.Store(false)
	p.updateOverallStatus()
}

// StartWallProfiling starts wall-clock profiling.
func (p *Profiler) StartWallProfiling() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.startWallLocked()
}

func (p *Profiler) startWallLocked() {
	if p.wallRunning.Load() {
		return
	}
	p.stopWallCh = make(chan struct{})
	p.wallDone = make(chan struct{})
	p.wallRunning.Store(true)
	p.overallRunning.Store(true)

	// Start goroutine sampling in background.
	go p.wallSampler()
}

// wallSampler periodically captures goroutine profiles (wall-clock proxy).
func (p *Profiler) wallSampler() {
	defer close(p.wallDone)

	interval := time.Duration(p.config.WallSamplingIntervalMicros) * time.Microsecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopWallCh:
			return
		case <-ticker.C:
			// Capture goroutine profile (wall-clock proxy).
			// In Pyroscope mode, this data is pushed by the library.
			_ = pprof.Lookup("goroutine")
		}
	}
}

// StopWallProfiling stops wall-clock profiling.
func (p *Profiler) StopWallProfiling() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopWallLocked()
}

func (p *Profiler) stopWallLocked() {
	if !p.wallRunning.Load() {
		return
	}
	if p.stopWallCh != nil {
		close(p.stopWallCh)
		<-p.wallDone
		p.stopWallCh = nil
		p.wallDone = nil
	}
	p.wallRunning.Store(false)
	p.updateOverallStatus()
}

func (p *Profiler) updateOverallStatus() {
	if !p.cpuRunning.Load() && !p.heapRunning.Load() && !p.wallRunning.Load() {
		p.overallRunning.Store(false)
	}
}

// IsRunning returns true if any profiling is active.
func (p *Profiler) IsRunning() bool {
	return p.overallRunning.Load()
}

// IsProfilingDisabled returns true if profiling is globally disabled.
func (p *Profiler) IsProfilingDisabled() bool {
	return !p.enabled.Load()
}

// IsCPUProfilingRunning returns true if CPU profiling is active.
func (p *Profiler) IsCPUProfilingRunning() bool {
	return p.cpuRunning.Load()
}

// IsHeapProfilingRunning returns true if heap profiling is active.
func (p *Profiler) IsHeapProfilingRunning() bool {
	return p.heapRunning.Load()
}

// IsWallProfilingRunning returns true if wall profiling is active.
func (p *Profiler) IsWallProfilingRunning() bool {
	return p.wallRunning.Load()
}

// GetDetailedStatus returns the detailed status of all profile types.
func (p *Profiler) GetDetailedStatus() DetailedStatus {
	return DetailedStatus{
		Overall: p.overallRunning.Load(),
		CPU: ProfileType{
			Enabled: p.config.CPUEnabled,
			Running: p.cpuRunning.Load(),
		},
		Heap: ProfileType{
			Enabled: p.config.HeapEnabled,
			Running: p.heapRunning.Load(),
		},
		Wall: ProfileType{
			Enabled: p.config.WallEnabled,
			Running: p.wallRunning.Load(),
		},
	}
}

// Status returns the profiler status as a generic map, suitable for JSON
// serialization in HTTP handlers.
func (p *Profiler) Status() map[string]interface{} {
	return map[string]interface{}{
		"enabled":         p.enabled.Load(),
		"running":         p.overallRunning.Load(),
		"applicationName": p.config.ApplicationName,
		"sampleRate": map[string]interface{}{
			"requested":    p.config.SampleRate,
			"effective":    p.config.EffectiveSampleRate,
			"configurable": p.config.SampleRateConfigurable,
		},
		"cpu": map[string]interface{}{
			"enabled": p.config.CPUEnabled,
			"running": p.cpuRunning.Load(),
		},
		"heap": map[string]interface{}{
			"enabled": p.config.HeapEnabled,
			"running": p.heapRunning.Load(),
		},
		"wall": map[string]interface{}{
			"enabled": p.config.WallEnabled,
			"running": p.wallRunning.Load(),
		},
	}
}

// GetConfig returns the current profiler configuration.
func (p *Profiler) GetConfig() Config {
	return p.config
}

// GetApplicationName returns the derived application name.
func (p *Profiler) GetApplicationName() string {
	return p.config.ApplicationName
}

// GetTags returns the profiler tags.
func (p *Profiler) GetTags() map[string]string {
	return p.config.Tags
}

// CaptureHeapProfile writes the current heap profile to a buffer and returns it.
func (p *Profiler) CaptureHeapProfile() ([]byte, error) {
	buf := &profileBuffer{}
	prof := pprof.Lookup("heap")
	if prof == nil {
		return nil, fmt.Errorf("heap profile not available")
	}
	if err := prof.WriteTo(buf, 0); err != nil {
		return nil, err
	}
	return buf.buf, nil
}

// CaptureGoroutineProfile writes the current goroutine profile to a buffer.
func (p *Profiler) CaptureGoroutineProfile() ([]byte, error) {
	buf := &profileBuffer{}
	prof := pprof.Lookup("goroutine")
	if prof == nil {
		return nil, fmt.Errorf("goroutine profile not available")
	}
	if err := prof.WriteTo(buf, 0); err != nil {
		return nil, err
	}
	return buf.buf, nil
}

// ---------------------------------------------------------------------------
// HTTP Routes -
// ---------------------------------------------------------------------------

// Routes returns an http.Handler that serves all profiling control routes.
// The routes match the profiling API:
//
//	GET /_profiling/start - start all profiling
//	GET /_profiling/stop - stop all profiling
//	GET /_profiling/start_cpu - start CPU profiling
//	GET /_profiling/stop_cpu - stop CPU profiling
//	GET /_profiling/start_heap - start heap profiling
//	GET /_profiling/stop_heap - stop heap profiling
//	GET /_profiling/start_wall - start wall profiling
//	GET /_profiling/stop_wall - stop wall profiling
//	GET /_profiling/status - get profiling status
//	GET /_profiling/config - get profiling config
func (p *Profiler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/_profiling/start", func(w http.ResponseWriter, r *http.Request) {
		p.Start()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "profiling started", "status": p.Status()})
	})

	mux.HandleFunc("/_profiling/stop", func(w http.ResponseWriter, r *http.Request) {
		p.Stop()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "profiling stopped", "status": p.Status()})
	})

	mux.HandleFunc("/_profiling/start_cpu", func(w http.ResponseWriter, r *http.Request) {
		p.StartCPUProfiling()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "CPU profiling started", "running": p.IsCPUProfilingRunning()})
	})

	mux.HandleFunc("/_profiling/stop_cpu", func(w http.ResponseWriter, r *http.Request) {
		p.StopCPUProfiling()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "CPU profiling stopped", "running": p.IsCPUProfilingRunning()})
	})

	mux.HandleFunc("/_profiling/start_heap", func(w http.ResponseWriter, r *http.Request) {
		p.StartHeapProfiling()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "heap profiling started", "running": p.IsHeapProfilingRunning()})
	})

	mux.HandleFunc("/_profiling/stop_heap", func(w http.ResponseWriter, r *http.Request) {
		p.StopHeapProfiling()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "heap profiling stopped", "running": p.IsHeapProfilingRunning()})
	})

	mux.HandleFunc("/_profiling/start_wall", func(w http.ResponseWriter, r *http.Request) {
		p.StartWallProfiling()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "wall profiling started", "running": p.IsWallProfilingRunning()})
	})

	mux.HandleFunc("/_profiling/stop_wall", func(w http.ResponseWriter, r *http.Request) {
		p.StopWallProfiling()
		writeJSON(w, http.StatusOK, map[string]interface{}{"message": "wall profiling stopped", "running": p.IsWallProfilingRunning()})
	})

	mux.HandleFunc("/_profiling/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.Status())
	})

	mux.HandleFunc("/_profiling/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.GetConfig())
	})

	return mux
}

// writeJSON encodes a value as JSON and writes it to the response.
func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func envString(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envBool(key string, defaultVal bool) bool {
	v := strings.ToLower(os.Getenv(key))
	if v == "" {
		return defaultVal
	}
	return v == "true" || v == "1"
}

func envInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	if i, err := strconv.Atoi(v); err == nil {
		return i
	}
	return defaultVal
}

// ---------------------------------------------------------------------------
// Package-level singleton for simple usage
// ---------------------------------------------------------------------------

type defaultProfilerOwner struct {
	profiler *Profiler
	previous *defaultProfilerOwner
	baseline *Profiler
	active   bool
}

var processDefault = struct {
	sync.RWMutex
	profiler *Profiler
	owner    *defaultProfilerOwner
}{profiler: NewFromEnv()}

// TagWrapper runs fn with scoped Pyroscope/pprof labels. Labels are attached to
// samples taken while fn executes and are also carried in the callback context,
// matching pyfit's per-function tag-wrapper capability without global mutation.
func TagWrapper(ctx context.Context, tags map[string]string, fn func(context.Context)) {
	if fn == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	labels := make([]string, 0, len(tags)*2)
	for _, key := range keys {
		labels = append(labels, key, tags[key])
	}
	pyroscope.TagWrapper(ctx, pyroscope.Labels(labels...), fn)
}

// Start starts the default profiler.
func Start() {
	Default().Start()
}

// Stop stops the default profiler.
func Stop() {
	Default().Stop()
}

// Default returns the default profiler instance.
func Default() *Profiler {
	processDefault.RLock()
	defer processDefault.RUnlock()
	return processDefault.profiler
}

// SetDefault installs profiler as the process default and returns an
// idempotent restore function. Out-of-order restores do not revive an inactive
// owner, matching the lifecycle guarantees of fit-go's tracing and metrics
// defaults.
func SetDefault(profiler *Profiler) func() {
	if profiler == nil {
		profiler = New(Config{})
	}
	processDefault.Lock()
	owner := &defaultProfilerOwner{
		profiler: profiler,
		previous: processDefault.owner,
		baseline: processDefault.profiler,
		active:   true,
	}
	processDefault.profiler = profiler
	processDefault.owner = owner
	processDefault.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			processDefault.Lock()
			owner.active = false
			if processDefault.owner == owner {
				fallback := owner.baseline
				previous := owner.previous
				for previous != nil && !previous.active {
					fallback = previous.baseline
					previous = previous.previous
				}
				processDefault.owner = previous
				if previous != nil {
					processDefault.profiler = previous.profiler
				} else {
					processDefault.profiler = fallback
				}
			}
			processDefault.Unlock()
		})
	}
}

// Routes returns the HTTP routes for the default profiler.
func Routes() http.Handler {
	return Default().Routes()
}

// Status returns the status of the default profiler.
func StatusMap() map[string]interface{} {
	return Default().Status()
}
