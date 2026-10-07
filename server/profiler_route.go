// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package server

import (
	"net/http"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/profiling"
)

// ProfilerState is retained for source compatibility with the original server
// package. Profiling lifecycle is now owned by profiling.Profiler; the pointer
// wrapper intentionally keeps the legacy type comparable.
// Deprecated: use profiling.Profiler and RegisterProfileRoutesWithProfiler.
type ProfilerState struct {
	legacy *legacyProfilerState
}

type legacyProfilerState struct {
	mu          sync.Mutex
	cpuRunning  atomic.Bool
	heapRunning atomic.Bool
	wallRunning atomic.Bool
	cpuFile     *profilerBuffer
	enabled     bool
}

type profilerBuffer struct{ buf []byte }

func (buffer *profilerBuffer) Write(value []byte) (int, error) {
	buffer.buf = append(buffer.buf, value...)
	return len(value), nil
}

var globalProfiler = &ProfilerState{legacy: &legacyProfilerState{}}

// RegisterProfileRoutes preserves the released local runtime/pprof control
// surface. It never starts the process Pyroscope exporter.
func RegisterProfileRoutes(engine *gin.Engine) {
	state := globalProfiler.legacy
	state.enabled = envGetBool("PROFILING_ENABLED")
	engine.GET("/_profiling/start", state.ginHandleStart)
	engine.GET("/_profiling/stop", state.ginHandleStop)
	engine.GET("/_profiling/start_cpu", state.ginHandleStartCPU)
	engine.GET("/_profiling/stop_cpu", state.ginHandleStopCPU)
	engine.GET("/_profiling/start_heap", state.ginHandleStartHeap)
	engine.GET("/_profiling/stop_heap", state.ginHandleStopHeap)
	engine.GET("/_profiling/start_wall", state.ginHandleStartWall)
	engine.GET("/_profiling/stop_wall", state.ginHandleStopWall)
	engine.GET("/_profiling/status", state.ginHandleStatus)
	engine.GET("/_profiling/config", state.ginHandleConfig)
}

func (state *legacyProfilerState) ginJSONOK(c *gin.Context, message string) {
	c.JSON(http.StatusOK, map[string]string{"status": "ok", "message": message})
}

func (state *legacyProfilerState) ginHandleStart(c *gin.Context) {
	if !state.enabled {
		state.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if state.cpuRunning.Load() && state.heapRunning.Load() && state.wallRunning.Load() {
		state.ginJSONOK(c, "Profiling is already running")
		return
	}
	state.startCPU()
	state.heapRunning.Store(true)
	state.wallRunning.Store(true)
	state.ginJSONOK(c, "Profiling started")
}

func (state *legacyProfilerState) ginHandleStop(c *gin.Context) {
	if !state.cpuRunning.Load() && !state.heapRunning.Load() && !state.wallRunning.Load() {
		state.ginJSONOK(c, "Profiling is not running")
		return
	}
	state.stopCPU()
	state.heapRunning.Store(false)
	state.wallRunning.Store(false)
	state.ginJSONOK(c, "Profiling stopped")
}

func (state *legacyProfilerState) ginHandleStartCPU(c *gin.Context) {
	if !state.enabled {
		state.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if state.cpuRunning.Load() && state.wallRunning.Load() {
		state.ginJSONOK(c, "CPU profiling is already running")
		return
	}
	state.startCPU()
	state.wallRunning.Store(true)
	state.ginJSONOK(c, "CPU profiling started")
}

func (state *legacyProfilerState) ginHandleStopCPU(c *gin.Context) {
	if !state.cpuRunning.Load() && !state.wallRunning.Load() {
		state.ginJSONOK(c, "CPU profiling is not running")
		return
	}
	state.stopCPU()
	state.wallRunning.Store(false)
	state.ginJSONOK(c, "CPU profiling stopped")
}

func (state *legacyProfilerState) ginHandleStartHeap(c *gin.Context) {
	if !state.enabled {
		state.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if state.heapRunning.Load() {
		state.ginJSONOK(c, "Heap profiling is already running")
		return
	}
	runtime.MemProfileRate = 512 * 1024
	state.heapRunning.Store(true)
	state.ginJSONOK(c, "Heap profiling started")
}

func (state *legacyProfilerState) ginHandleStopHeap(c *gin.Context) {
	if !state.heapRunning.Load() {
		state.ginJSONOK(c, "Heap profiling is not running")
		return
	}
	runtime.MemProfileRate = 0
	state.heapRunning.Store(false)
	state.ginJSONOK(c, "Heap profiling stopped")
}

func (state *legacyProfilerState) ginHandleStartWall(c *gin.Context) {
	if !state.enabled {
		state.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if state.wallRunning.Load() {
		state.ginJSONOK(c, "Wall profiling is already running")
		return
	}
	state.wallRunning.Store(true)
	state.ginJSONOK(c, "Wall profiling started")
}

func (state *legacyProfilerState) ginHandleStopWall(c *gin.Context) {
	if !state.wallRunning.Load() {
		state.ginJSONOK(c, "Wall profiling is not running")
		return
	}
	state.wallRunning.Store(false)
	state.ginJSONOK(c, "Wall profiling stopped")
}

func (state *legacyProfilerState) ginHandleStatus(c *gin.Context) {
	overall := state.cpuRunning.Load() || state.heapRunning.Load() || state.wallRunning.Load()
	message := "Profiling is not running"
	if overall {
		message = "Profiling is active"
	}
	c.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
		"profiling": map[string]interface{}{
			"overall": map[string]interface{}{"running": overall, "message": message},
			"types": map[string]interface{}{
				"cpu":  map[string]interface{}{"enabled": state.enabled, "running": state.cpuRunning.Load(), "description": "CPU profiling using Go runtime/pprof"},
				"heap": map[string]interface{}{"enabled": state.enabled, "running": state.heapRunning.Load(), "description": "Heap profiling for memory allocation analysis"},
				"wall": map[string]interface{}{"enabled": state.enabled, "running": state.wallRunning.Load(), "description": "Wall profiling for goroutine/wall-clock analysis"},
			},
		},
	})
}

func (state *legacyProfilerState) ginHandleConfig(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
		"configuration": map[string]interface{}{"profiler": map[string]interface{}{
			"enabled": state.enabled, "cpuEnabled": state.cpuRunning.Load(),
			"heapEnabled": state.heapRunning.Load(), "wallEnabled": state.wallRunning.Load(),
			"memProfileRate": runtime.MemProfileRate, "numGoroutine": runtime.NumGoroutine(),
			"goVersion": runtime.Version(), "numCPU": runtime.NumCPU(),
			"blockProfileRate": 0, "mutexProfileFrac": 0,
		}},
	})
}

func (state *legacyProfilerState) startCPU() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.cpuRunning.Load() {
		return
	}
	state.cpuFile = &profilerBuffer{}
	if err := pprof.StartCPUProfile(state.cpuFile); err == nil {
		state.cpuRunning.Store(true)
	}
}

func (state *legacyProfilerState) stopCPU() {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.cpuRunning.Load() {
		return
	}
	pprof.StopCPUProfile()
	state.cpuRunning.Store(false)
	state.cpuFile = nil
}

// RegisterProfileRoutesWithProfiler registers profiling routes against one
// explicit profiler. This keeps route state, Pyroscope state, and framework
// shutdown ownership on the same instance.
func RegisterProfileRoutesWithProfiler(engine *gin.Engine, profiler *profiling.Profiler) {
	if profiler == nil {
		profiler = profiling.New(profiling.Config{})
	}

	engine.GET("/_profiling/start", func(c *gin.Context) {
		if !profiler.GetConfig().Enabled {
			profilingOK(c, "Profiling is not enabled by global configuration")
			return
		}
		if profiler.IsCPUProfilingRunning() && profiler.IsHeapProfilingRunning() && profiler.IsWallProfilingRunning() {
			profilingOK(c, "Profiling is already running")
			return
		}
		profiler.Start()
		profilingOK(c, "Profiling started")
	})

	engine.GET("/_profiling/stop", func(c *gin.Context) {
		if !profiler.IsRunning() {
			profilingOK(c, "Profiling is not running")
			return
		}
		profiler.Stop()
		profilingOK(c, "Profiling stopped")
	})

	engine.GET("/_profiling/start_cpu", func(c *gin.Context) {
		if !profiler.GetConfig().Enabled {
			profilingOK(c, "Profiling is not enabled by global configuration")
			return
		}
		if profiler.IsCPUProfilingRunning() && profiler.IsWallProfilingRunning() {
			profilingOK(c, "CPU profiling is already running")
			return
		}
		if !profiler.IsCPUProfilingRunning() {
			profiler.StartCPUProfiling()
		}
		if !profiler.IsWallProfilingRunning() {
			profiler.StartWallProfiling()
		}
		profilingOK(c, "CPU profiling started")
	})

	engine.GET("/_profiling/stop_cpu", func(c *gin.Context) {
		if !profiler.IsCPUProfilingRunning() && !profiler.IsWallProfilingRunning() {
			profilingOK(c, "CPU profiling is not running")
			return
		}
		if profiler.IsCPUProfilingRunning() {
			profiler.StopCPUProfiling()
		}
		if profiler.IsWallProfilingRunning() {
			profiler.StopWallProfiling()
		}
		profilingOK(c, "CPU profiling stopped")
	})

	engine.GET("/_profiling/start_heap", func(c *gin.Context) {
		if !profiler.GetConfig().Enabled {
			profilingOK(c, "Profiling is not enabled by global configuration")
			return
		}
		if profiler.IsHeapProfilingRunning() {
			profilingOK(c, "Heap profiling is already running")
			return
		}
		profiler.StartHeapProfiling()
		profilingOK(c, "Heap profiling started")
	})

	engine.GET("/_profiling/stop_heap", func(c *gin.Context) {
		if !profiler.IsHeapProfilingRunning() {
			profilingOK(c, "Heap profiling is not running")
			return
		}
		profiler.StopHeapProfiling()
		profilingOK(c, "Heap profiling stopped")
	})

	engine.GET("/_profiling/start_wall", func(c *gin.Context) {
		if !profiler.GetConfig().Enabled {
			profilingOK(c, "Profiling is not enabled by global configuration")
			return
		}
		if profiler.IsWallProfilingRunning() {
			profilingOK(c, "Wall profiling is already running")
			return
		}
		profiler.StartWallProfiling()
		profilingOK(c, "Wall profiling started")
	})

	engine.GET("/_profiling/stop_wall", func(c *gin.Context) {
		if !profiler.IsWallProfilingRunning() {
			profilingOK(c, "Wall profiling is not running")
			return
		}
		profiler.StopWallProfiling()
		profilingOK(c, "Wall profiling stopped")
	})

	engine.GET("/_profiling/status", func(c *gin.Context) {
		status := profiler.GetDetailedStatus()
		message := "Profiling is not running"
		if status.Overall {
			message = "Profiling is active"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"profiling": gin.H{
				"overall": gin.H{"running": status.Overall, "message": message},
				"types": gin.H{
					"cpu":  gin.H{"enabled": status.CPU.Enabled, "running": status.CPU.Running, "description": "CPU profiling using Go runtime/pprof"},
					"heap": gin.H{"enabled": status.Heap.Enabled, "running": status.Heap.Running, "description": "Heap profiling for memory allocation analysis"},
					"wall": gin.H{"enabled": status.Wall.Enabled, "running": status.Wall.Running, "description": "Wall profiling for goroutine/wall-clock analysis"},
				},
			},
		})
	})

	engine.GET("/_profiling/config", func(c *gin.Context) {
		config := profiler.GetAdvancedConfig()
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"configuration": gin.H{"profiler": gin.H{
				"enabled": config.Enabled, "server": config.Server,
				"cpuEnabled": config.CPUEnabled, "heapEnabled": config.HeapEnabled,
				"cpuWallEnabled": config.WallEnabled, "tagsJson": config.TagsJSON,
				"flushIntervalMs":            config.FlushIntervalMs,
				"heapSamplingIntervalBytes":  config.HeapSamplingIntervalBytes,
				"heapStackDepth":             config.HeapStackDepth,
				"wallSamplingDurationMs":     config.WallSamplingDurationMs,
				"wallSamplingIntervalMicros": config.WallSamplingIntervalMicros,
				"wallCollectCpuTime":         config.WallCollectCPUTime,
			}},
		})
	})
}

func profilingOK(c *gin.Context, message string) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": message})
}

// pprofHandler serves Go's built-in profiles for callers that explicitly mount
// it. It is separate from the Pyroscope control API but owns no profile state.
func pprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[len("/debug/pprof/"):]
		if name == "" {
			profiles := pprof.Profiles()
			names := make([]string, 0, len(profiles))
			for _, profile := range profiles {
				names = append(names, profile.Name())
			}
			JSON(w, http.StatusOK, map[string]interface{}{"profiles": names})
			return
		}
		profile := pprof.Lookup(name)
		if profile == nil {
			JSON(w, http.StatusNotFound, map[string]string{"error": "profile not found: " + name})
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_ = profile.WriteTo(w, 0)
	})
	return mux
}

// ProfileSnapshotDuration is the default duration for on-demand CPU snapshots.
const ProfileSnapshotDuration = 30 * time.Second
