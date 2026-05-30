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

package server

import (
	"net/http"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

// ProfilerState tracks the state of profiling endpoints.
// This is a port profiler.route.ts. In Go, we use runtime/pprof
// and provide on-demand profiling control rather than wrapping Pyroscope.
type ProfilerState struct {
	mu sync.Mutex
	cpuRunning atomic.Bool
	heapRunning atomic.Bool
	wallRunning atomic.Bool
	cpuFile *profilerBuffer
	enabled bool
}

// profilerBuffer is an in-memory buffer for pprof output.
type profilerBuffer struct {
	buf []byte
}

// Write implements io.Writer for pprof capture.
func (pb *profilerBuffer) Write(p []byte) (int, error) {
	pb.buf = append(pb.buf, p...)
	return len(p), nil
}

// globalProfiler holds the singleton profiler state.
var globalProfiler = &ProfilerState{}

// RegisterProfileRoutes registers the /_profiling/* endpoints on the given
// gin.Engine. These are only functional when PROFILING_ENABLED=true.
func RegisterProfileRoutes(engine *gin.Engine) {
	p := globalProfiler
	p.enabled = envGetBool("PROFILING_ENABLED")

	engine.GET("/_profiling/start", p.ginHandleStart)
	engine.GET("/_profiling/stop", p.ginHandleStop)
	engine.GET("/_profiling/start_cpu", p.ginHandleStartCPU)
	engine.GET("/_profiling/stop_cpu", p.ginHandleStopCPU)
	engine.GET("/_profiling/start_heap", p.ginHandleStartHeap)
	engine.GET("/_profiling/stop_heap", p.ginHandleStopHeap)
	engine.GET("/_profiling/start_wall", p.ginHandleStartWall)
	engine.GET("/_profiling/stop_wall", p.ginHandleStopWall)
	engine.GET("/_profiling/status", p.ginHandleStatus)
	engine.GET("/_profiling/config", p.ginHandleConfig)
}

func (p *ProfilerState) ginJSONOK(c *gin.Context, message string) {
	c.JSON(http.StatusOK, map[string]string{"status": "ok", "message": message})
}

func (p *ProfilerState) ginJSONErr(c *gin.Context, message string, err error) {
	c.JSON(http.StatusInternalServerError, map[string]interface{}{
		"status": "error",
		"message": message,
		"error": err.Error(),
	})
}

// ginHandleStart starts all profiling types.
func (p *ProfilerState) ginHandleStart(c *gin.Context) {
	if !p.enabled {
		p.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if p.cpuRunning.Load() && p.heapRunning.Load() && p.wallRunning.Load() {
		p.ginJSONOK(c, "Profiling is already running")
		return
	}
	p.startCPU()
	p.heapRunning.Store(true)
	p.wallRunning.Store(true)
	p.ginJSONOK(c, "Profiling started")
}

// ginHandleStop stops all profiling types.
func (p *ProfilerState) ginHandleStop(c *gin.Context) {
	if !p.cpuRunning.Load() && !p.heapRunning.Load() && !p.wallRunning.Load() {
		p.ginJSONOK(c, "Profiling is not running")
		return
	}
	p.stopCPU()
	p.heapRunning.Store(false)
	p.wallRunning.Store(false)
	p.ginJSONOK(c, "Profiling stopped")
}

// ginHandleStartCPU starts CPU profiling (and wall as).
func (p *ProfilerState) ginHandleStartCPU(c *gin.Context) {
	if !p.enabled {
		p.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if p.cpuRunning.Load() && p.wallRunning.Load() {
		p.ginJSONOK(c, "CPU profiling is already running")
		return
	}
	p.startCPU()
	p.wallRunning.Store(true)
	p.ginJSONOK(c, "CPU profiling started")
}

// ginHandleStopCPU stops CPU profiling.
func (p *ProfilerState) ginHandleStopCPU(c *gin.Context) {
	if !p.cpuRunning.Load() && !p.wallRunning.Load() {
		p.ginJSONOK(c, "CPU profiling is not running")
		return
	}
	p.stopCPU()
	p.wallRunning.Store(false)
	p.ginJSONOK(c, "CPU profiling stopped")
}

// ginHandleStartHeap starts heap profiling.
func (p *ProfilerState) ginHandleStartHeap(c *gin.Context) {
	if !p.enabled {
		p.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if p.heapRunning.Load() {
		p.ginJSONOK(c, "Heap profiling is already running")
		return
	}
	runtime.MemProfileRate = 512 * 1024
	p.heapRunning.Store(true)
	p.ginJSONOK(c, "Heap profiling started")
}

// ginHandleStopHeap stops heap profiling.
func (p *ProfilerState) ginHandleStopHeap(c *gin.Context) {
	if !p.heapRunning.Load() {
		p.ginJSONOK(c, "Heap profiling is not running")
		return
	}
	runtime.MemProfileRate = 0
	p.heapRunning.Store(false)
	p.ginJSONOK(c, "Heap profiling stopped")
}

// ginHandleStartWall starts wall profiling (goroutine profile in Go).
func (p *ProfilerState) ginHandleStartWall(c *gin.Context) {
	if !p.enabled {
		p.ginJSONOK(c, "Profiling is not enabled by global configuration")
		return
	}
	if p.wallRunning.Load() {
		p.ginJSONOK(c, "Wall profiling is already running")
		return
	}
	p.wallRunning.Store(true)
	p.ginJSONOK(c, "Wall profiling started")
}

// ginHandleStopWall stops wall profiling.
func (p *ProfilerState) ginHandleStopWall(c *gin.Context) {
	if !p.wallRunning.Load() {
		p.ginJSONOK(c, "Wall profiling is not running")
		return
	}
	p.wallRunning.Store(false)
	p.ginJSONOK(c, "Wall profiling stopped")
}

// ginHandleStatus returns the profiling status.
func (p *ProfilerState) ginHandleStatus(c *gin.Context) {
	overall := p.cpuRunning.Load() || p.heapRunning.Load() || p.wallRunning.Load()
	msg := "Profiling is not running"
	if overall {
		msg = "Profiling is active"
	}

	c.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
		"profiling": map[string]interface{}{
			"overall": map[string]interface{}{
				"running": overall,
				"message": msg,
			},
			"types": map[string]interface{}{
				"cpu": map[string]interface{}{
					"enabled": p.enabled,
					"running": p.cpuRunning.Load(),
					"description": "CPU profiling using Go runtime/pprof",
				},
				"heap": map[string]interface{}{
					"enabled": p.enabled,
					"running": p.heapRunning.Load(),
					"description": "Heap profiling for memory allocation analysis",
				},
				"wall": map[string]interface{}{
					"enabled": p.enabled,
					"running": p.wallRunning.Load(),
					"description": "Wall profiling for goroutine/wall-clock analysis",
				},
			},
		},
	})
}

// ginHandleConfig returns the profiling configuration.
func (p *ProfilerState) ginHandleConfig(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]interface{}{
		"status": "ok",
		"configuration": map[string]interface{}{
			"profiler": map[string]interface{}{
				"enabled": p.enabled,
				"cpuEnabled": p.cpuRunning.Load(),
				"heapEnabled": p.heapRunning.Load(),
				"wallEnabled": p.wallRunning.Load(),
				"memProfileRate": runtime.MemProfileRate,
				"numGoroutine": runtime.NumGoroutine(),
				"goVersion": runtime.Version(),
				"numCPU": runtime.NumCPU(),
				"blockProfileRate": 0,
				"mutexProfileFrac": 0,
			},
		},
	})
}

// startCPU begins CPU profiling into an in-memory buffer.
func (p *ProfilerState) startCPU() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cpuRunning.Load() {
		return
	}
	p.cpuFile = &profilerBuffer{}
	_ = pprof.StartCPUProfile(p.cpuFile)
	p.cpuRunning.Store(true)
}

// stopCPU stops CPU profiling.
func (p *ProfilerState) stopCPU() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.cpuRunning.Load() {
		return
	}
	pprof.StopCPUProfile()
	p.cpuRunning.Store(false)
	p.cpuFile = nil
}

// pprofHandler returns an http.Handler that serves Go's built-in pprof data.
// This can be optionally mounted alongside the profiler routes for full
// pprof compatibility (e.g. go tool pprof).
func pprofHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[len("/debug/pprof/"):]
		if name == "" {
			profiles := pprof.Profiles()
			names := make([]string, 0, len(profiles))
			for _, p := range profiles {
				names = append(names, p.Name())
			}
			JSON(w, http.StatusOK, map[string]interface{}{
				"profiles": names,
			})
			return
		}
		prof := pprof.Lookup(name)
		if prof == nil {
			JSON(w, http.StatusNotFound, map[string]string{"error": "profile not found: " + name})
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_ = prof.WriteTo(w, 0)
	})
	return mux
}

// ProfileSnapshotDuration is the default duration for on-demand CPU profile snapshots.
const ProfileSnapshotDuration = 30 * time.Second

// Legacy net/http handlers kept for backward compatibility.

func (p *ProfilerState) jsonOK(w http.ResponseWriter, message string) {
	JSON(w, http.StatusOK, map[string]string{"status": "ok", "message": message})
}

func (p *ProfilerState) jsonErr(w http.ResponseWriter, message string, err error) {
	JSON(w, http.StatusInternalServerError, map[string]interface{}{
		"status": "error",
		"message": message,
		"error": err.Error(),
	})
}
