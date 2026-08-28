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
	"runtime/pprof"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/profiling"
)

// RegisterProfileRoutes registers the legacy FIT.js profiling control surface
// against the process-default profiler.
func RegisterProfileRoutes(engine *gin.Engine) {
	RegisterProfileRoutesWithProfiler(engine, profiling.Default())
}

// RegisterProfileRoutesWithProfiler registers profiling routes against one
// explicit profiler. This keeps route state, Pyroscope state, and framework
// shutdown ownership on the same instance.
func RegisterProfileRoutesWithProfiler(engine *gin.Engine, profiler *profiling.Profiler) {
	if profiler == nil {
		profiler = profiling.New(profiling.Config{})
	}

	engine.GET("/_profiling/start", func(c *gin.Context) {
		if profiler.IsProfilingDisabled() {
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
		if profiler.IsProfilingDisabled() {
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
		if profiler.IsProfilingDisabled() {
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
		if profiler.IsProfilingDisabled() {
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
		config := profiler.GetConfig()
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
