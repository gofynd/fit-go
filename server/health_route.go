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

package server

import (
	"net/http"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

// HealthChecker is the interface that the server uses to determine service health.
// Implementations should return a slice of error messages; an empty slice means healthy.
type HealthChecker interface {
	Check() []string
}

// defaultHealthChecker is a simple in-process health checker that external
// packages (e.g. health.Checker) can register checks with.
type defaultHealthChecker struct {
	mu     sync.RWMutex
	checks []func() string
}

// globalHealthChecker is the package-level health checker.
var globalHealthChecker = &defaultHealthChecker{}

// RegisterHealthCheck registers a function that returns an error message if
// unhealthy, or "" if healthy.
func RegisterHealthCheck(fn func() string) {
	globalHealthChecker.mu.Lock()
	defer globalHealthChecker.mu.Unlock()
	globalHealthChecker.checks = append(globalHealthChecker.checks, fn)
}

func (hc *defaultHealthChecker) Check() []string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	var msgs []string
	for _, fn := range hc.checks {
		if msg := fn(); msg != "" {
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

// healthChecker is the active HealthChecker used by health routes.
// Defaults to the package-level globalHealthChecker; can be overridden via
// SetHealthChecker.
var healthChecker HealthChecker = globalHealthChecker

// SetHealthChecker replaces the global health checker used by /_healthz and
// /_readyz routes. Typically called during server initialization to wire up
// the fit.Health checker.
func SetHealthChecker(hc HealthChecker) {
	if hc != nil {
		healthChecker = hc
	}
}

// RegisterHealthRoutes registers the /_healthz and /_readyz endpoints on the
// given gin.Engine. These endpoints are
func RegisterHealthRoutes(engine *gin.Engine) {
	engine.GET("/_healthz", ginHealthHandler)
	engine.GET("/_readyz", ginHealthHandler)
}

// ginHealthHandler returns {"status":"healthy","ok":"ok"} when all checks pass,
// or {"status":"unhealthy","meta":{"error_messages":"..."}} with 400 on failure.
func ginHealthHandler(c *gin.Context) {
	errorMsgs := healthChecker.Check()
	if len(errorMsgs) > 0 {
		c.JSON(http.StatusBadRequest, map[string]interface{}{
			"status": "unhealthy",
			"meta": map[string]interface{}{
				"error_messages": strings.Join(errorMsgs, ", "),
			},
		})
		return
	}

	c.JSON(http.StatusOK, map[string]interface{}{
		"status": "healthy",
		"ok":     "ok",
	})
}
