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
	"strconv"
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
	RegisterHealthRoutesWithCheckers(engine, nil, nil)
}

// RegisterLegacyStaticHealthRoutes registers the legacy fit.js static health
// contract.  Some Node services expose these probes as an unconditional 200
// with exactly {"ok":"ok"}; they do not run dependency checks and clients
// observe the absence of fit-go's status field.  Keep this separate from the
// default health routes so existing Go services retain their current behavior.
func RegisterLegacyStaticHealthRoutes(engine *gin.Engine) {
	// Express routes are case-insensitive and non-strict and automatically use
	// GET for HEAD. Gin's exact-path GET registration provides none of those
	// behaviors, so install the compatibility boundary before registering the
	// canonical routes. Other methods deliberately continue to NoRoute: the
	// legacy Galvatron app's app.all("*") catch-all owns OPTIONS/POST.
	engine.Use(func(c *gin.Context) {
		path := strings.TrimSuffix(c.Request.URL.Path, "/")
		if !strings.EqualFold(path, "/_healthz") && !strings.EqualFold(path, "/_readyz") {
			c.Next()
			return
		}
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Next()
			return
		}
		legacyStaticHealthHandler()(c)
		c.Abort()
	})
	engine.GET("/_healthz", legacyStaticHealthHandler())
	engine.GET("/_readyz", legacyStaticHealthHandler())
	engine.HEAD("/_healthz", legacyStaticHealthHandler())
	engine.HEAD("/_readyz", legacyStaticHealthHandler())
}

// RegisterHealthRoutesWithCheckers registers independent liveness and
// readiness checkers. A nil health checker uses the package default; a nil
// readiness checker reuses the selected health checker for fit.js compatibility.
func RegisterHealthRoutesWithCheckers(engine *gin.Engine, health, readiness HealthChecker) {
	if health == nil {
		health = healthChecker
	}
	if readiness == nil {
		readiness = health
	}
	engine.GET("/_healthz", healthHandler(health))
	engine.GET("/_readyz", healthHandler(readiness))
}

// ginHealthHandler returns {"status":"healthy","ok":"ok"} when all checks pass,
// or {"status":"unhealthy","meta":{"error_messages":"..."}} with 400 on failure.
func ginHealthHandler(c *gin.Context) {
	healthHandler(healthChecker)(c)
}

func healthHandler(checker HealthChecker) gin.HandlerFunc {
	return func(c *gin.Context) {
		errorMsgs := checker.Check()
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
}

func legacyStaticHealthHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Do not use c.JSON here: this is a byte-level compatibility mode for
		// fit.js services whose health endpoint returned this exact object.
		body := []byte(`{"ok":"ok"}`)
		c.Header("X-Powered-By", "Express")
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.Header("Content-Length", strconv.Itoa(len(body)))
		c.Header("ETag", `W/"b-2F/2BWc0KYbtLqL5U2Kv5B6uQUQ"`)
		if c.Request.Method == http.MethodHead {
			c.Status(http.StatusOK)
			return
		}
		c.Data(http.StatusOK, "application/json; charset=utf-8", body)
	}
}
