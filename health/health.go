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

// Package health provides health check orchestration for the fit.go framework.
package health

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// CheckFunc is a function that performs a health check.
// Returns an error message if unhealthy, or empty string if healthy.
type CheckFunc func() string

// Checker orchestrates health checks across all connections.
type Checker struct {
	mu     sync.RWMutex
	checks []CheckFunc
	// skipCounter is used for adaptive health checking to reduce load
	skipCounter int
}

// NewChecker creates a new health checker.
func NewChecker() *Checker {
	return &Checker{}
}

// AddCheck registers a custom health check function.
func (c *Checker) AddCheck(check CheckFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks = append(c.checks, check)
}

// Check runs all registered health checks and returns error messages.
// Returns empty slice if all healthy.
func (c *Checker) Check() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var errs []string
	for _, check := range c.checks {
		if msg := check(); msg != "" {
			errs = append(errs, msg)
		}
	}
	return errs
}

// IsHealthy returns true if all health checks pass.
func (c *Checker) IsHealthy() bool {
	return len(c.Check()) == 0
}

// StartPeriodicCheck starts a periodic health check that writes to /tmp/_healthz
// for Kubernetes liveness probes. Port startHealthCheck().
func (c *Checker) StartPeriodicCheck(intervalSeconds int) {
	if intervalSeconds <= 0 {
		intervalSeconds = 30
	}

	// Override from env
	if envVal := os.Getenv("HEALTH_CHECK_INTERVAL_SECONDS"); envVal != "" {
		fmt.Sscanf(envVal, "%d", &intervalSeconds)
	}

	go func() {
		ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
		defer ticker.Stop()

		// Run immediately
		c.writeHealthFile()

		for range ticker.C {
			c.writeHealthFile()
		}
	}()
}

// writeHealthFile writes to /tmp/_healthz if healthy.
func (c *Checker) writeHealthFile() {
	errs := c.Check()
	if len(errs) == 0 {
		os.WriteFile("/tmp/_healthz", []byte("ok"), 0644)
	} else {
		// Remove health file on failure
		os.Remove("/tmp/_healthz")
	}
}

// MongoCheck returns a health check function for MongoDB connections.
func MongoCheck(pingFunc func() error, name string) CheckFunc {
	return func() string {
		if shouldSkip("SKIP_HEALTH_CHECK_MONGO") {
			return ""
		}
		if err := pingFunc(); err != nil {
			return fmt.Sprintf("MongoDB(%s): %v", name, err)
		}
		return ""
	}
}

// RedisCheck returns a health check function for Redis connections.
func RedisCheck(pingFunc func() error, name string) CheckFunc {
	return func() string {
		if shouldSkip("SKIP_HEALTH_CHECK_REDIS") {
			return ""
		}
		if err := pingFunc(); err != nil {
			return fmt.Sprintf("Redis(%s): %v", name, err)
		}
		return ""
	}
}

// MySQLCheck returns a health check function for MySQL connections.
func MySQLCheck(pingFunc func() error, name string) CheckFunc {
	return func() string {
		if shouldSkip("SKIP_HEALTH_CHECK_MYSQL") {
			return ""
		}
		if err := pingFunc(); err != nil {
			return fmt.Sprintf("MySQL(%s): %v", name, err)
		}
		return ""
	}
}

// PostgresCheck returns a health check function for PostgreSQL connections.
func PostgresCheck(pingFunc func() error, name string) CheckFunc {
	return func() string {
		if shouldSkip("SKIP_HEALTH_CHECK_POSTGRES") {
			return ""
		}
		if err := pingFunc(); err != nil {
			return fmt.Sprintf("PostgreSQL(%s): %v", name, err)
		}
		return ""
	}
}

// GroupCacheCheck returns a health check function for GroupCache connections.
func GroupCacheCheck(pingFunc func() error, name string) CheckFunc {
	return func() string {
		if shouldSkip("SKIP_HEALTH_CHECK_GROUPCACHE") {
			return ""
		}
		if err := pingFunc(); err != nil {
			return fmt.Sprintf("GroupCache(%s): %v", name, err)
		}
		return ""
	}
}

func shouldSkip(envVar string) bool {
	return strings.EqualFold(os.Getenv(envVar), "true")
}
