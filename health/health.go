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

// Package health provides health check orchestration for the fit.go framework.
package health

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

	periodicStartMu    sync.Mutex
	periodicMu         sync.Mutex
	periodicGeneration uint64
	periodicStops      []chan struct{}
	periodicDones      []chan struct{}
	periodicStates     []*periodicCheckState
}

type periodicCheckState struct {
	stopped atomic.Bool
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
	checks := append([]CheckFunc(nil), c.checks...)
	c.mu.RUnlock()

	var errs []string
	for _, check := range checks {
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
	intervalSeconds = periodicIntervalSeconds(intervalSeconds, os.Getenv("HEALTH_CHECK_INTERVAL_SECONDS"))
	c.startAdditionalPeriodicCheck(time.Duration(intervalSeconds) * time.Second)
}

func periodicIntervalSeconds(configured int, environment string) int {
	if configured <= 0 {
		configured = 30
	}
	if environment != "" {
		parsed := configured
		if _, err := fmt.Sscanf(environment, "%d", &parsed); err == nil && parsed > 0 {
			configured = parsed
		}
	}
	return configured
}

// startPeriodicCheck owns one replaceable loop for managed framework tests and
// lifecycles. The public StartPeriodicCheck keeps official-main multi-call
// behavior by adding a loop on every call; StopPeriodicCheck can still clean up
// all loops during managed shutdown.
func (c *Checker) startPeriodicCheck(interval time.Duration) {
	if c == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	c.periodicMu.Lock()
	generation := c.periodicGeneration
	c.periodicMu.Unlock()
	c.startPeriodicCheckAtGeneration(interval, generation)
}

func (c *Checker) startPeriodicCheckAtGeneration(interval time.Duration, generation uint64) {
	// Serialize managed replacements, but never hold periodicMu while waiting
	// for user checks. Deadline-bound stops must be able to signal and wait too.
	c.periodicStartMu.Lock()
	defer c.periodicStartMu.Unlock()
	c.periodicMu.Lock()
	// Include replacements already queued behind another managed start when
	// deciding whether an intervening stop/reset superseded this request.
	if c.periodicGeneration != generation {
		c.periodicMu.Unlock()
		return
	}
	dones := c.stopPeriodicCheckLocked()
	c.periodicMu.Unlock()
	_ = waitForPeriodicChecks(context.Background(), dones)
	c.periodicMu.Lock()
	// A stop/reset that happened while the replacement waited takes priority.
	// It must not be followed by a late liveness loop from that old start.
	if c.periodicGeneration == generation {
		c.startPeriodicCheckLocked(interval)
	}
	c.periodicMu.Unlock()
}

func (c *Checker) startAdditionalPeriodicCheck(interval time.Duration) {
	if c == nil {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	c.periodicMu.Lock()
	c.startPeriodicCheckLocked(interval)
	c.periodicMu.Unlock()
}

func (c *Checker) startPeriodicCheckLocked(interval time.Duration) {
	stop := make(chan struct{})
	done := make(chan struct{})
	state := &periodicCheckState{}
	c.periodicStops = append(c.periodicStops, stop)
	c.periodicDones = append(c.periodicDones, done)
	c.periodicStates = append(c.periodicStates, state)

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run immediately. A stop that arrives while Check is blocked prevents
		// that stale result from recreating the liveness file during shutdown.
		if !c.writeHealthFileIfActive(state) {
			return
		}

		for {
			select {
			case <-ticker.C:
				if !c.writeHealthFileIfActive(state) {
					return
				}
			case <-stop:
				return
			}
		}
	}()
}

// StopPeriodicCheck stops periodic health-file work and waits for an in-flight
// check/write to finish. It is safe to call repeatedly and concurrently with
// StartPeriodicCheck.
func (c *Checker) StopPeriodicCheck() {
	_ = c.StopPeriodicCheckContext(context.Background())
}

// StopPeriodicCheckContext stops periodic health-file work and waits for
// in-flight checks only until ctx is done. The stop signal is always delivered,
// even when the caller's shutdown deadline expires.
func (c *Checker) StopPeriodicCheckContext(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.periodicMu.Lock()
	c.periodicGeneration++
	dones := c.stopPeriodicCheckLocked()
	c.periodicMu.Unlock()
	return waitForPeriodicChecks(ctx, dones)
}

// stopPeriodicCheckLocked only signals work. Keep unfinished done channels so
// concurrent callers also wait for checks signalled by an earlier stop.
func (c *Checker) stopPeriodicCheckLocked() []chan struct{} {
	for _, state := range c.periodicStates {
		// Signalling a stop must never wait for the writer's filesystem I/O.
		// The loop owns its writes; joining its done channel below remains the
		// barrier for an already admitted write on an unbounded graceful stop.
		state.stopped.Store(true)
	}
	for _, stop := range c.periodicStops {
		close(stop)
	}
	c.periodicStops = nil
	c.periodicStates = nil
	dones := make([]chan struct{}, 0, len(c.periodicDones))
	for _, done := range c.periodicDones {
		select {
		case <-done:
		default:
			dones = append(dones, done)
		}
	}
	c.periodicDones = dones
	return dones
}

func waitForPeriodicChecks(ctx context.Context, dones []chan struct{}) error {
	for _, done := range dones {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Reset stops background work, removes all registered checks, and removes the
// liveness file written by this checker. A reset checker can be reused, though
// fit.Init installs a fresh checker for each framework lifecycle.
func (c *Checker) Reset() {
	_ = c.ResetContext(context.Background())
}

// ResetContext resets the checker while bounding the wait for an in-flight
// periodic check and file cleanup by ctx. Registered checks are cleared even if
// the wait reaches its deadline. Already admitted filesystem I/O cannot be
// cancelled by Go and may finish later; cleanup is best-effort in that case.
func (c *Checker) ResetContext(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err := c.StopPeriodicCheckContext(ctx)
	c.mu.Lock()
	c.checks = nil
	c.skipCounter = 0
	c.mu.Unlock()
	cleanupErr := runHealthFileCleanup(ctx, func() { _ = os.Remove("/tmp/_healthz") })
	if err == nil {
		return cleanupErr
	}
	return err
}

// Preserve synchronous cleanup on the original unbounded Reset path. A managed
// caller with cancellation waits only for its own deadline, not for a stalled
// filesystem syscall. There is no polling goroutine or shared mutable hook.
func runHealthFileCleanup(ctx context.Context, cleanup func()) error {
	if ctx.Done() == nil {
		cleanup()
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		cleanup()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

func (c *Checker) writeHealthFileIfActive(state *periodicCheckState) bool {
	errs := c.Check()
	if state.stopped.Load() {
		return false
	}
	if len(errs) == 0 {
		_ = os.WriteFile("/tmp/_healthz", []byte("ok"), 0644)
	} else {
		_ = os.Remove("/tmp/_healthz")
	}
	return true
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
