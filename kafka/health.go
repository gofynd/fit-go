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

// Provides a periodic health check for Kafka consumers that writes to
// /tmp/_healthz on success, enabling Kubernetes liveness probes.
package kafka

import (
	"fmt"
	"os"
	"time"

	"github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/logging"
)

const (
	// fileWriteInterval is the interval between health check runs.
	// Matches the FILE_WRITE_INTERVAL = 30 * 1000.
	fileWriteInterval = 30 * time.Second

	// healthFilePath is the file written on successful health checks.
	// Kubernetes liveness probes check for its existence.
	healthFilePath = "/tmp/_healthz"
)

// StartHealthCheckWithChecker starts a health check loop using the provided
// health.Checker instance. This allows callers to register additional checks
// (e.g. database connectivity, consumer connectivity) before starting the loop.
// This is the Go implementation class.
//
// If the health checks fail, the health file is NOT written (and any existing
// file is removed), causing the Kubernetes liveness probe to fail and trigger
// a pod restart.
func StartHealthCheckWithChecker(checker *health.Checker, logger *logging.Logger) {
	if logger == nil {
		l, _ := logging.New(logging.Options{Level: "info"})
		logger = l
	}
	go runHealthLoop(checker, logger)
}

// runHealthLoop is the internal loop that runs health checks at the configured
// interval and manages the health file.
func runHealthLoop(checker *health.Checker, logger *logging.Logger) {
	ticker := time.NewTicker(fileWriteInterval)
	defer ticker.Stop()

	// Perform initial check.
	performHealthCheck(checker, logger)

	for range ticker.C {
		performHealthCheck(checker, logger)
	}
}

// performHealthCheck runs all registered checks. On success it touches the
// health file; on failure it logs the errors and removes the file.
func performHealthCheck(checker *health.Checker, logger *logging.Logger) {
	errs := checker.Check()
	if len(errs) > 0 {
		for _, msg := range errs {
			logger.Error(fmt.Sprintf("kafka healthz failed (file write skipped): %s", msg))
		}
		os.Remove(healthFilePath)
		return
	}

	f, err := os.OpenFile(healthFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Error("kafka healthz: failed to write health file", "path", healthFilePath, "error", err)
		return
	}
	f.Close()
}
