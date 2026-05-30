// Example 03: Structured logging
//
// The logging package emits structured JSON in production and colorized,
// human-readable output in development. Loggers are immutable: WithFields,
// WithService, and WithContext return derived loggers without mutating the
// parent.
//
// Run:
//
//	go run ./examples/03-logging
//	NODE_ENV=development go run ./examples/03-logging   # colorized output
package main

import (
	"context"
	"log"

	"github.com/gofynd/fit-go/logging"
)

func main() {
	logger, err := logging.New(logging.Options{
		Level:    "debug",
		Timezone: "UTC",
		Env:      "production",
		Service:  "orders-api",
	})
	if err != nil {
		log.Fatalf("logger init failed: %v", err)
	}

	// Key-value pairs become structured fields.
	logger.Info("request processed", "user_id", "u123", "duration_ms", 42)
	logger.Warn("cache miss", "key", "session:abc")
	logger.Error("downstream failed", "service", "payments", "status", 503)

	// Derive a logger with permanent fields; the parent is unaffected.
	reqLogger := logger.WithFields(map[string]interface{}{
		"request_id": "req-789",
		"route":      "/v1/orders",
	})
	reqLogger.Debug("entering handler")
	reqLogger.Info("order created", "order_id", "o-456")

	// Attach trace/span IDs from a context so logs correlate with traces.
	ctx := logging.ContextWithTrace(context.Background(), "trace-abc", "span-def")
	logger.WithContext(ctx).Info("handling within a trace")

	// Adjust verbosity at runtime.
	logger.SetLevel("warn")
	logger.Debug("this debug line is now suppressed")
	logger.Warn("this warning still shows")
}
