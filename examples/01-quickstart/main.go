// Example 01: Quickstart
//
// The smallest possible fit application. fit.Init wires up configuration,
// the structured logger, and (when enabled via env vars) tracing and metrics,
// then returns a *fit.Fit handle whose Shutdown cleans everything up.
//
// Run:
//
//	go run ./examples/01-quickstart
//
// Useful env vars:
//
//	SERVICE_NAME=my-service
//	LOG_LEVEL=debug
//	NODE_ENV=development
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	fit "github.com/gofynd/fit-go"
)

func main() {
	ctx := context.Background()

	// Initialize the framework: loads config, builds the logger, and starts
	// tracing/metrics if their env flags are set.
	f, err := fit.Init(ctx)
	if err != nil {
		log.Fatalf("fit init failed: %v", err)
	}
	defer f.Shutdown(ctx)

	f.Logger.Info("service started",
		"service", f.Config.GetString("SERVICE_NAME", "demo"),
		"env", f.Config.GetString("NODE_ENV", "development"),
	)

	// Block until we receive an interrupt/terminate signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	f.Logger.Info("shutdown signal received, exiting")
}
