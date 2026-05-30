// Example 04: HTTP server
//
// The server package wraps gin and mounts one or more handlers by ServerType.
// It builds the middleware chain (security headers, request logging, payload
// limits, body parsing) and automatically registers health endpoints
// (/_healthz, /_readyz). Profiling routes are added when PROFILING_ENABLED=true.
//
// Run:
//
//	PORT=8080 go run ./examples/04-http-server
//	curl localhost:8080/v1/ping
//	curl localhost:8080/_healthz
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/server"
)

func main() {
	// Build the application router. Any http.Handler works; here we use gin.
	engine := gin.New()
	engine.GET("/v1/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})
	engine.GET("/v1/orders/:id", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"id": c.Param("id"), "status": "shipped"})
	})

	srv := server.New(server.Config{
		Port:           "8080",
		Logger:         slog.Default(),
		MaxPayloadSize: "2mb",
	})

	// Mount the router. A single router is served at root; you can also mount
	// several handlers keyed by ServerType (platform, application, public, ...).
	// The last two args are request- and response-phase middleware slices.
	if err := srv.Init(
		map[server.ServerType]http.Handler{
			server.ServerTypeDefault: engine,
		},
		nil,
		nil,
	); err != nil {
		slog.Error("server init failed", "error", err)
		os.Exit(1)
	}

	// Start blocks, so run it in a goroutine and wait for a signal.
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			slog.Error("server stopped", "error", err)
			os.Exit(1)
		}
	}()
	slog.Info("listening on :8080")

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}
}
