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

// Package server provides the HTTP server module for the fit.go framework.
//
// manages server lifecycle, route mounting by ServerType (platform, application,
// partner, internal, webhook, administrator, public, portal, panel, dev, common),
// middleware chains, health checks, and profiling routes.
//
// Key environment variables (
//
// - SERVER_TYPE - comma-separated list of server types to load
// - UNIFY_SERVER - "true" to mount all types under /service/{type}/{name} (dev/test only)
// - SERVICE_NAME - required when UNIFY_SERVER is true
// - PORT - TCP port to listen on (required)
// - NODE_ENV - "production" disables UNIFY_SERVER
// - MAX_REQUEST_PAYLOAD_SIZE - body size limit, e.g. "2mb" (default "2mb")
// - PROFILING_ENABLED - "true" to register /_profiling/* routes
// - DISABLE_REQUEST_MIDDLEWARES - "true" to skip built-in request parsing middlewares
// - DISABLE_RESPONSE_MIDDLEWARES - "true" to skip the catch-all 404 handler
// - INCLUDE_HEADERS_IN_LOG - comma-separated header names to include in request logs
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Config holds the configuration for creating a new Server instance.
type Config struct {
	// Port is the TCP port to listen on. If empty, the PORT env var is used.
	Port string

	// Logger is the structured logger. If nil, slog.Default() is used.
	Logger *slog.Logger

	// ReadTimeout is the maximum duration for reading the entire request.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out writes of the response.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the next request
	// when keep-alives are enabled.
	IdleTimeout time.Duration

	// MaxPayloadSize limits request body size. Accepts human-readable strings
	// like "2mb", "500kb". Defaults to MAX_REQUEST_PAYLOAD_SIZE env var or "2mb".
	MaxPayloadSize string

	// IncludeHeadersInLog is a comma-separated list of header names to include
	// in request/response logs. Defaults to INCLUDE_HEADERS_IN_LOG env var.
	IncludeHeadersInLog string

	// MetricsRecorder is an optional callback for recording Prometheus metrics.
	MetricsRecorder func(method, route, status string, durationMs float64)

	// SecureHeaders controls whether to set basic security headers
	// (X-Content-Type-Options, X-Frame-Options, etc.). Defaults to true.
	SecureHeaders *bool

	// HealthChecker is the health checker used by /_healthz and /_readyz routes.
	// If nil, the package-level globalHealthChecker is used.
	HealthChecker HealthChecker

	// CORS, when non-nil, installs the dynamic CORS middleware (see DynamicCORS/CORSOptions)
	// engine-level so it also answers preflights on the no-route path. Nil disables
	// CORS entirely (a service whose config.enable_cors is off simply leaves this nil).
	CORS *CORSOptions
}

// Server is the fit.go HTTP server. It wraps net/http.Server and uses
// gin.Engine for routing and middleware chains, plus lifecycle management.
type Server struct {
	mu              sync.RWMutex
	cfg             Config
	engine          *gin.Engine
	server          *http.Server
	logger          *slog.Logger
	fallbackHandler http.Handler // used for single-type or internal-type root mounting

	// App is the top-level http.Handler with all middleware applied.
	// Points to the gin engine.
	App http.Handler

	// Router is the gin.Engine, available for direct route registration.
	//
	Router *gin.Engine
}

// New creates a new Server with the given configuration.
func New(cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 120 * time.Second
	}
	if cfg.HealthChecker != nil {
		SetHealthChecker(cfg.HealthChecker)
	}

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	s := &Server{
		cfg:    cfg,
		engine: engine,
		Router: engine,
		logger: cfg.Logger,
	}
	return s
}

// Init initialises the server by mounting routes, registering health/profiling
// endpoints, and building the middleware chain.
//
// routers maps ServerType to the http.Handler for that type. The SERVER_TYPE
// env var (comma-separated) determines which are actually mounted.
//
// requestMiddlewares are applied before route handlers.
// responseMiddlewares are applied after route handlers (e.g. error handler).
func (s *Server) Init(
	routers map[ServerType]http.Handler,
	requestMiddlewares []Middleware,
	responseMiddlewares []Middleware,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(routers) == 0 {
		return fmt.Errorf("server: no routes provided")
	}

	// Create a fresh engine for this init
	gin.SetMode(gin.ReleaseMode)
	root := gin.New()

	// 1. Build middleware chain on the engine

	// Security headers (outermost)
	secureHeaders := true
	if s.cfg.SecureHeaders != nil {
		secureHeaders = *s.cfg.SecureHeaders
	}
	if secureHeaders {
		root.Use(SecureHeaders())
	}

	// Request ID: forward an inbound X-Request-ID or mint one, expose it on the
	// response header and request context. Installed before OTel/logging so the
	// id is attached to the server span and every access-log line, and pairs with
	// the outbound x-request-id the httpclient sets for end-to-end correlation.
	// NOTE: this is an ENHANCEMENT beyond legacy fit.js — the Node fit server had
	// no request-id middleware, and fit/axios only logged a per-call UUID (it did
	// not propagate an x-request-id header). Additive and harmless.
	root.Use(RequestID())

	// Per-request OpenTelemetry server span. Self-gated: a no-op passthrough when
	// TRACING_ENABLED is off (default), so there is no added latency unless tracing
	// is enabled. Installed before request logging and the user/parse middlewares so
	// the entire request — including the access-log line and every handler — runs
	// within the span. This restores the auto-instrumentation Node got from the
	// OTel express plugin, with no per-service wiring. (/_healthz and /_readyz are
	// skipped inside the middleware via tracing.ShouldTrace.)
	root.Use(OTelMiddleware())

	// Make the server span the goroutine-local active context so handler logs
	// carry the trace without explicit threading (implicit propagation).
	root.Use(GoroutineContextMiddleware())

	// Request logging
	root.Use(GinLogRequestResponse(LogRequestResponseConfig{
		Logger:          s.logger,
		IncludeHeaders:  coalesce(s.cfg.IncludeHeadersInLog, os.Getenv("INCLUDE_HEADERS_IN_LOG")),
		MetricsRecorder: s.cfg.MetricsRecorder,
	}))

	// CORS (dynamic, callback-based). Installed engine-level AFTER logging but BEFORE
	// the payload/user-data parse middlewares so a preflight OPTIONS is answered without
	// parsing a body/user header, and — being engine-level — it runs on gin's no-route
	// path too, so a preflight to a GET/POST-only path is handled rather than 404/405'd.
	// nil = disabled (no middleware mounted).
	if s.cfg.CORS != nil {
		root.Use(DynamicCORS(*s.cfg.CORS))
	}

	// Request middlewares provided by user (pre-parse)
	for _, mw := range requestMiddlewares {
		root.Use(mw)
	}

	// Built-in request parsing middlewares (unless disabled)
	if !envGetBool("DISABLE_REQUEST_MIDDLEWARES") {
		payloadSize := coalesce(s.cfg.MaxPayloadSize, os.Getenv("MAX_REQUEST_PAYLOAD_SIZE"), "2mb")
		root.Use(GinMaxPayloadSize(payloadSize))
		root.Use(GinParseUserData)
		root.Use(GinParseApplicationData)
	}

	// Response middlewares
	for _, mw := range responseMiddlewares {
		root.Use(mw)
	}

	// 2. Register health routes (before service routes)
	RegisterHealthRoutes(root)

	// 3. Register profiling routes if enabled
	if envGetBool("PROFILING_ENABLED") {
		RegisterProfileRoutes(root)
		s.logger.Info("[Profiling] Profiling routes registered")
	}

	// 4. Mount service routers based on SERVER_TYPE / UNIFY_SERVER
	env := strings.TrimSpace(strings.ToLower(os.Getenv("NODE_ENV")))
	unify := envGetBool("UNIFY_SERVER")

	if env != "production" && unify {
		if err := s.mountUnified(root, routers); err != nil {
			return err
		}
	} else {
		if err := s.mountByServerType(root, routers); err != nil {
			return err
		}
	}

	// 5. Set NoRoute handler: delegate to fallback service handler or return 404
	if s.fallbackHandler != nil {
		root.NoRoute(gin.WrapH(s.fallbackHandler))
	} else if !envGetBool("DISABLE_RESPONSE_MIDDLEWARES") {
		root.NoRoute(func(c *gin.Context) {
			c.JSON(http.StatusNotFound, map[string]string{"message": "not found"})
		})
	}

	// Set App and Router
	s.engine = root
	s.App = root
	s.Router = root

	return nil
}

// Start starts the HTTP server. It blocks until the server is shut down.
func (s *Server) Start() error {
	s.mu.Lock()
	port := s.cfg.Port
	if port == "" {
		port = os.Getenv("PORT")
	}
	if port == "" {
		s.mu.Unlock()
		return fmt.Errorf("server: environment variable 'PORT' not available; it is required to start a REST server")
	}

	if s.App == nil {
		s.mu.Unlock()
		return fmt.Errorf("server: Init must be called before Start")
	}

	addr := net.JoinHostPort("", port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.App,
		ReadTimeout:  s.cfg.ReadTimeout,
		WriteTimeout: s.cfg.WriteTimeout,
		IdleTimeout:  s.cfg.IdleTimeout,
	}
	s.mu.Unlock()

	s.logger.Info("Server started", "addr", "http://localhost:"+port)
	err := s.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.RLock()
	srv := s.server
	s.mu.RUnlock()

	if srv == nil {
		return nil
	}

	s.logger.Info("Shutting down server")
	return srv.Shutdown(ctx)
}

// Addr returns the address the server is listening on, or "" if not started.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.server != nil {
		return s.server.Addr
	}
	return ""
}

// ---------------------------------------------------------------------------
// Route mounting helpers
// ---------------------------------------------------------------------------

// mountUnified mounts all router types under /service/{type}/{service_name},
// with administrator under /service/___/administrator/{service_name}.
// This method for dev/test environments.
func (s *Server) mountUnified(root *gin.Engine, routers map[ServerType]http.Handler) error {
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		return fmt.Errorf("server: SERVICE_NAME environment variable is empty; unable to unify server")
	}

	for st, handler := range routers {
		prefix := fmt.Sprintf("/service/%s/%s", st.String(), serviceName)
		if st == ServerTypeAdministrator {
			prefix = fmt.Sprintf("/service/___/%s/%s", st.String(), serviceName)
		}
		root.Any(prefix+"/*path", gin.WrapH(http.StripPrefix(prefix, handler)))
		s.logger.Info("Mounted unified route", "type", st.String(), "prefix", prefix+"/")
	}

	return nil
}

// mountByServerType mounts routers based on the SERVER_TYPE env var.
// Single type: mounted at root "/". Multiple types: each under "/{type}/".
// Internal type is always mounted at root "/".
func (s *Server) mountByServerType(root *gin.Engine, routers map[ServerType]http.Handler) error {
	serverTypeCSV := os.Getenv("SERVER_TYPE")
	if serverTypeCSV == "" {
		return fmt.Errorf("server: no SERVER_TYPE provided")
	}

	types, err := ParseServerTypes(serverTypeCSV)
	if err != nil {
		return fmt.Errorf("server: %w", err)
	}
	if len(types) == 0 {
		return fmt.Errorf("server: no SERVER_TYPE provided")
	}

	if len(types) == 1 {
		st := types[0]
		handler, ok := routers[st]
		if !ok {
			return fmt.Errorf("server: invalid SERVER_TYPE (%s); no matching router provided", st.String())
		}
		// Mount at root: set as the fallback handler for unmatched routes.
		s.fallbackHandler = handler
		s.logger.Info("Mounted single server type", "type", st.String())
		return nil
	}

	// Multiple types: mount each under its prefix using route groups
	for _, st := range types {
		handler, ok := routers[st]
		if !ok {
			continue
		}
		prefix := "/" + st.String()
		if st == ServerTypeInternal {
			// Internal mounts at root; set as fallback
			s.fallbackHandler = handler
		} else {
			group := root.Group(prefix)
			group.Any("/*path", gin.WrapH(http.StripPrefix(prefix, handler)))
		}
		s.logger.Info("Mounted server type", "type", st.String(), "prefix", prefix+"/")
	}

	return nil
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

// coalesce returns the first non-empty string from the arguments.
func coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
