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

// Package grpc provides gRPC server management for the fit.go framework.
//
// server lifecycle, proto file loading, service registration with middleware
// chains, request/response encoding, and health check auto-registration.
//
// Key environment variables (
//
// - SERVER_TYPE - the server type used to locate proto files
// - PORT - TCP port to listen on (required)
//
// Proto file path convention: ./proto/{SERVER_TYPE}/{FileName}.proto
package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	fithealth "github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/tracing"
)

// ---------------------------------------------------------------------------
// gRPC types
// ---------------------------------------------------------------------------

// StatusCode is an alias for grpc/codes.Code.
type StatusCode = codes.Code

// Status code constants re-exported for backward compatibility.
var (
	// OK indicates success.
	OK StatusCode = codes.OK
	// Internal indicates an internal server error.
	Internal StatusCode = codes.Internal
	// Unauthenticated indicates the request lacks valid authentication.
	Unauthenticated StatusCode = codes.Unauthenticated
)

// NewStatusError creates a gRPC status error with the given code and message.
func NewStatusError(code StatusCode, msg string) error {
	return status.Error(code, msg)
}

// Metadata is a simplified representation of gRPC metadata (headers/trailers).
type Metadata map[string][]string

// Get returns the first value for the given key, or empty string.
func (m Metadata) Get(key string) string {
	if vals, ok := m[strings.ToLower(key)]; ok && len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// Set sets a single value for the given key.
func (m Metadata) Set(key, value string) {
	m[strings.ToLower(key)] = []string{value}
}

// CallInfo represents an incoming gRPC call with its request and metadata.
// Mirrors the grpc ServerStream / unary call parameter.
type CallInfo struct {
	// FullMethod is the full RPC method string, e.g. "/package.Service/Method".
	FullMethod string

	// Request holds the decoded request payload.
	Request map[string]interface{}

	// Metadata holds the incoming gRPC metadata (headers).
	Metadata Metadata

	// Context is the request context.
	Context context.Context

	// Decoded holds the decoded JWT token payload set by auth middleware.
	Decoded interface{}
}

// RPCError represents a gRPC error response.
type RPCError struct {
	Code    StatusCode
	Message string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("rpc error: code = %d desc = %s", e.Code, e.Message)
}

// Callback is the response callback for unary RPCs.
// Pass a non-nil error to return a gRPC error; otherwise pass the response map.
type Callback func(err error, response map[string]interface{})

// HandlerFunc is a single gRPC method handler or middleware function.
// Handlers with 4 parameters in the original JS are error handlers; in Go we
// use the ErrorHandlerFunc signature instead.
type HandlerFunc func(call *CallInfo, callback Callback, next NextFunc)

// ErrorHandlerFunc handles errors propagated through the middleware chain.
type ErrorHandlerFunc func(err error, call *CallInfo, callback Callback)

// NextFunc advances to the next middleware in the chain.
// Pass a non-nil error to jump to the error handler.
type NextFunc func(err error)

// ServiceImplementation maps method names to their handler chain.
// Each method can have a single handler or a chain of middleware + handler.
type ServiceImplementation map[string][]HandlerFunc

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Config holds configuration for the gRPC server.
type Config struct {
	// ServerType identifies the server type for proto file lookup.
	// Falls back to SERVER_TYPE env var if empty.
	ServerType string

	// Port is the TCP port to listen on. Falls back to PORT env var.
	Port string

	// FileName is the proto file base name (without .proto extension).
	// Falls back to apiSpecifications.fileName in fit.config.json.
	FileName string

	// ProtoDir is the root directory containing proto files.
	// Defaults to "./proto" relative to the working directory.
	ProtoDir string

	// IdleTimeout is the max idle time before closing a connection.
	// Matches grpc.max_connection_idle_ms. Default: 60s.
	IdleTimeout time.Duration

	// KeepaliveInterval is how often keepalive pings are sent.
	// Matches grpc.keepalive_time_ms. Default: 20s.
	KeepaliveInterval time.Duration

	// KeepaliveTimeout is how long to wait for a keepalive ping ack.
	// Matches grpc.keepalive_timeout_ms. Default: 10s.
	KeepaliveTimeout time.Duration

	// Logger is the structured logger. If nil, slog.Default() is used.
	Logger *slog.Logger

	// HealthChecker is the health checker used for the gRPC health service.
	// If nil, a new default Checker is created.
	HealthChecker *fithealth.Checker
}

// defaults fills in zero-valued fields with sensible defaults.
func (c *Config) defaults() error {
	if c.ServerType == "" {
		c.ServerType = os.Getenv("SERVER_TYPE")
	}
	if c.ServerType == "" {
		return fmt.Errorf("grpc: SERVER_TYPE environment variable missing or not provided in Config")
	}
	if c.Port == "" {
		c.Port = os.Getenv("PORT")
	}
	if c.Port == "" {
		return fmt.Errorf("grpc: PORT environment variable missing or not provided in Config")
	}
	if c.ProtoDir == "" {
		c.ProtoDir = "./proto"
	}
	if c.FileName == "" {
		c.FileName = fileNameFromFitConfig()
	}
	if c.FileName == "" {
		return fmt.Errorf("grpc: FileName not provided and fit.config.json not found or missing apiSpecifications.fileName")
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.KeepaliveInterval == 0 {
		c.KeepaliveInterval = 20 * time.Second
	}
	if c.KeepaliveTimeout == 0 {
		c.KeepaliveTimeout = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.HealthChecker == nil {
		c.HealthChecker = fithealth.NewChecker()
	}
	return nil
}

// fileNameFromFitConfig reads apiSpecifications.fileName from fit.config.json.
func fileNameFromFitConfig() string {
	data, err := os.ReadFile("fit.config.json")
	if err != nil {
		return ""
	}
	var cfg struct {
		APISpecifications struct {
			FileName string `json:"fileName"`
		} `json:"apiSpecifications"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ""
	}
	return cfg.APISpecifications.FileName
}

// ---------------------------------------------------------------------------
// Response schema (for validate-and-encode)
// ---------------------------------------------------------------------------

// ProtoTypeSchema holds the response schema loaded from {FileName}.type.json.
// Maps method names (lowercase) to their schema definition.
type ProtoTypeSchema struct {
	Schema map[string]MethodSchema `json:"schema"`
}

// MethodSchema defines the response schema for a single gRPC method.
type MethodSchema struct {
	ResponseSchema map[string]string `json:"responseSchema"`
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server manages a gRPC server instance.
type Server struct {
	mu     sync.Mutex
	cfg    Config
	logger *slog.Logger

	// grpcServer is the real gRPC server instance.
	grpcServer *grpc.Server

	// healthServer is the gRPC health check service.
	healthServer *health.Server

	// listener is the TCP listener for the gRPC server.
	listener net.Listener

	// services stores registered service implementations keyed by service name.
	services map[string]ServiceImplementation

	// protoPath is the resolved path to the proto file.
	protoPath string

	// responseSchema is loaded from the .type.json file for response validation.
	responseSchema *ProtoTypeSchema

	// healthChecker is used for the built-in health check service.
	healthChecker *fithealth.Checker

	// running indicates whether the server is actively listening.
	running bool

	// done channel is closed when the server stops.
	done chan struct{}
}

// Init initializes a new gRPC server with the given configuration.
// It validates the config, verifies proto file existence, and optionally
// loads the response type schema.
func Init(cfg Config) (*Server, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}

	cfg.Logger.Info("Initializing gRPC Server")

	// Resolve and verify proto file path.
	protoPath := filepath.Join(cfg.ProtoDir, cfg.ServerType, cfg.FileName+".proto")
	if _, err := os.Stat(protoPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("grpc: proto file not found at %s - ensure the file exists (run proto generation if needed)", protoPath)
	}

	// Create the real gRPC server with keepalive parameters.
	serverOpts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: cfg.IdleTimeout,
			Time:              cfg.KeepaliveInterval,
			Timeout:           cfg.KeepaliveTimeout,
		}),
	}
	// Per-RPC OpenTelemetry server spans via the official otelgrpc StatsHandler,
	// using the global TracerProvider/propagator fit-go's tracing init installs.
	// Added only when tracing is enabled, so there is zero overhead when off.
	if t := tracing.Global(); t != nil && t.IsEnabled() {
		serverOpts = append(serverOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}
	grpcServer := grpc.NewServer(serverOpts...)

	// Register gRPC health check service.
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Enable server reflection for debugging.
	reflection.Register(grpcServer)

	s := &Server{
		cfg:           cfg,
		logger:        cfg.Logger,
		grpcServer:    grpcServer,
		healthServer:  healthServer,
		services:      make(map[string]ServiceImplementation),
		protoPath:     protoPath,
		healthChecker: cfg.HealthChecker,
		done:          make(chan struct{}),
	}

	// Attempt to load response type schema (optional, used for validation).
	typeSchemaPath := filepath.Join(cfg.ProtoDir, cfg.ServerType, cfg.FileName+".type.json")
	if data, err := os.ReadFile(typeSchemaPath); err == nil {
		var schema ProtoTypeSchema
		if err := json.Unmarshal(data, &schema); err != nil {
			s.logger.Warn("Failed to parse proto type schema", "path", typeSchemaPath, "error", err)
		} else {
			s.responseSchema = &schema
		}
	}

	cfg.Logger.Info("gRPC Server Initialized", "proto", protoPath)
	return s, nil
}

// ---------------------------------------------------------------------------
// Service registration
// ---------------------------------------------------------------------------

// AddServiceDefinitions registers service handler implementations with
// middleware support. This is the port GRPC.addServerDefinitions().
//
// The implementations map keys are method names, and values are handler chains.
// Each chain is executed in order; calling next() advances to the next handler.
// A handler with the ErrorHandlerFunc signature in the chain acts as an error
// handler (port of the JS "functions with 4 params" convention).
//
// A built-in gRPC health check service (Check/Watch) is auto-registered using
// the server's HealthChecker.
func (s *Server) AddServiceDefinitions(implementations ServiceImplementation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Auto-register health check service.
	s.services["grpc.health.v1.Health"] = ServiceImplementation{
		"Check": {s.healthCheckHandler()},
		"Watch": {s.healthCheckHandler()},
	}

	// Update gRPC health server status based on fit health checker.
	s.syncHealthStatus()

	// Register primary service with middleware processing.
	processed := s.handleMethodMiddlewares(implementations)
	s.services[s.cfg.FileName] = processed

	s.logger.Info("Service definitions registered",
		"service", s.cfg.FileName,
		"methods", len(implementations),
	)
	return nil
}

// syncHealthStatus updates the gRPC health server status based on the
// fit health checker results.
func (s *Server) syncHealthStatus() {
	if s.healthServer == nil || s.healthChecker == nil {
		return
	}
	errs := s.healthChecker.Check()
	if len(errs) > 0 {
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		s.healthServer.SetServingStatus(s.cfg.FileName, healthpb.HealthCheckResponse_NOT_SERVING)
	} else {
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		s.healthServer.SetServingStatus(s.cfg.FileName, healthpb.HealthCheckResponse_SERVING)
	}
}

// handleMethodMiddlewares wraps each method's handler chain into a single
// handler that executes the middleware chain with next() support.
func (s *Server) handleMethodMiddlewares(methods ServiceImplementation) ServiceImplementation {
	processed := make(ServiceImplementation, len(methods))

	for methodName, chain := range methods {
		chain := chain // capture loop variable
		methodName := methodName

		// Wrap response encoding if schema is available.
		wrappedChain := s.wrapWithResponseEncoding(methodName, chain)

		processed[methodName] = []HandlerFunc{
			func(call *CallInfo, callback Callback, _ NextFunc) {
				s.executeChain(wrappedChain, call, callback)
			},
		}
	}
	return processed
}

// executeChain runs a middleware chain sequentially. If next() is called with
// an error, it looks for an error handler in the chain (analogous to the JS
// convention of functions with 4 parameters).
func (s *Server) executeChain(chain []HandlerFunc, call *CallInfo, callback Callback) {
	// Find error handler (if any) - this is a convention from the JS version
	// where functions with 4 params are treated as error handlers.
	var errorHandler ErrorHandlerFunc

	nextCounter := 0

	next := func(err error) {
		if err != nil {
			if errorHandler != nil {
				errorHandler(err, call, callback)
				return
			}
			msg := err.Error()
			if msg == "" {
				msg = "Internal Server Error, Please try again!"
			}
			callback(&RPCError{Code: Internal, Message: msg}, nil)
			return
		}
		nextCounter++
	}

	// Execute each handler in sequence, stopping if next() was not called.
	for i, handler := range chain {
		if nextCounter < i {
			break
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("panic in gRPC handler",
						"method", call.FullMethod,
						"error", fmt.Sprintf("%v", r),
					)
					callback(&RPCError{
						Code:    Internal,
						Message: fmt.Sprintf("%v", r),
					}, nil)
				}
			}()
			handler(call, callback, next)
		}()
	}
}

// wrapWithResponseEncoding wraps the handler chain so that responses are
// validated against the proto type schema before being sent.
func (s *Server) wrapWithResponseEncoding(methodName string, chain []HandlerFunc) []HandlerFunc {
	if s.responseSchema == nil {
		return chain
	}

	lowerMethod := strings.ToLower(methodName)

	// Skip encoding for health check methods.
	if lowerMethod == "healthz" || lowerMethod == "readyz" {
		return chain
	}

	methodSchema, ok := s.responseSchema.Schema[lowerMethod]
	if !ok {
		return chain
	}

	// Wrap the last handler in the chain to intercept the callback.
	wrapped := make([]HandlerFunc, len(chain))
	copy(wrapped, chain)

	lastIdx := len(wrapped) - 1
	originalLast := wrapped[lastIdx]

	wrapped[lastIdx] = func(call *CallInfo, callback Callback, next NextFunc) {
		wrappedCallback := func(err error, response map[string]interface{}) {
			if err == nil && response != nil {
				if validationErr := validateResponse(response, methodSchema.ResponseSchema, "", false); validationErr != nil {
					s.logger.Error("response validation failed",
						"method", methodName,
						"error", validationErr,
					)
					callback(&RPCError{Code: Internal, Message: validationErr.Error()}, nil)
					return
				}
			}
			callback(err, response)
		}
		// Decode google.protobuf.Struct fields in the request.
		call.Request = decodeStructFields(call.Request)
		originalLast(call, wrappedCallback, next)
	}

	return wrapped
}

// ---------------------------------------------------------------------------
// Request/Response processing helpers
// ---------------------------------------------------------------------------

// decodeStructFields recursively decodes google.protobuf.Struct "fields"
// wrappers back into plain maps. Port GRPC.decodeRequest().
func decodeStructFields(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	for key, val := range m {
		if sub, ok := val.(map[string]interface{}); ok {
			if _, hasFields := sub["fields"]; hasFields {
				// This is a protobuf Struct encoding; decode it.
				m[key] = decodeProtobufStruct(sub)
			} else {
				m[key] = decodeStructFields(sub)
			}
		}
	}
	return m
}

// decodeProtobufStruct converts a protobuf Struct JSON representation
// (with "fields" key containing typed values) back into a plain Go map.
func decodeProtobufStruct(s map[string]interface{}) map[string]interface{} {
	fields, ok := s["fields"].(map[string]interface{})
	if !ok {
		return s
	}
	result := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		result[k] = decodeProtobufValue(v)
	}
	return result
}

// decodeProtobufValue decodes a single protobuf Value into a Go value.
func decodeProtobufValue(v interface{}) interface{} {
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}

	if nv, ok := m["numberValue"]; ok {
		return nv
	}
	if sv, ok := m["stringValue"]; ok {
		return sv
	}
	if bv, ok := m["boolValue"]; ok {
		return bv
	}
	if _, ok := m["nullValue"]; ok {
		return nil
	}
	if lv, ok := m["listValue"]; ok {
		if listMap, ok := lv.(map[string]interface{}); ok {
			if values, ok := listMap["values"].([]interface{}); ok {
				result := make([]interface{}, len(values))
				for i, item := range values {
					result[i] = decodeProtobufValue(item)
				}
				return result
			}
		}
	}
	if sv, ok := m["structValue"]; ok {
		if structMap, ok := sv.(map[string]interface{}); ok {
			return decodeProtobufStruct(structMap)
		}
	}

	// If it has a "fields" key, treat it as a nested struct.
	if _, hasFields := m["fields"]; hasFields {
		return decodeProtobufStruct(m)
	}

	return v
}

// validateResponse validates a response map against the proto type schema.
func validateResponse(response map[string]interface{}, schema map[string]string, prefix string, isArrayElement bool) error {
	for property, value := range response {
		schemaKey := property
		if prefix != "" {
			if isArrayElement {
				schemaKey = prefix
			} else {
				schemaKey = prefix + "." + property
			}
		}

		switch v := value.(type) {
		case map[string]interface{}:
			if expectedType, ok := schema[schemaKey]; ok && expectedType == "google.protobuf.Struct" {
				// Struct fields are already handled; skip deep validation.
				continue
			}
			if err := validateResponse(v, schema, schemaKey, false); err != nil {
				return err
			}
		case []interface{}:
			arrayKey := schemaKey + "[]"
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					if err := validateResponse(itemMap, schema, arrayKey, true); err != nil {
						return err
					}
				}
			}
		default:
			if value == nil {
				continue
			}
			if expectedType, ok := schema[schemaKey]; ok {
				actualType := goTypeToSchemaType(value)
				if actualType != expectedType && !isCompatibleType(actualType, expectedType) {
					return fmt.Errorf("expected %s to be %s but received %s in response", schemaKey, expectedType, actualType)
				}
			}
		}
	}
	return nil
}

// goTypeToSchemaType maps a Go value to the proto type schema string.
func goTypeToSchemaType(v interface{}) string {
	switch v.(type) {
	case float64:
		return "float"
	case float32:
		return "float"
	case int, int32, int64:
		return "number"
	case string:
		return "string"
	case bool:
		return "boolean"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// isCompatibleType checks if two types are compatible (e.g. number and float).
func isCompatibleType(actual, expected string) bool {
	// In JS, integers match both "number" and "float".
	if actual == "number" && expected == "float" {
		return true
	}
	if actual == "float" && expected == "number" {
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Health check handler
// ---------------------------------------------------------------------------

// healthCheckHandler returns a HandlerFunc that implements the gRPC Health
// Check protocol. Port healthCheckMethods.
func (s *Server) healthCheckHandler() HandlerFunc {
	return func(call *CallInfo, callback Callback, next NextFunc) {
		errs := s.healthChecker.Check()
		if len(errs) > 0 {
			// Update gRPC health server status.
			if s.healthServer != nil {
				s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			}
			callback(nil, map[string]interface{}{
				"status": "NOT_SERVING",
				"meta": map[string]interface{}{
					"error_messages": strings.Join(errs, ", "),
				},
			})
		} else {
			if s.healthServer != nil {
				s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
			}
			callback(nil, map[string]interface{}{
				"status": "SERVING",
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

// Start starts the gRPC server, listening on the configured port.
func (s *Server) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("grpc: server already running")
	}

	addr := net.JoinHostPort("0.0.0.0", s.cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("grpc: failed to listen on %s: %w", addr, err)
	}
	s.listener = ln
	s.running = true
	s.mu.Unlock()

	s.logger.Info("gRPC Server running", "address", ln.Addr().String())

	// Serve blocks until the server is stopped.
	err = s.grpcServer.Serve(ln)

	// Mark as not running after Serve returns.
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()

	// Signal done so any goroutines waiting on it can proceed.
	select {
	case <-s.done:
		// Already closed.
	default:
		close(s.done)
	}

	return err
}

// Shutdown gracefully shuts down the gRPC server.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false

	// GracefulStop stops the gRPC server gracefully. It stops accepting new
	// connections and RPCs and blocks until all pending RPCs finish.
	s.grpcServer.GracefulStop()

	select {
	case <-s.done:
		// Already closed.
	default:
		close(s.done)
	}

	s.logger.Info("gRPC Server Stopped")
	return nil
}

// Stop forcefully stops the gRPC server without waiting for pending RPCs.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	s.grpcServer.Stop()

	select {
	case <-s.done:
	default:
		close(s.done)
	}

	s.logger.Info("gRPC Server Force Stopped")
	return nil
}

// IsRunning returns whether the server is currently running.
func (s *Server) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// GRPCServer returns the underlying *grpc.Server for direct registration
// of protobuf-generated service implementations.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Listener returns the active TCP listener, or nil if the server is not running.
func (s *Server) Listener() net.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener
}

// Services returns the registered service implementations (for testing).
func (s *Server) Services() map[string]ServiceImplementation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.services
}

// ProtoPath returns the resolved proto file path.
func (s *Server) ProtoPath() string {
	return s.protoPath
}

// Config returns the server configuration.
func (s *Server) Config() Config {
	return s.cfg
}

// Capitalize returns the string with its first letter uppercased.
// Port of lodash capitalize used for service name derivation.
func Capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
