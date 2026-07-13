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

	"github.com/bufbuild/protocompile"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	fithealth "github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/redact"
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

// ServiceRegistrationOptions controls FIT-style dynamic service registration.
type ServiceRegistrationOptions struct {
	// ErrorHandler receives errors passed to next. When absent, callers receive
	// a generic Internal response and the original error is logged server-side.
	ErrorHandler ErrorHandlerFunc
}

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

	// ShutdownTimeout bounds graceful shutdown before in-flight RPCs are
	// forcefully stopped. Default: 10s.
	ShutdownTimeout time.Duration

	// Logger is the structured logger. If nil, slog.Default() is used.
	Logger *slog.Logger

	// HealthChecker is the health checker used for the gRPC health service.
	// If nil, a new default Checker is created.
	HealthChecker *fithealth.Checker

	// UnaryInterceptors and StreamInterceptors are installed on the native gRPC
	// server. They apply to generated and dynamic registrations alike.
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor
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
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 10 * time.Second
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
	mu         sync.Mutex
	shutdownMu sync.Mutex
	cfg        Config
	logger     *slog.Logger

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

	// dynamicService is compiled from the configured proto file and backs
	// FIT-style AddServiceDefinitions registration.
	dynamicService protoreflect.ServiceDescriptor

	// healthChecker is used for the built-in health check service.
	healthChecker *fithealth.Checker

	// running indicates whether the server is actively listening.
	running bool

	// done channel is closed when the server stops.
	done     chan struct{}
	doneOnce sync.Once
}

// currentHealthServer refreshes dependency state before serving the standard
// gRPC health API. health.Server otherwise caches the status assigned during
// initialization and can continue reporting SERVING after a dependency fails.
type currentHealthServer struct {
	healthpb.UnimplementedHealthServer
	owner    *Server
	delegate *health.Server
}

func (h *currentHealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	h.owner.syncHealthStatus()
	return h.delegate.Check(ctx, req)
}

func (h *currentHealthServer) List(ctx context.Context, req *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	h.owner.syncHealthStatus()
	return h.delegate.List(ctx, req)
}

func (h *currentHealthServer) Watch(req *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	h.owner.syncHealthStatus()
	return h.delegate.Watch(req, stream)
}

// Init initializes a new gRPC server with the given configuration.
// It validates the config and optionally loads the response type schema.
// Source proto files are only required when AddServiceDefinitions is used;
// generated registrations can use GRPCServer without runtime proto sources.
func Init(cfg Config) (*Server, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}

	cfg.Logger.Info("Initializing gRPC Server")

	// Resolve the proto path for optional FIT-style dynamic registration. Do not
	// require it here: generated-only callers register descriptors directly on
	// GRPCServer and do not need source protos or their imports at runtime.
	protoPath := filepath.Join(cfg.ProtoDir, cfg.ServerType, cfg.FileName+".proto")
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
	if len(cfg.UnaryInterceptors) > 0 {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(cfg.UnaryInterceptors...))
	}
	if len(cfg.StreamInterceptors) > 0 {
		serverOpts = append(serverOpts, grpc.ChainStreamInterceptor(cfg.StreamInterceptors...))
	}
	grpcServer := grpc.NewServer(serverOpts...)

	// Create the health state store. Registration happens after Server exists so
	// each health RPC can evaluate the current checker through its owner.
	healthServer := health.NewServer()

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
	healthpb.RegisterHealthServer(grpcServer, &currentHealthServer{owner: s, delegate: healthServer})
	s.syncHealthStatus()

	// Enable server reflection for debugging.
	reflection.Register(grpcServer)

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

func compileDynamicService(cfg Config, protoPath string) (protoreflect.ServiceDescriptor, error) {
	info, err := os.Stat(protoPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("grpc: proto file not found at %s - source protos are required for dynamic registration", protoPath)
		}
		return nil, fmt.Errorf("grpc: cannot stat proto file %s: %w", protoPath, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("grpc: proto path %s is not a regular file", protoPath)
	}

	protoDir := filepath.Dir(protoPath)
	resolver := protocompile.WithStandardImports(&protocompile.SourceResolver{
		ImportPaths: []string{protoDir, cfg.ProtoDir, "."},
	})
	files, err := (&protocompile.Compiler{Resolver: resolver}).Compile(context.Background(), filepath.Base(protoPath))
	if err != nil {
		return nil, fmt.Errorf("grpc: compile proto %s: %w", protoPath, err)
	}
	if len(files) != 1 {
		return nil, fmt.Errorf("grpc: compile proto %s returned %d root descriptors", protoPath, len(files))
	}

	services := files[0].Services()
	wanted := protoreflect.Name(Capitalize(cfg.FileName))
	for i := 0; i < services.Len(); i++ {
		if services.Get(i).Name() == wanted {
			return services.Get(i), nil
		}
	}
	if services.Len() == 1 {
		return services.Get(0), nil
	}
	// Generated-only users may keep a proto with no service and register their
	// generated descriptor through GRPCServer. Dynamic registration will return
	// a precise error if attempted.
	if services.Len() == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("grpc: proto %s has no unambiguous service named %s", protoPath, wanted)
}

// ---------------------------------------------------------------------------
// Service registration
// ---------------------------------------------------------------------------

// AddServiceDefinitions registers service handler implementations with
// middleware support. This is the port GRPC.addServerDefinitions().
//
// The implementations map keys are method names, and values are handler chains.
// Each chain is executed in order; calling next() advances to the next handler.
// AddServiceDefinitionsWithOptions accepts the explicit ErrorHandlerFunc used
// to port the JS "functions with 4 params" convention.
//
// A built-in gRPC health check service (Check/Watch) is auto-registered using
// the server's HealthChecker.
func (s *Server) AddServiceDefinitions(implementations ServiceImplementation) error {
	return s.AddServiceDefinitionsWithOptions(implementations, ServiceRegistrationOptions{})
}

// AddServiceDefinitionsWithOptions registers a descriptor-backed dynamic
// service and optional FIT-style error handler.
func (s *Server) AddServiceDefinitionsWithOptions(implementations ServiceImplementation, opts ServiceRegistrationOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return fmt.Errorf("grpc: service definitions must be registered before Start")
	}
	if s.dynamicService == nil {
		dynamicService, err := compileDynamicService(s.cfg, s.protoPath)
		if err != nil {
			return err
		}
		if dynamicService == nil {
			return fmt.Errorf("grpc: configured proto %s does not define a dynamic service", s.protoPath)
		}
		s.dynamicService = dynamicService
	}
	serviceName := string(s.dynamicService.FullName())
	if _, registered := s.services[serviceName]; registered {
		return fmt.Errorf("grpc: service %s is already registered", serviceName)
	}
	for i := 0; i < s.dynamicService.Methods().Len(); i++ {
		method := s.dynamicService.Methods().Get(i)
		if method.IsStreamingClient() || method.IsStreamingServer() {
			return fmt.Errorf("grpc: FIT-style dynamic method %s is streaming; register generated streaming services through GRPCServer", method.FullName())
		}
	}

	// Auto-register health check service.
	s.services["grpc.health.v1.Health"] = ServiceImplementation{
		"Check": {s.healthCheckHandler()},
		"Watch": {s.healthCheckHandler()},
	}

	// Update gRPC health server status based on fit health checker.
	s.syncHealthStatus()

	// Register primary service with middleware processing.
	processed := s.handleMethodMiddlewares(implementations, opts.ErrorHandler)
	s.services[serviceName] = processed
	s.grpcServer.RegisterService(s.dynamicServiceDescription(processed), dynamicServiceReceiver{})

	s.logger.Info("Service definitions registered",
		"service", s.dynamicService.FullName(),
		"methods", len(implementations),
	)
	return nil
}

type dynamicServiceInterface interface{}
type dynamicServiceReceiver struct{}

func (s *Server) dynamicServiceDescription(implementations ServiceImplementation) *grpc.ServiceDesc {
	desc := &grpc.ServiceDesc{
		ServiceName: string(s.dynamicService.FullName()),
		HandlerType: (*dynamicServiceInterface)(nil),
		Metadata:    s.protoPath,
	}
	methods := s.dynamicService.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: string(method.Name()),
			Handler:    s.dynamicUnaryHandler(method, lookupDynamicHandler(implementations, method.Name())),
		})
	}
	return desc
}

func lookupDynamicHandler(implementations ServiceImplementation, name protoreflect.Name) []HandlerFunc {
	if handlers, ok := implementations[string(name)]; ok {
		return handlers
	}
	for candidate, handlers := range implementations {
		if strings.EqualFold(candidate, string(name)) {
			return handlers
		}
	}
	return nil
}

func (s *Server) dynamicUnaryHandler(method protoreflect.MethodDescriptor, chain []HandlerFunc) grpc.MethodHandler {
	fullMethod := "/" + string(method.Parent().FullName()) + "/" + string(method.Name())
	return func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		request := dynamicpb.NewMessage(method.Input())
		if err := decode(request); err != nil {
			return nil, err
		}
		invoke := func(ctx context.Context, req any) (any, error) {
			return s.invokeDynamicUnary(ctx, fullMethod, method, chain, req.(proto.Message))
		}
		if interceptor == nil {
			return invoke(ctx, request)
		}
		return interceptor(ctx, request, &grpc.UnaryServerInfo{
			Server:     dynamicServiceReceiver{},
			FullMethod: fullMethod,
		}, invoke)
	}
}

type dynamicResult struct {
	response map[string]interface{}
	err      error
}

func (s *Server) invokeDynamicUnary(
	ctx context.Context,
	fullMethod string,
	method protoreflect.MethodDescriptor,
	chain []HandlerFunc,
	request proto.Message,
) (proto.Message, error) {
	if len(chain) == 0 {
		return nil, status.Error(codes.Unimplemented, "method not implemented")
	}
	requestMap, err := protobufMessageToMap(request)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to decode request")
	}
	call := &CallInfo{
		FullMethod: fullMethod,
		Request:    requestMap,
		Metadata:   incomingMetadata(ctx),
		Context:    ctx,
	}
	result := make(chan dynamicResult, 1)
	var callbackOnce sync.Once
	callback := func(err error, response map[string]interface{}) {
		callbackOnce.Do(func() { result <- dynamicResult{response: response, err: err} })
	}

	s.executeChain(chain, call, callback)
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case completed := <-result:
		if completed.err != nil {
			return nil, dynamicStatusError(completed.err)
		}
		response := dynamicpb.NewMessage(method.Output())
		if err := mapToProtobufMessage(completed.response, response); err != nil {
			s.logger.Error("gRPC response encoding failed", "method", fullMethod, "error", err)
			return nil, status.Error(codes.Internal, "failed to encode response")
		}
		return response, nil
	}
}

func incomingMetadata(ctx context.Context) Metadata {
	values := Metadata{}
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		for key, entries := range incoming {
			values[strings.ToLower(key)] = append([]string(nil), entries...)
		}
	}
	return values
}

func dynamicStatusError(err error) error {
	if err == nil {
		return nil
	}
	if rpcErr, ok := err.(*RPCError); ok {
		return publicDynamicStatus(rpcErr.Code, rpcErr.Message)
	}
	if grpcStatus, ok := status.FromError(err); ok {
		return publicDynamicStatus(grpcStatus.Code(), grpcStatus.Message())
	}
	return status.Error(codes.Internal, "Internal Server Error, Please try again!")
}

func publicDynamicStatus(code codes.Code, message string) error {
	switch code {
	case codes.Internal, codes.Unknown, codes.DataLoss:
		message = "Internal Server Error, Please try again!"
	case codes.Unavailable:
		message = "Service temporarily unavailable"
	}
	return status.Error(code, redact.Text(message))
}

// syncHealthStatus updates the gRPC health server status based on the
// fit health checker results.
func (s *Server) syncHealthStatus() []string {
	if s.healthServer == nil || s.healthChecker == nil {
		return nil
	}
	errs := s.healthChecker.Check()
	if len(errs) > 0 {
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		s.healthServer.SetServingStatus(s.cfg.FileName, healthpb.HealthCheckResponse_NOT_SERVING)
	} else {
		s.healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		s.healthServer.SetServingStatus(s.cfg.FileName, healthpb.HealthCheckResponse_SERVING)
	}
	return errs
}

// handleMethodMiddlewares wraps each method's handler chain into a single
// handler that executes the middleware chain with next() support.
func (s *Server) handleMethodMiddlewares(methods ServiceImplementation, errorHandler ErrorHandlerFunc) ServiceImplementation {
	processed := make(ServiceImplementation, len(methods))

	for methodName, chain := range methods {
		chain := chain // capture loop variable
		methodName := methodName

		// Wrap response encoding if schema is available.
		wrappedChain := s.wrapWithResponseEncoding(methodName, chain)

		processed[methodName] = []HandlerFunc{
			func(call *CallInfo, callback Callback, _ NextFunc) {
				s.executeChainWithErrorHandler(wrappedChain, call, callback, errorHandler)
			},
		}
	}
	return processed
}

// executeChain runs a middleware chain sequentially with the default sanitized
// internal-error mapping.
func (s *Server) executeChain(chain []HandlerFunc, call *CallInfo, callback Callback) {
	s.executeChainWithErrorHandler(chain, call, callback, nil)
}

func (s *Server) executeChainWithErrorHandler(chain []HandlerFunc, call *CallInfo, callback Callback, errorHandler ErrorHandlerFunc) {

	nextCounter := 0

	next := func(err error) {
		if err != nil {
			if errorHandler != nil {
				errorHandler(err, call, callback)
				return
			}
			s.logger.Error("gRPC middleware failed", "method", call.FullMethod, "error", redact.Text(err.Error()))
			callback(&RPCError{Code: Internal, Message: "Internal Server Error, Please try again!"}, nil)
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
						"error", redact.Text(fmt.Sprintf("%v", r)),
					)
					callback(&RPCError{
						Code:    Internal,
						Message: "Internal Server Error, Please try again!",
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
					callback(&RPCError{Code: Internal, Message: "Internal Server Error, Please try again!"}, nil)
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
		errs := s.syncHealthStatus()
		if len(errs) > 0 {
			callback(nil, map[string]interface{}{
				"status": "NOT_SERVING",
				"meta": map[string]interface{}{
					"error_messages": strings.Join(errs, ", "),
				},
			})
		} else {
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

	s.signalDone()

	return err
}

// Shutdown gracefully shuts down the gRPC server within Config.ShutdownTimeout,
// then forcefully stops remaining RPCs.
func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	return s.ShutdownContext(ctx)
}

// ShutdownContext gracefully shuts down until ctx expires. On expiry it calls
// grpc.Server.Stop so shutdown remains bounded and returns the context error.
func (s *Server) ShutdownContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	server := s.grpcServer
	s.mu.Unlock()

	if s.healthServer != nil {
		s.healthServer.Shutdown()
	}
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		s.signalDone()
		s.logger.Info("gRPC Server Stopped")
		return nil
	case <-ctx.Done():
		server.Stop()
		<-stopped
		s.signalDone()
		s.logger.Warn("gRPC Server Force Stopped after graceful shutdown deadline")
		return fmt.Errorf("grpc: graceful shutdown: %w", ctx.Err())
	}
}

// Stop forcefully stops the gRPC server without waiting for pending RPCs.
func (s *Server) Stop() error {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	server := s.grpcServer
	s.mu.Unlock()

	if s.healthServer != nil {
		s.healthServer.Shutdown()
	}
	server.Stop()
	s.signalDone()

	s.logger.Info("gRPC Server Force Stopped")
	return nil
}

func (s *Server) signalDone() {
	s.doneOnce.Do(func() { close(s.done) })
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
