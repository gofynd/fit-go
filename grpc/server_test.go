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

package grpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/gofynd/fit-go/health"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newTestServer creates a Server with a temp proto file, ready for testing.
// It uses port "0" so the OS assigns a random available port.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	tmpDir := t.TempDir()
	serverType := "testservice"
	protoDir := filepath.Join(tmpDir, serverType)
	os.MkdirAll(protoDir, 0755)
	os.WriteFile(filepath.Join(protoDir, "test.proto"), []byte(`syntax = "proto3";`), 0644)

	os.Setenv("SERVER_TYPE", serverType)
	os.Setenv("PORT", "0")
	t.Cleanup(func() {
		os.Unsetenv("SERVER_TYPE")
		os.Unsetenv("PORT")
	})

	srv, err := Init(Config{
		FileName: "test",
		ProtoDir: tmpDir,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return srv
}

// startServer starts the server in a background goroutine and waits until
// it is running. Returns a cleanup function that shuts down the server.
func startServer(t *testing.T, srv *Server) func() {
	t.Helper()
	started := make(chan struct{})

	go func() {
		// Signal started once we detect the server is running.
		go func() {
			for i := 0; i < 100; i++ {
				if srv.IsRunning() {
					close(started)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
		srv.Start()
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not start within timeout")
	}

	return func() {
		srv.Shutdown()
	}
}

type blockingService interface {
	Block(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
}

type blockingServiceImpl struct {
	entered chan struct{}
	once    sync.Once
}

func (s *blockingServiceImpl) Block(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func blockingServiceHandler(
	server any,
	ctx context.Context,
	decode func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	req := new(emptypb.Empty)
	if err := decode(req); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return server.(blockingService).Block(ctx, req)
	}
	info := &grpc.UnaryServerInfo{Server: server, FullMethod: "/fit.test.Blocking/Block"}
	handler := func(ctx context.Context, req any) (any, error) {
		return server.(blockingService).Block(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, req, info, handler)
}

var blockingServiceDescription = grpc.ServiceDesc{
	ServiceName: "fit.test.Blocking",
	HandlerType: (*blockingService)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Block",
		Handler:    blockingServiceHandler,
	}},
}

// generateRSAKeyPair generates an RSA key pair for testing RS256 JWT.
func generateRSAKeyPair(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("failed to marshal public key: %v", err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: pubBytes,
	})
	return priv, string(pubPEM)
}

// createHS256Token creates a signed HS256 JWT for testing.
func createHS256Token(t *testing.T, claims jwt.MapClaims, secret string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return signed
}

// ---------------------------------------------------------------------------
// Config tests
// ---------------------------------------------------------------------------

func TestConfig_Defaults(t *testing.T) {
	os.Setenv("SERVER_TYPE", "platform")
	os.Setenv("PORT", "50051")
	defer func() {
		os.Unsetenv("SERVER_TYPE")
		os.Unsetenv("PORT")
	}()

	cfg := Config{FileName: "testfile"}
	err := cfg.defaults()
	if err != nil {
		t.Fatalf("defaults() error = %v", err)
	}

	if cfg.ServerType != "platform" {
		t.Errorf("ServerType = %q, want platform", cfg.ServerType)
	}
	if cfg.Port != "50051" {
		t.Errorf("Port = %q, want 50051", cfg.Port)
	}
	if cfg.ProtoDir != "./proto" {
		t.Errorf("ProtoDir = %q, want ./proto", cfg.ProtoDir)
	}
	if cfg.IdleTimeout != 60*time.Second {
		t.Errorf("IdleTimeout = %v, want 60s", cfg.IdleTimeout)
	}
	if cfg.KeepaliveInterval != 20*time.Second {
		t.Errorf("KeepaliveInterval = %v, want 20s", cfg.KeepaliveInterval)
	}
	if cfg.KeepaliveTimeout != 10*time.Second {
		t.Errorf("KeepaliveTimeout = %v, want 10s", cfg.KeepaliveTimeout)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
	if cfg.Logger == nil {
		t.Error("Logger should be set to default")
	}
	if cfg.HealthChecker == nil {
		t.Error("HealthChecker should be set to default")
	}
}

func TestConfig_Defaults_MissingServerType(t *testing.T) {
	os.Unsetenv("SERVER_TYPE")
	os.Setenv("PORT", "50051")
	defer os.Unsetenv("PORT")

	cfg := Config{}
	err := cfg.defaults()
	if err == nil {
		t.Error("defaults() should fail without SERVER_TYPE")
	}
}

func TestConfig_Defaults_MissingPort(t *testing.T) {
	os.Setenv("SERVER_TYPE", "platform")
	os.Unsetenv("PORT")
	defer os.Unsetenv("SERVER_TYPE")

	cfg := Config{}
	err := cfg.defaults()
	if err == nil {
		t.Error("defaults() should fail without PORT")
	}
}

func TestConfig_CustomValues(t *testing.T) {
	os.Setenv("SERVER_TYPE", "platform")
	os.Setenv("PORT", "50051")
	defer func() {
		os.Unsetenv("SERVER_TYPE")
		os.Unsetenv("PORT")
	}()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	checker := health.NewChecker()

	cfg := Config{
		ServerType:        "custom",
		Port:              "8080",
		FileName:          "myservice",
		ProtoDir:          "/custom/proto",
		IdleTimeout:       30 * time.Second,
		KeepaliveInterval: 10 * time.Second,
		KeepaliveTimeout:  5 * time.Second,
		Logger:            logger,
		HealthChecker:     checker,
	}
	err := cfg.defaults()
	if err != nil {
		t.Fatalf("defaults() error = %v", err)
	}

	if cfg.ServerType != "custom" {
		t.Errorf("ServerType = %q, want custom", cfg.ServerType)
	}
	if cfg.Port != "8080" {
		t.Errorf("Port = %q, want 8080", cfg.Port)
	}
	if cfg.IdleTimeout != 30*time.Second {
		t.Errorf("IdleTimeout = %v, want 30s", cfg.IdleTimeout)
	}
}

// ---------------------------------------------------------------------------
// Init tests
// ---------------------------------------------------------------------------

func TestInit_DefaultConfig(t *testing.T) {
	srv := newTestServer(t)

	if srv.GRPCServer() == nil {
		t.Error("GRPCServer() should not be nil after Init")
	}
	if !strings.HasSuffix(srv.ProtoPath(), "test.proto") {
		t.Errorf("ProtoPath() = %q, should end with test.proto", srv.ProtoPath())
	}
	if srv.IsRunning() {
		t.Error("Server should not be running before Start()")
	}
}

func TestInit_CustomConfig(t *testing.T) {
	tmpDir := t.TempDir()
	serverType := "customsvc"
	protoDir := filepath.Join(tmpDir, serverType)
	os.MkdirAll(protoDir, 0755)
	os.WriteFile(filepath.Join(protoDir, "custom.proto"), []byte(`syntax = "proto3";`), 0644)

	checker := health.NewChecker()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv, err := Init(Config{
		ServerType:        serverType,
		Port:              "0",
		FileName:          "custom",
		ProtoDir:          tmpDir,
		IdleTimeout:       120 * time.Second,
		KeepaliveInterval: 30 * time.Second,
		KeepaliveTimeout:  15 * time.Second,
		Logger:            logger,
		HealthChecker:     checker,
	})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	cfg := srv.Config()
	if cfg.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want 120s", cfg.IdleTimeout)
	}
	if cfg.KeepaliveInterval != 30*time.Second {
		t.Errorf("KeepaliveInterval = %v, want 30s", cfg.KeepaliveInterval)
	}
	if cfg.KeepaliveTimeout != 15*time.Second {
		t.Errorf("KeepaliveTimeout = %v, want 15s", cfg.KeepaliveTimeout)
	}
}

func TestInit_MissingProtoFile(t *testing.T) {
	os.Setenv("SERVER_TYPE", "testserver")
	os.Setenv("PORT", "50051")
	defer func() {
		os.Unsetenv("SERVER_TYPE")
		os.Unsetenv("PORT")
	}()

	_, err := Init(Config{
		FileName: "nonexistent",
		ProtoDir: "/tmp/nonexistent",
	})
	if err == nil {
		t.Error("Init() should fail when proto file doesn't exist")
	}
}

func TestInit_WithProtoFile(t *testing.T) {
	tmpDir := t.TempDir()
	serverType := "testservice"
	protoDir := filepath.Join(tmpDir, serverType)
	os.MkdirAll(protoDir, 0755)
	protoFile := filepath.Join(protoDir, "test.proto")
	os.WriteFile(protoFile, []byte(`syntax = "proto3";`), 0644)

	os.Setenv("SERVER_TYPE", serverType)
	os.Setenv("PORT", "50051")
	defer func() {
		os.Unsetenv("SERVER_TYPE")
		os.Unsetenv("PORT")
	}()

	server, err := Init(Config{
		FileName: "test",
		ProtoDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if server == nil {
		t.Fatal("Init() returned nil server")
	}
	if server.ProtoPath() != protoFile {
		t.Errorf("ProtoPath() = %q, want %q", server.ProtoPath(), protoFile)
	}
}

// ---------------------------------------------------------------------------
// Server lifecycle tests
// ---------------------------------------------------------------------------

func TestServer_StartAndShutdown(t *testing.T) {
	srv := newTestServer(t)
	cleanup := startServer(t, srv)
	defer cleanup()

	if !srv.IsRunning() {
		t.Error("Server should be running after Start()")
	}

	// Verify we can get the listener address.
	ln := srv.Listener()
	if ln == nil {
		t.Fatal("Listener() should not be nil while running")
	}
	addr := ln.Addr().String()
	if addr == "" {
		t.Error("Listener address should not be empty")
	}

	// Verify we can connect to the port.
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}
	conn.Close()

	// Shutdown.
	if err := srv.Shutdown(); err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}

	// Wait for server to fully stop.
	time.Sleep(50 * time.Millisecond)

	if srv.IsRunning() {
		t.Error("Server should not be running after Shutdown()")
	}
}

func TestServer_StartTwice(t *testing.T) {
	srv := newTestServer(t)
	cleanup := startServer(t, srv)
	defer cleanup()

	// Try to start again - should error.
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("Second Start() should return error")
		}
		if !strings.Contains(err.Error(), "already running") {
			t.Errorf("error = %q, should mention 'already running'", err.Error())
		}
	case <-time.After(1 * time.Second):
		t.Error("Second Start() should return immediately with error")
	}
}

func TestServer_ShutdownTwice(t *testing.T) {
	srv := newTestServer(t)
	cleanup := startServer(t, srv)
	_ = cleanup // we'll shut down manually

	err1 := srv.Shutdown()
	time.Sleep(50 * time.Millisecond)
	err2 := srv.Shutdown() // Second shutdown should be safe (no-op).

	if err1 != nil {
		t.Errorf("First Shutdown() error = %v", err1)
	}
	if err2 != nil {
		t.Errorf("Second Shutdown() error = %v", err2)
	}
}

func TestServer_ShutdownWithoutStart(t *testing.T) {
	srv := newTestServer(t)
	err := srv.Shutdown()
	if err != nil {
		t.Errorf("Shutdown() on non-started server should succeed, got: %v", err)
	}
}

func TestServer_ShutdownDeadlineForcesActiveRPC(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.ShutdownTimeout = 40 * time.Millisecond
	service := &blockingServiceImpl{entered: make(chan struct{})}
	srv.GRPCServer().RegisterService(&blockingServiceDescription, service)
	cleanup := startServer(t, srv)
	defer cleanup()

	conn, err := grpc.NewClient(
		srv.Listener().Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	callDone := make(chan error, 1)
	go func() {
		callDone <- conn.Invoke(
			context.Background(),
			"/fit.test.Blocking/Block",
			&emptypb.Empty{},
			&emptypb.Empty{},
		)
	}()
	select {
	case <-service.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking RPC did not start")
	}

	started := time.Now()
	err = srv.Shutdown()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("forced shutdown took %v, want bounded completion", elapsed)
	}
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("forced stop did not cancel the active RPC")
	}
}

// ---------------------------------------------------------------------------
// Health check tests
// ---------------------------------------------------------------------------

func TestServer_HealthCheck(t *testing.T) {
	srv := newTestServer(t)
	cleanup := startServer(t, srv)
	defer cleanup()

	addr := srv.Listener().Addr().String()

	// Connect with a gRPC client.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health Check RPC failed: %v", err)
	}

	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health status = %v, want SERVING", resp.Status)
	}
}

func TestServer_HealthCheckReflectsCurrentDependencyState(t *testing.T) {
	srv := newTestServer(t)
	checker := health.NewChecker()
	var unhealthy atomic.Bool
	checker.AddCheck(func() string {
		if unhealthy.Load() {
			return "database unavailable"
		}
		return ""
	})
	srv.healthChecker = checker
	cleanup := startServer(t, srv)
	defer cleanup()

	conn, err := grpc.NewClient(
		srv.Listener().Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)

	check := func(service string, want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("Health.Check(%q): %v", service, err)
		}
		if resp.Status != want {
			t.Fatalf("Health.Check(%q) = %s, want %s", service, resp.Status, want)
		}
	}

	check("", healthpb.HealthCheckResponse_SERVING)
	unhealthy.Store(true)
	check("", healthpb.HealthCheckResponse_NOT_SERVING)
	check("test", healthpb.HealthCheckResponse_NOT_SERVING)
	unhealthy.Store(false)
	check("test", healthpb.HealthCheckResponse_SERVING)
}

func TestServer_HealthCheckHandler_Healthy(t *testing.T) {
	srv := newTestServer(t)

	checker := health.NewChecker()
	checker.AddCheck(func() string { return "" }) // healthy
	srv.healthChecker = checker

	handler := srv.healthCheckHandler()

	var response map[string]interface{}
	callback := func(err error, resp map[string]interface{}) {
		response = resp
	}

	handler(&CallInfo{}, callback, nil)

	if response["status"] != "SERVING" {
		t.Errorf("Status = %v, want SERVING", response["status"])
	}
}

func TestServer_HealthCheckHandler_Unhealthy(t *testing.T) {
	srv := newTestServer(t)

	checker := health.NewChecker()
	checker.AddCheck(func() string { return "database down" })
	srv.healthChecker = checker

	handler := srv.healthCheckHandler()

	var response map[string]interface{}
	callback := func(err error, resp map[string]interface{}) {
		response = resp
	}

	handler(&CallInfo{}, callback, nil)

	if response["status"] != "NOT_SERVING" {
		t.Errorf("Status = %v, want NOT_SERVING", response["status"])
	}
	if meta, ok := response["meta"].(map[string]interface{}); ok {
		if !strings.Contains(meta["error_messages"].(string), "database down") {
			t.Error("Error message should contain the check error")
		}
	} else {
		t.Error("meta should be present in unhealthy response")
	}
}

// ---------------------------------------------------------------------------
// Service registration tests
// ---------------------------------------------------------------------------

func TestServer_AddServiceDefinitions(t *testing.T) {
	srv := newTestServer(t)

	impl := ServiceImplementation{
		"GetUser": {
			func(call *CallInfo, callback Callback, next NextFunc) {
				callback(nil, map[string]interface{}{"name": "test"})
			},
		},
	}

	err := srv.AddServiceDefinitions(impl)
	if err != nil {
		t.Fatalf("AddServiceDefinitions() error = %v", err)
	}

	services := srv.Services()
	if len(services) != 2 { // health + main service
		t.Errorf("Services count = %d, want 2", len(services))
	}

	if _, ok := services["grpc.health.v1.Health"]; !ok {
		t.Error("Health service should be auto-registered")
	}
}

// ---------------------------------------------------------------------------
// Middleware chain tests
// ---------------------------------------------------------------------------

func TestMiddlewareChain(t *testing.T) {
	srv := newTestServer(t)

	t.Run("single handler", func(t *testing.T) {
		var response map[string]interface{}
		callback := func(err error, resp map[string]interface{}) {
			response = resp
		}

		chain := []HandlerFunc{
			func(call *CallInfo, cb Callback, next NextFunc) {
				cb(nil, map[string]interface{}{"result": "success"})
			},
		}

		srv.executeChain(chain, &CallInfo{}, callback)

		if response["result"] != "success" {
			t.Errorf("Response = %v, want success", response["result"])
		}
	})

	t.Run("middleware chain with next", func(t *testing.T) {
		var order []string
		callback := func(err error, resp map[string]interface{}) {}

		chain := []HandlerFunc{
			func(call *CallInfo, cb Callback, next NextFunc) {
				order = append(order, "m1-before")
				next(nil)
				order = append(order, "m1-after")
			},
			func(call *CallInfo, cb Callback, next NextFunc) {
				order = append(order, "m2")
				cb(nil, map[string]interface{}{})
			},
		}

		srv.executeChain(chain, &CallInfo{}, callback)

		expected := []string{"m1-before", "m1-after", "m2"}
		if len(order) != len(expected) {
			t.Errorf("Order length = %d, want %d: %v", len(order), len(expected), order)
		}
	})

	t.Run("error in next triggers default error response", func(t *testing.T) {
		var gotErr error
		callback := func(err error, resp map[string]interface{}) {
			gotErr = err
		}

		chain := []HandlerFunc{
			func(call *CallInfo, cb Callback, next NextFunc) {
				next(fmt.Errorf("something broke"))
			},
		}

		srv.executeChain(chain, &CallInfo{}, callback)

		if gotErr == nil {
			t.Error("Should receive error from next(err)")
		}
		rpcErr, ok := gotErr.(*RPCError)
		if !ok {
			t.Fatalf("expected *RPCError, got %T", gotErr)
		}
		if rpcErr.Code != Internal {
			t.Errorf("error code = %v, want Internal", rpcErr.Code)
		}
	})

	t.Run("panic recovery", func(t *testing.T) {
		var gotErr error
		callback := func(err error, resp map[string]interface{}) {
			gotErr = err
		}

		chain := []HandlerFunc{
			func(call *CallInfo, cb Callback, next NextFunc) {
				panic("test panic")
			},
		}

		srv.executeChain(chain, &CallInfo{FullMethod: "/test"}, callback)

		if gotErr == nil {
			t.Error("Should receive error from panic")
		}
	})

	t.Run("chain stops when next not called", func(t *testing.T) {
		var executed []string
		callback := func(err error, resp map[string]interface{}) {}

		chain := []HandlerFunc{
			func(call *CallInfo, cb Callback, next NextFunc) {
				executed = append(executed, "first")
				// Does NOT call next.
				cb(nil, map[string]interface{}{})
			},
			func(call *CallInfo, cb Callback, next NextFunc) {
				executed = append(executed, "second")
			},
		}

		srv.executeChain(chain, &CallInfo{}, callback)

		// The second handler should not execute since next() was not called.
		if len(executed) != 1 {
			t.Errorf("executed = %v, want only [first]", executed)
		}
	})
}

// ---------------------------------------------------------------------------
// JWT Authorization tests
// ---------------------------------------------------------------------------

func TestJWTAuthorization(t *testing.T) {
	secret := "test-secret-key-for-jwt"

	t.Run("valid HS256 token", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
			"iat":        float64(time.Now().Unix()),
		}
		tokenStr := createHS256Token(t, claims, secret)

		handler := AuthorizeJWTToken(JWTConfig{
			Secret: secret,
		})

		var nextCalled bool
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{"company_id": float64(42)},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}, func(err error) {
			nextCalled = true
			if err != nil {
				t.Errorf("next called with error: %v", err)
			}
		})

		if !nextCalled {
			t.Error("next() should be called for valid token")
		}
		if call.Decoded == nil {
			t.Error("Decoded should be set on successful auth")
		}
	})

	t.Run("missing authorization header", func(t *testing.T) {
		handler := AuthorizeJWTToken(JWTConfig{Secret: secret})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{},
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called")
		})

		if gotErr == nil {
			t.Error("expected Unauthenticated error")
		}
	})

	t.Run("invalid token signature", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		tokenStr := createHS256Token(t, claims, "wrong-secret")

		handler := AuthorizeJWTToken(JWTConfig{Secret: secret})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called for invalid token")
		})

		if gotErr == nil {
			t.Error("expected Unauthenticated error for bad signature")
		}
	})

	t.Run("expired token", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(-1 * time.Hour).Unix()),
			"iat":        float64(time.Now().Add(-2 * time.Hour).Unix()),
		}
		tokenStr := createHS256Token(t, claims, secret)

		handler := AuthorizeJWTToken(JWTConfig{Secret: secret})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called for expired token")
		})

		if gotErr == nil {
			t.Error("expected Unauthenticated error for expired token")
		}
	})

	t.Run("expired token with clock skew", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(-30 * time.Second).Unix()),
			"iat":        float64(time.Now().Add(-2 * time.Hour).Unix()),
		}
		tokenStr := createHS256Token(t, claims, secret)

		handler := AuthorizeJWTToken(JWTConfig{
			Secret:           secret,
			AllowedClockSkew: 1 * time.Minute,
		})

		var nextCalled bool
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{"company_id": float64(42)},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}, func(err error) {
			nextCalled = true
		})

		if !nextCalled {
			t.Error("next() should be called when clock skew covers expiration")
		}
	})

	t.Run("payload mismatch", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(99),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		tokenStr := createHS256Token(t, claims, secret)

		handler := AuthorizeJWTToken(JWTConfig{
			Secret:  secret,
			Payload: map[string]interface{}{"company_id": float64(42)},
		})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called for payload mismatch")
		})

		if gotErr == nil {
			t.Error("expected Unauthenticated error for payload mismatch")
		}
	})

	t.Run("HS384 token", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS384, claims)
		tokenStr, err := token.SignedString([]byte(secret))
		if err != nil {
			t.Fatalf("failed to sign HS384 token: %v", err)
		}

		handler := AuthorizeJWTToken(JWTConfig{
			Secret:            secret,
			AllowedAlgorithms: []string{"HS256", "HS384"},
		})

		var nextCalled bool
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{"company_id": float64(42)},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}, func(err error) {
			nextCalled = true
		})

		if !nextCalled {
			t.Error("next() should be called for valid HS384 token")
		}
	})

	t.Run("HS512 token", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS512, claims)
		tokenStr, err := token.SignedString([]byte(secret))
		if err != nil {
			t.Fatalf("failed to sign HS512 token: %v", err)
		}

		handler := AuthorizeJWTToken(JWTConfig{
			Secret:            secret,
			AllowedAlgorithms: []string{"HS512"},
		})

		var nextCalled bool
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{"company_id": float64(42)},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}, func(err error) {
			nextCalled = true
		})

		if !nextCalled {
			t.Error("next() should be called for valid HS512 token")
		}
	})

	t.Run("RS256 token", func(t *testing.T) {
		privKey, pubPEM := generateRSAKeyPair(t)

		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tokenStr, err := token.SignedString(privKey)
		if err != nil {
			t.Fatalf("failed to sign RS256 token: %v", err)
		}

		handler := AuthorizeJWTToken(JWTConfig{
			RSAPublicKeyPEM:   pubPEM,
			AllowedAlgorithms: []string{"RS256"},
		})

		var nextCalled bool
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{"company_id": float64(42)},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}, func(err error) {
			nextCalled = true
		})

		if !nextCalled {
			t.Error("next() should be called for valid RS256 token")
		}
	})

	t.Run("algorithm not in allowed list", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(42),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS384, claims)
		tokenStr, err := token.SignedString([]byte(secret))
		if err != nil {
			t.Fatalf("failed to sign token: %v", err)
		}

		// Only allow HS256, but token is HS384.
		handler := AuthorizeJWTToken(JWTConfig{
			Secret:            secret,
			AllowedAlgorithms: []string{"HS256"},
		})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called for disallowed algorithm")
		})

		if gotErr == nil {
			t.Error("expected error for disallowed algorithm")
		}
	})

	t.Run("malformed token", func(t *testing.T) {
		handler := AuthorizeJWTToken(JWTConfig{Secret: secret})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer not.a.valid.jwt"}},
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called for malformed token")
		})

		if gotErr == nil {
			t.Error("expected error for malformed token")
		}
	})

	t.Run("token without Bearer prefix", func(t *testing.T) {
		tokenStr := createHS256Token(t, jwt.MapClaims{"company_id": float64(42)}, secret)

		handler := AuthorizeJWTToken(JWTConfig{Secret: secret})

		var gotErr error
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{tokenStr}}, // No "Bearer " prefix.
			Request:  map[string]interface{}{},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			gotErr = err
		}, func(err error) {
			t.Error("next() should not be called without Bearer prefix")
		})

		if gotErr == nil {
			t.Error("expected Unauthenticated error without Bearer prefix")
		}
	})

	t.Run("auto extract company_id from request", func(t *testing.T) {
		claims := jwt.MapClaims{
			"company_id": float64(77),
			"exp":        float64(time.Now().Add(1 * time.Hour).Unix()),
		}
		tokenStr := createHS256Token(t, claims, secret)

		// No Payload set - should auto-extract from request.
		handler := AuthorizeJWTToken(JWTConfig{Secret: secret})

		var nextCalled bool
		call := &CallInfo{
			Metadata: Metadata{"authorization": []string{"Bearer " + tokenStr}},
			Request:  map[string]interface{}{"company_id": float64(77)},
			Context:  context.Background(),
		}

		handler(call, func(err error, resp map[string]interface{}) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}, func(err error) {
			nextCalled = true
		})

		if !nextCalled {
			t.Error("next() should be called when company_id matches")
		}
	})
}

// ---------------------------------------------------------------------------
// Protobuf decoding tests
// ---------------------------------------------------------------------------

func TestDecodeStructFields(t *testing.T) {
	t.Run("plain map", func(t *testing.T) {
		input := map[string]interface{}{
			"name": "test",
			"age":  25,
		}
		result := decodeStructFields(input)
		if result["name"] != "test" {
			t.Errorf("name = %v, want test", result["name"])
		}
	})

	t.Run("with fields wrapper", func(t *testing.T) {
		input := map[string]interface{}{
			"data": map[string]interface{}{
				"fields": map[string]interface{}{
					"key": map[string]interface{}{"stringValue": "value"},
				},
			},
		}
		result := decodeStructFields(input)
		if data, ok := result["data"].(map[string]interface{}); ok {
			if data["key"] != "value" {
				t.Errorf("Decoded value = %v, want value", data["key"])
			}
		} else {
			t.Error("data should be a decoded map")
		}
	})

	t.Run("nil input", func(t *testing.T) {
		result := decodeStructFields(nil)
		if result != nil {
			t.Errorf("decodeStructFields(nil) = %v, want nil", result)
		}
	})

	t.Run("nested struct", func(t *testing.T) {
		input := map[string]interface{}{
			"outer": map[string]interface{}{
				"fields": map[string]interface{}{
					"inner": map[string]interface{}{
						"structValue": map[string]interface{}{
							"fields": map[string]interface{}{
								"deep": map[string]interface{}{"numberValue": 42.0},
							},
						},
					},
				},
			},
		}
		result := decodeStructFields(input)
		outer, ok := result["outer"].(map[string]interface{})
		if !ok {
			t.Fatal("outer should be decoded map")
		}
		inner, ok := outer["inner"].(map[string]interface{})
		if !ok {
			t.Fatal("inner should be decoded map")
		}
		if inner["deep"] != 42.0 {
			t.Errorf("deep = %v, want 42.0", inner["deep"])
		}
	})

	t.Run("list value", func(t *testing.T) {
		input := map[string]interface{}{
			"items": map[string]interface{}{
				"fields": map[string]interface{}{
					"list": map[string]interface{}{
						"listValue": map[string]interface{}{
							"values": []interface{}{
								map[string]interface{}{"stringValue": "a"},
								map[string]interface{}{"stringValue": "b"},
							},
						},
					},
				},
			},
		}
		result := decodeStructFields(input)
		items, ok := result["items"].(map[string]interface{})
		if !ok {
			t.Fatal("items should be decoded map")
		}
		list, ok := items["list"].([]interface{})
		if !ok {
			t.Fatal("list should be decoded array")
		}
		if len(list) != 2 || list[0] != "a" || list[1] != "b" {
			t.Errorf("list = %v, want [a, b]", list)
		}
	})
}

func TestDecodeProtobufValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{"stringValue", map[string]interface{}{"stringValue": "hello"}, "hello"},
		{"numberValue", map[string]interface{}{"numberValue": 42.5}, 42.5},
		{"boolValue", map[string]interface{}{"boolValue": true}, true},
		{"nullValue", map[string]interface{}{"nullValue": 0}, nil},
		{"plain value", "not a map", "not a map"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeProtobufValue(tt.input)
			if got != tt.expected {
				t.Errorf("decodeProtobufValue() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Response validation tests
// ---------------------------------------------------------------------------

func TestValidateResponse(t *testing.T) {
	t.Run("valid response", func(t *testing.T) {
		response := map[string]interface{}{
			"name":  "test",
			"count": float64(42),
		}
		schema := map[string]string{
			"name":  "string",
			"count": "float",
		}
		err := validateResponse(response, schema, "", false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("type mismatch", func(t *testing.T) {
		response := map[string]interface{}{
			"name": float64(123), // Should be string.
		}
		schema := map[string]string{
			"name": "string",
		}
		err := validateResponse(response, schema, "", false)
		if err == nil {
			t.Error("expected validation error for type mismatch")
		}
	})

	t.Run("nil values skipped", func(t *testing.T) {
		response := map[string]interface{}{
			"name": nil,
		}
		schema := map[string]string{
			"name": "string",
		}
		err := validateResponse(response, schema, "", false)
		if err != nil {
			t.Errorf("nil values should be skipped: %v", err)
		}
	})

	t.Run("compatible types accepted", func(t *testing.T) {
		response := map[string]interface{}{
			"value": 42, // int -> "number", schema says "float".
		}
		schema := map[string]string{
			"value": "float",
		}
		err := validateResponse(response, schema, "", false)
		if err != nil {
			t.Errorf("number/float should be compatible: %v", err)
		}
	})

	t.Run("nested object", func(t *testing.T) {
		response := map[string]interface{}{
			"user": map[string]interface{}{
				"name": "test",
			},
		}
		schema := map[string]string{
			"user.name": "string",
		}
		err := validateResponse(response, schema, "", false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("protobuf struct skipped", func(t *testing.T) {
		response := map[string]interface{}{
			"data": map[string]interface{}{
				"anything": "goes",
			},
		}
		schema := map[string]string{
			"data": "google.protobuf.Struct",
		}
		err := validateResponse(response, schema, "", false)
		if err != nil {
			t.Errorf("Struct fields should be skipped: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Type mapping tests
// ---------------------------------------------------------------------------

func TestGoTypeToSchemaType(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{float64(1.5), "float"},
		{float32(1.5), "float"},
		{42, "number"},
		{int32(42), "number"},
		{int64(42), "number"},
		{"hello", "string"},
		{true, "boolean"},
		{[]int{1, 2, 3}, "[]int"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := goTypeToSchemaType(tt.input)
			if got != tt.expected {
				t.Errorf("goTypeToSchemaType(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestIsCompatibleType(t *testing.T) {
	tests := []struct {
		actual   string
		expected string
		result   bool
	}{
		{"number", "float", true},
		{"float", "number", true},
		{"string", "number", false},
		{"boolean", "string", false},
	}

	for _, tt := range tests {
		t.Run(tt.actual+"_"+tt.expected, func(t *testing.T) {
			if got := isCompatibleType(tt.actual, tt.expected); got != tt.result {
				t.Errorf("isCompatibleType(%q, %q) = %v, want %v",
					tt.actual, tt.expected, got, tt.result)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Metadata tests
// ---------------------------------------------------------------------------

func TestMetadata_Get(t *testing.T) {
	m := Metadata{
		"content-type": []string{"application/json"},
		"x-request-id": []string{"req-123", "req-456"},
	}

	if got := m.Get("content-type"); got != "application/json" {
		t.Errorf("Get(content-type) = %q, want application/json", got)
	}
	if got := m.Get("x-request-id"); got != "req-123" {
		t.Errorf("Get(x-request-id) = %q, want req-123 (first value)", got)
	}
	if got := m.Get("nonexistent"); got != "" {
		t.Errorf("Get(nonexistent) = %q, want empty", got)
	}
	if got := m.Get("Content-Type"); got != "application/json" {
		t.Errorf("Get(Content-Type) = %q, want application/json", got)
	}
}

func TestMetadata_Set(t *testing.T) {
	m := Metadata{}
	m.Set("X-Custom-Header", "value1")

	if got := m.Get("x-custom-header"); got != "value1" {
		t.Errorf("After Set, Get() = %q, want value1", got)
	}
}

// ---------------------------------------------------------------------------
// RPCError tests
// ---------------------------------------------------------------------------

func TestRPCError_Error(t *testing.T) {
	err := &RPCError{
		Code:    Internal,
		Message: "something went wrong",
	}

	expected := "rpc error: code = 13 desc = something went wrong"
	if got := err.Error(); got != expected {
		t.Errorf("Error() = %q, want %q", got, expected)
	}
}

// ---------------------------------------------------------------------------
// Capitalize tests
// ---------------------------------------------------------------------------

func TestCapitalize(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "Hello"},
		{"Hello", "Hello"},
		{"hELLO", "HELLO"},
		{"", ""},
		{"a", "A"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := Capitalize(tt.input); got != tt.expected {
				t.Errorf("Capitalize(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GRPCServer accessor test
// ---------------------------------------------------------------------------

func TestServer_GRPCServerAccessor(t *testing.T) {
	srv := newTestServer(t)
	gs := srv.GRPCServer()
	if gs == nil {
		t.Error("GRPCServer() should return non-nil *grpc.Server")
	}
}

// ---------------------------------------------------------------------------
// CallInfo context tests
// ---------------------------------------------------------------------------

func TestCallInfo_Context(t *testing.T) {
	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("key"), "value")
	call := &CallInfo{
		FullMethod: "/package.Service/Method",
		Context:    ctx,
		Request:    map[string]interface{}{"param": "value"},
		Metadata:   Metadata{"auth": []string{"token"}},
	}

	if call.Context.Value(ctxKey("key")) != "value" {
		t.Error("Context value not preserved")
	}
	if call.FullMethod != "/package.Service/Method" {
		t.Errorf("FullMethod = %q", call.FullMethod)
	}
	if call.Request["param"] != "value" {
		t.Errorf("Request param = %v", call.Request["param"])
	}
	if call.Metadata.Get("auth") != "token" {
		t.Errorf("Metadata auth = %v", call.Metadata.Get("auth"))
	}
}

// ---------------------------------------------------------------------------
// Chain interceptors test
// ---------------------------------------------------------------------------

func TestChainUnaryInterceptors(t *testing.T) {
	var order []string

	i1 := UnaryInterceptor(func(call *CallInfo, callback Callback, next NextFunc) {
		order = append(order, "i1")
		next(nil)
	})
	i2 := UnaryInterceptor(func(call *CallInfo, callback Callback, next NextFunc) {
		order = append(order, "i2")
		next(nil)
	})

	chained := ChainUnaryInterceptors(i1, i2)

	var finalNextCalled bool
	chained(&CallInfo{}, func(err error, resp map[string]interface{}) {}, func(err error) {
		finalNextCalled = true
	})

	if !finalNextCalled {
		t.Error("final next should be called")
	}
	if len(order) != 2 || order[0] != "i1" || order[1] != "i2" {
		t.Errorf("order = %v, want [i1, i2]", order)
	}
}

func TestChainUnaryInterceptors_Empty(t *testing.T) {
	chained := ChainUnaryInterceptors()

	var nextCalled bool
	chained(&CallInfo{}, func(err error, resp map[string]interface{}) {}, func(err error) {
		nextCalled = true
	})

	if !nextCalled {
		t.Error("empty chain should call next immediately")
	}
}

// ---------------------------------------------------------------------------
// comparePayloads test
// ---------------------------------------------------------------------------

func TestComparePayloads(t *testing.T) {
	t.Run("matching payloads", func(t *testing.T) {
		expected := map[string]interface{}{"company_id": float64(42)}
		decoded := map[string]interface{}{
			"company_id": float64(42),
			"iat":        float64(1234567890),
			"exp":        float64(9999999999),
		}
		if !comparePayloads(expected, decoded) {
			t.Error("matching payloads should return true")
		}
	})

	t.Run("mismatched payloads", func(t *testing.T) {
		expected := map[string]interface{}{"company_id": float64(42)}
		decoded := map[string]interface{}{
			"company_id": float64(99),
			"iat":        float64(1234567890),
		}
		if comparePayloads(expected, decoded) {
			t.Error("mismatched payloads should return false")
		}
	})

	t.Run("extra non-standard claims", func(t *testing.T) {
		expected := map[string]interface{}{"company_id": float64(42)}
		decoded := map[string]interface{}{
			"company_id":  float64(42),
			"extra_field": "surprise",
			"iat":         float64(1234567890),
		}
		if comparePayloads(expected, decoded) {
			t.Error("extra non-standard claims should cause mismatch")
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrency safety test
// ---------------------------------------------------------------------------

func TestServer_ConcurrentAccess(t *testing.T) {
	srv := newTestServer(t)
	cleanup := startServer(t, srv)
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = srv.IsRunning()
			_ = srv.Services()
			_ = srv.ProtoPath()
			_ = srv.Config()
			_ = srv.GRPCServer()
			_ = srv.Listener()
		}()
	}
	wg.Wait()
}
