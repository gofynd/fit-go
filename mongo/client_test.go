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

package mongo

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Mock connection for testing
// ---------------------------------------------------------------------------

type mockConnection struct {
	pingCalled  atomic.Bool
	closeCalled atomic.Bool
	pingErr     error
	closeErr    error
}

func (m *mockConnection) Ping(ctx context.Context) error {
	m.pingCalled.Store(true)
	return m.pingErr
}

func (m *mockConnection) Close(ctx context.Context) error {
	m.closeCalled.Store(true)
	return m.closeErr
}

func (m *mockConnection) Raw() interface{} {
	return m
}

// mockDial creates a mock connection for testing.
func mockDial(ctx context.Context, uri string, opts *DialOptions) (Connection, error) {
	return &mockConnection{}, nil
}

// ---------------------------------------------------------------------------
// Environment variable regex tests
// ---------------------------------------------------------------------------

func TestEnvRegex(t *testing.T) {
	tests := []struct {
		input           string
		shouldMatch     bool
		expectedService string
		expectedType    string
	}{
		{"MONGO_USERS_READ_WRITE", true, "USERS", "WRITE"},
		{"MONGO_ORDERS_READ_ONLY", true, "ORDERS", "ONLY"},
		{"MONGO_FOO_BAR_READ_WRITE", true, "FOO_BAR", "WRITE"},
		{"MONGO_X_READ_ONLY", true, "X", "ONLY"},
		{"MONGO__READ_WRITE", false, "", ""},      // empty service
		{"REDIS_USERS_READ_WRITE", false, "", ""}, // wrong prefix
		{"MONGO_USERS_WRITE", false, "", ""},      // missing READ_
		{"MONGO_USERS_READ_", false, "", ""},      // missing type
		{"mongo_users_read_write", false, "", ""}, // lowercase
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			matches := envRegex.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				if matches == nil {
					t.Errorf("Expected %q to match", tt.input)
					return
				}
				if matches[1] != tt.expectedService {
					t.Errorf("Service = %q, want %q", matches[1], tt.expectedService)
				}
				if matches[2] != tt.expectedType {
					t.Errorf("Type = %q, want %q", matches[2], tt.expectedType)
				}
			} else {
				if matches != nil {
					t.Errorf("Expected %q not to match, but got %v", tt.input, matches)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// getAppName and getDeploymentName tests
// ---------------------------------------------------------------------------

func TestGetDeploymentName(t *testing.T) {
	tests := []struct {
		podName  string
		expected string
	}{
		{"order-service-dply-abc123-xyz", "order-service-dply"},
		{"payment-cron-job-12345", "payment-cron"},
		{"simple-pod-name", ""},
		{"", ""},
		{"dply-start", "dply"}, // edge case: dply at start
		{"my-cron", "my-cron"}, // edge case: ends with cron
	}

	for _, tt := range tests {
		t.Run(tt.podName, func(t *testing.T) {
			if got := getDeploymentName(tt.podName); got != tt.expected {
				t.Errorf("getDeploymentName(%q) = %q, want %q", tt.podName, got, tt.expected)
			}
		})
	}
}

func TestGetAppName(t *testing.T) {
	// Save and restore env vars
	origPodName := os.Getenv("K8S_POD_NAME")
	origNamespace := os.Getenv("K8S_POD_NAMESPACE")
	origServiceName := os.Getenv("SERVICE_NAME")
	defer func() {
		os.Setenv("K8S_POD_NAME", origPodName)
		os.Setenv("K8S_POD_NAMESPACE", origNamespace)
		os.Setenv("SERVICE_NAME", origServiceName)
	}()

	tests := []struct {
		name        string
		podName     string
		namespace   string
		serviceName string
		expected    string
	}{
		{
			name:        "full deployment name with namespace",
			podName:     "order-service-dply-abc123",
			namespace:   "production",
			serviceName: "order-service",
			expected:    "production-order-service-dply",
		},
		{
			name:        "cron job with namespace",
			podName:     "payment-cron-12345",
			namespace:   "staging",
			serviceName: "payment-processor",
			expected:    "staging-payment-cron",
		},
		{
			name:        "no deployment pattern, use service name",
			podName:     "random-pod-name",
			namespace:   "production",
			serviceName: "my-service",
			expected:    "production-my-service",
		},
		{
			name:        "default namespace ignored",
			podName:     "order-dply-abc",
			namespace:   "default",
			serviceName: "order",
			expected:    "order-dply",
		},
		{
			name:        "empty namespace",
			podName:     "service-dply-xyz",
			namespace:   "",
			serviceName: "service",
			expected:    "service-dply",
		},
		{
			name:        "no pod name, use service name",
			podName:     "",
			namespace:   "",
			serviceName: "standalone-service",
			expected:    "standalone-service",
		},
		{
			name:        "only namespace",
			podName:     "",
			namespace:   "test-ns",
			serviceName: "",
			expected:    "test-ns",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("K8S_POD_NAME", tt.podName)
			os.Setenv("K8S_POD_NAMESPACE", tt.namespace)
			os.Setenv("SERVICE_NAME", tt.serviceName)

			if got := getAppName(); got != tt.expected {
				t.Errorf("getAppName() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// envBool and envWithFallback tests
// ---------------------------------------------------------------------------

func TestEnvBool(t *testing.T) {
	key := "TEST_ENV_BOOL"
	defer os.Unsetenv(key)

	tests := []struct {
		value      string
		defaultVal bool
		expected   bool
	}{
		{"true", false, true},
		{"TRUE", false, true},
		{" true ", false, true},
		{"false", true, false},
		{"FALSE", true, false},
		{"", true, true},   // use default
		{"", false, false}, // use default
		{"yes", false, false},
		{"1", false, false},
		{"invalid", true, true}, // use default
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if tt.value != "" {
				os.Setenv(key, tt.value)
			} else {
				os.Unsetenv(key)
			}

			if got := envBool(key, tt.defaultVal); got != tt.expected {
				t.Errorf("envBool(%q, %v) with value %q = %v, want %v",
					key, tt.defaultVal, tt.value, got, tt.expected)
			}
		})
	}
}

func TestEnvWithFallback(t *testing.T) {
	// Save and restore
	orig1 := os.Getenv("TEST_FALLBACK_1")
	orig2 := os.Getenv("TEST_FALLBACK_2")
	defer func() {
		os.Setenv("TEST_FALLBACK_1", orig1)
		os.Setenv("TEST_FALLBACK_2", orig2)
	}()

	t.Run("first key has value", func(t *testing.T) {
		os.Setenv("TEST_FALLBACK_1", "first")
		os.Setenv("TEST_FALLBACK_2", "second")

		got := envWithFallback("TEST_FALLBACK_1", "TEST_FALLBACK_2")
		if got != "first" {
			t.Errorf("envWithFallback() = %q, want 'first'", got)
		}
	})

	t.Run("fallback to second key", func(t *testing.T) {
		os.Unsetenv("TEST_FALLBACK_1")
		os.Setenv("TEST_FALLBACK_2", "second")

		got := envWithFallback("TEST_FALLBACK_1", "TEST_FALLBACK_2")
		if got != "second" {
			t.Errorf("envWithFallback() = %q, want 'second'", got)
		}
	})

	t.Run("all keys empty", func(t *testing.T) {
		os.Unsetenv("TEST_FALLBACK_1")
		os.Unsetenv("TEST_FALLBACK_2")

		got := envWithFallback("TEST_FALLBACK_1", "TEST_FALLBACK_2")
		if got != "" {
			t.Errorf("envWithFallback() = %q, want empty", got)
		}
	})
}

// ---------------------------------------------------------------------------
// buildDialOptions tests
// ---------------------------------------------------------------------------

func TestBuildDialOptions(t *testing.T) {
	// Clean up env vars
	defer func() {
		for _, k := range []string{
			"MONGO_USERS_READ_WRITE_MAX_POOL_SIZE",
			"MONGO_USERS_READ_WRITE_MIN_POOL_SIZE",
			"MONGO_USERS_READ_WRITE_MAX_IDLE_TIME",
			"MONGO_USERS_READ_WRITE_CONNECTION_TIMEOUT",
			"MONGO_USERS_READ_WRITE_SOCKET_TIMEOUT",
			"K8S_POD_NAME",
			"K8S_POD_NAMESPACE",
			"SERVICE_NAME",
		} {
			os.Unsetenv(k)
		}
	}()

	t.Run("reads env vars", func(t *testing.T) {
		os.Setenv("MONGO_USERS_READ_WRITE_MAX_POOL_SIZE", "100")
		os.Setenv("MONGO_USERS_READ_WRITE_MIN_POOL_SIZE", "10")
		os.Setenv("MONGO_USERS_READ_WRITE_MAX_IDLE_TIME", "30000")
		os.Setenv("SERVICE_NAME", "test-service")

		opts := ConnectionOptions{
			ConnectTimeout: 5 * time.Second,
		}

		dialOpts := buildDialOptions("USERS", "write", "users", opts, true, false)

		if dialOpts.MaxPoolSize != 100 {
			t.Errorf("MaxPoolSize = %d, want 100", dialOpts.MaxPoolSize)
		}
		if dialOpts.MinPoolSize != 10 {
			t.Errorf("MinPoolSize = %d, want 10", dialOpts.MinPoolSize)
		}
		if dialOpts.MaxIdleTimeMS != 30000 {
			t.Errorf("MaxIdleTimeMS = %d, want 30000", dialOpts.MaxIdleTimeMS)
		}
		if dialOpts.AutoIndex != true {
			t.Error("AutoIndex should be true")
		}
		if dialOpts.AutoCreate != false {
			t.Error("AutoCreate should be false")
		}
	})

	t.Run("applies caller overrides", func(t *testing.T) {
		os.Setenv("MONGO_ORDERS_READ_ONLY_MAX_POOL_SIZE", "50")

		opts := ConnectionOptions{
			ConnectTimeout: time.Minute,
			PerService: map[string]ServicePoolOverrides{
				"orders": {
					Read: &PoolOverrides{
						MaxPoolSize: 200, // override env var
						MinPoolSize: 25,
					},
				},
			},
		}

		dialOpts := buildDialOptions("ORDERS", "read", "orders", opts, false, true)

		if dialOpts.MaxPoolSize != 200 {
			t.Errorf("MaxPoolSize = %d, want 200 (override)", dialOpts.MaxPoolSize)
		}
		if dialOpts.MinPoolSize != 25 {
			t.Errorf("MinPoolSize = %d, want 25 (override)", dialOpts.MinPoolSize)
		}
	})

	t.Run("connect timeout from opts", func(t *testing.T) {
		opts := ConnectionOptions{
			ConnectTimeout: 30 * time.Second,
		}

		dialOpts := buildDialOptions("SVC", "write", "svc", opts, false, false)

		if dialOpts.ConnectTimeoutMS != 30000 {
			t.Errorf("ConnectTimeoutMS = %d, want 30000", dialOpts.ConnectTimeoutMS)
		}
	})
}

// ---------------------------------------------------------------------------
// applyPoolOverrides tests
// ---------------------------------------------------------------------------

func TestApplyPoolOverrides(t *testing.T) {
	d := &DialOptions{
		MaxPoolSize: 50,
		MinPoolSize: 5,
	}

	po := &PoolOverrides{
		MaxPoolSize:   100,
		MaxIdleTimeMS: 60000,
		// MinPoolSize intentionally 0 - should not override
	}

	applyPoolOverrides(d, po)

	if d.MaxPoolSize != 100 {
		t.Errorf("MaxPoolSize = %d, want 100", d.MaxPoolSize)
	}
	if d.MinPoolSize != 5 {
		t.Errorf("MinPoolSize = %d, want 5 (unchanged)", d.MinPoolSize)
	}
	if d.MaxIdleTimeMS != 60000 {
		t.Errorf("MaxIdleTimeMS = %d, want 60000", d.MaxIdleTimeMS)
	}
}

// ---------------------------------------------------------------------------
// Client tests
// ---------------------------------------------------------------------------

func TestClient_Service(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"users": {
				Read:  &mockConnection{},
				Write: &mockConnection{},
			},
			"orders": {
				Read: &mockConnection{},
			},
		},
	}

	t.Run("existing service", func(t *testing.T) {
		sc := c.Service("users")
		if sc == nil {
			t.Fatal("Expected service connection for 'users'")
		}
		if sc.Read == nil || sc.Write == nil {
			t.Error("Expected both read and write connections")
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		sc := c.Service("USERS")
		if sc == nil {
			t.Error("Service lookup should be case-insensitive")
		}
	})

	t.Run("non-existent service", func(t *testing.T) {
		sc := c.Service("nonexistent")
		if sc != nil {
			t.Error("Expected nil for non-existent service")
		}
	})
}

func TestClient_Services(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"users":  {Read: &mockConnection{}},
			"orders": {Write: &mockConnection{}},
		},
	}

	services := c.Services()

	if len(services) != 2 {
		t.Errorf("Services() returned %d services, want 2", len(services))
	}

	// Verify it's a copy
	services["users"] = nil
	if c.services["users"] == nil {
		t.Error("Services() should return a copy, not the original map")
	}
}

func TestClient_Ping(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"users": {
					Read:  &mockConnection{},
					Write: &mockConnection{},
				},
			},
		}

		ctx := context.Background()
		if err := c.Ping(ctx); err != nil {
			t.Errorf("Ping() error = %v, want nil", err)
		}
	})

	t.Run("read connection fails", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"users": {
					Read:  &mockConnection{pingErr: context.DeadlineExceeded},
					Write: &mockConnection{},
				},
			},
		}

		ctx := context.Background()
		err := c.Ping(ctx)
		if err == nil {
			t.Error("Ping() should return error when connection fails")
		}
		if !strings.Contains(err.Error(), "users_read") {
			t.Errorf("Error should mention 'users_read', got: %v", err)
		}
	})
}

func TestClient_Close(t *testing.T) {
	read := &mockConnection{}
	write := &mockConnection{}

	c := &Client{
		services: map[string]*ServiceConnection{
			"users": {
				Read:  read,
				Write: write,
			},
		},
	}

	err := c.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}

	if !read.closeCalled.Load() {
		t.Error("Read connection Close() should be called")
	}
	if !write.closeCalled.Load() {
		t.Error("Write connection Close() should be called")
	}

	// Services should be cleared
	if len(c.services) != 0 {
		t.Error("Services map should be cleared after Close()")
	}
}

func TestClient_HealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"users": {Read: &mockConnection{}},
			},
		}

		checkFn := c.HealthCheck()
		result := checkFn()
		if result != "" {
			t.Errorf("HealthCheck() = %q, want empty (healthy)", result)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"users": {Read: &mockConnection{pingErr: context.DeadlineExceeded}},
			},
		}

		checkFn := c.HealthCheck()
		result := checkFn()
		if result == "" {
			t.Error("HealthCheck() should return error message")
		}
	})
}

// ---------------------------------------------------------------------------
// Init tests (with mock dial)
// ---------------------------------------------------------------------------

func TestInit(t *testing.T) {
	// Save and restore env vars
	origEnv := os.Environ()
	defer func() {
		os.Clearenv()
		for _, e := range origEnv {
			idx := strings.IndexByte(e, '=')
			if idx > 0 {
				os.Setenv(e[:idx], e[idx+1:])
			}
		}
	}()

	t.Run("no dial func", func(t *testing.T) {
		_, err := Init(ConnectionOptions{})
		if err == nil || !strings.Contains(err.Error(), "DialFunc must be provided") {
			t.Errorf("Expected DialFunc error, got: %v", err)
		}
	})

	t.Run("no mongo env vars", func(t *testing.T) {
		// Clear any MONGO_ env vars
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "MONGO_") {
				idx := strings.IndexByte(e, '=')
				if idx > 0 {
					os.Unsetenv(e[:idx])
				}
			}
		}

		c, err := Init(ConnectionOptions{Dial: mockDial})
		if err != nil {
			t.Fatalf("Init() error = %v, want nil", err)
		}
		if len(c.services) != 0 {
			t.Errorf("Expected no services, got %d", len(c.services))
		}
	})

	t.Run("discovers env vars and creates connections", func(t *testing.T) {
		os.Setenv("MONGO_USERS_READ_WRITE", "mongodb://localhost:27017/users")
		os.Setenv("MONGO_USERS_READ_ONLY", "mongodb://localhost:27017/users?readPreference=secondary")
		os.Setenv("MONGO_ORDERS_READ_WRITE", "mongodb://localhost:27017/orders")
		defer func() {
			os.Unsetenv("MONGO_USERS_READ_WRITE")
			os.Unsetenv("MONGO_USERS_READ_ONLY")
			os.Unsetenv("MONGO_ORDERS_READ_WRITE")
		}()

		c, err := Init(ConnectionOptions{
			Dial:           mockDial,
			ConnectTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Init() error = %v, want nil", err)
		}

		// Verify users service has both connections
		users := c.Service("users")
		if users == nil {
			t.Fatal("Expected 'users' service")
		}
		if users.Read == nil {
			t.Error("Expected read connection for 'users'")
		}
		if users.Write == nil {
			t.Error("Expected write connection for 'users'")
		}

		// Verify orders service has only write
		orders := c.Service("orders")
		if orders == nil {
			t.Fatal("Expected 'orders' service")
		}
		if orders.Write == nil {
			t.Error("Expected write connection for 'orders'")
		}
		if orders.Read != nil {
			t.Error("Did not expect read connection for 'orders'")
		}
	})

	t.Run("default connect timeout", func(t *testing.T) {
		os.Setenv("MONGO_TEST_READ_WRITE", "mongodb://localhost:27017/test")
		defer os.Unsetenv("MONGO_TEST_READ_WRITE")

		var capturedOpts *DialOptions
		captureDial := func(ctx context.Context, uri string, opts *DialOptions) (Connection, error) {
			capturedOpts = opts
			return &mockConnection{}, nil
		}

		_, err := Init(ConnectionOptions{
			Dial: captureDial,
			// No ConnectTimeout - should default to 1 hour
		})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		if capturedOpts == nil {
			t.Fatal("DialOptions not captured")
		}
		// 1 hour = 3600000 milliseconds
		if capturedOpts.ConnectTimeoutMS != 3600000 {
			t.Errorf("ConnectTimeoutMS = %d, want 3600000 (1 hour)", capturedOpts.ConnectTimeoutMS)
		}
	})
}

// ---------------------------------------------------------------------------
// GSM resolver tests
// ---------------------------------------------------------------------------

func TestSetGSMResolver(t *testing.T) {
	// Reset after test
	defer func() {
		gsmMu.Lock()
		gsmResolver = nil
		gsmMu.Unlock()
	}()

	resolver := func(name, version string) (string, error) {
		return "resolved-" + name, nil
	}

	SetGSMResolver(resolver)

	gsmMu.RLock()
	if gsmResolver == nil {
		t.Error("GSM resolver should be set")
	}
	gsmMu.RUnlock()
}

func TestFetchGSMSecret_NoResolver(t *testing.T) {
	// Ensure no resolver is set
	gsmMu.Lock()
	gsmResolver = nil
	gsmMu.Unlock()

	_, err := fetchGSMSecret("test-secret")
	if err == nil || !strings.Contains(err.Error(), "GSM resolver not configured") {
		t.Errorf("Expected 'GSM resolver not configured' error, got: %v", err)
	}
}

func TestFetchGSMSecret_WithResolver(t *testing.T) {
	resolver := func(name, version string) (string, error) {
		return "mongodb://resolved:" + version + "/" + name, nil
	}
	SetGSMResolver(resolver)
	defer func() {
		gsmMu.Lock()
		gsmResolver = nil
		gsmMu.Unlock()
	}()

	os.Setenv("DB_CONNECTION_SECRET_VERSION", "v2")
	defer os.Unsetenv("DB_CONNECTION_SECRET_VERSION")

	result, err := fetchGSMSecret("my-db-secret")
	if err != nil {
		t.Fatalf("fetchGSMSecret() error = %v", err)
	}

	expected := "mongodb://resolved:v2/my-db-secret"
	if result != expected {
		t.Errorf("fetchGSMSecret() = %q, want %q", result, expected)
	}
}

func TestFetchGSMSecret_DefaultVersion(t *testing.T) {
	var capturedVersion string
	resolver := func(name, version string) (string, error) {
		capturedVersion = version
		return "connection-string", nil
	}
	SetGSMResolver(resolver)
	defer func() {
		gsmMu.Lock()
		gsmResolver = nil
		gsmMu.Unlock()
	}()

	os.Unsetenv("DB_CONNECTION_SECRET_VERSION")

	_, err := fetchGSMSecret("secret")
	if err != nil {
		t.Fatalf("fetchGSMSecret() error = %v", err)
	}

	if capturedVersion != "latest" {
		t.Errorf("version = %q, want 'latest'", capturedVersion)
	}
}

// ---------------------------------------------------------------------------
// resolveConnectionString tests
// ---------------------------------------------------------------------------

func TestResolveConnectionString(t *testing.T) {
	// Save and restore
	orig := os.Getenv("DB_CONNECTION_PROVIDER")
	defer os.Setenv("DB_CONNECTION_PROVIDER", orig)

	t.Run("direct connection string", func(t *testing.T) {
		os.Unsetenv("DB_CONNECTION_PROVIDER")

		result, err := resolveConnectionString("mongodb://localhost:27017/test")
		if err != nil {
			t.Fatalf("resolveConnectionString() error = %v", err)
		}
		if result != "mongodb://localhost:27017/test" {
			t.Errorf("result = %q, want original string", result)
		}
	})

	t.Run("GSM provider", func(t *testing.T) {
		os.Setenv("DB_CONNECTION_PROVIDER", "GSM")

		resolver := func(name, version string) (string, error) {
			return "gsm-resolved-" + name, nil
		}
		SetGSMResolver(resolver)
		defer func() {
			gsmMu.Lock()
			gsmResolver = nil
			gsmMu.Unlock()
		}()

		result, err := resolveConnectionString("secret-name")
		if err != nil {
			t.Fatalf("resolveConnectionString() error = %v", err)
		}
		if result != "gsm-resolved-secret-name" {
			t.Errorf("result = %q, want 'gsm-resolved-secret-name'", result)
		}
	})

	t.Run("GSM provider case insensitive", func(t *testing.T) {
		os.Setenv("DB_CONNECTION_PROVIDER", "gsm")

		resolver := func(name, version string) (string, error) {
			return "resolved", nil
		}
		SetGSMResolver(resolver)
		defer func() {
			gsmMu.Lock()
			gsmResolver = nil
			gsmMu.Unlock()
		}()

		result, err := resolveConnectionString("secret")
		if err != nil {
			t.Fatalf("resolveConnectionString() error = %v", err)
		}
		if result != "resolved" {
			t.Error("GSM resolution should be case-insensitive")
		}
	})
}

// ---------------------------------------------------------------------------
// TLS config tests (without actual certificates)
// ---------------------------------------------------------------------------

func TestLoadTLSConfig_NoCerts(t *testing.T) {
	// Clear any SSL env vars
	for _, k := range []string{
		"MONGO_TEST_SSL_CA",
		"MONGO_TEST_SSL_CERT",
		"MONGO_TEST_SSL_KEY",
		"MONGO_SSL_CA",
		"MONGO_SSL_CERT",
		"MONGO_SSL_KEY",
	} {
		os.Unsetenv(k)
	}

	cfg := loadTLSConfig("MONGO", "TEST")
	if cfg != nil {
		t.Error("loadTLSConfig should return nil when no certs are configured")
	}
}

func TestLoadTLSConfig_PartialCerts(t *testing.T) {
	// Only CA is set
	os.Setenv("MONGO_TEST_SSL_CA", "/path/to/ca.pem")
	defer os.Unsetenv("MONGO_TEST_SSL_CA")

	cfg := loadTLSConfig("MONGO", "TEST")
	if cfg != nil {
		t.Error("loadTLSConfig should return nil when certs are incomplete")
	}
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

func TestClientConcurrentAccess(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"users": {Read: &mockConnection{}, Write: &mockConnection{}},
		},
	}

	done := make(chan bool)

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func() {
			_ = c.Service("users")
			_ = c.Services()
			done <- true
		}()
	}

	// Concurrent ping
	for i := 0; i < 5; i++ {
		go func() {
			_ = c.Ping(context.Background())
			done <- true
		}()
	}

	// Wait for all
	for i := 0; i < 15; i++ {
		<-done
	}
}
