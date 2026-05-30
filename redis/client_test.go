// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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

package redis

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Mock connection for testing
// ---------------------------------------------------------------------------

type mockConnection struct {
	pingCalled  bool
	closeCalled bool
	pingErr     error
	closeErr    error
	isCluster   bool
}

func (m *mockConnection) Ping(ctx context.Context) error {
	m.pingCalled = true
	return m.pingErr
}

func (m *mockConnection) Close() error {
	m.closeCalled = true
	return m.closeErr
}

func (m *mockConnection) Raw() interface{} {
	return m
}

func (m *mockConnection) IsCluster() bool {
	return m.isCluster
}

// mockDial creates a mock standalone connection.
func mockDial(ctx context.Context, opts *DialOptions) (Connection, error) {
	return &mockConnection{}, nil
}

// mockClusterDial creates a mock cluster connection.
func mockClusterDial(ctx context.Context, opts *ClusterDialOptions) (Connection, error) {
	return &mockConnection{isCluster: true}, nil
}

// mockSentinelDial creates a mock sentinel connection.
func mockSentinelDial(ctx context.Context, opts *SentinelDialOptions) (Connection, error) {
	return &mockConnection{}, nil
}

// ---------------------------------------------------------------------------
// Environment regex tests
// ---------------------------------------------------------------------------

func TestEnvPattern(t *testing.T) {
	tests := []struct {
		input           string
		shouldMatch     bool
		expectedService string
		expectedType    string
	}{
		{"REDIS_CACHE_READ_WRITE", true, "CACHE", "WRITE"},
		{"REDIS_SESSIONS_READ_ONLY", true, "SESSIONS", "ONLY"},
		{"REDIS_FOO_BAR_READ_WRITE", true, "FOO_BAR", "WRITE"},
		{"REDIS_X_READ_ONLY", true, "X", "ONLY"},
		{"REDIS__READ_WRITE", false, "", ""},      // empty service
		{"MONGO_CACHE_READ_WRITE", false, "", ""}, // wrong prefix
		{"REDIS_CACHE_WRITE", false, "", ""},      // missing READ_
		{"redis_cache_read_write", false, "", ""}, // lowercase
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			matches := envPattern.FindStringSubmatch(tt.input)
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
					t.Errorf("Expected %q not to match", tt.input)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// URI parsing tests
// ---------------------------------------------------------------------------

func TestParseRedisURI(t *testing.T) {
	tests := []struct {
		name      string
		uri       string
		wantHosts []string
		wantDB    int
		wantUser  string
		wantPass  string
		wantOpts  map[string]string
		wantErr   bool
	}{
		{
			name:      "simple standalone",
			uri:       "redis://localhost:6379/0",
			wantHosts: []string{"localhost:6379"},
			wantDB:    0,
		},
		{
			name:      "with auth",
			uri:       "redis://user:pass@localhost:6379/1",
			wantHosts: []string{"localhost:6379"},
			wantDB:    1,
			wantUser:  "user",
			wantPass:  "pass",
		},
		{
			name:      "cluster multiple hosts",
			uri:       "redis://host1:6379,host2:6380,host3:6381",
			wantHosts: []string{"host1:6379", "host2:6380", "host3:6381"},
			wantDB:    0,
		},
		{
			name:      "with query params",
			uri:       "redis://localhost:6379/0?sharded_db=true",
			wantHosts: []string{"localhost:6379"},
			wantOpts:  map[string]string{"sharded_db": "true"},
		},
		{
			name:      "sentinel",
			uri:       "redis-sentinel://sentinel1:26379,sentinel2:26379?master=mymaster",
			wantHosts: []string{"sentinel1:26379", "sentinel2:26379"},
			wantOpts:  map[string]string{"master": "mymaster"},
		},
		{
			name:      "default host",
			uri:       "redis://",
			wantHosts: []string{"localhost:6379"},
		},
		{
			name:      "no scheme",
			uri:       "localhost:6379",
			wantHosts: []string{"localhost:6379"},
		},
		{
			name:      "host without port",
			uri:       "redis://myhost/2",
			wantHosts: []string{"myhost:6379"},
			wantDB:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseRedisURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseRedisURI() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			if len(parsed.Hosts) != len(tt.wantHosts) {
				t.Errorf("Hosts count = %d, want %d", len(parsed.Hosts), len(tt.wantHosts))
			} else {
				for i, h := range parsed.Hosts {
					if h.Addr() != tt.wantHosts[i] {
						t.Errorf("Host[%d] = %q, want %q", i, h.Addr(), tt.wantHosts[i])
					}
				}
			}

			if parsed.DB != tt.wantDB {
				t.Errorf("DB = %d, want %d", parsed.DB, tt.wantDB)
			}

			if parsed.Username != tt.wantUser {
				t.Errorf("Username = %q, want %q", parsed.Username, tt.wantUser)
			}

			if parsed.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", parsed.Password, tt.wantPass)
			}

			for k, v := range tt.wantOpts {
				if parsed.Options[k] != v {
					t.Errorf("Options[%q] = %q, want %q", k, parsed.Options[k], v)
				}
			}
		})
	}
}

func TestHostPort_Addr(t *testing.T) {
	tests := []struct {
		host     string
		port     string
		expected string
	}{
		{"localhost", "6379", "localhost:6379"},
		{"myhost", "", "myhost:6379"}, // default port
		{"10.0.0.1", "6380", "10.0.0.1:6380"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			hp := hostPort{Host: tt.host, Port: tt.port}
			if got := hp.Addr(); got != tt.expected {
				t.Errorf("Addr() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// getRedisEnvOptions tests
// ---------------------------------------------------------------------------

func TestGetRedisEnvOptions(t *testing.T) {
	// Save and restore
	envs := []string{
		"REDIS_CACHE_READ_WRITE_CONNECTION_TIMEOUT",
		"REDIS_CACHE_READ_WRITE_SOCKET_TIMEOUT",
		"REDIS_CACHE_READ_WRITE_KEEP_ALIVE",
	}
	origVals := make(map[string]string)
	for _, k := range envs {
		origVals[k] = os.Getenv(k)
	}
	defer func() {
		for k, v := range origVals {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	t.Run("reads env vars", func(t *testing.T) {
		os.Setenv("REDIS_CACHE_READ_WRITE_CONNECTION_TIMEOUT", "5000")
		os.Setenv("REDIS_CACHE_READ_WRITE_SOCKET_TIMEOUT", "10000")
		os.Setenv("REDIS_CACHE_READ_WRITE_KEEP_ALIVE", "30000")

		opts := getRedisEnvOptions("CACHE", "write")

		if opts.ConnectTimeout != 5*time.Second {
			t.Errorf("ConnectTimeout = %v, want 5s", opts.ConnectTimeout)
		}
		if opts.SocketTimeout != 10*time.Second {
			t.Errorf("SocketTimeout = %v, want 10s", opts.SocketTimeout)
		}
		if opts.KeepAlive != 30*time.Second {
			t.Errorf("KeepAlive = %v, want 30s", opts.KeepAlive)
		}
	})

	t.Run("read type uses READ_ONLY prefix", func(t *testing.T) {
		os.Setenv("REDIS_SESSIONS_READ_ONLY_CONNECTION_TIMEOUT", "3000")
		defer os.Unsetenv("REDIS_SESSIONS_READ_ONLY_CONNECTION_TIMEOUT")

		opts := getRedisEnvOptions("SESSIONS", "read")

		if opts.ConnectTimeout != 3*time.Second {
			t.Errorf("ConnectTimeout = %v, want 3s", opts.ConnectTimeout)
		}
	})

	t.Run("missing env vars", func(t *testing.T) {
		for _, k := range envs {
			os.Unsetenv(k)
		}

		opts := getRedisEnvOptions("CACHE", "write")

		if opts.ConnectTimeout != 0 {
			t.Errorf("ConnectTimeout = %v, want 0 (unset)", opts.ConnectTimeout)
		}
	})
}

// ---------------------------------------------------------------------------
// getAppName and getDeploymentName tests
// ---------------------------------------------------------------------------

func TestGetDeploymentName(t *testing.T) {
	tests := []struct {
		podName  string
		expected string
	}{
		{"cache-service-dply-abc123", "cache-service-dply"},
		{"redis-cron-job-12345", "redis-cron"},
		{"simple-pod", ""},
		{"", ""},
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
	// Save and restore
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
			name:        "full deployment",
			podName:     "redis-svc-dply-xyz",
			namespace:   "production",
			serviceName: "redis-svc",
			expected:    "production-redis-svc-dply",
		},
		{
			name:        "service name fallback",
			podName:     "random-pod",
			namespace:   "",
			serviceName: "my-service",
			expected:    "my-service",
		},
		{
			name:        "namespace only",
			podName:     "",
			namespace:   "staging",
			serviceName: "",
			expected:    "staging",
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
// Client tests
// ---------------------------------------------------------------------------

func TestClient_Service(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"cache": {
				Read:  &mockConnection{},
				Write: &mockConnection{},
			},
		},
	}

	t.Run("existing service", func(t *testing.T) {
		sc := c.Service("cache")
		if sc == nil {
			t.Fatal("Expected service connection for 'cache'")
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		sc := c.Service("CACHE")
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
			"cache":    {Read: &mockConnection{}},
			"sessions": {Write: &mockConnection{}},
		},
	}

	services := c.Services()

	if len(services) != 2 {
		t.Errorf("Services() returned %d services, want 2", len(services))
	}

	// Verify it's a copy
	services["cache"] = nil
	if c.services["cache"] == nil {
		t.Error("Services() should return a copy")
	}
}

func TestClient_Ping(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"cache": {
					Read:  &mockConnection{},
					Write: &mockConnection{},
				},
			},
		}

		if err := c.Ping(context.Background()); err != nil {
			t.Errorf("Ping() error = %v, want nil", err)
		}
	})

	t.Run("connection fails", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"cache": {
					Read: &mockConnection{pingErr: context.DeadlineExceeded},
				},
			},
		}

		err := c.Ping(context.Background())
		if err == nil {
			t.Error("Ping() should return error")
		}
		if !strings.Contains(err.Error(), "cache_read") {
			t.Errorf("Error should mention 'cache_read', got: %v", err)
		}
	})
}

func TestClient_Close(t *testing.T) {
	read := &mockConnection{}
	write := &mockConnection{}

	c := &Client{
		services: map[string]*ServiceConnection{
			"cache": {Read: read, Write: write},
		},
	}

	err := c.Close()
	if err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}

	if !read.closeCalled {
		t.Error("Read connection Close() should be called")
	}
	if !write.closeCalled {
		t.Error("Write connection Close() should be called")
	}

	if len(c.services) != 0 {
		t.Error("Services should be cleared after Close()")
	}
}

func TestClient_HealthCheck(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		c := &Client{
			services: map[string]*ServiceConnection{
				"cache": {Read: &mockConnection{}},
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
				"cache": {Read: &mockConnection{pingErr: context.DeadlineExceeded}},
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
// Init tests
// ---------------------------------------------------------------------------

func TestInit(t *testing.T) {
	// Save and restore env
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

	t.Run("no redis env vars", func(t *testing.T) {
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "REDIS_") {
				idx := strings.IndexByte(e, '=')
				if idx > 0 {
					os.Unsetenv(e[:idx])
				}
			}
		}

		c, err := Init(ConnectionOptions{Dial: mockDial})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		if len(c.services) != 0 {
			t.Errorf("Expected no services, got %d", len(c.services))
		}
	})

	t.Run("discovers standalone connections", func(t *testing.T) {
		os.Setenv("REDIS_CACHE_READ_WRITE", "redis://localhost:6379/0")
		os.Setenv("REDIS_CACHE_READ_ONLY", "redis://localhost:6379/0")
		defer func() {
			os.Unsetenv("REDIS_CACHE_READ_WRITE")
			os.Unsetenv("REDIS_CACHE_READ_ONLY")
		}()

		c, err := Init(ConnectionOptions{
			Dial:                  mockDial,
			DefaultConnectTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		cache := c.Service("cache")
		if cache == nil {
			t.Fatal("Expected 'cache' service")
		}
		if cache.Read == nil || cache.Write == nil {
			t.Error("Expected both read and write connections")
		}
	})

	t.Run("routes to cluster dial", func(t *testing.T) {
		os.Setenv("REDIS_CLUSTER_READ_WRITE", "redis://host1:6379,host2:6379,host3:6379")
		defer os.Unsetenv("REDIS_CLUSTER_READ_WRITE")

		var clusterDialCalled bool
		clusterDial := func(ctx context.Context, opts *ClusterDialOptions) (Connection, error) {
			clusterDialCalled = true
			if len(opts.Addrs) != 3 {
				t.Errorf("Cluster addrs = %d, want 3", len(opts.Addrs))
			}
			return &mockConnection{isCluster: true}, nil
		}

		_, err := Init(ConnectionOptions{
			Dial:        mockDial,
			ClusterDial: clusterDial,
		})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		if !clusterDialCalled {
			t.Error("ClusterDial should be called for multi-host URI")
		}
	})

	t.Run("routes to sentinel dial", func(t *testing.T) {
		os.Setenv("REDIS_SENTINEL_READ_WRITE", "redis-sentinel://sentinel1:26379?master=mymaster")
		defer os.Unsetenv("REDIS_SENTINEL_READ_WRITE")

		var sentinelDialCalled bool
		sentinelDial := func(ctx context.Context, opts *SentinelDialOptions) (Connection, error) {
			sentinelDialCalled = true
			if opts.MasterName != "mymaster" {
				t.Errorf("MasterName = %q, want 'mymaster'", opts.MasterName)
			}
			return &mockConnection{}, nil
		}

		_, err := Init(ConnectionOptions{
			Dial:         mockDial,
			SentinelDial: sentinelDial,
		})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		if !sentinelDialCalled {
			t.Error("SentinelDial should be called for sentinel scheme")
		}
	})

	t.Run("missing cluster dial", func(t *testing.T) {
		os.Setenv("REDIS_CLUSTER_READ_WRITE", "redis://h1:6379,h2:6379")
		defer os.Unsetenv("REDIS_CLUSTER_READ_WRITE")

		_, err := Init(ConnectionOptions{
			Dial: mockDial,
			// ClusterDial not provided
		})
		if err == nil || !strings.Contains(err.Error(), "ClusterDial not provided") {
			t.Errorf("Expected ClusterDial error, got: %v", err)
		}
	})

	t.Run("missing sentinel dial", func(t *testing.T) {
		os.Setenv("REDIS_SENT_READ_WRITE", "redis-sentinel://s:26379?master=m")
		defer os.Unsetenv("REDIS_SENT_READ_WRITE")

		_, err := Init(ConnectionOptions{
			Dial: mockDial,
			// SentinelDial not provided
		})
		if err == nil || !strings.Contains(err.Error(), "SentinelDial not provided") {
			t.Errorf("Expected SentinelDial error, got: %v", err)
		}
	})

	t.Run("default connect timeout", func(t *testing.T) {
		os.Setenv("REDIS_TEST_READ_WRITE", "redis://localhost:6379")
		defer os.Unsetenv("REDIS_TEST_READ_WRITE")

		var capturedOpts *DialOptions
		captureDial := func(ctx context.Context, opts *DialOptions) (Connection, error) {
			capturedOpts = opts
			return &mockConnection{}, nil
		}

		_, err := Init(ConnectionOptions{
			Dial: captureDial,
			// No DefaultConnectTimeout - should default to 10s
		})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}

		if capturedOpts == nil {
			t.Fatal("DialOptions not captured")
		}
		if capturedOpts.ConnectTimeout != 10*time.Second {
			t.Errorf("ConnectTimeout = %v, want 10s", capturedOpts.ConnectTimeout)
		}
	})
}

// ---------------------------------------------------------------------------
// GSM resolver tests
// ---------------------------------------------------------------------------

func TestSetGSMResolver(t *testing.T) {
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

func TestResolveConnectionString(t *testing.T) {
	orig := os.Getenv("DB_CONNECTION_PROVIDER")
	defer os.Setenv("DB_CONNECTION_PROVIDER", orig)

	t.Run("direct connection string", func(t *testing.T) {
		os.Unsetenv("DB_CONNECTION_PROVIDER")

		result, err := resolveConnectionString("redis://localhost:6379")
		if err != nil {
			t.Fatalf("resolveConnectionString() error = %v", err)
		}
		if result != "redis://localhost:6379" {
			t.Errorf("result = %q, want original", result)
		}
	})

	t.Run("GSM provider", func(t *testing.T) {
		os.Setenv("DB_CONNECTION_PROVIDER", "GSM")

		SetGSMResolver(func(name, version string) (string, error) {
			return "gsm-resolved-" + name, nil
		})
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

	t.Run("GSM no resolver", func(t *testing.T) {
		os.Setenv("DB_CONNECTION_PROVIDER", "GSM")

		gsmMu.Lock()
		gsmResolver = nil
		gsmMu.Unlock()

		_, err := resolveConnectionString("secret")
		if err == nil || !strings.Contains(err.Error(), "GSM resolver not configured") {
			t.Errorf("Expected GSM resolver error, got: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TLS config tests
// ---------------------------------------------------------------------------

func TestLoadTLSConfig_NoCerts(t *testing.T) {
	for _, k := range []string{
		"REDIS_TEST_SSL_CA",
		"REDIS_TEST_SSL_CERT",
		"REDIS_TEST_SSL_KEY",
		"REDIS_SSL_CA",
		"REDIS_SSL_CERT",
		"REDIS_SSL_KEY",
	} {
		os.Unsetenv(k)
	}

	cfg := loadTLSConfig("REDIS", "TEST")
	if cfg != nil {
		t.Error("loadTLSConfig should return nil when no certs")
	}
}

// ---------------------------------------------------------------------------
// envWithFallback tests
// ---------------------------------------------------------------------------

func TestEnvWithFallback(t *testing.T) {
	orig1 := os.Getenv("TEST_KEY_1")
	orig2 := os.Getenv("TEST_KEY_2")
	defer func() {
		os.Setenv("TEST_KEY_1", orig1)
		os.Setenv("TEST_KEY_2", orig2)
	}()

	t.Run("first key", func(t *testing.T) {
		os.Setenv("TEST_KEY_1", "first")
		os.Setenv("TEST_KEY_2", "second")

		result := envWithFallback("TEST_KEY_1", "TEST_KEY_2")
		if result != "first" {
			t.Errorf("result = %q, want 'first'", result)
		}
	})

	t.Run("fallback", func(t *testing.T) {
		os.Unsetenv("TEST_KEY_1")
		os.Setenv("TEST_KEY_2", "second")

		result := envWithFallback("TEST_KEY_1", "TEST_KEY_2")
		if result != "second" {
			t.Errorf("result = %q, want 'second'", result)
		}
	})

	t.Run("all empty", func(t *testing.T) {
		os.Unsetenv("TEST_KEY_1")
		os.Unsetenv("TEST_KEY_2")

		result := envWithFallback("TEST_KEY_1", "TEST_KEY_2")
		if result != "" {
			t.Errorf("result = %q, want empty", result)
		}
	})
}

// ---------------------------------------------------------------------------
// dialFromURI routing tests
// ---------------------------------------------------------------------------

func TestDialFromURI_Routing(t *testing.T) {
	ctx := context.Background()
	job := connJobEntry{serviceName: "test", connType: "write"}
	opts := ConnectionOptions{
		Dial:                  mockDial,
		ClusterDial:           mockClusterDial,
		SentinelDial:          mockSentinelDial,
		DefaultConnectTimeout: 5 * time.Second,
	}

	t.Run("standalone", func(t *testing.T) {
		conn, err := dialFromURI(ctx, "redis://localhost:6379/0", job, opts, "client", nil, envPoolOpts{})
		if err != nil {
			t.Fatalf("dialFromURI() error = %v", err)
		}
		if conn.IsCluster() {
			t.Error("Standalone should not be cluster")
		}
	})

	t.Run("cluster by multiple hosts", func(t *testing.T) {
		conn, err := dialFromURI(ctx, "redis://h1:6379,h2:6379", job, opts, "client", nil, envPoolOpts{})
		if err != nil {
			t.Fatalf("dialFromURI() error = %v", err)
		}
		if !conn.IsCluster() {
			t.Error("Multiple hosts should be cluster")
		}
	})

	t.Run("cluster by sharded_db param", func(t *testing.T) {
		conn, err := dialFromURI(ctx, "redis://localhost:6379?sharded_db=true", job, opts, "client", nil, envPoolOpts{})
		if err != nil {
			t.Fatalf("dialFromURI() error = %v", err)
		}
		if !conn.IsCluster() {
			t.Error("sharded_db=true should be cluster")
		}
	})

	t.Run("sentinel", func(t *testing.T) {
		conn, err := dialFromURI(ctx, "redis-sentinel://s:26379?master=m", job, opts, "client", nil, envPoolOpts{})
		if err != nil {
			t.Fatalf("dialFromURI() error = %v", err)
		}
		if conn == nil {
			t.Error("Sentinel should return connection")
		}
	})

	t.Run("sentinel missing master", func(t *testing.T) {
		_, err := dialFromURI(ctx, "redis-sentinel://s:26379", job, opts, "client", nil, envPoolOpts{})
		if err == nil || !strings.Contains(err.Error(), "master name missing") {
			t.Errorf("Expected master name error, got: %v", err)
		}
	})

	t.Run("read connection sets ReadOnly", func(t *testing.T) {
		var capturedOpts *DialOptions
		captureDial := func(ctx context.Context, opts *DialOptions) (Connection, error) {
			capturedOpts = opts
			return &mockConnection{}, nil
		}

		readJob := connJobEntry{serviceName: "test", connType: "read"}
		readOpts := ConnectionOptions{
			Dial:                  captureDial,
			DefaultConnectTimeout: 5 * time.Second,
		}

		_, err := dialFromURI(ctx, "redis://localhost:6379", readJob, readOpts, "client", nil, envPoolOpts{})
		if err != nil {
			t.Fatalf("dialFromURI() error = %v", err)
		}

		if !capturedOpts.ReadOnly {
			t.Error("Read connection should set ReadOnly=true")
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

func TestClientConcurrentAccess(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"cache": {Read: &mockConnection{}, Write: &mockConnection{}},
		},
	}

	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			_ = c.Service("cache")
			_ = c.Services()
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		go func() {
			_ = c.Ping(context.Background())
			done <- true
		}()
	}

	for i := 0; i < 15; i++ {
		<-done
	}
}
