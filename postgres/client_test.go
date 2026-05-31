package postgres

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInitDefault_NoEnv(t *testing.T) {
	clearPostgresEnv(t)

	c, err := InitDefault()
	if err != nil {
		t.Fatalf("InitDefault() error = %v", err)
	}
	if len(c.services) != 0 {
		t.Errorf("Expected no services, got %d", len(c.services))
	}
}

func TestConnectionDiscovery(t *testing.T) {
	clearPostgresEnv(t)

	t.Run("env regex matches correctly", func(t *testing.T) {
		tests := []struct {
			input           string
			shouldMatch     bool
			expectedService string
			expectedType    string
		}{
			{"POSTGRES_CATALOG_READ_WRITE", true, "CATALOG", "WRITE"},
			{"POSTGRES_USERS_READ_ONLY", true, "USERS", "ONLY"},
			{"POSTGRES_FOO_BAR_READ_WRITE", true, "FOO_BAR", "WRITE"},
			{"MYSQL_CATALOG_READ_WRITE", false, "", ""},
			{"POSTGRES__READ_WRITE", false, "", ""},
			{"postgres_catalog_read_write", false, "", ""},
		}

		for _, tt := range tests {
			matches := envRegex.FindStringSubmatch(tt.input)
			if tt.shouldMatch {
				if matches == nil {
					t.Errorf("Expected %q to match", tt.input)
					continue
				}
				if matches[1] != tt.expectedService {
					t.Errorf("Service = %q, want %q", matches[1], tt.expectedService)
				}
				if matches[2] != tt.expectedType {
					t.Errorf("Type = %q, want %q", matches[2], tt.expectedType)
				}
			} else if matches != nil {
				t.Errorf("Expected %q not to match", tt.input)
			}
		}
	})

	t.Run("empty client on no env", func(t *testing.T) {
		clearPostgresEnv(t)
		c, err := InitWithContext(context.Background(), ConnectionOptions{})
		if err != nil {
			t.Fatalf("InitWithContext() error = %v", err)
		}
		if c == nil {
			t.Fatal("Expected non-nil client")
		}
		if len(c.services) != 0 {
			t.Errorf("Expected 0 services, got %d", len(c.services))
		}
	})
}

func TestPoolSettingsFromEnv(t *testing.T) {
	t.Run("reads env vars", func(t *testing.T) {
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_MAX_POOL_SIZE", "50")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_MIN_POOL_SIZE", "10")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_MAX_IDLE_TIME", "60000")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_CONNECTION_TIMEOUT", "30000")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_HEALTH_CHECK_PERIOD", "5000")

		opts := applyOptionDefaults(ConnectionOptions{})
		ps := poolSettingsFromEnv("CATALOG", "WRITE", opts, "catalog", "write")

		if ps.MaxConns != 50 {
			t.Errorf("MaxConns = %d, want 50", ps.MaxConns)
		}
		if ps.MinConns != 10 {
			t.Errorf("MinConns = %d, want 10", ps.MinConns)
		}
		if ps.MaxConnIdleTime != 60*time.Second {
			t.Errorf("MaxConnIdleTime = %v, want 60s", ps.MaxConnIdleTime)
		}
		if ps.MaxConnLifetime != 30*time.Second {
			t.Errorf("MaxConnLifetime = %v, want 30s", ps.MaxConnLifetime)
		}
		if ps.HealthCheckPeriod != 5*time.Second {
			t.Errorf("HealthCheckPeriod = %v, want 5s", ps.HealthCheckPeriod)
		}
	})

	t.Run("applies caller overrides", func(t *testing.T) {
		opts := applyOptionDefaults(ConnectionOptions{
			PerService: map[string]ServicePoolOverrides{
				"catalog": {
					Write: &PoolOverrides{
						MaxConns:          100,
						MinConns:          20,
						MaxConnIdleTime:   5 * time.Minute,
						MaxConnLifetime:   30 * time.Minute,
						HealthCheckPeriod: 2 * time.Minute,
					},
				},
			},
		})

		ps := poolSettingsFromEnv("CATALOG", "WRITE", opts, "catalog", "write")
		if ps.MaxConns != 100 {
			t.Errorf("MaxConns = %d, want 100", ps.MaxConns)
		}
		if ps.MinConns != 20 {
			t.Errorf("MinConns = %d, want 20", ps.MinConns)
		}
	})

	t.Run("uses defaults when no env or overrides", func(t *testing.T) {
		clearPostgresEnv(t)
		opts := applyOptionDefaults(ConnectionOptions{})
		ps := poolSettingsFromEnv("MYSERVICE", "WRITE", opts, "myservice", "write")

		if ps.MaxConns != 20 {
			t.Errorf("MaxConns = %d, want default 20", ps.MaxConns)
		}
		if ps.MinConns != 5 {
			t.Errorf("MinConns = %d, want default 5", ps.MinConns)
		}
	})
}

func TestTLSConfig(t *testing.T) {
	t.Run("returns nil when no certs configured", func(t *testing.T) {
		for _, k := range []string{
			"POSTGRES_TEST_SSL_CA", "POSTGRES_TEST_SSL_CERT", "POSTGRES_TEST_SSL_KEY",
			"POSTGRES_TEST_SSL_SERVER_NAME", "POSTGRES_SSL_CA", "POSTGRES_SSL_CERT", "POSTGRES_SSL_KEY",
		} {
			os.Unsetenv(k)
		}

		cfg := loadPostgresTLSConfig("TEST")
		if cfg != nil {
			t.Error("Expected nil TLS config without certs")
		}
	})

	t.Run("returns nil without server name", func(t *testing.T) {
		t.Setenv("POSTGRES_TEST_SSL_CA", "/tmp/ca.crt")
		t.Setenv("POSTGRES_TEST_SSL_CERT", "/tmp/cert.crt")
		t.Setenv("POSTGRES_TEST_SSL_KEY", "/tmp/key.pem")
		os.Unsetenv("POSTGRES_TEST_SSL_SERVER_NAME")

		cfg := loadPostgresTLSConfig("TEST")
		if cfg != nil {
			t.Error("Expected nil TLS config without server name")
		}
	})
}

func TestClient_Service(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"catalog": {},
		},
	}

	if c.Service("catalog") == nil {
		t.Error("Expected service connection")
	}
	if c.Service("CATALOG") == nil {
		t.Error("Should be case-insensitive")
	}
	if c.Service("nonexistent") != nil {
		t.Error("Expected nil for non-existent service")
	}
}

func TestClient_Services(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"catalog": {},
			"users":   {},
		},
	}

	services := c.Services()
	if len(services) != 2 {
		t.Errorf("Expected 2 services, got %d", len(services))
	}

	services["catalog"] = nil
	if c.services["catalog"] == nil {
		t.Error("Services() should return a copy")
	}
}

func TestClient_Close_Empty(t *testing.T) {
	c := &Client{services: map[string]*ServiceConnection{}}
	if err := c.Close(); err != nil {
		t.Errorf("Close() on empty client should not error: %v", err)
	}
}

func TestClient_HealthCheck_Empty(t *testing.T) {
	c := &Client{services: map[string]*ServiceConnection{}}
	checkFn := c.HealthCheck()
	if result := checkFn(); result != "" {
		t.Errorf("HealthCheck() = %q, want empty", result)
	}
}

func TestClientConcurrentAccess(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"catalog": {},
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Service("catalog")
			_ = c.Services()
		}()
	}
	wg.Wait()
}

func TestResolveConnectionString(t *testing.T) {
	t.Run("direct connection string", func(t *testing.T) {
		os.Unsetenv("DB_CONNECTION_PROVIDER")
		result, err := resolveConnectionString("postgres://localhost:5432/mydb")
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if result != "postgres://localhost:5432/mydb" {
			t.Errorf("result = %q", result)
		}
	})

	t.Run("GSM without resolver", func(t *testing.T) {
		t.Setenv("DB_CONNECTION_PROVIDER", "GSM")
		gsmMu.Lock()
		gsmResolver = nil
		gsmMu.Unlock()

		_, err := resolveConnectionString("secret")
		if err == nil || !strings.Contains(err.Error(), "GSM resolver not configured") {
			t.Errorf("Expected GSM resolver error, got: %v", err)
		}
	})
}

func TestGetAppName(t *testing.T) {
	t.Setenv("K8S_POD_NAME", "catalog-svc-dply-xyz")
	t.Setenv("K8S_POD_NAMESPACE", "production")
	t.Setenv("SERVICE_NAME", "catalog-svc")

	if got := getAppName(); got != "production-catalog-svc-dply" {
		t.Errorf("getAppName() = %q, want 'production-catalog-svc-dply'", got)
	}
}

func TestGetDeploymentName(t *testing.T) {
	tests := []struct {
		podName  string
		expected string
	}{
		{"catalog-service-dply-abc123", "catalog-service-dply"},
		{"auth-cron-job-12345", "auth-cron"},
		{"simple-pod", ""},
		{"", ""},
	}

	for _, tt := range tests {
		if got := getDeploymentName(tt.podName); got != tt.expected {
			t.Errorf("getDeploymentName(%q) = %q, want %q", tt.podName, got, tt.expected)
		}
	}
}

func TestApplyOptionDefaults(t *testing.T) {
	opts := applyOptionDefaults(ConnectionOptions{})
	if opts.MaxConns != 20 {
		t.Errorf("MaxConns = %d, want 20", opts.MaxConns)
	}
	if opts.MinConns != 5 {
		t.Errorf("MinConns = %d, want 5", opts.MinConns)
	}
	if opts.MaxConnLifetime != time.Hour {
		t.Errorf("MaxConnLifetime = %v, want 1h", opts.MaxConnLifetime)
	}
	if opts.MaxConnIdleTime != 30*time.Minute {
		t.Errorf("MaxConnIdleTime = %v, want 30m", opts.MaxConnIdleTime)
	}
	if opts.HealthCheckPeriod != time.Minute {
		t.Errorf("HealthCheckPeriod = %v, want 1m", opts.HealthCheckPeriod)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func clearPostgresEnv(t *testing.T) {
	t.Helper()
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "POSTGRES_") {
			idx := strings.IndexByte(e, '=')
			if idx > 0 {
				key := e[:idx]
				old := e[idx+1:]
				os.Unsetenv(key)
				t.Cleanup(func() {
					if old != "" {
						os.Setenv(key, old)
					}
				})
			}
		}
	}
}
