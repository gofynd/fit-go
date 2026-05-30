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

package postgres

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The real "postgres" driver is registered via the side-effect import in client.go.

// ---------------------------------------------------------------------------
// InitDefault / Connection Discovery tests
// ---------------------------------------------------------------------------

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

	t.Run("default driver name", func(t *testing.T) {
		clearPostgresEnv(t)
		c, err := Init(ConnectionOptions{})
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		if c == nil {
			t.Fatal("Expected non-nil client")
		}
	})
}

// ---------------------------------------------------------------------------
// Pool settings tests
// ---------------------------------------------------------------------------

func TestApplyPoolSettings(t *testing.T) {
	t.Run("reads env vars", func(t *testing.T) {
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_MAX_POOL_SIZE", "50")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_MIN_POOL_SIZE", "10")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_MAX_IDLE_TIME", "60000")
		t.Setenv("POSTGRES_CATALOG_READ_WRITE_CONNECTION_TIMEOUT", "30000")

		db, err := sql.Open("postgres", "postgres://localhost:5432/test?sslmode=disable")
		if err != nil {
			t.Fatalf("sql.Open error: %v", err)
		}
		defer db.Close()

		applyPoolSettings(db, "CATALOG", "WRITE", nil, "catalog", "write")

		stats := db.Stats()
		if stats.MaxOpenConnections != 50 {
			t.Errorf("Expected MaxOpenConns=50, got %d", stats.MaxOpenConnections)
		}
	})

	t.Run("applies caller overrides", func(t *testing.T) {
		db, err := sql.Open("postgres", "postgres://localhost:5432/test?sslmode=disable")
		if err != nil {
			t.Fatalf("sql.Open error: %v", err)
		}
		defer db.Close()

		perService := map[string]ServicePoolOverrides{
			"catalog": {
				Write: &PoolOverrides{
					MaxOpenConns:    100,
					MaxIdleConns:    20,
					ConnMaxIdleTime: 5 * time.Minute,
					ConnMaxLifetime: 30 * time.Minute,
				},
			},
		}

		applyPoolSettings(db, "CATALOG", "WRITE", perService, "catalog", "write")

		stats := db.Stats()
		if stats.MaxOpenConnections != 100 {
			t.Errorf("Expected MaxOpenConns=100, got %d", stats.MaxOpenConnections)
		}
	})

	t.Run("read type uses ONLY prefix", func(t *testing.T) {
		t.Setenv("POSTGRES_USERS_READ_ONLY_MAX_POOL_SIZE", "25")

		db, err := sql.Open("postgres", "postgres://localhost:5432/test?sslmode=disable")
		if err != nil {
			t.Fatalf("sql.Open error: %v", err)
		}
		defer db.Close()

		applyPoolSettings(db, "USERS", "ONLY", nil, "users", "read")

		stats := db.Stats()
		if stats.MaxOpenConnections != 25 {
			t.Errorf("Expected MaxOpenConns=25, got %d", stats.MaxOpenConnections)
		}
	})
}

// ---------------------------------------------------------------------------
// TLS config tests
// ---------------------------------------------------------------------------

func TestTLSConfig(t *testing.T) {
	t.Run("returns nil when no certs configured", func(t *testing.T) {
		for _, k := range []string{
			"POSTGRES_TEST_SSL_CA", "POSTGRES_TEST_SSL_CERT", "POSTGRES_TEST_SSL_KEY",
			"POSTGRES_TEST_SSL_SERVER_NAME", "POSTGRES_SSL_CA", "POSTGRES_SSL_CERT", "POSTGRES_SSL_KEY",
		} {
			os.Unsetenv(k)
		}

		cfg, serverName := loadPostgresTLSConfig("TEST")
		if cfg != nil {
			t.Error("Expected nil TLS config without certs")
		}
		if serverName != "" {
			t.Error("Expected empty server name")
		}
	})

	t.Run("returns nil without server name", func(t *testing.T) {
		t.Setenv("POSTGRES_TEST_SSL_CA", "/tmp/ca.crt")
		t.Setenv("POSTGRES_TEST_SSL_CERT", "/tmp/cert.crt")
		t.Setenv("POSTGRES_TEST_SSL_KEY", "/tmp/key.pem")
		os.Unsetenv("POSTGRES_TEST_SSL_SERVER_NAME")

		cfg, _ := loadPostgresTLSConfig("TEST")
		if cfg != nil {
			t.Error("Expected nil TLS config without server name")
		}
	})
}

// ---------------------------------------------------------------------------
// URI parsing tests
// ---------------------------------------------------------------------------

func TestParseURI(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		wantHost string
		wantPort string
		wantDB   string
		wantUser string
		wantPass string
		wantSSL  string
	}{
		{
			name:     "postgres URI format",
			uri:      "postgres://user:pass@localhost:5432/mydb?sslmode=require",
			wantHost: "localhost",
			wantPort: "5432",
			wantDB:   "mydb",
			wantUser: "user",
			wantPass: "pass",
			wantSSL:  "require",
		},
		{
			name:     "postgresql scheme",
			uri:      "postgresql://user:pass@localhost/mydb",
			wantHost: "localhost",
			wantDB:   "mydb",
			wantUser: "user",
			wantPass: "pass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseURI(tt.uri)
			if err != nil {
				t.Fatalf("parseURI() error: %v", err)
			}
			if parsed.Host != tt.wantHost {
				t.Errorf("Host = %q, want %q", parsed.Host, tt.wantHost)
			}
			if parsed.Port != tt.wantPort {
				t.Errorf("Port = %q, want %q", parsed.Port, tt.wantPort)
			}
			if parsed.Database != tt.wantDB {
				t.Errorf("Database = %q, want %q", parsed.Database, tt.wantDB)
			}
			if parsed.Username != tt.wantUser {
				t.Errorf("Username = %q, want %q", parsed.Username, tt.wantUser)
			}
			if parsed.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", parsed.Password, tt.wantPass)
			}
			if tt.wantSSL != "" && parsed.SSLMode != tt.wantSSL {
				t.Errorf("SSLMode = %q, want %q", parsed.SSLMode, tt.wantSSL)
			}
		})
	}
}

func TestParseKeyValue(t *testing.T) {
	p := parseKeyValue("host=localhost port=5432 dbname=mydb user=testuser sslmode=disable")
	if p.Host != "localhost" {
		t.Errorf("Host = %q", p.Host)
	}
	if p.Port != "5432" {
		t.Errorf("Port = %q", p.Port)
	}
	if p.Database != "mydb" {
		t.Errorf("Database = %q", p.Database)
	}
	if p.Username != "testuser" {
		t.Errorf("Username = %q", p.Username)
	}
	if p.SSLMode != "disable" {
		t.Errorf("SSLMode = %q", p.SSLMode)
	}
}

func TestDefaultPostgresDSN(t *testing.T) {
	parsed := &ParsedURI{
		Username:        "user",
		Password:        "pass",
		Host:            "localhost",
		Port:            "5432",
		Database:        "mydb",
		ApplicationName: "test-app",
		SSLMode:         "require",
		Options:         map[string]string{},
	}

	dsn := DefaultPostgresDSN(parsed)
	if !strings.Contains(dsn, "postgres://") {
		t.Errorf("Expected postgres:// scheme, got: %s", dsn)
	}
	if !strings.Contains(dsn, "localhost:5432") {
		t.Errorf("Expected host:port, got: %s", dsn)
	}
	if !strings.Contains(dsn, "sslmode=require") {
		t.Errorf("Expected sslmode param, got: %s", dsn)
	}
	if !strings.Contains(dsn, "application_name=test-app") {
		t.Errorf("Expected application_name param, got: %s", dsn)
	}
}

// ---------------------------------------------------------------------------
// Client tests
// ---------------------------------------------------------------------------

func TestClient_Service(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"catalog": {Read: &sql.DB{}, Write: &sql.DB{}},
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
			"catalog": {Read: &sql.DB{}},
			"users":   {Write: &sql.DB{}},
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
			"catalog": {Read: &sql.DB{}, Write: &sql.DB{}},
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

// ---------------------------------------------------------------------------
// GSM resolver tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// App name tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// InitDefaultWithContext test
// ---------------------------------------------------------------------------

func TestInitDefaultWithContext(t *testing.T) {
	clearPostgresEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := InitDefaultWithContext(ctx)
	if err != nil {
		t.Fatalf("InitDefaultWithContext() error = %v", err)
	}
	if c == nil {
		t.Fatal("Expected non-nil client")
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
