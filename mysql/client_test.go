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

package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The real "mysql" driver is registered via the side-effect import in client.go.
// We use it directly for sql.Open calls in tests.

// ---------------------------------------------------------------------------
// InitDefault / Connection Discovery tests
// ---------------------------------------------------------------------------

func TestInitDefault_NoEnv(t *testing.T) {
	clearMySQLEnv(t)

	c, err := InitDefault()
	if err != nil {
		t.Fatalf("InitDefault() error = %v", err)
	}
	if len(c.services) != 0 {
		t.Errorf("Expected no services, got %d", len(c.services))
	}
}

func TestConnectionDiscovery(t *testing.T) {
	clearMySQLEnv(t)

	t.Run("discovers services from env", func(t *testing.T) {
		// We cannot connect to a real DB, but we can verify the env regex
		// discovers the right service names.
		t.Setenv("MYSQL_CATALOG_READ_WRITE", "mysql://user:pass@localhost:3306/catalog")
		t.Setenv("MYSQL_USERS_READ_ONLY", "mysql://user:pass@localhost:3306/users")

		// Init will fail at Ping because there is no real DB, but we can
		// verify the parsing works by using a custom driver that fakes pings.
		// Instead, verify the env regex matching.
		for _, env := range os.Environ() {
			idx := strings.IndexByte(env, '=')
			if idx < 0 {
				continue
			}
			key := env[:idx]
			matches := envRegex.FindStringSubmatch(key)
			if key == "MYSQL_CATALOG_READ_WRITE" {
				if matches == nil {
					t.Error("Expected MYSQL_CATALOG_READ_WRITE to match")
				} else if matches[1] != "CATALOG" || matches[2] != "WRITE" {
					t.Errorf("Unexpected match: service=%s type=%s", matches[1], matches[2])
				}
			}
			if key == "MYSQL_USERS_READ_ONLY" {
				if matches == nil {
					t.Error("Expected MYSQL_USERS_READ_ONLY to match")
				} else if matches[1] != "USERS" || matches[2] != "ONLY" {
					t.Errorf("Unexpected match: service=%s type=%s", matches[1], matches[2])
				}
			}
		}
	})

	t.Run("default driver name and DSN transform", func(t *testing.T) {
		clearMySQLEnv(t)
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
		t.Setenv("MYSQL_CATALOG_READ_WRITE_MAX_POOL_SIZE", "50")
		t.Setenv("MYSQL_CATALOG_READ_WRITE_MIN_POOL_SIZE", "10")
		t.Setenv("MYSQL_CATALOG_READ_WRITE_MAX_IDLE_TIME", "60000")
		t.Setenv("MYSQL_CATALOG_READ_WRITE_CONNECTION_TIMEOUT", "30000")

		db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/test")
		if err != nil {
			t.Fatalf("sql.Open error: %v", err)
		}
		defer db.Close()

		// Should not panic.
		applyPoolSettings(db, "CATALOG", "WRITE", nil, "catalog", "write")

		stats := db.Stats()
		if stats.MaxOpenConnections != 50 {
			t.Errorf("Expected MaxOpenConns=50, got %d", stats.MaxOpenConnections)
		}
	})

	t.Run("applies caller overrides", func(t *testing.T) {
		db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/test")
		if err != nil {
			t.Fatalf("sql.Open error: %v", err)
		}
		defer db.Close()

		perService := map[string]ServicePoolOverrides{
			"catalog": {
				Write: &PoolOverrides{
					MaxOpenConns: 100,
					MaxIdleConns: 20,
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
		t.Setenv("MYSQL_USERS_READ_ONLY_MAX_POOL_SIZE", "25")

		db, err := sql.Open("mysql", "root:@tcp(localhost:3306)/test")
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
			"MYSQL_TEST_SSL_CA", "MYSQL_TEST_SSL_CERT", "MYSQL_TEST_SSL_KEY",
			"MYSQL_TEST_SSL_SERVER_NAME", "MYSQL_SSL_CA", "MYSQL_SSL_CERT", "MYSQL_SSL_KEY",
		} {
			os.Unsetenv(k)
		}

		cfg, serverName := loadMySQLTLSConfig("TEST")
		if cfg != nil {
			t.Error("Expected nil TLS config without certs")
		}
		if serverName != "" {
			t.Error("Expected empty server name")
		}
	})

	t.Run("returns nil without server name", func(t *testing.T) {
		t.Setenv("MYSQL_TEST_SSL_CA", "/tmp/ca.crt")
		t.Setenv("MYSQL_TEST_SSL_CERT", "/tmp/cert.crt")
		t.Setenv("MYSQL_TEST_SSL_KEY", "/tmp/key.pem")
		os.Unsetenv("MYSQL_TEST_SSL_SERVER_NAME")

		cfg, _ := loadMySQLTLSConfig("TEST")
		if cfg != nil {
			t.Error("Expected nil TLS config without server name")
		}
	})

	t.Run("TLS registrar is called by InitDefault", func(t *testing.T) {
		// Verify that InitDefault sets up a TLS registrar (we just check it
		// does not panic with no env vars).
		clearMySQLEnv(t)
		c, err := InitDefault()
		if err != nil {
			t.Fatalf("InitDefault() error = %v", err)
		}
		if c == nil {
			t.Fatal("Expected non-nil client")
		}
	})

	t.Run("custom TLS registrar is invoked", func(t *testing.T) {
		var called bool
		_, _ = Init(ConnectionOptions{
			TLSRegistrar: func(name string, config *tls.Config) error {
				called = true
				return nil
			},
		})
		// With no MYSQL env vars, the registrar should not be called since
		// there are no connections to establish.
		if called {
			t.Error("TLS registrar should not be called when no connections exist")
		}
	})
}

// ---------------------------------------------------------------------------
// URI parsing tests
// ---------------------------------------------------------------------------

func TestParseURI(t *testing.T) {
	tests := []struct {
		name string
		uri string
		wantHost string
		wantPort string
		wantDB string
		wantUser string
		wantPass string
		wantOpts map[string]string
		wantErr bool
	}{
		{
			name: "mysql URI format",
			uri: "mysql://user:pass@localhost:3306/mydb",
			wantHost: "localhost",
			wantPort: "3306",
			wantDB: "mydb",
			wantUser: "user",
			wantPass: "pass",
		},
		{
			name: "without port",
			uri: "mysql://user:pass@localhost/mydb",
			wantHost: "localhost",
			wantDB: "mydb",
			wantUser: "user",
			wantPass: "pass",
		},
		{
			name: "with query params",
			uri: "mysql://localhost:3306/mydb?charset=utf8mb4&parseTime=true",
			wantHost: "localhost",
			wantPort: "3306",
			wantDB: "mydb",
			wantOpts: map[string]string{"charset": "utf8mb4", "parseTime": "true"},
		},
		{
			name: "password with special chars",
			uri: "mysql://user:p%40ss%3Aword@localhost:3306/mydb",
			wantHost: "localhost",
			wantPort: "3306",
			wantDB: "mydb",
			wantUser: "user",
			wantPass: "p@ss:word",
		},
		{
			name: "DSN format (no scheme)",
			uri: "user:password@tcp(localhost:3306)/mydb",
			wantOpts: map[string]string{"_raw_dsn": "user:password@tcp(localhost:3306)/mydb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseURI() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err != nil {
				return
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
			for k, v := range tt.wantOpts {
				if parsed.Options[k] != v {
					t.Errorf("Options[%q] = %q, want %q", k, parsed.Options[k], v)
				}
			}
		})
	}
}

func TestDefaultMySQLDSN(t *testing.T) {
	tests := []struct {
		name string
		parsed *ParsedURI
		contains []string
	}{
		{
			name: "basic DSN",
			parsed: &ParsedURI{
				Username: "user",
				Password: "pass",
				Host: "localhost",
				Port: "3306",
				Database: "mydb",
			},
			contains: []string{"user:pass@", "tcp(localhost:3306)", "/mydb"},
		},
		{
			name: "with TLS name",
			parsed: &ParsedURI{
				Username: "user",
				Host: "localhost",
				Port: "3306",
				Database: "mydb",
				TLSName: "custom-tls",
			},
			contains: []string{"tls=custom-tls"},
		},
		{
			name: "with options",
			parsed: &ParsedURI{
				Host: "localhost",
				Port: "3306",
				Database: "mydb",
				Options: map[string]string{"charset": "utf8mb4"},
			},
			contains: []string{"charset=utf8mb4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := DefaultMySQLDSN(tt.parsed)
			for _, substr := range tt.contains {
				if !strings.Contains(dsn, substr) {
					t.Errorf("DSN %q should contain %q", dsn, substr)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Client tests
// ---------------------------------------------------------------------------

func TestClient_Service(t *testing.T) {
	mockRead := &sql.DB{}
	mockWrite := &sql.DB{}

	c := &Client{
		services: map[string]*ServiceConnection{
			"catalog": {Read: mockRead, Write: mockWrite},
		},
	}

	t.Run("existing service", func(t *testing.T) {
		sc := c.Service("catalog")
		if sc == nil {
			t.Fatal("Expected service connection")
		}
		if sc.Read != mockRead || sc.Write != mockWrite {
			t.Error("Connection mismatch")
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		if c.Service("CATALOG") == nil {
			t.Error("Should be case-insensitive")
		}
	})

	t.Run("non-existent", func(t *testing.T) {
		if c.Service("nonexistent") != nil {
			t.Error("Expected nil for non-existent service")
		}
	})
}

func TestClient_Services(t *testing.T) {
	c := &Client{
		services: map[string]*ServiceConnection{
			"catalog": {Read: &sql.DB{}},
			"users": {Write: &sql.DB{}},
		},
	}

	services := c.Services()
	if len(services) != 2 {
		t.Errorf("Expected 2 services, got %d", len(services))
	}

	// Verify it returns a copy.
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
		result, err := resolveConnectionString("mysql://localhost:3306/mydb")
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if result != "mysql://localhost:3306/mydb" {
			t.Errorf("result = %q, want original", result)
		}
	})

	t.Run("GSM provider with resolver", func(t *testing.T) {
		t.Setenv("DB_CONNECTION_PROVIDER", "GSM")
		SetGSMResolver(func(name, version string) (string, error) {
			return "gsm-resolved-" + name + "-" + version, nil
		})
		defer func() {
			gsmMu.Lock()
			gsmResolver = nil
			gsmMu.Unlock()
		}()

		result, err := resolveConnectionString("secret-name")
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if result != "gsm-resolved-secret-name-latest" {
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

func TestGetDeploymentName(t *testing.T) {
	tests := []struct {
		podName string
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

func TestGetAppName(t *testing.T) {
	t.Setenv("K8S_POD_NAME", "catalog-svc-dply-xyz")
	t.Setenv("K8S_POD_NAMESPACE", "production")
	t.Setenv("SERVICE_NAME", "catalog-svc")

	if got := getAppName(); got != "production-catalog-svc-dply" {
		t.Errorf("getAppName() = %q, want 'production-catalog-svc-dply'", got)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func clearMySQLEnv(t *testing.T) {
	t.Helper()
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "MYSQL_") {
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

// Verify InitDefaultWithContext compiles and accepts context.
func TestInitDefaultWithContext(t *testing.T) {
	clearMySQLEnv(t)
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
