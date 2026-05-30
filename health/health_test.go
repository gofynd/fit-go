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

package health

import (
	"errors"
	"os"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Checker tests
// ---------------------------------------------------------------------------

func TestNewChecker(t *testing.T) {
	c := NewChecker()
	if c == nil {
		t.Fatal("NewChecker() returned nil")
	}
	if len(c.checks) != 0 {
		t.Errorf("New checker should have no checks, got %d", len(c.checks))
	}
}

func TestChecker_AddCheck(t *testing.T) {
	c := NewChecker()

	c.AddCheck(func() string { return "" })
	c.AddCheck(func() string { return "error" })

	if len(c.checks) != 2 {
		t.Errorf("Expected 2 checks, got %d", len(c.checks))
	}
}

func TestChecker_Check(t *testing.T) {
	t.Run("all healthy", func(t *testing.T) {
		c := NewChecker()
		c.AddCheck(func() string { return "" })
		c.AddCheck(func() string { return "" })

		errs := c.Check()
		if len(errs) != 0 {
			t.Errorf("Expected no errors, got %v", errs)
		}
	})

	t.Run("some unhealthy", func(t *testing.T) {
		c := NewChecker()
		c.AddCheck(func() string { return "" })
		c.AddCheck(func() string { return "mongo down" })
		c.AddCheck(func() string { return "redis timeout" })

		errs := c.Check()
		if len(errs) != 2 {
			t.Errorf("Expected 2 errors, got %d: %v", len(errs), errs)
		}
	})

	t.Run("all unhealthy", func(t *testing.T) {
		c := NewChecker()
		c.AddCheck(func() string { return "error1" })
		c.AddCheck(func() string { return "error2" })

		errs := c.Check()
		if len(errs) != 2 {
			t.Errorf("Expected 2 errors, got %d", len(errs))
		}
	})

	t.Run("no checks", func(t *testing.T) {
		c := NewChecker()
		errs := c.Check()
		if len(errs) != 0 {
			t.Errorf("Empty checker should return no errors, got %v", errs)
		}
	})
}

func TestChecker_IsHealthy(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		c := NewChecker()
		c.AddCheck(func() string { return "" })

		if !c.IsHealthy() {
			t.Error("IsHealthy() = false, want true")
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		c := NewChecker()
		c.AddCheck(func() string { return "error" })

		if c.IsHealthy() {
			t.Error("IsHealthy() = true, want false")
		}
	})
}

// ---------------------------------------------------------------------------
// Database check factory tests
// ---------------------------------------------------------------------------

func TestMongoCheck(t *testing.T) {
	// Save and restore
	origSkip := os.Getenv("SKIP_HEALTH_CHECK_MONGO")
	defer os.Setenv("SKIP_HEALTH_CHECK_MONGO", origSkip)

	t.Run("healthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_MONGO")
		check := MongoCheck(func() error { return nil }, "users")
		if msg := check(); msg != "" {
			t.Errorf("Expected empty, got %q", msg)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_MONGO")
		check := MongoCheck(func() error { return errors.New("connection refused") }, "users")
		msg := check()
		if msg == "" {
			t.Error("Expected error message")
		}
		if msg != "MongoDB(users): connection refused" {
			t.Errorf("Message = %q, want specific format", msg)
		}
	})

	t.Run("skipped", func(t *testing.T) {
		os.Setenv("SKIP_HEALTH_CHECK_MONGO", "true")
		check := MongoCheck(func() error { return errors.New("would fail") }, "users")
		if msg := check(); msg != "" {
			t.Errorf("Skipped check should return empty, got %q", msg)
		}
	})

	t.Run("skip case insensitive", func(t *testing.T) {
		os.Setenv("SKIP_HEALTH_CHECK_MONGO", "TRUE")
		check := MongoCheck(func() error { return errors.New("would fail") }, "users")
		if msg := check(); msg != "" {
			t.Errorf("Skipped check should return empty, got %q", msg)
		}
	})
}

func TestRedisCheck(t *testing.T) {
	origSkip := os.Getenv("SKIP_HEALTH_CHECK_REDIS")
	defer os.Setenv("SKIP_HEALTH_CHECK_REDIS", origSkip)

	t.Run("healthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_REDIS")
		check := RedisCheck(func() error { return nil }, "cache")
		if msg := check(); msg != "" {
			t.Errorf("Expected empty, got %q", msg)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_REDIS")
		check := RedisCheck(func() error { return errors.New("timeout") }, "cache")
		msg := check()
		if msg == "" {
			t.Error("Expected error message")
		}
		if msg != "Redis(cache): timeout" {
			t.Errorf("Message = %q, want specific format", msg)
		}
	})

	t.Run("skipped", func(t *testing.T) {
		os.Setenv("SKIP_HEALTH_CHECK_REDIS", "true")
		check := RedisCheck(func() error { return errors.New("would fail") }, "cache")
		if msg := check(); msg != "" {
			t.Errorf("Skipped check should return empty, got %q", msg)
		}
	})
}

func TestMySQLCheck(t *testing.T) {
	origSkip := os.Getenv("SKIP_HEALTH_CHECK_MYSQL")
	defer os.Setenv("SKIP_HEALTH_CHECK_MYSQL", origSkip)

	t.Run("healthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_MYSQL")
		check := MySQLCheck(func() error { return nil }, "orders")
		if msg := check(); msg != "" {
			t.Errorf("Expected empty, got %q", msg)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_MYSQL")
		check := MySQLCheck(func() error { return errors.New("too many connections") }, "orders")
		msg := check()
		if msg != "MySQL(orders): too many connections" {
			t.Errorf("Message = %q, want specific format", msg)
		}
	})

	t.Run("skipped", func(t *testing.T) {
		os.Setenv("SKIP_HEALTH_CHECK_MYSQL", "true")
		check := MySQLCheck(func() error { return errors.New("would fail") }, "orders")
		if msg := check(); msg != "" {
			t.Errorf("Skipped check should return empty, got %q", msg)
		}
	})
}

func TestPostgresCheck(t *testing.T) {
	origSkip := os.Getenv("SKIP_HEALTH_CHECK_POSTGRES")
	defer os.Setenv("SKIP_HEALTH_CHECK_POSTGRES", origSkip)

	t.Run("healthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_POSTGRES")
		check := PostgresCheck(func() error { return nil }, "analytics")
		if msg := check(); msg != "" {
			t.Errorf("Expected empty, got %q", msg)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		os.Unsetenv("SKIP_HEALTH_CHECK_POSTGRES")
		check := PostgresCheck(func() error { return errors.New("connection reset") }, "analytics")
		msg := check()
		if msg != "PostgreSQL(analytics): connection reset" {
			t.Errorf("Message = %q, want specific format", msg)
		}
	})

	t.Run("skipped", func(t *testing.T) {
		os.Setenv("SKIP_HEALTH_CHECK_POSTGRES", "true")
		check := PostgresCheck(func() error { return errors.New("would fail") }, "analytics")
		if msg := check(); msg != "" {
			t.Errorf("Skipped check should return empty, got %q", msg)
		}
	})
}

// ---------------------------------------------------------------------------
// shouldSkip tests
// ---------------------------------------------------------------------------

func TestShouldSkip(t *testing.T) {
	key := "TEST_SHOULD_SKIP"
	defer os.Unsetenv(key)

	tests := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"false", false},
		{"", false},
		{"1", false},
		{"yes", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if tt.value != "" {
				os.Setenv(key, tt.value)
			} else {
				os.Unsetenv(key)
			}

			if got := shouldSkip(key); got != tt.expected {
				t.Errorf("shouldSkip(%q) with value %q = %v, want %v", key, tt.value, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

func TestChecker_ConcurrentAccess(t *testing.T) {
	c := NewChecker()

	// Add some checks
	for i := 0; i < 5; i++ {
		c.AddCheck(func() string { return "" })
	}

	var wg sync.WaitGroup
	done := make(chan bool)

	// Concurrent Check() calls
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Check()
		}()
	}

	// Concurrent IsHealthy() calls
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.IsHealthy()
		}()
	}

	// Concurrent AddCheck() calls
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.AddCheck(func() string { return "" })
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test passed - no race conditions
	}
}

// ---------------------------------------------------------------------------
// Integration-style tests
// ---------------------------------------------------------------------------

func TestChecker_MultipleDBChecks(t *testing.T) {
	// Clear skip env vars
	for _, v := range []string{
		"SKIP_HEALTH_CHECK_MONGO",
		"SKIP_HEALTH_CHECK_REDIS",
		"SKIP_HEALTH_CHECK_MYSQL",
		"SKIP_HEALTH_CHECK_POSTGRES",
	} {
		os.Unsetenv(v)
	}

	c := NewChecker()

	// Simulate multiple database connections
	c.AddCheck(MongoCheck(func() error { return nil }, "users"))
	c.AddCheck(MongoCheck(func() error { return nil }, "orders"))
	c.AddCheck(RedisCheck(func() error { return nil }, "cache"))
	c.AddCheck(MySQLCheck(func() error { return nil }, "inventory"))

	if !c.IsHealthy() {
		t.Error("All checks should pass")
	}

	// Add a failing check
	c.AddCheck(PostgresCheck(func() error { return errors.New("down") }, "analytics"))

	if c.IsHealthy() {
		t.Error("Should be unhealthy with one failing check")
	}

	errs := c.Check()
	if len(errs) != 1 {
		t.Errorf("Expected 1 error, got %d: %v", len(errs), errs)
	}
}

func TestChecker_WriteHealthFile(t *testing.T) {
	healthPath := "/tmp/_healthz"

	// Clean up
	os.Remove(healthPath)
	defer os.Remove(healthPath)

	t.Run("healthy writes file", func(t *testing.T) {
		c := NewChecker()
		c.AddCheck(func() string { return "" })

		c.writeHealthFile()

		if _, err := os.Stat(healthPath); os.IsNotExist(err) {
			t.Error("Health file should exist when healthy")
		}
	})

	t.Run("unhealthy removes file", func(t *testing.T) {
		// First create the file
		os.WriteFile(healthPath, []byte("ok"), 0644)

		c := NewChecker()
		c.AddCheck(func() string { return "error" })

		c.writeHealthFile()

		if _, err := os.Stat(healthPath); !os.IsNotExist(err) {
			t.Error("Health file should be removed when unhealthy")
		}
	})
}
