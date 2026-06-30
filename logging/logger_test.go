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

package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/goroutinectx"
)

// ---------------------------------------------------------------------------
// Level tests
// ---------------------------------------------------------------------------

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"WARN", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"fatal", LevelFatal},
		{"FATAL", LevelFatal},
		{"unknown", LevelInfo},
		{"", LevelInfo},
		{" info ", LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseLevel(tt.input); got != tt.expected {
				t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLevel_String(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarn, "warn"},
		{LevelError, "error"},
		{LevelFatal, "fatal"},
		{Level(99), "info"}, // unknown level
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("Level.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// IsHealthCheckPath tests
// ---------------------------------------------------------------------------

func TestIsHealthCheckPath(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/_healthz", true},
		{"/_readyz", true},
		{"/api/users", false},
		{"/", false},
		{"/_health", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := IsHealthCheckPath(tt.path); got != tt.expected {
				t.Errorf("IsHealthCheckPath(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Logger creation tests
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	t.Run("default options", func(t *testing.T) {
		logger, err := New(Options{})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if logger == nil {
			t.Fatal("New() returned nil")
		}
		if logger.level != LevelInfo {
			t.Errorf("Default level = %v, want info", logger.level)
		}
	})

	t.Run("with custom level", func(t *testing.T) {
		logger, err := New(Options{Level: "debug"})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if logger.level != LevelDebug {
			t.Errorf("Level = %v, want debug", logger.level)
		}
	})

	t.Run("with timezone", func(t *testing.T) {
		logger, err := New(Options{Timezone: "America/New_York"})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if logger.loc.String() != "America/New_York" {
			t.Errorf("Timezone = %q, want America/New_York", logger.loc.String())
		}
	})

	t.Run("with invalid timezone", func(t *testing.T) {
		_, err := New(Options{Timezone: "Invalid/Timezone"})
		if err == nil {
			t.Error("New() should return error for invalid timezone")
		}
	})

	t.Run("with service name", func(t *testing.T) {
		logger, err := New(Options{Service: "my-service"})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if logger.service != "my-service" {
			t.Errorf("Service = %q, want my-service", logger.service)
		}
	})

	t.Run("production env uses stdout", func(t *testing.T) {
		logger, err := New(Options{Env: "production"})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if logger.colorize {
			t.Error("Production should not colorize")
		}
	})

	t.Run("development env colorizes", func(t *testing.T) {
		logger, err := New(Options{Env: "development"})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if !logger.colorize {
			t.Error("Development should colorize")
		}
	})

	t.Run("with env var fallbacks", func(t *testing.T) {
		os.Setenv("LOG_LEVEL", "warn")
		os.Setenv("LOG_TIMEZONE", "Europe/London")
		os.Setenv("NODE_ENV", "staging")
		defer func() {
			os.Unsetenv("LOG_LEVEL")
			os.Unsetenv("LOG_TIMEZONE")
			os.Unsetenv("NODE_ENV")
		}()

		logger, err := New(Options{})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if logger.level != LevelWarn {
			t.Errorf("Level = %v, want warn from env", logger.level)
		}
		if logger.loc.String() != "Europe/London" {
			t.Errorf("Timezone = %q, want Europe/London from env", logger.loc.String())
		}
	})
}

// ---------------------------------------------------------------------------
// Logger methods tests
// ---------------------------------------------------------------------------

func TestLogger_LogMethods(t *testing.T) {
	tests := []struct {
		name      string
		logMethod func(*Logger, string, ...interface{})
		level     string
	}{
		{"Debug", (*Logger).Debug, "debug"},
		{"Info", (*Logger).Info, "info"},
		{"Warn", (*Logger).Warn, "warn"},
		{"Error", (*Logger).Error, "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger, _ := New(Options{
				Level:  "debug",
				Output: &buf,
				Env:    "production", // no colors
			})

			tt.logMethod(logger, "test message", "key", "value")

			var entry map[string]interface{}
			if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
				t.Fatalf("Failed to parse log output: %v", err)
			}

			if entry["level"] != tt.level {
				t.Errorf("Level = %v, want %q", entry["level"], tt.level)
			}
			if entry["message"] != "test message" {
				t.Errorf("Message = %v, want 'test message'", entry["message"])
			}
			if extra, ok := entry["extra"].(map[string]interface{}); ok {
				if extra["key"] != "value" {
					t.Errorf("Extra key = %v, want 'value'", extra["key"])
				}
			} else {
				t.Error("Extra should be present")
			}
		})
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	t.Run("debug not logged at info level", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Debug("this should not appear")

		if buf.Len() > 0 {
			t.Error("Debug message should not be logged at info level")
		}
	})

	t.Run("info logged at info level", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Info("this should appear")

		if buf.Len() == 0 {
			t.Error("Info message should be logged at info level")
		}
	})

	t.Run("warn logged at info level", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Warn("warning message")

		if buf.Len() == 0 {
			t.Error("Warn message should be logged at info level")
		}
	})
}

func TestLogger_SetLevel(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "error",
		Output: &buf,
		Env:    "production",
	})

	logger.Info("should not appear")
	if buf.Len() > 0 {
		t.Error("Info should not be logged at error level")
	}

	logger.SetLevel("info")
	logger.Info("should appear")
	if buf.Len() == 0 {
		t.Error("Info should be logged after SetLevel")
	}
}

// ---------------------------------------------------------------------------
// Logger derivation tests
// ---------------------------------------------------------------------------

func TestLogger_WithContext(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "info",
		Output: &buf,
		Env:    "production",
	})

	ctx := ContextWithTrace(context.Background(), "trace-123", "span-456")
	ctxLogger := logger.WithContext(ctx)

	ctxLogger.Info("test with context")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["trace_id"] != "trace-123" {
		t.Errorf("trace_id = %v, want trace-123", entry["trace_id"])
	}
	if entry["span_id"] != "span-456" {
		t.Errorf("span_id = %v, want span-456", entry["span_id"])
	}
}

func TestLogger_WithFields(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "info",
		Output: &buf,
		Env:    "production",
	})

	fieldLogger := logger.WithFields(map[string]interface{}{
		"request_id": "req-123",
		"user_id":    "user-456",
	})

	fieldLogger.Info("test with fields")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	extra, ok := entry["extra"].(map[string]interface{})
	if !ok {
		t.Fatal("Extra should be present")
	}
	if extra["request_id"] != "req-123" {
		t.Errorf("request_id = %v, want req-123", extra["request_id"])
	}
	if extra["user_id"] != "user-456" {
		t.Errorf("user_id = %v, want user-456", extra["user_id"])
	}
}

func TestLogger_WithService(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "info",
		Output: &buf,
		Env:    "production",
	})

	serviceLogger := logger.WithService("api-gateway")
	serviceLogger.Info("test with service")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["service"] != "api-gateway" {
		t.Errorf("service = %v, want api-gateway", entry["service"])
	}
}

func TestLogger_ChainedDerivation(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "info",
		Output: &buf,
		Env:    "production",
	})

	ctx := ContextWithTrace(context.Background(), "trace-123", "span-456")
	derivedLogger := logger.
		WithService("my-service").
		WithContext(ctx).
		WithFields(map[string]interface{}{"env": "test"})

	derivedLogger.Info("chained derivation")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["service"] != "my-service" {
		t.Errorf("service = %v, want my-service", entry["service"])
	}
	if entry["trace_id"] != "trace-123" {
		t.Errorf("trace_id = %v, want trace-123", entry["trace_id"])
	}
	if extra, ok := entry["extra"].(map[string]interface{}); ok {
		if extra["env"] != "test" {
			t.Errorf("env = %v, want test", extra["env"])
		}
	}
}

func TestLogger_DerivationIndependence(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "info",
		Output: &buf,
		Env:    "production",
	})

	// Create two derived loggers
	derived1 := logger.WithFields(map[string]interface{}{"key": "derived1"})
	derived2 := logger.WithFields(map[string]interface{}{"other": "derived2"})

	// Verify derived loggers have independent fields
	buf.Reset()
	derived1.Info("from derived1")
	var entry1 map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry1)
	extra1 := entry1["extra"].(map[string]interface{})
	if extra1["key"] != "derived1" {
		t.Error("derived1 should have its own field")
	}
	if _, hasOther := extra1["other"]; hasOther {
		t.Error("derived1 should not have derived2's field")
	}

	buf.Reset()
	derived2.Info("from derived2")
	var entry2 map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry2)
	extra2 := entry2["extra"].(map[string]interface{})
	if extra2["other"] != "derived2" {
		t.Error("derived2 should have its own field")
	}
	if _, hasKey := extra2["key"]; hasKey {
		t.Error("derived2 should not have derived1's field")
	}
}

// ---------------------------------------------------------------------------
// Key-value pair handling tests
// ---------------------------------------------------------------------------

func TestLogger_KeyValuePairs(t *testing.T) {
	t.Run("normal pairs", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Info("test", "k1", "v1", "k2", 123)

		var entry map[string]interface{}
		json.Unmarshal(buf.Bytes(), &entry)

		extra := entry["extra"].(map[string]interface{})
		if extra["k1"] != "v1" {
			t.Errorf("k1 = %v, want v1", extra["k1"])
		}
		if extra["k2"] != float64(123) { // JSON numbers are float64
			t.Errorf("k2 = %v, want 123", extra["k2"])
		}
	})

	t.Run("odd number of args", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Info("test", "k1", "v1", "k2")

		var entry map[string]interface{}
		json.Unmarshal(buf.Bytes(), &entry)

		extra := entry["extra"].(map[string]interface{})
		if extra["k2"] != "!MISSING_VALUE" {
			t.Errorf("k2 = %v, want !MISSING_VALUE", extra["k2"])
		}
	})

	t.Run("non-string key", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Info("test", 123, "value")

		var entry map[string]interface{}
		json.Unmarshal(buf.Bytes(), &entry)

		extra := entry["extra"].(map[string]interface{})
		found := false
		for k := range extra {
			if strings.HasPrefix(k, "!BADKEY") {
				found = true
			}
		}
		if !found {
			t.Error("Should create BADKEY for non-string key")
		}
	})

	t.Run("error value", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Info("test", "error", &testError{msg: "something went wrong"})

		var entry map[string]interface{}
		json.Unmarshal(buf.Bytes(), &entry)

		extra := entry["extra"].(map[string]interface{})
		if extra["error"] != "something went wrong" {
			t.Errorf("error = %v, want 'something went wrong'", extra["error"])
		}
	})
}

// ---------------------------------------------------------------------------
// Caller info tests
// ---------------------------------------------------------------------------

func TestLogger_CallerInfo(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "debug",
		Output: &buf,
		Env:    "production",
	})

	logger.Error("error with caller")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	caller, ok := entry["caller"].(string)
	if !ok || caller == "" {
		t.Error("Error level should include caller info")
	}
	if !strings.Contains(caller, "logger_test.go") {
		t.Errorf("Caller = %q, should contain logger_test.go", caller)
	}
}

func TestLogger_NoCallerForInfo(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "info",
		Output: &buf,
		Env:    "production",
	})

	logger.Info("info without caller")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if _, ok := entry["caller"]; ok {
		t.Error("Info level should not include caller info")
	}
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

func TestLogger_ConcurrentAccess(t *testing.T) {
	var buf bytes.Buffer
	logger, _ := New(Options{
		Level:  "debug",
		Output: &buf,
		Env:    "production",
	})

	var wg sync.WaitGroup

	// Concurrent logging
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logger.Info("concurrent log", "index", i)
		}(i)
	}

	// Concurrent level changes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.SetLevel("info")
		}()
	}

	// Concurrent derivation
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			derived := logger.WithFields(map[string]interface{}{"key": "value"})
			derived.Info("derived log")
		}()
	}

	wg.Wait()

	// Verify output contains valid JSON lines
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", i, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Colorization tests
// ---------------------------------------------------------------------------

func TestLogger_Colorization(t *testing.T) {
	t.Run("development mode colorizes", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "development",
		})

		logger.Info("colored message")

		output := buf.String()
		if !strings.Contains(output, "\033[") {
			t.Error("Development output should contain ANSI color codes")
		}
	})

	t.Run("production mode does not colorize", func(t *testing.T) {
		var buf bytes.Buffer
		logger, _ := New(Options{
			Level:  "info",
			Output: &buf,
			Env:    "production",
		})

		logger.Info("plain message")

		output := buf.String()
		if strings.Contains(output, "\033[") {
			t.Error("Production output should not contain ANSI color codes")
		}
	})
}

// ---------------------------------------------------------------------------
// ContextWithTrace tests
// ---------------------------------------------------------------------------

func TestContextWithTrace(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithTrace(ctx, "trace-abc", "span-xyz")

	traceID := ctx.Value(ctxKeyTraceID).(string)
	spanID := ctx.Value(ctxKeySpanID).(string)

	if traceID != "trace-abc" {
		t.Errorf("TraceID = %q, want trace-abc", traceID)
	}
	if spanID != "span-xyz" {
		t.Errorf("SpanID = %q, want span-xyz", spanID)
	}
}

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// When no trace context is explicitly bound, a plain log call must pick up the
// goroutine-local active context's OTel span (implicit trace propagation).
func TestLog_ImplicitTraceFromGoroutineLocal(t *testing.T) {
	var buf bytes.Buffer
	lg, err := New(Options{Level: "info", Env: "production", Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tid, _ := oteltrace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	sid, _ := oteltrace.SpanIDFromHex("0123456789abcdef")
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: tid, SpanID: sid})
	ctx := oteltrace.ContextWithSpanContext(context.Background(), sc)

	cleanup := goroutinectx.Inject(ctx)
	defer cleanup()

	lg.Info("hello") // no WithContext — must still carry the trace

	out := buf.String()
	if !strings.Contains(out, "0123456789abcdef0123456789abcdef") {
		t.Fatalf("log line missing implicit trace_id; got: %s", out)
	}
	if !strings.Contains(out, "0123456789abcdef") {
		t.Fatalf("log line missing implicit span_id; got: %s", out)
	}
}

// Without any goroutine-local context, logs carry no trace id (no false data).
func TestLog_NoImplicitTraceWhenNoneInjected(t *testing.T) {
	var buf bytes.Buffer
	lg, err := New(Options{Level: "info", Env: "production", Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lg.Info("plain")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatalf("unexpected trace_id in log without injected context: %s", buf.String())
	}
}
