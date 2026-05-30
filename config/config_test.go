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

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	cfg := New()
	if cfg == nil {
		t.Fatal("New() returned nil")
	}
	if cfg.values == nil {
		t.Fatal("New() returned Config with nil values map")
	}
}

func TestGetString(t *testing.T) {
	cfg := New()
	cfg.Set("TEST_KEY", "test_value")

	tests := []struct {
		name     string
		key      string
		defVal   string
		expected string
	}{
		{"existing key", "TEST_KEY", "default", "test_value"},
		{"missing key", "MISSING_KEY", "default", "default"},
		{"empty value key", "EMPTY_KEY", "default", "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.GetString(tt.key, tt.defVal)
			if got != tt.expected {
				t.Errorf("GetString(%q, %q) = %q; want %q", tt.key, tt.defVal, got, tt.expected)
			}
		})
	}
}

func TestGetInt(t *testing.T) {
	cfg := New()
	cfg.Set("INT_VAL", "42")
	cfg.Set("INVALID_INT", "not_a_number")

	tests := []struct {
		name     string
		key      string
		defVal   int
		expected int
	}{
		{"valid int", "INT_VAL", 0, 42},
		{"missing key", "MISSING", 99, 99},
		{"invalid int", "INVALID_INT", 100, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.GetInt(tt.key, tt.defVal)
			if got != tt.expected {
				t.Errorf("GetInt(%q, %d) = %d; want %d", tt.key, tt.defVal, got, tt.expected)
			}
		})
	}
}

func TestGetInt64(t *testing.T) {
	cfg := New()
	cfg.Set("INT64_VAL", "9223372036854775807") // max int64

	got := cfg.GetInt64("INT64_VAL", 0)
	if got != 9223372036854775807 {
		t.Errorf("GetInt64 returned %d; want max int64", got)
	}

	got = cfg.GetInt64("MISSING", 123)
	if got != 123 {
		t.Errorf("GetInt64 for missing key returned %d; want 123", got)
	}
}

func TestGetBool(t *testing.T) {
	cfg := New()
	cfg.Set("BOOL_TRUE", "true")
	cfg.Set("BOOL_YES", "yes")
	cfg.Set("BOOL_ONE", "1")
	cfg.Set("BOOL_ON", "ON")
	cfg.Set("BOOL_FALSE", "false")
	cfg.Set("BOOL_NO", "no")
	cfg.Set("BOOL_ZERO", "0")
	cfg.Set("BOOL_OFF", "off")
	cfg.Set("BOOL_INVALID", "maybe")

	tests := []struct {
		name     string
		key      string
		defVal   bool
		expected bool
	}{
		{"true", "BOOL_TRUE", false, true},
		{"yes", "BOOL_YES", false, true},
		{"1", "BOOL_ONE", false, true},
		{"ON", "BOOL_ON", false, true},
		{"false", "BOOL_FALSE", true, false},
		{"no", "BOOL_NO", true, false},
		{"0", "BOOL_ZERO", true, false},
		{"off", "BOOL_OFF", true, false},
		{"invalid uses default", "BOOL_INVALID", true, true},
		{"missing uses default", "MISSING", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.GetBool(tt.key, tt.defVal)
			if got != tt.expected {
				t.Errorf("GetBool(%q, %v) = %v; want %v", tt.key, tt.defVal, got, tt.expected)
			}
		})
	}
}

func TestGetFloat(t *testing.T) {
	cfg := New()
	cfg.Set("FLOAT_VAL", "3.14159")
	cfg.Set("INT_AS_FLOAT", "42")

	got := cfg.GetFloat("FLOAT_VAL", 0)
	if got < 3.14 || got > 3.15 {
		t.Errorf("GetFloat returned %f; want ~3.14159", got)
	}

	got = cfg.GetFloat("INT_AS_FLOAT", 0)
	if got != 42.0 {
		t.Errorf("GetFloat for int value returned %f; want 42.0", got)
	}

	got = cfg.GetFloat("MISSING", 1.5)
	if got != 1.5 {
		t.Errorf("GetFloat for missing key returned %f; want 1.5", got)
	}
}

func TestGetDuration(t *testing.T) {
	cfg := New()
	cfg.Set("DUR_GO", "30s")
	cfg.Set("DUR_MS", "5000") // plain integer treated as milliseconds
	cfg.Set("DUR_HOUR", "1h")
	cfg.Set("DUR_INVALID", "not_a_duration")

	tests := []struct {
		name     string
		key      string
		defVal   time.Duration
		expected time.Duration
	}{
		{"Go duration 30s", "DUR_GO", 0, 30 * time.Second},
		{"milliseconds 5000", "DUR_MS", 0, 5000 * time.Millisecond},
		{"Go duration 1h", "DUR_HOUR", 0, time.Hour},
		{"invalid uses default", "DUR_INVALID", 10 * time.Second, 10 * time.Second},
		{"missing uses default", "MISSING", 2 * time.Minute, 2 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.GetDuration(tt.key, tt.defVal)
			if got != tt.expected {
				t.Errorf("GetDuration(%q, %v) = %v; want %v", tt.key, tt.defVal, got, tt.expected)
			}
		})
	}
}

func TestGetStringSlice(t *testing.T) {
	cfg := New()
	cfg.Set("SLICE_VAL", "a,b,c")
	cfg.Set("SLICE_SPACES", " x , y , z ")
	cfg.Set("SINGLE_VAL", "only_one")
	cfg.Set("EMPTY_SLICE", "")

	tests := []struct {
		name     string
		key      string
		defVal   []string
		expected []string
	}{
		{"comma separated", "SLICE_VAL", nil, []string{"a", "b", "c"}},
		{"with spaces", "SLICE_SPACES", nil, []string{"x", "y", "z"}},
		{"single value", "SINGLE_VAL", nil, []string{"only_one"}},
		{"empty uses default", "EMPTY_SLICE", []string{"def"}, []string{"def"}},
		{"missing uses default", "MISSING", []string{"def"}, []string{"def"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cfg.GetStringSlice(tt.key, tt.defVal)
			if len(got) != len(tt.expected) {
				t.Errorf("GetStringSlice(%q) length = %d; want %d", tt.key, len(got), len(tt.expected))
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("GetStringSlice(%q)[%d] = %q; want %q", tt.key, i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestSet(t *testing.T) {
	cfg := New()

	cfg.Set("KEY1", "value1")
	if got := cfg.GetString("KEY1", ""); got != "value1" {
		t.Errorf("Set then Get returned %q; want %q", got, "value1")
	}

	// Override
	cfg.Set("KEY1", "value2")
	if got := cfg.GetString("KEY1", ""); got != "value2" {
		t.Errorf("Set override returned %q; want %q", got, "value2")
	}
}

func TestSetAll(t *testing.T) {
	cfg := New()
	cfg.Set("EXISTING", "old")

	cfg.SetAll(map[string]string{
		"NEW_KEY":  "new_value",
		"EXISTING": "overwritten",
	})

	if got := cfg.GetString("NEW_KEY", ""); got != "new_value" {
		t.Errorf("SetAll new key returned %q; want %q", got, "new_value")
	}
	if got := cfg.GetString("EXISTING", ""); got != "overwritten" {
		t.Errorf("SetAll override returned %q; want %q", got, "overwritten")
	}
}

func TestHas(t *testing.T) {
	cfg := New()
	cfg.Set("EXISTS", "value")
	cfg.Set("EMPTY", "")

	if !cfg.Has("EXISTS") {
		t.Error("Has() returned false for existing key")
	}
	if !cfg.Has("EMPTY") {
		t.Error("Has() returned false for key with empty value")
	}
	if cfg.Has("MISSING") {
		t.Error("Has() returned true for missing key")
	}
}

func TestRaw(t *testing.T) {
	cfg := New()
	cfg.Set("RAW_KEY", "raw_value")

	val, ok := cfg.Raw("RAW_KEY")
	if !ok {
		t.Error("Raw() returned false for existing key")
	}
	if val != "raw_value" {
		t.Errorf("Raw() returned %q; want %q", val, "raw_value")
	}

	_, ok = cfg.Raw("MISSING")
	if ok {
		t.Error("Raw() returned true for missing key")
	}
}

func TestKeys(t *testing.T) {
	cfg := New()
	cfg.Set("A", "1")
	cfg.Set("B", "2")
	cfg.Set("C", "3")

	keys := cfg.Keys()
	if len(keys) != 3 {
		t.Errorf("Keys() returned %d keys; want 3", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, expected := range []string{"A", "B", "C"} {
		if !keySet[expected] {
			t.Errorf("Keys() missing expected key %q", expected)
		}
	}
}

func TestValidate(t *testing.T) {
	cfg := New()
	cfg.Set("SERVICE_NAME", "my-service")
	cfg.Set("NODE_ENV", "production")
	cfg.Set("LOG_LEVEL", "info")

	// Valid config
	rules := []ValidationRule{
		{Key: "SERVICE_NAME", Required: true},
		{Key: "NODE_ENV", AllowedValues: []string{"development", "staging", "production"}},
		{Key: "LOG_LEVEL", AllowedValues: []string{"debug", "info", "warn", "error"}},
	}

	if err := cfg.Validate(rules); err != nil {
		t.Errorf("Validate() returned error for valid config: %v", err)
	}

	// Missing required
	cfg2 := New()
	err := cfg2.Validate([]ValidationRule{
		{Key: "REQUIRED_KEY", Required: true, Description: "This is required"},
	})
	if err == nil {
		t.Error("Validate() should return error for missing required key")
	}
	if _, ok := err.(*ValidationError); !ok {
		t.Error("Validate() should return *ValidationError")
	}

	// Invalid allowed value
	cfg3 := New()
	cfg3.Set("ENV", "invalid_env")
	err = cfg3.Validate([]ValidationRule{
		{Key: "ENV", AllowedValues: []string{"dev", "prod"}},
	})
	if err == nil {
		t.Error("Validate() should return error for invalid allowed value")
	}
}

func TestLoadJSON(t *testing.T) {
	// Create temp JSON file
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "config.json")

	jsonContent := `{
		"port": 8080,
		"debug": true,
		"name": "test-service",
		"database": {
			"host": "localhost",
			"port": 5432
		}
	}`

	if err := os.WriteFile(jsonPath, []byte(jsonContent), 0644); err != nil {
		t.Fatalf("Failed to write test JSON: %v", err)
	}

	cfg, err := Load(jsonPath)
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check flat values
	if got := cfg.GetInt("PORT", 0); got != 8080 {
		t.Errorf("PORT = %d; want 8080", got)
	}
	if got := cfg.GetBool("DEBUG", false); got != true {
		t.Error("DEBUG should be true")
	}
	if got := cfg.GetString("NAME", ""); got != "test-service" {
		t.Errorf("NAME = %q; want %q", got, "test-service")
	}

	// Check nested values (flattened with underscore)
	if got := cfg.GetString("DATABASE_HOST", ""); got != "localhost" {
		t.Errorf("DATABASE_HOST = %q; want %q", got, "localhost")
	}
	if got := cfg.GetInt("DATABASE_PORT", 0); got != 5432 {
		t.Errorf("DATABASE_PORT = %d; want 5432", got)
	}
}

func TestLoadEnvPrecedence(t *testing.T) {
	// Set env var before loading config
	os.Setenv("TEST_PRECEDENCE", "from_env")
	defer os.Unsetenv("TEST_PRECEDENCE")

	// Create temp JSON with same key
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "config.json")
	jsonContent := `{"test_precedence": "from_file"}`

	if err := os.WriteFile(jsonPath, []byte(jsonContent), 0644); err != nil {
		t.Fatalf("Failed to write test JSON: %v", err)
	}

	cfg, err := Load(jsonPath)
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Env var should take precedence
	got := cfg.GetString("TEST_PRECEDENCE", "")
	if got != "from_env" {
		t.Errorf("Environment variable should take precedence; got %q, want %q", got, "from_env")
	}
}

func TestParseDotenvLine(t *testing.T) {
	tests := []struct {
		line      string
		wantKey   string
		wantValue string
		wantOk    bool
	}{
		{"KEY=value", "KEY", "value", true},
		{"KEY=\"quoted value\"", "KEY", "quoted value", true},
		{"KEY='single quoted'", "KEY", "single quoted", true},
		{"export KEY=exported", "KEY", "exported", true},
		{" KEY = spaced ", "KEY", "spaced", true},
		{"EMPTY=", "EMPTY", "", true},
		{"# comment", "", "", false},
		{"invalid line", "", "", false},
		{"=no_key", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.line, func(t *testing.T) {
			key, value, ok := parseDotenvLine(tt.line)
			if ok != tt.wantOk {
				t.Errorf("parseDotenvLine(%q) ok = %v; want %v", tt.line, ok, tt.wantOk)
				return
			}
			if ok {
				if key != tt.wantKey {
					t.Errorf("parseDotenvLine(%q) key = %q; want %q", tt.line, key, tt.wantKey)
				}
				if value != tt.wantValue {
					t.Errorf("parseDotenvLine(%q) value = %q; want %q", tt.line, value, tt.wantValue)
				}
			}
		})
	}
}

func TestFlattenMap(t *testing.T) {
	input := map[string]interface{}{
		"simple": "value",
		"number": float64(42),
		"nested": map[string]interface{}{
			"inner": "nested_value",
			"deep": map[string]interface{}{
				"leaf": "deep_value",
			},
		},
	}

	result := flattenMap("", input)

	expected := map[string]string{
		"SIMPLE":           "value",
		"NUMBER":           "42",
		"NESTED_INNER":     "nested_value",
		"NESTED_DEEP_LEAF": "deep_value",
	}

	for k, v := range expected {
		if result[k] != v {
			t.Errorf("flattenMap()[%q] = %q; want %q", k, result[k], v)
		}
	}
}

func TestDefaultValidationRules(t *testing.T) {
	rules := DefaultValidationRules()
	if len(rules) == 0 {
		t.Error("DefaultValidationRules() returned empty slice")
	}

	// Check SERVICE_NAME is required
	var hasServiceName bool
	for _, r := range rules {
		if r.Key == "SERVICE_NAME" {
			hasServiceName = true
			if !r.Required {
				t.Error("SERVICE_NAME should be required")
			}
		}
	}
	if !hasServiceName {
		t.Error("DefaultValidationRules() should include SERVICE_NAME")
	}
}

// TestConcurrentAccess verifies thread-safety of Config operations.
func TestConcurrentAccess(t *testing.T) {
	cfg := New()
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			cfg.Set("CONCURRENT_KEY", "value")
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			_ = cfg.GetString("CONCURRENT_KEY", "default")
		}
		done <- true
	}()

	<-done
	<-done
	// If we get here without a race condition crash, the test passes
}
