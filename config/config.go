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

// Package config provides type-safe configuration management for the Fit framework.
// Go implementation of convict module and environment variable handling.
//
// Config supports loading values from environment variables, JSON/YAML config files,
// and provides type-safe getters with default values. All access is thread-safe via
// sync.RWMutex.
//
// Usage:
//
//	cfg, err := config.Load("config.json", "config.yaml")
//	port := cfg.GetInt("PORT", 8080)
//	debug := cfg.GetBool("DEBUG", false)
//	name := cfg.GetString("SERVICE_NAME", "my-service")
package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config holds all configuration values loaded from environment variables and
// config files. It provides type-safe getters with defaults and is safe for
// concurrent access.
type Config struct {
	mu     sync.RWMutex
	values map[string]string
}

// New creates a new empty Config instance.
func New() *Config {
	return &Config{
		values: make(map[string]string),
	}
}

// Load initializes a Config by loading environment variables first, then
// overlaying values from the provided config file paths. Files are applied in
// order, so later files override earlier ones. Environment variables always
// take precedence over file values.
//
// Supported file formats: .json.yaml.yml. The .env format is also loaded
// automatically if a .env file exists in the working directory.
func Load(paths ...string) (*Config, error) {
	cfg := New()

	// 1. Load .env file if present.
	if err := cfg.loadDotenv(); err != nil {
		// Non-fatal: .env file is optional.
		_ = err
	}

	// 2. Load all environment variables.
	cfg.loadEnvVars()

	// 3. Overlay config files in order. File values do NOT override env vars
	// that are already set, matching the convict behavior where env always wins.
	for _, path := range paths {
		if err := cfg.loadFile(path); err != nil {
			return nil, fmt.Errorf("config: failed to load %s: %w", path, err)
		}
	}

	return cfg, nil
}

// loadDotenv reads a .env file from the current directory or the path specified
// by DOTENV_PATH. Lines are parsed as KEY=VALUE pairs. Lines starting with #
// are treated as comments.
func (c *Config) loadDotenv() error {
	dotenvPath := os.Getenv("DOTENV_PATH")
	if dotenvPath == "" {
		dotenvPath = ".env"
	}

	f, err := os.Open(dotenvPath)
	if err != nil {
		return err // file doesn't exist, that's fine
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := parseDotenvLine(line)
		if !ok {
			continue
		}

		// Only set in the process environment if not already set,
		// matching dotenv behavior.
		if os.Getenv(key) == "" {
			os.Setenv(key, value)
		}
	}

	return scanner.Err()
}

// parseDotenvLine parses a single line from a .env file. It handles quoted
// values (single and double quotes) and inline comments.
func parseDotenvLine(line string) (key, value string, ok bool) {
	// Split on first '='.
	idx := strings.IndexByte(line, '=')
	if idx < 0 {
		return "", "", false
	}

	key = strings.TrimSpace(line[:idx])
	value = strings.TrimSpace(line[idx+1:])

	if key == "" {
		return "", "", false
	}

	// Handle 'export KEY=VALUE' syntax.
	if strings.HasPrefix(key, "export ") {
		key = strings.TrimSpace(strings.TrimPrefix(key, "export"))
	}

	// Strip surrounding quotes.
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') ||
			(value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}
	}

	return key, value, true
}

// loadEnvVars reads all current environment variables into the config map.
func (c *Config) loadEnvVars() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, env := range os.Environ() {
		idx := strings.IndexByte(env, '=')
		if idx < 0 {
			continue
		}
		key := env[:idx]
		value := env[idx+1:]
		c.values[key] = value
	}
}

// loadFile loads a config file and merges its values into the config. Values
// from the file only apply if the key is not already set (environment variables
// take precedence).
func (c *Config) loadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".json":
		return c.loadJSON(data)
	case ".yaml", ".yml":
		return c.loadYAML(data)
	default:
		return fmt.Errorf("unsupported config file format: %s", ext)
	}
}

// loadJSON parses JSON config data. Supports flat key-value objects and nested
// objects which are flattened with underscore separators (e.g., {"db": {"host": "x"}}
// becomes DB_HOST).
func (c *Config) loadJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	flat := flattenMap("", raw)

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range flat {
		// Only set if not already present (env vars win).
		if _, exists := c.values[k]; !exists {
			c.values[k] = v
		}
	}

	return nil
}

// loadYAML parses a subset of YAML (simple key: value pairs and one level of
// nesting). This avoids pulling in a YAML dependency. For full YAML support,
// use JSON config files or set values via environment variables.
func (c *Config) loadYAML(data []byte) error {
	result := make(map[string]interface{})
	var currentSection string

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and comments.
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// Check indentation to determine nesting.
		indent := len(line) - len(strings.TrimLeft(line, " \t"))

		idx := strings.IndexByte(trimmed, ':')
		if idx < 0 {
			continue
		}

		key := strings.TrimSpace(trimmed[:idx])
		value := strings.TrimSpace(trimmed[idx+1:])

		if indent == 0 {
			if value == "" {
				// Section header.
				currentSection = key
				if _, ok := result[currentSection]; !ok {
					result[currentSection] = make(map[string]interface{})
				}
			} else {
				currentSection = ""
				result[key] = stripYAMLQuotes(value)
			}
		} else if currentSection != "" {
			section, ok := result[currentSection].(map[string]interface{})
			if !ok {
				section = make(map[string]interface{})
				result[currentSection] = section
			}
			section[key] = stripYAMLQuotes(value)
		}
	}

	flat := flattenMap("", result)

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range flat {
		if _, exists := c.values[k]; !exists {
			c.values[k] = v
		}
	}

	return scanner.Err()
}

// stripYAMLQuotes removes surrounding quotes from a YAML value string.
func stripYAMLQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') ||
			(s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// flattenMap recursively flattens a nested map into a flat map with
// UPPER_SNAKE_CASE keys joined by underscores.
func flattenMap(prefix string, m map[string]interface{}) map[string]string {
	result := make(map[string]string)

	for k, v := range m {
		fullKey := strings.ToUpper(k)
		if prefix != "" {
			fullKey = prefix + "_" + fullKey
		}

		switch val := v.(type) {
		case map[string]interface{}:
			for fk, fv := range flattenMap(fullKey, val) {
				result[fk] = fv
			}
		case string:
			result[fullKey] = val
		case float64:
			if val == float64(int64(val)) {
				result[fullKey] = strconv.FormatInt(int64(val), 10)
			} else {
				result[fullKey] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		case bool:
			result[fullKey] = strconv.FormatBool(val)
		case nil:
			result[fullKey] = ""
		default:
			result[fullKey] = fmt.Sprintf("%v", val)
		}
	}

	return result
}

// ---------------------------------------------------------------------------
// Type-safe getters
// ---------------------------------------------------------------------------

// GetString returns the config value for the given key as a string.
// If the key is not found, it returns the provided default value.
func (c *Config) GetString(key, defaultValue string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		return v
	}
	return defaultValue
}

// GetInt returns the config value for the given key as an int.
// If the key is not found or cannot be parsed, it returns the default.
func (c *Config) GetInt(key string, defaultValue int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// GetInt64 returns the config value for the given key as an int64.
// If the key is not found or cannot be parsed, it returns the default.
func (c *Config) GetInt64(key string, defaultValue int64) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// GetBool returns the config value for the given key as a bool.
// Truthy values: "true", "1", "yes", "on" (case-insensitive).
// If the key is not found or cannot be parsed, it returns the default.
func (c *Config) GetBool(key string, defaultValue bool) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		normalized := strings.ToLower(strings.TrimSpace(v))
		switch normalized {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return defaultValue
}

// GetFloat returns the config value for the given key as a float64.
// If the key is not found or cannot be parsed, it returns the default.
func (c *Config) GetFloat(key string, defaultValue float64) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// GetDuration returns the config value for the given key as a time.Duration.
// The value is parsed using time.ParseDuration (e.g., "30s", "5m", "1h").
// If the value is a plain integer, it is treated as milliseconds.
// If the key is not found or cannot be parsed, it returns the default.
func (c *Config) GetDuration(key string, defaultValue time.Duration) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		// First try standard Go duration format.
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		// Fall back to treating plain integers as milliseconds.
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultValue
}

// GetStringSlice returns the config value for the given key as a string slice.
// The value is split on commas. Each element is trimmed of whitespace.
// If the key is not found, it returns the default.
func (c *Config) GetStringSlice(key string, defaultValue []string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.values[key]; ok && v != "" {
		parts := strings.Split(v, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
		if len(result) > 0 {
			return result
		}
	}
	return defaultValue
}

// ---------------------------------------------------------------------------
// Mutation
// ---------------------------------------------------------------------------

// Set stores a key-value pair in the config. This is useful for programmatic
// overrides (e.g., from command-line flags).
func (c *Config) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.values[key] = value
}

// SetAll merges a map of key-value pairs into the config, overwriting any
// existing values.
func (c *Config) SetAll(m map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range m {
		c.values[k] = v
	}
}

// Has returns true if the given key exists in the config (even if empty).
func (c *Config) Has(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.values[key]
	return ok
}

// Raw returns the raw string value for a key, along with a boolean indicating
// whether the key was found.
func (c *Config) Raw(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	v, ok := c.values[key]
	return v, ok
}

// Keys returns all config keys.
func (c *Config) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	return keys
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// ValidationRule defines a validation constraint for a config key.
type ValidationRule struct {
	// Key is the config key to validate.
	Key string

	// Required indicates the key must be present and non-empty.
	Required bool

	// AllowedValues restricts the value to one of the listed strings.
	// An empty slice means any value is accepted.
	AllowedValues []string

	// Description is a human-readable explanation, used in error messages.
	Description string
}

// ValidationError is returned when config validation fails.
type ValidationError struct {
	Errors []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("config validation failed:\n - %s", strings.Join(e.Errors, "\n - "))
}

// Validate checks the config against the provided rules. It returns a
// *ValidationError listing all violations, or nil if everything passes.
func (c *Config) Validate(rules []ValidationRule) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var errs []string

	for _, rule := range rules {
		value, exists := c.values[rule.Key]

		if rule.Required && (!exists || strings.TrimSpace(value) == "") {
			msg := fmt.Sprintf("required key %q is missing or empty", rule.Key)
			if rule.Description != "" {
				msg += fmt.Sprintf(" (%s)", rule.Description)
			}
			errs = append(errs, msg)
			continue
		}

		if len(rule.AllowedValues) > 0 && exists && value != "" {
			found := false
			for _, allowed := range rule.AllowedValues {
				if strings.EqualFold(value, allowed) {
					found = true
					break
				}
			}
			if !found {
				errs = append(errs, fmt.Sprintf(
					"key %q has value %q, must be one of: %s",
					rule.Key, value, strings.Join(rule.AllowedValues, ", "),
				))
			}
		}
	}

	if len(errs) > 0 {
		return &ValidationError{Errors: errs}
	}
	return nil
}

// DefaultValidationRules returns a baseline set of validation rules covering
// the standard environment variables Services
// can extend this list with their own rules.
func DefaultValidationRules() []ValidationRule {
	return []ValidationRule{
		{Key: "SERVICE_NAME", Required: true, Description: "service name for logging and tracing"},
		{Key: "SERVICE_NAME_CODE", Required: false, Description: "short code for error prefixes"},
		{Key: "NODE_ENV", Required: false, AllowedValues: []string{"development", "staging", "production"}, Description: "runtime environment"},
		{Key: "SERVER_TYPE", Required: false, Description: "platform, application, partner, etc."},
		{Key: "LOG_LEVEL", Required: false, AllowedValues: []string{"debug", "info", "warn", "error", "fatal", "silly"}, Description: "logging verbosity"},
	}
}
