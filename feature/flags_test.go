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

package feature

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Init tests
// ---------------------------------------------------------------------------

func TestInit_Disabled(t *testing.T) {
	os.Unsetenv("FEATURE_FLAG_ENABLED")

	client, err := Init()
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if client != nil {
		t.Error("Client should be nil when disabled")
	}
}

func TestInit_DisabledExplicitly(t *testing.T) {
	os.Setenv("FEATURE_FLAG_ENABLED", "false")
	defer os.Unsetenv("FEATURE_FLAG_ENABLED")

	client, err := Init()
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if client != nil {
		t.Error("Client should be nil when explicitly disabled")
	}
}

func TestInit_MissingURL(t *testing.T) {
	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Unsetenv("FEATURE_FLAG_URL")
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	_, err := Init()
	if err == nil {
		t.Error("Init() should fail when URL is missing")
	}
}

func TestInit_MissingAPIKey(t *testing.T) {
	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", "http://localhost")
	os.Unsetenv("FEATURE_FLAG_API_KEY")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
	}()

	_, err := Init()
	if err == nil {
		t.Error("Init() should fail when API key is missing")
	}
}

func TestInit_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"feature1": true,
			"feature2": "value",
		})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, err := Init()
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if client == nil {
		t.Fatal("Client should not be nil")
	}
	defer client.Stop()

	if !client.IsEnabled("feature1") {
		t.Error("feature1 should be enabled")
	}
}

func TestInit_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	_, err := Init()
	if err == nil {
		t.Error("Init() should fail when server returns error")
	}
}

// ---------------------------------------------------------------------------
// IsEnabled tests
// ---------------------------------------------------------------------------

func TestClient_IsEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"bool_true":    true,
			"bool_false":   false,
			"string_true":  "true",
			"string_false": "false",
			"number_one":   float64(1),
			"number_zero":  float64(0),
			"string_value": "some_value",
		})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, err := Init()
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer client.Stop()

	tests := []struct {
		key      string
		expected bool
	}{
		{"bool_true", true},
		{"bool_false", false},
		{"string_true", true},
		{"string_false", false},
		{"number_one", true},
		{"number_zero", false},
		{"string_value", false},
		{"nonexistent", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := client.IsEnabled(tt.key); got != tt.expected {
				t.Errorf("IsEnabled(%q) = %v, want %v", tt.key, got, tt.expected)
			}
		})
	}
}

func TestClient_IsEnabled_NilClient(t *testing.T) {
	var client *Client
	if client.IsEnabled("any") {
		t.Error("Nil client should return false")
	}
}

// ---------------------------------------------------------------------------
// GetValue tests
// ---------------------------------------------------------------------------

func TestClient_GetValue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"string":  "hello",
			"number":  42.5,
			"boolean": true,
		})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	if got := client.GetValue("string"); got != "hello" {
		t.Errorf("GetValue(string) = %v, want hello", got)
	}
	if got := client.GetValue("number"); got != 42.5 {
		t.Errorf("GetValue(number) = %v, want 42.5", got)
	}
	if got := client.GetValue("boolean"); got != true {
		t.Errorf("GetValue(boolean) = %v, want true", got)
	}
	if got := client.GetValue("nonexistent"); got != nil {
		t.Errorf("GetValue(nonexistent) = %v, want nil", got)
	}
}

func TestClient_GetValue_NilClient(t *testing.T) {
	var client *Client
	if client.GetValue("any") != nil {
		t.Error("Nil client should return nil")
	}
}

// ---------------------------------------------------------------------------
// Context setting tests
// ---------------------------------------------------------------------------

func TestClient_SetUserKey(t *testing.T) {
	var receivedUserKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserKey = r.Header.Get("X-User-Key")
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	client.SetUserKey("user-123")

	// Wait for refresh to complete
	time.Sleep(50 * time.Millisecond)

	if receivedUserKey != "user-123" {
		t.Errorf("User key header = %q, want user-123", receivedUserKey)
	}
}

func TestClient_SetUserKey_NilClient(t *testing.T) {
	var client *Client
	// Should not panic
	client.SetUserKey("user-123")
}

func TestClient_SetSessionKey(t *testing.T) {
	var receivedSessionKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSessionKey = r.Header.Get("X-Session-Key")
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	client.SetSessionKey("session-456")

	// Wait for refresh to complete
	time.Sleep(50 * time.Millisecond)

	if receivedSessionKey != "session-456" {
		t.Errorf("Session key header = %q, want session-456", receivedSessionKey)
	}
}

func TestClient_SetSessionKey_NilClient(t *testing.T) {
	var client *Client
	// Should not panic
	client.SetSessionKey("session-456")
}

func TestClient_ResetContext(t *testing.T) {
	var receivedUserKey, receivedSessionKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserKey = r.Header.Get("X-User-Key")
		receivedSessionKey = r.Header.Get("X-Session-Key")
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	client.SetUserKey("user-123")
	client.SetSessionKey("session-456")
	client.ResetContext()

	// Wait for refresh to complete
	time.Sleep(50 * time.Millisecond)

	if receivedUserKey != "" {
		t.Errorf("User key should be cleared, got %q", receivedUserKey)
	}
	if receivedSessionKey != "" {
		t.Errorf("Session key should be cleared, got %q", receivedSessionKey)
	}
}

func TestClient_ResetContext_NilClient(t *testing.T) {
	var client *Client
	// Should not panic
	client.ResetContext()
}

// ---------------------------------------------------------------------------
// Stop tests
// ---------------------------------------------------------------------------

func TestClient_Stop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()

	// Should not panic when called multiple times
	client.Stop()
	client.Stop()
}

func TestClient_Stop_NilClient(t *testing.T) {
	var client *Client
	// Should not panic
	client.Stop()
}

// ---------------------------------------------------------------------------
// FeatureHub array format tests
// ---------------------------------------------------------------------------

func TestClient_FeatureHubFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// FeatureHub returns array of features
		features := []map[string]interface{}{
			{"key": "feature1", "value": true},
			{"key": "feature2", "value": "enabled"},
			{"key": "feature3", "value": 42},
		}
		json.NewEncoder(w).Encode(features)
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, err := Init()
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer client.Stop()

	if !client.IsEnabled("feature1") {
		t.Error("feature1 should be enabled")
	}
	if client.GetValue("feature2") != "enabled" {
		t.Errorf("feature2 = %v, want enabled", client.GetValue("feature2"))
	}
	if client.GetValue("feature3") != float64(42) {
		t.Errorf("feature3 = %v, want 42", client.GetValue("feature3"))
	}
}

// ---------------------------------------------------------------------------
// Polling tests
// ---------------------------------------------------------------------------

func TestClient_Polling(t *testing.T) {
	var requestCount int
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	os.Setenv("FEATURE_FLAG_POLL_INTERVAL", "1") // 1 second
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
		os.Unsetenv("FEATURE_FLAG_POLL_INTERVAL")
	}()

	client, _ := Init()
	defer client.Stop()

	// Wait for at least one poll
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	count := requestCount
	mu.Unlock()

	if count < 2 { // Initial + at least one poll
		t.Errorf("Request count = %d, expected at least 2", count)
	}
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

func TestClient_ConcurrentAccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"feature1": true,
			"feature2": "value",
		})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.IsEnabled("feature1")
			_ = client.GetValue("feature2")
		}()
	}

	// Concurrent context updates
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				client.SetUserKey("user-" + string(rune('0'+i)))
			} else {
				client.SetSessionKey("session-" + string(rune('0'+i)))
			}
		}(i)
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// API Key header tests
// ---------------------------------------------------------------------------

func TestClient_APIKeyHeader(t *testing.T) {
	var receivedAPIKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAPIKey = r.Header.Get("X-Api-Key")
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL)
	os.Setenv("FEATURE_FLAG_API_KEY", "secret-api-key-123")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	if receivedAPIKey != "secret-api-key-123" {
		t.Errorf("API key header = %q, want secret-api-key-123", receivedAPIKey)
	}
}

// ---------------------------------------------------------------------------
// URL trimming tests
// ---------------------------------------------------------------------------

func TestClient_URLTrimming(t *testing.T) {
	var receivedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	// URL with trailing slash
	os.Setenv("FEATURE_FLAG_ENABLED", "true")
	os.Setenv("FEATURE_FLAG_URL", server.URL+"/")
	os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
	defer func() {
		os.Unsetenv("FEATURE_FLAG_ENABLED")
		os.Unsetenv("FEATURE_FLAG_URL")
		os.Unsetenv("FEATURE_FLAG_API_KEY")
	}()

	client, _ := Init()
	defer client.Stop()

	// Should not have double slash
	if receivedPath != "/features" {
		t.Errorf("Path = %q, want /features", receivedPath)
	}
}

// ---------------------------------------------------------------------------
// Case insensitive enabled check tests
// ---------------------------------------------------------------------------

func TestInit_CaseInsensitiveEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}))
	defer server.Close()

	tests := []string{"TRUE", "True", "tRuE", " true "}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			os.Setenv("FEATURE_FLAG_ENABLED", value)
			os.Setenv("FEATURE_FLAG_URL", server.URL)
			os.Setenv("FEATURE_FLAG_API_KEY", "test-key")
			defer func() {
				os.Unsetenv("FEATURE_FLAG_ENABLED")
				os.Unsetenv("FEATURE_FLAG_URL")
				os.Unsetenv("FEATURE_FLAG_API_KEY")
			}()

			client, err := Init()
			if err != nil {
				t.Errorf("Init() error = %v for FEATURE_FLAG_ENABLED=%q", err, value)
				return
			}
			if client == nil {
				t.Errorf("Client should not be nil for FEATURE_FLAG_ENABLED=%q", value)
			} else {
				client.Stop()
			}
		})
	}
}
