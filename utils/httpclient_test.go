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

package utils

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Mock logger / metrics
// ---------------------------------------------------------------------------

type testLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *testLogger) Debug(msg string, kvs ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, "DEBUG: "+msg)
}
func (l *testLogger) Info(msg string, kvs ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, "INFO: "+msg)
}
func (l *testLogger) Error(msg string, kvs ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, "ERROR: "+msg)
}
func (l *testLogger) Messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]string, len(l.messages))
	copy(cp, l.messages)
	return cp
}

type testMetrics struct {
	mu      sync.Mutex
	records []metricRecord
}

type metricRecord struct {
	Method   string
	Host     string
	Status   string
	Duration time.Duration
}

func (m *testMetrics) RecordHTTPClient(method, host, statusCode string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, metricRecord{
		Method:   method,
		Host:     host,
		Status:   statusCode,
		Duration: duration,
	})
}

func (m *testMetrics) Records() []metricRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]metricRecord, len(m.records))
	copy(cp, m.records)
	return cp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHTTPClient_Get(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{Timeout: 5 * time.Second})
	resp, err := client.Get(ts.URL + "/test")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"status":"ok"}` {
		t.Errorf("Unexpected body: %s", body)
	}
}

func TestHTTPClient_Post(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"received":"%s"}`, string(body))
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{Timeout: 5 * time.Second})
	resp, err := client.Post(ts.URL+"/create", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatalf("Post() error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected 201, got %d", resp.StatusCode)
	}
}

func TestHTTPClient_Put(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("Expected PUT, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{})
	resp, err := client.Put(ts.URL+"/update", bytes.NewBufferString("data"))
	if err != nil {
		t.Fatalf("Put() error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPClient_Patch(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("Expected PATCH, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{})
	resp, err := client.Patch(ts.URL+"/patch", bytes.NewBufferString("partial"))
	if err != nil {
		t.Fatalf("Patch() error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPClient_Delete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("Expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{})
	resp, err := client.Delete(ts.URL + "/resource")
	if err != nil {
		t.Fatalf("Delete() error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204, got %d", resp.StatusCode)
	}
}

func TestHTTPClient_Logging(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer ts.Close()

	logger := &testLogger{}
	client := NewHTTPClient(HTTPClientOptions{
		Logger:  logger,
		Timeout: 5 * time.Second,
	})

	resp, err := client.Get(ts.URL + "/data")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	resp.Body.Close()

	msgs := logger.Messages()
	if len(msgs) < 2 {
		t.Fatalf("Expected at least 2 log messages, got %d: %v", len(msgs), msgs)
	}

	// Should have a DEBUG request log and an INFO response log.
	foundDebug := false
	foundInfo := false
	for _, msg := range msgs {
		if strings.Contains(msg, "DEBUG:") && strings.Contains(msg, "[EXT] Request GET") {
			foundDebug = true
		}
		if strings.Contains(msg, "INFO:") && strings.Contains(msg, "[EXT] Successful GET") {
			foundInfo = true
		}
	}
	if !foundDebug {
		t.Error("Expected DEBUG request log message")
	}
	if !foundInfo {
		t.Error("Expected INFO response log message")
	}
}

func TestHTTPClient_ErrorLogging(t *testing.T) {
	logger := &testLogger{}
	client := NewHTTPClient(HTTPClientOptions{
		Logger:  logger,
		Timeout: 100 * time.Millisecond,
	})

	// Request to a non-existent server.
	_, err := client.Get("http://192.0.2.1:1/unreachable")
	if err == nil {
		t.Fatal("Expected error for unreachable host")
	}

	msgs := logger.Messages()
	foundError := false
	for _, msg := range msgs {
		if strings.Contains(msg, "ERROR:") && strings.Contains(msg, "[EXT] Failed") {
			foundError = true
		}
	}
	if !foundError {
		t.Error("Expected ERROR log message on failure")
	}
}

func TestHTTPClient_ProxyConfig(t *testing.T) {
	t.Run("uses HTTPS_PROXY env var", func(t *testing.T) {
		t.Setenv("HTTPS_PROXY", "http://proxy.example.com:8080")
		t.Setenv("FORCE_PROXY_DOMAINS", "forced.example.com, another.example.com")
		t.Setenv("NO_PROXY", "internal.example.com")

		client := NewHTTPClient(HTTPClientOptions{})

		if client.proxyURL != "http://proxy.example.com:8080" {
			t.Errorf("Expected proxy from env, got %s", client.proxyURL)
		}
		if len(client.forceProxyList) != 2 {
			t.Errorf("Expected 2 force proxy domains, got %d", len(client.forceProxyList))
		}
		if len(client.noProxyList) != 1 {
			t.Errorf("Expected 1 no-proxy domain, got %d", len(client.noProxyList))
		}
	})

	t.Run("ProxyURL option overrides env", func(t *testing.T) {
		t.Setenv("HTTPS_PROXY", "http://env-proxy:8080")

		client := NewHTTPClient(HTTPClientOptions{
			ProxyURL: "http://option-proxy:9090",
		})

		if client.proxyURL != "http://option-proxy:9090" {
			t.Errorf("Expected option proxy, got %s", client.proxyURL)
		}
	})
}

func TestHTTPClient_MetricsRecording(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	metrics := &testMetrics{}
	client := NewHTTPClient(HTTPClientOptions{
		MetricsRecorder: metrics,
		Timeout:         5 * time.Second,
	})

	resp, err := client.Get(ts.URL + "/metrics-test")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	resp.Body.Close()

	records := metrics.Records()
	if len(records) != 1 {
		t.Fatalf("Expected 1 metric record, got %d", len(records))
	}

	rec := records[0]
	if rec.Method != "GET" {
		t.Errorf("Expected method=GET, got %s", rec.Method)
	}
	if rec.Status != "200" {
		t.Errorf("Expected status=200, got %s", rec.Status)
	}
	if rec.Duration <= 0 {
		t.Error("Expected positive duration")
	}
	if rec.Host == "" || rec.Host == "unknown" {
		t.Error("Expected non-empty host")
	}
}

func TestHTTPClient_MetricsOnError(t *testing.T) {
	metrics := &testMetrics{}
	client := NewHTTPClient(HTTPClientOptions{
		MetricsRecorder: metrics,
		Timeout:         100 * time.Millisecond,
	})

	_, _ = client.Get("http://192.0.2.1:1/unreachable")

	records := metrics.Records()
	if len(records) != 1 {
		t.Fatalf("Expected 1 metric record on error, got %d", len(records))
	}
	if records[0].Status != "0" {
		t.Errorf("Expected status=0 on error, got %s", records[0].Status)
	}
}

func TestHTTPClient_DefaultHeaders(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Custom") != "header-value" {
			t.Errorf("Expected default header X-Custom, got %s", r.Header.Get("X-Custom"))
		}
		if r.Header.Get("X-Override") != "from-request" {
			t.Errorf("Expected request header to take precedence, got %s", r.Header.Get("X-Override"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{
		Headers: map[string]string{
			"X-Custom":   "header-value",
			"X-Override": "from-default",
		},
	})

	req, _ := http.NewRequest(http.MethodGet, ts.URL, nil)
	req.Header.Set("X-Override", "from-request")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}
	resp.Body.Close()
}

func TestHTTPClient_SetDefaultHeader(t *testing.T) {
	client := NewHTTPClient(HTTPClientOptions{})
	client.SetDefaultHeader("X-Dynamic", "dynamic-value")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Dynamic") != "dynamic-value" {
			t.Errorf("Expected X-Dynamic header, got %s", r.Header.Get("X-Dynamic"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	resp.Body.Close()
}

func TestHTTPClient_BaseURL(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	client := NewHTTPClient(HTTPClientOptions{
		BaseURL: ts.URL,
	})

	// Using an absolute URL should bypass baseURL.
	resp, err := client.Get(ts.URL + "/absolute")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	resp.Body.Close()
}

func TestHTTPClient_Interceptors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Intercepted") != "true" {
			t.Error("Expected request interceptor to set header")
		}
		w.Header().Set("X-Response", "modified")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	var responseHeaderSeen string

	client := NewHTTPClient(HTTPClientOptions{
		RequestInterceptors: []RequestInterceptor{
			func(req *http.Request) error {
				req.Header.Set("X-Intercepted", "true")
				return nil
			},
		},
		ResponseInterceptors: []ResponseInterceptor{
			func(resp *http.Response) error {
				responseHeaderSeen = resp.Header.Get("X-Response")
				return nil
			},
		},
	})

	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	resp.Body.Close()

	if responseHeaderSeen != "modified" {
		t.Errorf("Expected response interceptor to capture header, got %s", responseHeaderSeen)
	}
}

func TestHTTPClient_InterceptorError(t *testing.T) {
	client := NewHTTPClient(HTTPClientOptions{
		RequestInterceptors: []RequestInterceptor{
			func(req *http.Request) error {
				return fmt.Errorf("blocked by interceptor")
			},
		},
	})

	_, err := client.Get("http://localhost:1234/test")
	if err == nil {
		t.Fatal("Expected error from request interceptor")
	}
	if !strings.Contains(err.Error(), "blocked by interceptor") {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestHTTPClient_DefaultTimeout(t *testing.T) {
	client := NewHTTPClient(HTTPClientOptions{})
	if client.client.Timeout != 30*time.Second {
		t.Errorf("Expected default timeout 30s, got %v", client.client.Timeout)
	}
}

func TestExtractHost(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{"https://api.example.com/path", "api.example.com"},
		{"http://localhost:8080/test", "localhost"},
		{"invalid-url", "unknown"},
		{"", "unknown"},
	}

	for _, tt := range tests {
		if got := extractHost(tt.url); got != tt.expected {
			t.Errorf("extractHost(%q) = %q, want %q", tt.url, got, tt.expected)
		}
	}
}

func TestGenerateRequestID(t *testing.T) {
	id1 := generateRequestID()
	id2 := generateRequestID()

	if id1 == "" {
		t.Error("Expected non-empty request ID")
	}
	if id1 == id2 {
		t.Error("Expected unique request IDs")
	}
	if len(id1) != 32 { // 16 bytes hex-encoded
		t.Errorf("Expected 32-char hex ID, got length %d", len(id1))
	}
}
