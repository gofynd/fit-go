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

package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/errors"
	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/profiling"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// ---------------------------------------------------------------------------
// ServerType tests
// ---------------------------------------------------------------------------

func TestParseServerType(t *testing.T) {
	tests := []struct {
		input    string
		expected ServerType
		wantErr  bool
	}{
		{"platform", ServerTypePlatform, false},
		{"application", ServerTypeApplication, false},
		{"partner", ServerTypePartner, false},
		{"internal", ServerTypeInternal, false},
		{"webhook", ServerTypeWebhook, false},
		{"administrator", ServerTypeAdministrator, false},
		{"public", ServerTypePublic, false},
		{"portal", ServerTypePortal, false},
		{"panel", ServerTypePanel, false},
		{"dev", ServerTypeDev, false},
		{"common", ServerTypeCommon, false},
		{"central", ServerTypeCentral, false},
		{" Platform ", ServerTypePlatform, false},
		{"APPLICATION", ServerTypeApplication, false},
		{"", ServerTypeDefault, true},
		{"invalid", ServerTypeDefault, true},
		{"platformm", ServerTypeDefault, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseServerType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseServerType(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ParseServerType(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestParseServerTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []ServerType
		wantErr  bool
	}{
		{"single type", "platform", []ServerType{ServerTypePlatform}, false},
		{"multiple types", "platform,application,internal", []ServerType{ServerTypePlatform, ServerTypeApplication, ServerTypeInternal}, false},
		{"with whitespace", " platform , application , internal ", []ServerType{ServerTypePlatform, ServerTypeApplication, ServerTypeInternal}, false},
		{"empty string", "", nil, false},
		{"invalid type in list", "platform,invalid,internal", nil, true},
		{"trailing comma", "platform,", []ServerType{ServerTypePlatform}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseServerTypes(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseServerTypes(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
				return
			}
			if len(got) != len(tt.expected) {
				t.Errorf("ParseServerTypes(%q) returned %d types, want %d", tt.input, len(got), len(tt.expected))
				return
			}
			for i, st := range got {
				if st != tt.expected[i] {
					t.Errorf("ParseServerTypes(%q)[%d] = %v, want %v", tt.input, i, st, tt.expected[i])
				}
			}
		})
	}
}

func TestServerType_String(t *testing.T) {
	tests := []struct {
		st       ServerType
		expected string
	}{
		{ServerTypePlatform, "platform"},
		{ServerTypeApplication, "application"},
		{ServerTypePartner, "partner"},
		{ServerTypeInternal, "internal"},
		{ServerTypeWebhook, "webhook"},
		{ServerTypeAdministrator, "administrator"},
		{ServerTypePublic, "public"},
		{ServerTypePortal, "portal"},
		{ServerTypePanel, "panel"},
		{ServerTypeDev, "dev"},
		{ServerTypeCommon, "common"},
		{ServerTypeCentral, "central"},
		{ServerTypeDefault, "default"},
		{ServerType(999), "default"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.st.String(); got != tt.expected {
				t.Errorf("ServerType.String() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestAllServerTypes(t *testing.T) {
	types := AllServerTypes()
	if len(types) != 12 {
		t.Errorf("AllServerTypes() returned %d types, want 12", len(types))
	}

	for _, st := range types {
		if st == ServerTypeDefault {
			t.Error("AllServerTypes() should not include ServerTypeDefault")
		}
	}
}

func TestValidServerTypes(t *testing.T) {
	for _, st := range AllServerTypes() {
		if _, ok := ValidServerTypes[st]; !ok {
			t.Errorf("ValidServerTypes missing %v", st)
		}
	}

	if _, ok := ValidServerTypes[ServerTypeDefault]; ok {
		t.Error("ValidServerTypes should not include ServerTypeDefault")
	}
}

// ---------------------------------------------------------------------------
// Response helper tests
// ---------------------------------------------------------------------------

func TestJSON(t *testing.T) {
	t.Run("with data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		data := map[string]string{"message": "hello"}
		JSON(rec, http.StatusOK, data)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Errorf("Content-Type = %q, want %q", ct, "application/json; charset=utf-8")
		}

		var result map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		if result["message"] != "hello" {
			t.Errorf("Response message = %q, want %q", result["message"], "hello")
		}
	})

	t.Run("with nil data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		JSON(rec, http.StatusNoContent, nil)

		if rec.Code != http.StatusNoContent {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusNoContent)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("Body should be empty for nil data")
		}
	})
}

func TestSuccess(t *testing.T) {
	rec := httptest.NewRecorder()
	data := map[string]int{"count": 42}
	Success(rec, data)

	if rec.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
	}

	var result map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	if result["count"] != 42 {
		t.Errorf("Response count = %d, want %d", result["count"], 42)
	}
}

func TestError(t *testing.T) {
	reg := &errors.ErrorRegistry{}
	_ = reg.Init("TST", map[string]int{"TEST_ERROR": 1}, nil, nil)
	oldDefault := errors.DefaultRegistry
	errors.DefaultRegistry = reg
	defer func() { errors.DefaultRegistry = oldDefault }()

	t.Run("with FitError", func(t *testing.T) {
		rec := httptest.NewRecorder()
		fitErr := errors.New(fmt.Errorf("test error"), 42).SetStatus(http.StatusBadRequest)
		Error(rec, fitErr)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusBadRequest)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		errObj := result["error"].(map[string]interface{})
		if errObj["code"] != "TST0042" {
			t.Errorf("Error code = %v, want TST0042", errObj["code"])
		}
	})

	t.Run("with regular error", func(t *testing.T) {
		rec := httptest.NewRecorder()
		Error(rec, fmt.Errorf("something went wrong"))

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		errObj := result["error"].(map[string]interface{})
		if errObj["message"] != "something went wrong" {
			t.Errorf("Error message = %v, want 'something went wrong'", errObj["message"])
		}
	})

	t.Run("with nil error", func(t *testing.T) {
		rec := httptest.NewRecorder()
		Error(rec, nil)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		errObj := result["error"].(map[string]interface{})
		if errObj["message"] != "unknown error" {
			t.Errorf("Error message = %v, want 'unknown error'", errObj["message"])
		}
	})
}

// ---------------------------------------------------------------------------
// Gin middleware tests
// ---------------------------------------------------------------------------

func TestParseUserData(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		r := gin.New()
		r.Use(GinParseUserData)
		r.GET("/test", func(c *gin.Context) {
			data := UserDataFromContext(c.Request.Context())
			if data == nil {
				t.Error("UserDataFromContext returned nil")
				return
			}
			if data["user_id"] != "123" {
				t.Errorf("user_id = %v, want '123'", data["user_id"])
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-user-data", `{"user_id": "123", "role": "admin"}`)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		r := gin.New()
		r.Use(GinParseUserData)
		r.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called for invalid JSON")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-user-data", "not-json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		errObj := result["error"].(map[string]interface{})
		if errObj["code"] != "USER_HEADER_JSON_PARSE_FAILURE" {
			t.Errorf("Error code = %v, want USER_HEADER_JSON_PARSE_FAILURE", errObj["code"])
		}
	})

	t.Run("empty header", func(t *testing.T) {
		handlerCalled := false
		r := gin.New()
		r.Use(GinParseUserData)
		r.GET("/test", func(c *gin.Context) {
			handlerCalled = true
			data := UserDataFromContext(c.Request.Context())
			if data != nil {
				t.Error("UserDataFromContext should return nil for empty header")
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if !handlerCalled {
			t.Error("Handler should be called for empty header")
		}
	})
}

func TestParseApplicationData(t *testing.T) {
	t.Run("valid JSON", func(t *testing.T) {
		r := gin.New()
		r.Use(GinParseApplicationData)
		r.GET("/test", func(c *gin.Context) {
			data := ApplicationDataFromContext(c.Request.Context())
			if data == nil {
				t.Error("ApplicationDataFromContext returned nil")
				return
			}
			if data["app_id"] != "myapp" {
				t.Errorf("app_id = %v, want 'myapp'", data["app_id"])
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-application-data", `{"app_id": "myapp", "version": "1.0"}`)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		r := gin.New()
		r.Use(GinParseApplicationData)
		r.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called for invalid JSON")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-application-data", "{invalid}")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})
}

func TestRequestID(t *testing.T) {
	t.Run("generates new ID", func(t *testing.T) {
		var capturedID string
		r := gin.New()
		r.Use(RequestID())
		r.GET("/test", func(c *gin.Context) {
			capturedID = RequestIDFromContext(c.Request.Context())
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if capturedID == "" {
			t.Error("RequestID should generate an ID")
		}
		if rec.Header().Get("X-Request-ID") != capturedID {
			t.Error("X-Request-ID header should match context value")
		}
		if len(capturedID) != 36 {
			t.Errorf("RequestID length = %d, want 36", len(capturedID))
		}
	})

	t.Run("preserves existing ID", func(t *testing.T) {
		existingID := "existing-request-id"
		var capturedID string
		r := gin.New()
		r.Use(RequestID())
		r.GET("/test", func(c *gin.Context) {
			capturedID = RequestIDFromContext(c.Request.Context())
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Request-ID", existingID)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if capturedID != existingID {
			t.Errorf("RequestID = %q, want %q", capturedID, existingID)
		}
	})
}

func TestInitCanDisableRequestIDWithoutChangingDefault(t *testing.T) {
	t.Setenv("SERVER_TYPE", "platform")
	router := http.NewServeMux()
	router.HandleFunc("/probe", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	disabled := false
	server := New(Config{Port: "0", RequestID: &disabled})
	if err := server.Init(map[ServerType]http.Handler{ServerTypePlatform: router}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/probe", nil)
	server.App.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Request-ID"); got != "" {
		t.Fatalf("X-Request-ID = %q, want absent", got)
	}
}

func TestInitCanDisableRequestLoggingWhileRetainingMetrics(t *testing.T) {
	t.Setenv("SERVER_TYPE", "platform")
	var logBuffer bytes.Buffer
	metricsCalls := 0
	router := http.NewServeMux()
	router.HandleFunc("/probe", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	disabled := false
	server := New(Config{
		Port:           "0",
		Logger:         slog.New(slog.NewJSONHandler(&logBuffer, nil)),
		RequestLogging: &disabled,
		MetricsRecorder: func(_, _, _ string, _ float64) {
			metricsCalls++
		},
	})
	if err := server.Init(map[ServerType]http.Handler{ServerTypePlatform: router}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/probe", nil)
	server.App.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}
	if strings.Contains(logBuffer.String(), "[REQ]") || strings.Contains(logBuffer.String(), "[RES]") {
		t.Fatalf("request logs must be absent when disabled: %s", logBuffer.String())
	}
	if metricsCalls != 1 {
		t.Fatalf("metrics calls = %d, want 1", metricsCalls)
	}
}

func TestCORS(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		r := gin.New()
		r.Use(CORS(CORSConfig{}))
		r.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
			t.Error("Default origin should be *")
		}
		if !strings.Contains(rec.Header().Get("Access-Control-Allow-Methods"), "GET") {
			t.Error("Methods should include GET")
		}
		if !strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
			t.Error("Headers should include Authorization")
		}
	})

	t.Run("OPTIONS preflight", func(t *testing.T) {
		handlerCalled := false
		r := gin.New()
		r.Use(CORS(CORSConfig{}))
		r.OPTIONS("/test", func(c *gin.Context) {
			handlerCalled = true
		})

		req := httptest.NewRequest("OPTIONS", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if handlerCalled {
			t.Error("Handler should not be called for OPTIONS")
		}
		if rec.Code != http.StatusNoContent {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusNoContent)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		r := gin.New()
		cfg := CORSConfig{
			AllowOrigins: []string{"https://example.com"},
			AllowMethods: []string{"GET", "POST"},
			AllowHeaders: []string{"X-Custom-Header"},
			MaxAge:       3600,
		}
		r.Use(CORS(cfg))
		r.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Header().Get("Access-Control-Allow-Origin") != "https://example.com" {
			t.Error("Origin should match config")
		}
		if rec.Header().Get("Access-Control-Max-Age") != "3600" {
			t.Error("MaxAge should match config")
		}
	})
}

func TestMaxPayloadSize(t *testing.T) {
	t.Run("within limit", func(t *testing.T) {
		r := gin.New()
		r.Use(MaxPayloadSize("1kb"))
		r.POST("/test", func(c *gin.Context) {
			_, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.String(http.StatusRequestEntityTooLarge, err.Error())
				return
			}
			c.Status(http.StatusOK)
		})

		body := bytes.NewReader(make([]byte, 100))
		req := httptest.NewRequest("POST", "/test", body)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("exceeds limit", func(t *testing.T) {
		r := gin.New()
		r.Use(MaxPayloadSize("1kb"))
		r.POST("/test", func(c *gin.Context) {
			_, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.String(http.StatusRequestEntityTooLarge, err.Error())
				return
			}
			c.Status(http.StatusOK)
		})

		body := bytes.NewReader(make([]byte, 2000))
		req := httptest.NewRequest("POST", "/test", body)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
		}
	})
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"2mb", 2 * 1024 * 1024},
		{"500kb", 500 * 1024},
		{"1gb", 1024 * 1024 * 1024},
		{"100b", 100},
		{" 2MB ", 2 * 1024 * 1024},
		{"", 2 * 1024 * 1024},
		{"invalid", 2 * 1024 * 1024},
		{"-5mb", 2 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := parseSize(tt.input); got != tt.expected {
				t.Errorf("parseSize(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestNormalizeRoutePath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/users/123", "/users/:id"},
		{"/orders/456/items/789", "/orders/:id/items/:id"},
		{"/companies/550e8400-e29b-41d4-a716-446655440000", "/companies/:uuid"},
		{"/users/123/profile", "/users/:id/profile"},
		{"/static/path", "/static/path"},
		{"/mix/123/uuid/550e8400-e29b-41d4-a716-446655440000/end", "/mix/:id/uuid/:uuid/end"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := normalizeRoutePath(tt.input); got != tt.expected {
				t.Errorf("normalizeRoutePath(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestRecovery(t *testing.T) {
	r := gin.New()
	r.Use(Recovery(slog.New(slog.NewTextHandler(io.Discard, nil))))
	r.GET("/test", func(c *gin.Context) {
		panic("test panic")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	errObj := result["error"].(map[string]interface{})
	if errObj["message"] != "internal server error" {
		t.Errorf("Error message = %v, want 'internal server error'", errObj["message"])
	}
}

func TestErrorHandler(t *testing.T) {
	reg := &errors.ErrorRegistry{}
	_ = reg.Init("TST", map[string]int{"TEST_ERROR": 1}, nil, nil)
	oldDefault := errors.DefaultRegistry
	errors.DefaultRegistry = reg
	defer func() { errors.DefaultRegistry = oldDefault }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("FitError panic", func(t *testing.T) {
		r := gin.New()
		r.Use(ErrorHandler(logger))
		r.GET("/test", func(c *gin.Context) {
			panic(errors.New(fmt.Errorf("fit error"), 42).SetStatus(http.StatusBadRequest))
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("regular error panic", func(t *testing.T) {
		r := gin.New()
		r.Use(ErrorHandler(logger))
		r.GET("/test", func(c *gin.Context) {
			panic(fmt.Errorf("regular error"))
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("string panic", func(t *testing.T) {
		r := gin.New()
		r.Use(ErrorHandler(logger))
		r.GET("/test", func(c *gin.Context) {
			panic("string panic")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusInternalServerError)
		}
	})
}

func TestLogRequestResponse(t *testing.T) {
	t.Run("logs request and response", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		var metricsRecorded bool
		cfg := LogRequestResponseConfig{
			Logger: logger,
			MetricsRecorder: func(method, route, status string, durationMs float64) {
				metricsRecorded = true
				if method != "GET" {
					t.Errorf("Method = %q, want GET", method)
				}
			},
		}

		r := gin.New()
		r.Use(LogRequestResponse(cfg))
		r.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if !metricsRecorded {
			t.Error("MetricsRecorder should be called")
		}
		if !strings.Contains(logBuf.String(), "REQ") {
			t.Error("Request log should contain REQ")
		}
		if !strings.Contains(logBuf.String(), "RES") {
			t.Error("Response log should contain RES")
		}
	})

	t.Run("skips health paths", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		for _, path := range []string{"/_healthz", "/_readyz"} {
			logBuf.Reset()

			r := gin.New()
			r.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
			r.GET(path, func(c *gin.Context) {
				c.Status(http.StatusOK)
			})

			req := httptest.NewRequest("GET", path, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if logBuf.Len() > 0 {
				t.Errorf("Path %s should not be logged", path)
			}
		}
	})
}

func TestSecureHeadersMiddleware(t *testing.T) {
	r := gin.New()
	r.Use(SecureHeaders())
	r.GET("/test", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	expectedHeaders := map[string]string{
		"X-Content-Type-Options":    "nosniff",
		"X-DNS-Prefetch-Control":    "off",
		"X-Download-Options":        "noopen",
		"X-Frame-Options":           "SAMEORIGIN",
		"X-XSS-Protection":          "0",
		"Strict-Transport-Security": "max-age=15552000; includeSubDomains",
		"Referrer-Policy":           "no-referrer",
	}

	for header, expected := range expectedHeaders {
		if got := rec.Header().Get(header); got != expected {
			t.Errorf("%s = %q, want %q", header, got, expected)
		}
	}
}

// ---------------------------------------------------------------------------
// JWT tests (gin)
// ---------------------------------------------------------------------------

func TestAuthorizeJWTToken(t *testing.T) {
	secret := "test-secret-key"

	createJWT := func(claims map[string]interface{}) string {
		header := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(
			[]byte(`{"alg":"HS256","typ":"JWT"}`),
		)
		claimsJSON, _ := json.Marshal(claims)
		payload := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(claimsJSON)
		signingInput := header + "." + payload
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(signingInput))
		sig := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(mac.Sum(nil))
		return header + "." + payload + "." + sig
	}

	t.Run("valid token", func(t *testing.T) {
		claims := map[string]interface{}{
			"company_id": "123",
			"exp":        float64(time.Now().Add(time.Hour).Unix()),
		}
		token := createJWT(claims)

		var decodedClaims map[string]interface{}
		r := gin.New()
		r.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
		r.GET("/test", func(c *gin.Context) {
			decodedClaims = DecodedTokenFromContext(c.Request.Context())
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
		if decodedClaims["company_id"] != "123" {
			t.Errorf("company_id = %v, want '123'", decodedClaims["company_id"])
		}
	})

	t.Run("missing token", func(t *testing.T) {
		r := gin.New()
		r.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
		r.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called without token")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("invalid signature", func(t *testing.T) {
		claims := map[string]interface{}{"company_id": "123"}
		token := createJWT(claims)
		token = token[:len(token)-5] + "xxxxx"

		r := gin.New()
		r.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
		r.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called with invalid signature")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("expired token", func(t *testing.T) {
		claims := map[string]interface{}{
			"company_id": "123",
			"exp":        float64(time.Now().Add(-time.Hour).Unix()),
		}
		token := createJWT(claims)

		r := gin.New()
		r.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
		r.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called with expired token")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("payload mismatch", func(t *testing.T) {
		claims := map[string]interface{}{
			"company_id": "123",
			"exp":        float64(time.Now().Add(time.Hour).Unix()),
		}
		token := createJWT(claims)

		opts := JWTOptions{
			Secret:          secret,
			ExpectedPayload: map[string]interface{}{"company_id": "456"},
		}

		r := gin.New()
		r.Use(AuthorizeJWTToken(opts))
		r.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called with mismatched payload")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})
}

func TestVerifyHS256(t *testing.T) {
	secret := "test-secret"

	t.Run("invalid format", func(t *testing.T) {
		_, err := verifyHS256("not.a.valid.token.format", secret)
		if err == nil {
			t.Error("Expected error for invalid format")
		}
	})

	t.Run("unsupported algorithm", func(t *testing.T) {
		header := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(
			[]byte(`{"alg":"RS256","typ":"JWT"}`),
		)
		payload := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(
			[]byte(`{"sub":"123"}`),
		)
		token := header + "." + payload + ".signature"

		_, err := verifyHS256(token, secret)
		if err == nil || !strings.Contains(err.Error(), "unsupported algorithm") {
			t.Errorf("Expected unsupported algorithm error, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Health route tests (gin)
// ---------------------------------------------------------------------------

func TestHealthRoutes(t *testing.T) {
	oldChecker := healthChecker
	globalHealthChecker = &defaultHealthChecker{}
	healthChecker = globalHealthChecker
	defer func() { healthChecker = oldChecker }()

	r := gin.New()
	RegisterHealthRoutes(r)

	t.Run("healthy", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/_healthz", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		if result["status"] != "healthy" {
			t.Errorf("status = %v, want 'healthy'", result["status"])
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		RegisterHealthCheck(func() string {
			return "database connection failed"
		})

		req := httptest.NewRequest("GET", "/_healthz", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusBadRequest)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		if result["status"] != "unhealthy" {
			t.Errorf("status = %v, want 'unhealthy'", result["status"])
		}
	})
}

type staticHealthChecker []string

func (checker staticHealthChecker) Check() []string { return checker }

func TestHealthAndReadinessCanUseIndependentCheckers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterHealthRoutesWithCheckers(
		engine,
		staticHealthChecker(nil),
		staticHealthChecker([]string{"dependencies are not ready"}),
	)

	health := httptest.NewRecorder()
	engine.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/_healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d; want 200", health.Code)
	}

	readiness := httptest.NewRecorder()
	engine.ServeHTTP(readiness, httptest.NewRequest(http.MethodGet, "/_readyz", nil))
	if readiness.Code != http.StatusBadRequest {
		t.Fatalf("readiness status = %d; want 400", readiness.Code)
	}
}

func TestLegacyStaticHealthRoutesPreserveFitJSBytes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterLegacyStaticHealthRoutes(engine)

	for _, path := range []string{"/_healthz", "/_readyz", "/_HEALTHZ/", "/_READYZ/"} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
			if recorder.Code != http.StatusOK {
				t.Fatalf("GET %s = %d, want 200", path, recorder.Code)
			}
			if got := recorder.Body.String(); got != `{"ok":"ok"}` {
				t.Fatalf("GET %s body = %q, want exact fit.js body", path, got)
			}
			wantHeaders := map[string]string{
				"Content-Type":   "application/json; charset=utf-8",
				"Content-Length": "11",
				"ETag":           `W/"b-2F/2BWc0KYbtLqL5U2Kv5B6uQUQ"`,
				"X-Powered-By":   "Express",
			}
			for name, want := range wantHeaders {
				if got := recorder.Header().Get(name); got != want {
					t.Fatalf("GET %s %s = %q, want %q", path, name, got, want)
				}
			}
		})
	}

	for _, path := range []string{"/_healthz", "/_READYZ/"} {
		recorder := httptest.NewRecorder()
		engine.ServeHTTP(recorder, httptest.NewRequest(http.MethodHead, path, nil))
		if recorder.Code != http.StatusOK || recorder.Body.Len() != 0 {
			t.Fatalf("HEAD %s = %d body %q", path, recorder.Code, recorder.Body.String())
		}
		if recorder.Header().Get("Content-Length") != "11" ||
			recorder.Header().Get("ETag") != `W/"b-2F/2BWc0KYbtLqL5U2Kv5B6uQUQ"` {
			t.Fatalf("HEAD %s headers = %#v", path, recorder.Header())
		}
	}
}

func TestProfileRoutesControlTheProvidedProfiler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	profiler := profiling.New(profiling.Config{
		Enabled:                   true,
		HeapEnabled:               true,
		HeapSamplingIntervalBytes: 524288,
	})
	defer profiler.Stop()
	engine := gin.New()
	RegisterProfileRoutesWithProfiler(engine, profiler)

	start := httptest.NewRecorder()
	engine.ServeHTTP(start, httptest.NewRequest(http.MethodGet, "/_profiling/start_heap", nil))
	if start.Code != http.StatusOK || !profiler.IsHeapProfilingRunning() {
		t.Fatalf("start_heap status/running = %d/%v", start.Code, profiler.IsHeapProfilingRunning())
	}

	status := httptest.NewRecorder()
	engine.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/_profiling/status", nil))
	if status.Code != http.StatusOK || !strings.Contains(status.Body.String(), `"overall":{"message":"Profiling is active","running":true}`) {
		t.Fatalf("status response = %d %s", status.Code, status.Body.String())
	}

	stop := httptest.NewRecorder()
	engine.ServeHTTP(stop, httptest.NewRequest(http.MethodGet, "/_profiling/stop_heap", nil))
	if stop.Code != http.StatusOK || profiler.IsHeapProfilingRunning() {
		t.Fatalf("stop_heap status/running = %d/%v", stop.Code, profiler.IsHeapProfilingRunning())
	}
}

// ---------------------------------------------------------------------------
// Server lifecycle tests
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		s := New(Config{})

		if s.Router == nil {
			t.Error("Router should not be nil")
		}
		if s.logger == nil {
			t.Error("logger should not be nil")
		}
		if s.cfg.ReadTimeout != 30*time.Second {
			t.Errorf("ReadTimeout = %v, want 30s", s.cfg.ReadTimeout)
		}
		if s.cfg.WriteTimeout != 30*time.Second {
			t.Errorf("WriteTimeout = %v, want 30s", s.cfg.WriteTimeout)
		}
		if s.cfg.IdleTimeout != 120*time.Second {
			t.Errorf("IdleTimeout = %v, want 120s", s.cfg.IdleTimeout)
		}
	})

	t.Run("custom config", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		s := New(Config{
			Logger:       logger,
			ReadTimeout:  60 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  300 * time.Second,
		})

		if s.cfg.ReadTimeout != 60*time.Second {
			t.Errorf("ReadTimeout = %v, want 60s", s.cfg.ReadTimeout)
		}
	})
}

func TestServer_Init(t *testing.T) {
	t.Run("no routers", func(t *testing.T) {
		s := New(Config{})
		err := s.Init(nil, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "no routes provided") {
			t.Errorf("Expected 'no routes provided' error, got %v", err)
		}
	})

	t.Run("with routers", func(t *testing.T) {
		os.Setenv("SERVER_TYPE", "platform")
		os.Setenv("DISABLE_RESPONSE_MIDDLEWARES", "true")
		defer os.Unsetenv("SERVER_TYPE")
		defer os.Unsetenv("DISABLE_RESPONSE_MIDDLEWARES")

		s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		routers := map[ServerType]http.Handler{
			ServerTypePlatform: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
		}

		err := s.Init(routers, nil, nil)
		if err != nil {
			t.Fatalf("Init failed: %v", err)
		}
		if s.App == nil {
			t.Error("App should not be nil after Init")
		}
	})

	t.Run("missing SERVER_TYPE", func(t *testing.T) {
		os.Unsetenv("SERVER_TYPE")
		os.Unsetenv("UNIFY_SERVER")
		os.Setenv("NODE_ENV", "production")
		defer os.Unsetenv("NODE_ENV")

		s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		routers := map[ServerType]http.Handler{
			ServerTypePlatform: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		}

		err := s.Init(routers, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "no SERVER_TYPE provided") {
			t.Errorf("Expected 'no SERVER_TYPE provided' error, got %v", err)
		}
	})
}

func TestServer_Start_NoPort(t *testing.T) {
	os.Unsetenv("PORT")
	s := New(Config{})
	s.App = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

	err := s.Start()
	if err == nil || !strings.Contains(err.Error(), "PORT") {
		t.Errorf("Expected PORT error, got %v", err)
	}
}

func TestServer_Start_NoInit(t *testing.T) {
	os.Setenv("PORT", "8080")
	defer os.Unsetenv("PORT")

	s := New(Config{})

	err := s.Start()
	if err == nil || !strings.Contains(err.Error(), "Init must be called") {
		t.Errorf("Expected 'Init must be called' error, got %v", err)
	}
}

func TestServer_Shutdown(t *testing.T) {
	s := New(Config{})
	err := s.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Shutdown on unstarted server should not error: %v", err)
	}
}

func TestServer_Addr(t *testing.T) {
	s := New(Config{})
	if addr := s.Addr(); addr != "" {
		t.Errorf("Addr() on unstarted server = %q, want empty", addr)
	}
}

// ---------------------------------------------------------------------------
// Utility tests
// ---------------------------------------------------------------------------

func TestCoalesce(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{"first non-empty", []string{"", "first", "second"}, "first"},
		{"all empty", []string{"", "", ""}, ""},
		{"single value", []string{"only"}, "only"},
		{"no values", []string{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coalesce(tt.values...); got != tt.expected {
				t.Errorf("coalesce(%v) = %q, want %q", tt.values, got, tt.expected)
			}
		})
	}
}

func TestEnvGetBool(t *testing.T) {
	tests := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{" true ", true},
		{"false", false},
		{"", false},
		{"1", false},
		{"yes", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			os.Setenv("TEST_BOOL", tt.value)
			defer os.Unsetenv("TEST_BOOL")

			if got := envGetBool("TEST_BOOL"); got != tt.expected {
				t.Errorf("envGetBool(%q) = %v, want %v", tt.value, got, tt.expected)
			}
		})
	}
}

func TestResponseRecorder(t *testing.T) {
	t.Run("captures status code", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rr := newResponseRecorder(rec)

		rr.WriteHeader(http.StatusCreated)

		if rr.statusCode != http.StatusCreated {
			t.Errorf("statusCode = %d, want %d", rr.statusCode, http.StatusCreated)
		}
		if !rr.written {
			t.Error("written should be true after WriteHeader")
		}
	})

	t.Run("default status on Write", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rr := newResponseRecorder(rec)

		rr.Write([]byte("hello"))

		if rr.statusCode != http.StatusOK {
			t.Errorf("statusCode = %d, want %d", rr.statusCode, http.StatusOK)
		}
	})

	t.Run("ignores multiple WriteHeader", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rr := newResponseRecorder(rec)

		rr.WriteHeader(http.StatusCreated)
		rr.WriteHeader(http.StatusBadRequest)

		if rr.statusCode != http.StatusCreated {
			t.Errorf("statusCode = %d, want %d (first call)", rr.statusCode, http.StatusCreated)
		}
	})
}

// ---------------------------------------------------------------------------
// Decryption middleware tests (gin)
// ---------------------------------------------------------------------------

func TestDecryptionMiddleware(t *testing.T) {
	decrypt := func(ciphertext string) (string, error) {
		if ciphertext == "encrypted" {
			return "decrypted", nil
		}
		return "", fmt.Errorf("unknown ciphertext")
	}

	t.Run("decrypts nested field", func(t *testing.T) {
		r := gin.New()
		r.Use(DecryptionMiddleware([]string{"data.secret"}, decrypt))
		r.GET("/test", func(c *gin.Context) {
			c.JSON(http.StatusOK, map[string]interface{}{
				"data": map[string]interface{}{
					"secret": "encrypted",
				},
			})
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		data := result["data"].(map[string]interface{})
		if data["secret"] != "decrypted" {
			t.Errorf("secret = %v, want 'decrypted'", data["secret"])
		}
	})

	t.Run("no locations", func(t *testing.T) {
		r := gin.New()
		r.Use(DecryptionMiddleware(nil, decrypt))
		r.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent access tests
// ---------------------------------------------------------------------------

func TestConcurrentServerAccess(t *testing.T) {
	s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			_ = s.Addr()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestServerInit_UsesProcessDefaultMetrics(t *testing.T) {
	t.Setenv("SERVER_TYPE", "application")
	t.Setenv("NODE_ENV", "production")
	registry, err := metrics.New(metrics.Options{
		ServerEnabled:      true,
		DeploymentName:     "test",
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	defer registry.Shutdown()
	restore := metrics.SetDefault(registry)
	defer restore()

	s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	if err := s.Init(map[ServerType]http.Handler{ServerTypeApplication: handler}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	recorder := httptest.NewRecorder()
	s.App.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/orders/123", nil))

	output := registry.GetMetricsOutput()
	if !strings.Contains(output, "fit_http_request_duration_ms") ||
		!strings.Contains(output, `status_code="204"`) {
		t.Fatalf("server did not use process-default metrics:\n%s", output)
	}
}
