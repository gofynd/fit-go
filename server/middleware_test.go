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

package server

import (
	"bytes"
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
)

// ---------------------------------------------------------------------------
// ErrorHandler tests (gin middleware)
// ---------------------------------------------------------------------------

func TestErrorHandler_FitError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := &errors.ErrorRegistry{}
	_ = reg.Init("TST", map[string]int{"TEST_ERROR": 1}, nil, nil)
	oldDefault := errors.DefaultRegistry
	errors.DefaultRegistry = reg
	defer func() { errors.DefaultRegistry = oldDefault }()

	engine := gin.New()
	engine.Use(ErrorHandler(slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.GET("/test", func(c *gin.Context) {
		panic(errors.New(fmt.Errorf("fit error"), 42).
			SetStatus(http.StatusNotFound).
			SetMeta(map[string]interface{}{"detail": "not found"}))
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatal("Response missing error object")
	}
	if errObj["code"] != "TST0042" {
		t.Errorf("Error code = %v, want TST0042", errObj["code"])
	}
	meta, ok := errObj["meta"].(map[string]interface{})
	if !ok {
		t.Fatal("Error missing meta")
	}
	if meta["detail"] != "not found" {
		t.Errorf("Meta detail = %v, want 'not found'", meta["detail"])
	}
}

func TestErrorHandler_Panic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(ErrorHandler(slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.GET("/test", func(c *gin.Context) {
		panic("unexpected string panic")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	errObj := result["error"].(map[string]interface{})
	if !strings.Contains(errObj["message"].(string), "unexpected string panic") {
		t.Errorf("Error message = %v, want to contain 'unexpected string panic'", errObj["message"])
	}
}

func TestErrorHandler_StandardError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(ErrorHandler(slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.GET("/test", func(c *gin.Context) {
		panic(fmt.Errorf("standard error message"))
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}
	errObj := result["error"].(map[string]interface{})
	if errObj["message"] != "standard error message" {
		t.Errorf("Error message = %v, want 'standard error message'", errObj["message"])
	}
}

func TestErrorHandlerWithLogger_Middleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reg := &errors.ErrorRegistry{}
	_ = reg.Init("TST", map[string]int{"TEST_ERROR": 1}, nil, nil)
	oldDefault := errors.DefaultRegistry
	errors.DefaultRegistry = reg
	defer func() { errors.DefaultRegistry = oldDefault }()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	engine := gin.New()
	engine.Use(ErrorHandlerWithLogger(logger))
	engine.GET("/test", func(c *gin.Context) {
		panic(errors.New(fmt.Errorf("fit error"), 42).SetStatus(http.StatusBadRequest))
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if !strings.Contains(logBuf.String(), "panic recovered") {
		t.Error("Logger should have recorded the panic")
	}
}

// ---------------------------------------------------------------------------
// AuthorizeJWT tests (gin middleware)
// ---------------------------------------------------------------------------

func createTestJWT(claims map[string]interface{}, secret string) string {
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

func TestAuthorizeJWT_ValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	secret := "test-jwt-secret"
	claims := map[string]interface{}{
		"company_id": "abc123",
		"exp":        float64(time.Now().Add(time.Hour).Unix()),
	}
	token := createTestJWT(claims, secret)

	var decodedClaims map[string]interface{}
	engine := gin.New()
	engine.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
	engine.GET("/test", func(c *gin.Context) {
		decodedClaims = DecodedTokenFromContext(c.Request.Context())
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
	}
	if decodedClaims == nil {
		t.Fatal("DecodedTokenFromContext returned nil")
	}
	if decodedClaims["company_id"] != "abc123" {
		t.Errorf("company_id = %v, want 'abc123'", decodedClaims["company_id"])
	}
}

func TestAuthorizeJWT_InvalidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	secret := "test-jwt-secret"
	claims := map[string]interface{}{"company_id": "123"}
	token := createTestJWT(claims, secret)
	token = token[:len(token)-5] + "XXXXX"

	engine := gin.New()
	engine.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
	engine.GET("/test", func(c *gin.Context) {
		t.Error("Handler should not be called with invalid token")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthorizeJWT_ExpiredToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	secret := "test-jwt-secret"
	claims := map[string]interface{}{
		"company_id": "123",
		"exp":        float64(time.Now().Add(-time.Hour).Unix()),
	}
	token := createTestJWT(claims, secret)

	engine := gin.New()
	engine.Use(AuthorizeJWTToken(JWTOptions{Secret: secret}))
	engine.GET("/test", func(c *gin.Context) {
		t.Error("Handler should not be called with expired token")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthorizeJWT_MissingToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(AuthorizeJWTToken(JWTOptions{Secret: "secret"}))
	engine.GET("/test", func(c *gin.Context) {
		t.Error("Handler should not be called without token")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthorizeJWT_EnvSecret(t *testing.T) {
	gin.SetMode(gin.TestMode)

	secret := "env-secret-value"
	os.Setenv("JWT_SECRET_DELETE_ENTITY", secret)
	defer os.Unsetenv("JWT_SECRET_DELETE_ENTITY")

	claims := map[string]interface{}{
		"company_id": "123",
		"exp":        float64(time.Now().Add(time.Hour).Unix()),
	}
	token := createTestJWT(claims, secret)

	engine := gin.New()
	engine.Use(AuthorizeJWTToken(JWTOptions{}))
	engine.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthorizeJWT_PayloadMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)

	secret := "test-secret"
	claims := map[string]interface{}{
		"company_id": "123",
		"exp":        float64(time.Now().Add(time.Hour).Unix()),
	}
	token := createTestJWT(claims, secret)

	engine := gin.New()
	opts := JWTOptions{
		Secret:          secret,
		ExpectedPayload: map[string]interface{}{"company_id": "999"},
	}
	engine.Use(AuthorizeJWTToken(opts))
	engine.GET("/test", func(c *gin.Context) {
		t.Error("Handler should not be called with mismatched payload")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

// ---------------------------------------------------------------------------
// LogRequestResponse tests (gin middleware)
// ---------------------------------------------------------------------------

func TestLogRequestResponse_Middleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("logs request and response with levels", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		var metricsRecorded bool
		cfg := LogRequestResponseConfig{
			Logger: logger,
			MetricsRecorder: func(method, route, status string, durationMs float64) {
				metricsRecorded = true
				if method != "POST" {
					t.Errorf("Method = %q, want POST", method)
				}
				if status != "201" {
					t.Errorf("Status = %q, want 201", status)
				}
			},
		}

		engine := gin.New()
		engine.Use(LogRequestResponse(cfg))
		engine.POST("/api/items", func(c *gin.Context) {
			c.Status(http.StatusCreated)
		})

		req := httptest.NewRequest("POST", "/api/items", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if !metricsRecorded {
			t.Error("MetricsRecorder should be called")
		}
		logStr := logBuf.String()
		if !strings.Contains(logStr, "REQ") {
			t.Error("Log should contain REQ step")
		}
		if !strings.Contains(logStr, "RES") {
			t.Error("Log should contain RES step")
		}
	})

	t.Run("skips health check paths", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		engine := gin.New()
		engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
		engine.GET("/_healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
		engine.GET("/_readyz", func(c *gin.Context) { c.Status(http.StatusOK) })

		for _, path := range []string{"/_healthz", "/_readyz"} {
			logBuf.Reset()
			req := httptest.NewRequest("GET", path, nil)
			rec := httptest.NewRecorder()
			engine.ServeHTTP(rec, req)

			if logBuf.Len() > 0 {
				t.Errorf("Path %s should not be logged", path)
			}
		}
	})

	t.Run("error level for 5xx", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		engine := gin.New()
		engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
		engine.GET("/fail", func(c *gin.Context) {
			c.Status(http.StatusInternalServerError)
		})

		req := httptest.NewRequest("GET", "/fail", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		logStr := logBuf.String()
		if !strings.Contains(logStr, "ERROR") {
			t.Error("5xx responses should be logged at ERROR level")
		}
	})

	t.Run("warn level for 4xx", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		engine := gin.New()
		engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
		engine.GET("/missing", func(c *gin.Context) {
			c.Status(http.StatusNotFound)
		})

		req := httptest.NewRequest("GET", "/missing", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		logStr := logBuf.String()
		if !strings.Contains(logStr, "WARN") {
			t.Error("4xx responses should be logged at WARN level")
		}
	})

	t.Run("includes specified headers", func(t *testing.T) {
		var logBuf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

		engine := gin.New()
		cfg := LogRequestResponseConfig{
			Logger:         logger,
			IncludeHeaders: "X-Trace-Id",
		}
		engine.Use(LogRequestResponse(cfg))
		engine.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Trace-Id", "trace-123")
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if !strings.Contains(logBuf.String(), "trace-123") {
			t.Error("Log should include specified header value")
		}
	})
}

// ---------------------------------------------------------------------------
// GinParseUserData tests
// ---------------------------------------------------------------------------

func TestParseUserData_Middleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid JSON object", func(t *testing.T) {
		engine := gin.New()
		engine.Use(GinParseUserData)
		engine.GET("/test", func(c *gin.Context) {
			data := UserDataFromContext(c.Request.Context())
			if data == nil {
				t.Error("UserDataFromContext returned nil")
				return
			}
			if data["user_id"] != "u123" {
				t.Errorf("user_id = %v, want 'u123'", data["user_id"])
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-user-data", `{"user_id": "u123"}`)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("array of JSON strings", func(t *testing.T) {
		engine := gin.New()
		engine.Use(GinParseUserData)
		engine.GET("/test", func(c *gin.Context) {
			data := UserDataFromContext(c.Request.Context())
			if data == nil {
				t.Error("UserDataFromContext returned nil")
				return
			}
			if data["user_id"] != "arr-user" {
				t.Errorf("user_id = %v, want 'arr-user'", data["user_id"])
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		arrJSON, _ := json.Marshal([]string{`{"user_id":"arr-user","role":"admin"}`})
		req.Header.Set("x-user-data", string(arrJSON))
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		engine := gin.New()
		engine.Use(GinParseUserData)
		engine.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called for invalid JSON")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-user-data", "not-json")
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("Failed to unmarshal: %v", err)
		}
		errObj := result["error"].(map[string]interface{})
		if errObj["code"] != "USER_HEADER_JSON_PARSE_FAILURE" {
			t.Errorf("Error code = %v, want USER_HEADER_JSON_PARSE_FAILURE", errObj["code"])
		}
	})

	t.Run("empty header passes through", func(t *testing.T) {
		handlerCalled := false
		engine := gin.New()
		engine.Use(GinParseUserData)
		engine.GET("/test", func(c *gin.Context) {
			handlerCalled = true
			data := UserDataFromContext(c.Request.Context())
			if data != nil {
				t.Error("UserDataFromContext should return nil for empty header")
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if !handlerCalled {
			t.Error("Handler should be called for empty header")
		}
	})
}

// ---------------------------------------------------------------------------
// GinParseApplicationData tests
// ---------------------------------------------------------------------------

func TestParseApplicationData_Middleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("valid JSON object", func(t *testing.T) {
		engine := gin.New()
		engine.Use(GinParseApplicationData)
		engine.GET("/test", func(c *gin.Context) {
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
		req.Header.Set("x-application-data", `{"app_id": "myapp"}`)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("array of JSON strings", func(t *testing.T) {
		engine := gin.New()
		engine.Use(GinParseApplicationData)
		engine.GET("/test", func(c *gin.Context) {
			data := ApplicationDataFromContext(c.Request.Context())
			if data == nil {
				t.Error("ApplicationDataFromContext returned nil")
				return
			}
			if data["app_id"] != "arr-app" {
				t.Errorf("app_id = %v, want 'arr-app'", data["app_id"])
			}
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		arrJSON, _ := json.Marshal([]string{`{"app_id":"arr-app"}`})
		req.Header.Set("x-application-data", string(arrJSON))
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("invalid JSON returns 401", func(t *testing.T) {
		engine := gin.New()
		engine.Use(GinParseApplicationData)
		engine.GET("/test", func(c *gin.Context) {
			t.Error("Handler should not be called for invalid JSON")
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-application-data", "{invalid}")
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})
}

// ---------------------------------------------------------------------------
// MaxPayloadSize tests (gin middleware)
// ---------------------------------------------------------------------------

func TestMaxPayloadSize_Middleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("within limit", func(t *testing.T) {
		engine := gin.New()
		engine.Use(MaxPayloadSize("1kb"))
		engine.POST("/test", func(c *gin.Context) {
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
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("exceeds limit", func(t *testing.T) {
		engine := gin.New()
		engine.Use(MaxPayloadSize("1kb"))
		engine.POST("/test", func(c *gin.Context) {
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
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
		}
	})

	t.Run("mb format", func(t *testing.T) {
		engine := gin.New()
		engine.Use(MaxPayloadSize("2mb"))
		engine.POST("/test", func(c *gin.Context) {
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
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusOK)
		}
	})

	t.Run("default size on empty", func(t *testing.T) {
		size := parseSize("")
		if size != 2*1024*1024 {
			t.Errorf("parseSize(\"\") = %d, want %d", size, 2*1024*1024)
		}
	})
}

// ---------------------------------------------------------------------------
// SecureHeaders tests (gin middleware)
// ---------------------------------------------------------------------------

func TestSecureHeaders_GinMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(SecureHeaders())
	engine.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

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
// parseHeaderJSON tests
// ---------------------------------------------------------------------------

func TestParseHeaderJSON_Middleware(t *testing.T) {
	t.Run("single JSON object", func(t *testing.T) {
		data, err := parseHeaderJSON(`{"key": "value"}`)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if data["key"] != "value" {
			t.Errorf("key = %v, want 'value'", data["key"])
		}
	})

	t.Run("array of JSON strings", func(t *testing.T) {
		input, _ := json.Marshal([]string{`{"key": "from_array"}`})
		data, err := parseHeaderJSON(string(input))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if data["key"] != "from_array" {
			t.Errorf("key = %v, want 'from_array'", data["key"])
		}
	})

	t.Run("invalid input", func(t *testing.T) {
		_, err := parseHeaderJSON("not json at all")
		if err == nil {
			t.Error("Expected error for invalid input")
		}
	})
}

// ---------------------------------------------------------------------------
// CORS tests (gin middleware)
// ---------------------------------------------------------------------------

func TestCORS_GinMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("default config", func(t *testing.T) {
		engine := gin.New()
		engine.Use(CORS(CORSConfig{}))
		engine.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

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
		engine := gin.New()
		engine.Use(CORS(CORSConfig{}))
		engine.OPTIONS("/test", func(c *gin.Context) {
			handlerCalled = true
		})

		req := httptest.NewRequest("OPTIONS", "/test", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if handlerCalled {
			t.Error("Handler should not be called for OPTIONS")
		}
		if rec.Code != http.StatusNoContent {
			t.Errorf("Status = %d, want %d", rec.Code, http.StatusNoContent)
		}
	})
}

// ---------------------------------------------------------------------------
// RequestID tests (gin middleware)
// ---------------------------------------------------------------------------

func TestRequestID_GinMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("generates new ID", func(t *testing.T) {
		var capturedID string
		engine := gin.New()
		engine.Use(RequestID())
		engine.GET("/test", func(c *gin.Context) {
			capturedID = RequestIDFromContext(c.Request.Context())
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

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
		engine := gin.New()
		engine.Use(RequestID())
		engine.GET("/test", func(c *gin.Context) {
			capturedID = RequestIDFromContext(c.Request.Context())
			c.Status(http.StatusOK)
		})

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Request-ID", existingID)
		rec := httptest.NewRecorder()
		engine.ServeHTTP(rec, req)

		if capturedID != existingID {
			t.Errorf("RequestID = %q, want %q", capturedID, existingID)
		}
	})
}

// ---------------------------------------------------------------------------
// Recovery tests (gin middleware)
// ---------------------------------------------------------------------------

func TestRecovery_GinMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(Recovery(slog.New(slog.NewTextHandler(io.Discard, nil))))
	engine.GET("/test", func(c *gin.Context) {
		panic("test panic")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

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
