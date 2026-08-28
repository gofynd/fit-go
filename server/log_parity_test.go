// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// The access logger keeps the legacy field shape while enforcing fit-go's
// no-PII/no-secrets telemetry boundary.
func TestLogRequestResponse_SecureLegacyFieldShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{
		Logger:         logger,
		IncludeHeaders: "Authorization,X-Request-Id",
	}))
	engine.GET("/v1/company/:company_id/ticket", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	req := httptest.NewRequest("GET", "/v1/company/42/ticket?limit=50&email=jane@x.com", nil)
	req.Header.Set("Authorization", "Bearer topsecret")
	req.Header.Set("X-Request-Id", "req-123")
	engine.ServeHTTP(httptest.NewRecorder(), req)
	out := buf.String()

	// Operational query controls remain visible; arbitrary values are masked.
	if !strings.Contains(out, `"limit":"50"`) || !strings.Contains(out, `"email":"[REDACTED]"`) {
		t.Fatalf("query params should use allowlist redaction: %s", out)
	}
	if strings.Contains(out, "jane@x.com") {
		t.Fatalf("query PII leaked: %s", out)
	}
	// Route params logged (fit.js request_params / pyfit path_params).
	if !strings.Contains(out, `"company_id":"42"`) {
		t.Fatalf("path_params should be logged: %s", out)
	}
	// Safe opted-in headers remain visible; credentials are always masked.
	if !strings.Contains(out, `"Authorization":"[REDACTED]"`) || !strings.Contains(out, "req-123") {
		t.Fatalf("headers should apply credential masking: %s", out)
	}
	if strings.Contains(out, "Bearer topsecret") {
		t.Fatalf("authorization secret leaked: %s", out)
	}
	// request_url is path only (no query string).
	if strings.Contains(out, "/v1/company/42/ticket?") {
		t.Fatalf("request_url must be path-only: %s", out)
	}
}

// Legacy parity: every response line is info level, including 5xx (no
// level-by-status).
func TestLogRequestResponse_InfoLevelParity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
	engine.GET("/boom", func(c *gin.Context) { c.String(http.StatusInternalServerError, "boom") })

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/boom", nil))
	out := buf.String()

	if strings.Contains(out, `"level":"ERROR"`) || strings.Contains(out, `"level":"WARN"`) {
		t.Fatalf("parity: response line must be info even for 5xx: %s", out)
	}
	if !strings.Contains(out, `"response_status":500`) {
		t.Fatalf("response_status should be logged: %s", out)
	}
}

func TestLogRequestResponse_StatusBasedOptIn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{
		Logger:              logger,
		ResponseLogSeverity: ResponseLogSeverityStatusBased,
	}))
	engine.GET("/missing", func(c *gin.Context) { c.String(http.StatusNotFound, "missing") })
	engine.GET("/boom", func(c *gin.Context) { c.String(http.StatusInternalServerError, "boom") })

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/missing", nil))
	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/boom", nil))
	out := buf.String()

	if !strings.Contains(out, `"level":"WARN"`) {
		t.Fatalf("status-based mode should log 4xx response as WARN: %s", out)
	}
	if !strings.Contains(out, `"level":"ERROR"`) {
		t.Fatalf("status-based mode should log 5xx response as ERROR: %s", out)
	}
}

func TestLogRequestResponse_OriginalURLOptIn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger, IncludeQueryInRequestURL: true}))
	engine.GET("/v1/items", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/items?limit=10&token=topsecret", nil))
	out := buf.String()

	if !strings.Contains(out, `/v1/items?limit=10&amp;token=[REDACTED]`) &&
		!strings.Contains(out, `/v1/items?limit=10&token=[REDACTED]`) {
		t.Fatalf("original URL mode should include a redacted query string: %s", out)
	}
	if strings.Contains(out, "topsecret") {
		t.Fatalf("original URL mode leaked query secret: %s", out)
	}
}

func TestLogRequestResponse_TraceClueAccessShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("FIT_LOG_SCHEMA", "")
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
	engine.GET("/v1/company/:company_id/ticket", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/company/42/ticket?limit=10&token=topsecret", nil))
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected request and response records, got %d: %s", len(lines), buf.String())
	}
	for _, line := range lines {
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode access record: %v (%s)", err, line)
		}
		requestURL, _ := record["request_url"].(string)
		if !strings.Contains(requestURL, "/v1/company/42/ticket?limit=10&token=[REDACTED]") {
			t.Fatalf("TraceClue request_url shape is wrong: %s", line)
		}
		if _, ok := record["query_params"]; ok {
			t.Fatalf("TraceClue record must not emit query_params: %s", line)
		}
		if _, ok := record["path_params"]; ok {
			t.Fatalf("TraceClue record must not emit path_params: %s", line)
		}
		if _, ok := record["duration"]; ok {
			t.Fatalf("TraceClue access record must not emit duration: %s", line)
		}
		params, ok := record["request_params"].(map[string]interface{})
		if !ok || params["company_id"] != "42" {
			t.Fatalf("TraceClue request_params missing route params: %s", line)
		}
		if strings.Contains(line, "topsecret") {
			t.Fatalf("TraceClue request URL leaked query secret: %s", line)
		}
	}
}

// A catch-all route duplicates request_url, so it is omitted from path_params.
func TestLogRequestResponse_CatchAllPathParamSuppressed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{Logger: logger}))
	engine.GET("/platform/*path", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/platform/v1.0/company/42/ticket", nil))
	out := buf.String()

	if strings.Contains(out, "path_params") {
		t.Fatalf("catch-all path param must be suppressed (redundant with request_url): %s", out)
	}
	// request_url still carries the full path.
	if !strings.Contains(out, "/platform/v1.0/company/42/ticket") {
		t.Fatalf("request_url should still be logged: %s", out)
	}
}
