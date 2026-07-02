// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// The access logger keeps legacy parity with fit.js/pyfit: request_url is
// path-only, query params (full values) + route params are structured fields,
// opted-in header values are logged verbatim, and there is no redaction.
func TestLogRequestResponse_LegacyParity(t *testing.T) {
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

	// Full query values (verbatim, NOT redacted).
	if !strings.Contains(out, "jane@x.com") || !strings.Contains(out, `"limit":"50"`) {
		t.Fatalf("query params should be logged in full: %s", out)
	}
	// Route params logged (fit.js request_params / pyfit path_params).
	if !strings.Contains(out, `"company_id":"42"`) {
		t.Fatalf("path_params should be logged: %s", out)
	}
	// Opted-in header values logged verbatim (no masking).
	if !strings.Contains(out, "Bearer topsecret") || !strings.Contains(out, "req-123") {
		t.Fatalf("header values should be logged verbatim: %s", out)
	}
	// request_url is path only (no query string).
	if strings.Contains(out, "/v1/company/42/ticket?") {
		t.Fatalf("request_url must be path-only: %s", out)
	}
	// Nothing is redacted.
	if strings.Contains(out, "[REDACTED]") {
		t.Fatalf("parity mode must not redact: %s", out)
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

// A catch-all (/*wildcard) route param just duplicates request_url, so it must be
// suppressed — no path_params field for a service that mounts a wildcard + does
// internal dispatch (the metroplex case). Named params are still logged.
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
