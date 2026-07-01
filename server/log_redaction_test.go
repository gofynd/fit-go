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

// The access logger must never emit raw query values or sensitive header values:
// request_url is path-only, query params are allowlist-redacted, and sensitive
// headers are masked.
func TestLogRequestResponse_RedactsQueryAndHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	engine := gin.New()
	engine.Use(LogRequestResponse(LogRequestResponseConfig{
		Logger:         logger,
		IncludeHeaders: "Authorization,X-Request-Id",
	}))
	engine.GET("/v1/tickets", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	req := httptest.NewRequest("GET", "/v1/tickets?limit=50&email=jane@x.com&token=supersecret", nil)
	req.Header.Set("Authorization", "Bearer topsecret")
	req.Header.Set("X-Request-Id", "req-123")
	engine.ServeHTTP(httptest.NewRecorder(), req)

	out := buf.String()

	// 1. No raw PII / secret anywhere.
	for _, leak := range []string{"jane@x.com", "supersecret", "topsecret"} {
		if strings.Contains(out, leak) {
			t.Fatalf("access log leaked %q:\n%s", leak, out)
		}
	}
	// 2. request_url is path-only (no query string).
	if strings.Contains(out, "/v1/tickets?") {
		t.Fatalf("request_url carried the query string:\n%s", out)
	}
	// 3. Allowlisted param value kept; sensitive ones masked.
	if !strings.Contains(out, "limit") || !strings.Contains(out, "50") {
		t.Fatalf("allowlisted query value lost:\n%s", out)
	}
	if !strings.Contains(out, "[REDACTED]") {
		t.Fatalf("expected redaction markers for email/token/Authorization:\n%s", out)
	}
	// 4. Non-sensitive header retained.
	if !strings.Contains(out, "req-123") {
		t.Fatalf("safe header X-Request-Id should be kept:\n%s", out)
	}
}
