// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Upstream-default redaction is a deliberate, pinned behaviour change for
// existing main users: server.New keeps the original LogRequestResponse field
// shape, but the logged query string is always redacted (allowlisted paging
// and sorting keys stay verbatim) and opted-in credential headers are masked.
func TestUpstreamDefaultServerAccessLogRedactionContract(t *testing.T) {
	t.Setenv("SERVER_TYPE", "platform")
	t.Setenv("DISABLE_RESPONSE_MIDDLEWARES", "true")
	t.Setenv("INCLUDE_HEADERS_IN_LOG", "Authorization,X-Request-Id")

	var output bytes.Buffer
	s := New(Config{Logger: slog.New(slog.NewJSONHandler(&output, nil))})
	router := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := s.Init(map[ServerType]http.Handler{ServerTypePlatform: router}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}

	query := url.Values{
		"password":     {"hunter2Secret"},
		"email":        {"jane.doe@example.com"},
		"phone":        {"9876543210"},
		"card":         {"4111111111111111"},
		"access_token": {"Bearer eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.c2ln"},
		"limit":        {"50"},
		"sort":         {"created_at"},
	}
	request := httptest.NewRequest(http.MethodGet,
		"/v1/orders/3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b/items?"+query.Encode(), nil)
	request.Header.Set("Authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.c2ln")
	request.Header.Set("X-Request-Id", "req-1791364800006")
	s.App.ServeHTTP(httptest.NewRecorder(), request)

	out := output.String()
	if !strings.Contains(out, "[REQ]") || !strings.Contains(out, "[RES]") {
		t.Fatalf("access log lines missing: %s", out)
	}
	for _, secret := range []string{
		"hunter2Secret", "jane.doe", "9876543210", "4111111111111111", "eyJhbGciOiJIUzI1NiJ9",
	} {
		if strings.Contains(out, secret) {
			t.Errorf("server.New access log leaked %q: %s", secret, out)
		}
	}
	for _, kept := range []string{
		"limit=50", "sort=created_at", "password=[REDACTED]",
		"/v1/orders/3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b/items",
		`"Authorization":"[REDACTED]"`, `"X-Request-Id":"req-1791364800006"`,
	} {
		if !strings.Contains(out, kept) {
			t.Errorf("server.New access log missing %q: %s", kept, out)
		}
	}
}
