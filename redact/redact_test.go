// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redact

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
)

func TestSafeURL(t *testing.T) {
	cases := map[string]string{
		"https://user:pass@api.x.com/v1/orders?token=secret&limit=5":        "https://api.x.com/v1/orders",
		"http://svc.local:8080/path":                                        "http://svc.local:8080/path",
		"https://x.com":                                                     "https://x.com/",
		"https://api.x.com/reset-password/path-secret?token=query-secret":   "https://api.x.com/reset-password/[REDACTED]",
		"https://api.x.com/users/jane%40example.com":                        "https://api.x.com/users/[REDACTED]",
		"mongodb://user:pass@mongo.internal/customer?authSource=admin":      "mongodb://mongo.internal/[REDACTED]",
		"postgresql://user:pass@postgres.internal/customer?sslmode=require": "postgresql://postgres.internal/[REDACTED]",
		"redis://default:pass@redis.internal:6379/4?token=secret":           "redis://redis.internal:6379/[REDACTED]",
	}
	for in, want := range cases {
		u, _ := url.Parse(in)
		if got := SafeURL(u); got != want {
			t.Errorf("SafeURL(%q) = %q, want %q", in, got, want)
		}
		for _, leaked := range []string{"user:pass@", "path-secret", "query-secret", "authSource=", "sslmode=", "token=secret"} {
			if strings.Contains(SafeURL(u), leaked) {
				t.Errorf("SafeURL leaked %q: %q", leaked, SafeURL(u))
			}
		}
	}
	if SafeURL(nil) != "" {
		t.Error("SafeURL(nil) must be empty")
	}
}

func TestQueryMap_AllowlistAndRedact(t *testing.T) {
	v, _ := url.ParseQuery("limit=200&page=1&email=jane@x.com&q=John+Doe&token=abc123")
	m := QueryMap(v, nil)
	if m["limit"] != "200" || m["page"] != "1" {
		t.Errorf("allowlisted values dropped: %v", m)
	}
	for _, k := range []string{"email", "q", "token"} {
		if m[k] != Mask {
			t.Errorf("PII key %q not masked: %v", k, m[k])
		}
	}
	// keys are always retained (reveal shape without value)
	for _, k := range []string{"limit", "page", "email", "q", "token"} {
		if _, ok := m[k]; !ok {
			t.Errorf("key %q dropped from map", k)
		}
	}
	if QueryMap(url.Values{}, nil) != nil {
		t.Error("empty query must yield nil map")
	}
}

func TestQuery_StringForm(t *testing.T) {
	s := Query("email=jane@x.com&limit=5&token=abc", nil)
	if strings.Contains(s, "jane@x.com") || strings.Contains(s, "abc") {
		t.Errorf("raw PII/secret leaked in Query(): %q", s)
	}
	if !strings.Contains(s, "limit=5") {
		t.Errorf("allowlisted value lost: %q", s)
	}
	if !strings.Contains(s, "email="+Mask) || !strings.Contains(s, "token="+Mask) {
		t.Errorf("sensitive values not masked: %q", s)
	}
	// deterministic (sorted) output
	if Query("b=2&a=1", map[string]bool{"a": true, "b": true}) != "a=1&b=2" {
		t.Errorf("Query not sorted/stable: %q", Query("b=2&a=1", map[string]bool{"a": true, "b": true}))
	}
	if Query("", nil) != "" {
		t.Error("empty in -> empty out")
	}
}

func TestHeaderRedaction(t *testing.T) {
	if HeaderValue("Authorization", "Bearer xyz") != Mask {
		t.Error("Authorization must be masked")
	}
	if HeaderValue("cookie", "sid=1") != Mask || HeaderValue("Set-Cookie", "x") != Mask {
		t.Error("cookies must be masked")
	}
	if HeaderValue("X-Request-Id", "abc") != "abc" {
		t.Error("non-sensitive header must pass through")
	}
	if !IsSensitiveHeader("AUTHORIZATION") || !IsSensitiveHeader("x-api-key") {
		t.Error("sensitive detection must be case-insensitive")
	}
	for _, name := range []string{"X-User-Data", "X-Amz-Security-Token", "Vendor-Credential", "x_custom_api_key"} {
		if !IsSensitiveHeader(name) {
			t.Errorf("platform or suffix-sensitive header %q was not detected", name)
		}
	}
	if IsSensitiveHeader("X-Trace-Id") {
		t.Error("false positive on a safe header")
	}
}

func TestErrorMessageNeverIncludesRawURLOrBackendText(t *testing.T) {
	const secretURL = "https://user:password@example.com/send?email=jane@example.com&token=api-secret"
	err := &url.Error{Op: "post", URL: secretURL, Err: errors.New("provider echoed api-secret and jane@example.com")}
	got := ErrorMessage(err)
	for _, secret := range []string{"password", "jane@example.com", "api-secret", "provider echoed"} {
		if strings.Contains(got, secret) {
			t.Fatalf("ErrorMessage leaked %q in %q", secret, got)
		}
	}
	if got != "POST https://example.com/send: operation failed" {
		t.Fatalf("ErrorMessage = %q", got)
	}

	if got := ErrorMessage(context.DeadlineExceeded); got != "operation timed out" {
		t.Fatalf("deadline classification = %q", got)
	}
	if got := ErrorMessage(errors.New("password=secret")); got != "password=[REDACTED]" {
		t.Fatalf("generic classification = %q", got)
	}
}

func TestTextRedactsDiagnosticSecretsAndPII(t *testing.T) {
	input := strings.Join([]string{
		`POST https://alice:hunter2@example.com/reset-token/path-secret?email=user@example.com&api_key=http-secret`,
		`mongodb://mongo-user:mongo-pass@mongo.internal/customer?authSource=admin`,
		`postgresql://pg-user:pg-pass@postgres.internal/customer?sslmode=require`,
		`redis://default:redis-pass@redis.internal:6379/4?token=redis-secret`,
		`path=/verification-code/path-code?email=path@example.com`,
		`Authorization: Basic dXNlcjpwYXNz`,
		`X-Custom-Api-Key: header-secret`,
		`Bearer bearer-secret phone=+919876543210 {"password":"json-secret"}`,
	}, "\n")
	got := Text(input)
	for _, forbidden := range []string{
		"alice", "hunter2", "user@example.com", "path-secret", "http-secret",
		"mongo-user", "mongo-pass", "/customer", "pg-user", "pg-pass",
		"default", "redis-pass", "path-code", "path@example.com", "dXNlcjpwYXNz",
		"header-secret", "bearer-secret", "+919876543210", "json-secret",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("Text leaked %q: %s", forbidden, got)
		}
	}
	if !strings.Contains(got, "https://example.com/reset-token/"+Mask) {
		t.Fatalf("Text removed safe URL context: %s", got)
	}
	for _, host := range []string{"mongo.internal", "postgres.internal", "redis.internal"} {
		if !strings.Contains(got, host) {
			t.Fatalf("Text removed safe DSN host %q: %s", host, got)
		}
	}
}
