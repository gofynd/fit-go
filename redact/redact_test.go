// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redact

import (
	"net/url"
	"strings"
	"testing"
)

func TestSafeURL(t *testing.T) {
	cases := map[string]string{
		"https://user:pass@api.x.com/v1/orders?token=secret&limit=5": "https://api.x.com/v1/orders",
		"http://svc.local:8080/path":                                 "http://svc.local:8080/path",
		"https://x.com":                                              "https://x.com/",
	}
	for in, want := range cases {
		u, _ := url.Parse(in)
		if got := SafeURL(u); got != want {
			t.Errorf("SafeURL(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(SafeURL(u), "secret") || strings.Contains(SafeURL(u), "pass") {
			t.Errorf("SafeURL leaked query/userinfo: %q", SafeURL(u))
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
	if IsSensitiveHeader("X-Trace-Id") {
		t.Error("false positive on a safe header")
	}
}
