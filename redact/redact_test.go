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
		"https://api.x.com/traces/550e8400-e29b-41d4-a716-123456789012":     "https://api.x.com/traces/550e8400-e29b-41d4-a716-123456789012",
		"https://api.x.com/events/1791331200000":                            "https://api.x.com/events/1791331200000",
		"https://api.x.com/users/415.555.2671":                              "https://api.x.com/users/[REDACTED]",
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

func TestTextRedactsStructuredCodesNumericSecretsAndBareJWT(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123"
	input := `{"password":1234,"otp":987654,"pin":4321,"cvv":123} token ` + jwt
	got := Text(input)
	for _, secret := range []string{"1234", "987654", "4321", `"cvv":123`, jwt} {
		if strings.Contains(got, secret) {
			t.Fatalf("Text leaked %q: %s", secret, got)
		}
	}
}

func TestTextDoesNotTreatIPsOrDatesAsPhones(t *testing.T) {
	input := "redis 10.12.3.45:5432 at 2026-10-07 12:00:00 timestamp=1791331200000 request=550e8400-e29b-41d4-a716-123456789012; call +919876543210"
	got := Text(input)
	for _, safe := range []string{"10.12.3.45", "2026-10-07", "1791331200000", "550e8400-e29b-41d4-a716-123456789012"} {
		if !strings.Contains(got, safe) {
			t.Fatalf("Text redacted operational value %q: %s", safe, got)
		}
	}
	if strings.Contains(got, "+919876543210") || !strings.Contains(got, "[REDACTED_PHONE]") {
		t.Fatalf("Text did not redact phone: %s", got)
	}
}

func TestTextRedactsCardsDottedPhonesAndShortSecretAliases(t *testing.T) {
	input := `card=4111 1111 1111 1111 phone=415.555.2671 pwd=hunter2 pass=opensesame`
	got := Text(input)
	for _, secret := range []string{"4111 1111 1111 1111", "415.555.2671", "hunter2", "opensesame"} {
		if strings.Contains(got, secret) {
			t.Fatalf("Text leaked %q: %s", secret, got)
		}
	}
	for _, marker := range []string{"[REDACTED_CARD]", "[REDACTED_PHONE]", "pwd=[REDACTED]", "pass=[REDACTED]"} {
		if !strings.Contains(got, marker) {
			t.Fatalf("Text omitted marker %q: %s", marker, got)
		}
	}
}

func TestTextJWTDetectionRequiresDecodableJWTHeaderAndClaims(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123"
	input := strings.Join([]string{
		"host=metroplex.services.internal:8080",
		"topic=fulfillment.shipments.dispatched",
		"token=" + jwt,
	}, " ")
	got := Text(input)
	for _, safe := range []string{"metroplex.services.internal:8080", "fulfillment.shipments.dispatched"} {
		if !strings.Contains(got, safe) {
			t.Fatalf("Text redacted safe dotted value %q: %s", safe, got)
		}
	}
	if strings.Contains(got, jwt) {
		t.Fatalf("Text leaked JWT: %s", got)
	}
}

func TestTextDoesNotRedactLuhnInvalidLongNumbersAsCards(t *testing.T) {
	const reference = "4111 1111 1111 1112"
	if got := Text("reference=" + reference); !strings.Contains(got, reference) {
		t.Fatalf("Text redacted a Luhn-invalid reference: %s", got)
	}
}

func TestTextKeepsOperationalDottedNumbers(t *testing.T) {
	input := strings.Join([]string{
		"timestamp=12:34:56.123456789Z",
		"timestamp=1791331200",
		"latency_ms=1234.5678",
		"amount=12345678.90",
		"coordinates=19.07601234,72.87771234",
		"build=1.45.0+20261007.1234",
		"peers=10.4.0.128 10.4.0.129",
		"partitions=12 18 23 41 57 64",
		"durations=1000-2000-3000",
		"origin=0.000000 0.000000",
		"retry pass: 3",
		"PWD=/app",
	}, " ")
	if got := Text(input); got != input {
		t.Fatalf("Text redacted operational dotted values:\n got: %s\nwant: %s", got, input)
	}
}

func TestPaymentCardScannerUsesWholeStructuredTokens(t *testing.T) {
	for _, input := range []string{
		"peers 10.4.0.128 10.4.0.129",
		"version 1.45.0+20261007.1234",
		"partitions 12 18 23 41 57 64",
		"durations 1000-2000-3000",
		"zeros 0.000000 0.000000",
	} {
		if got := redactPaymentCards(input); got != input {
			t.Fatalf("payment-card scanner redacted operational input %q as %q", input, got)
		}
	}
	for _, input := range []string{
		"card_4111111111111111",
		"pan4111111111111111",
		"card=4000000000000000006",
	} {
		if got := redactPaymentCards(input); !strings.Contains(got, "[REDACTED_CARD]") {
			t.Fatalf("payment-card scanner leaked %q as %q", input, got)
		}
	}
}

func TestPaymentCardScannerDoesNotAllocatePerDigitCandidate(t *testing.T) {
	input := strings.Repeat("partitions=12,18,23,41,57,64;", 1<<15)
	if allocations := testing.AllocsPerRun(10, func() { _ = redactPaymentCards(input) }); allocations > 1 {
		t.Fatalf("safe card scan allocations = %.0f, want at most one", allocations)
	}
}

func TestTextRedactsLooseCodesAndInternationalDottedPhone(t *testing.T) {
	input := "cvv 123 phone=+91.98765.43210"
	got := Text(input)
	for _, secret := range []string{"123", "+91.98765.43210"} {
		if strings.Contains(got, secret) {
			t.Fatalf("Text leaked %q: %s", secret, got)
		}
	}
}

func TestTextRedactsBoundedAndAdjacentCardsWithoutMaskingOrderIDs(t *testing.T) {
	for _, input := range []string{
		"card=4111 1111 1111 1111 12/27",
		"order 12 4111 1111 1111 1111",
		"cards=4111 1111 1111 1111 5555 5555 5555 4444",
		"card=4111.1111.1111.1111",
	} {
		got := Text(input)
		if strings.Contains(got, "4111") {
			t.Fatalf("Text leaked a payment card from %q: %s", input, got)
		}
	}
	if got := Text("cards=4111 1111 1111 1111 5555 5555 5555 4444"); strings.Count(got, "[REDACTED_CARD]") != 2 {
		t.Fatalf("adjacent card markers = %q; want two", got)
	}
	const orderID = "FY6512345678901239"
	if got := Text("order=" + orderID); !strings.Contains(got, orderID) {
		t.Fatalf("Text masked a Fynd order ID: %s", got)
	}
}

func TestTextRedactsCompoundPasswordKeys(t *testing.T) {
	input := `passphrase=open-sesame db_password=db-secret {"database_password":"json-secret"}`
	got := Text(input)
	for _, secret := range []string{"open-sesame", "db-secret", "json-secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("Text leaked %q: %s", secret, got)
		}
	}
}

func TestTextRedactsJWTAfterDottedPrefixAndCompactJWE(t *testing.T) {
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123"
	jwe := "eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIn0..aXYxMjM0NTY3ODkw.Y2lwaGVydGV4dA.dGFnMTIzNDU2Nzg5MA"
	input := "topic=fulfillment.shipments." + jwt + " jwe=" + jwe
	got := Text(input)
	if strings.Contains(got, jwt) || strings.Contains(got, jwe) {
		t.Fatalf("Text leaked a compact token: %s", got)
	}
	if !strings.Contains(got, "topic=fulfillment.shipments."+Mask) {
		t.Fatalf("Text did not preserve safe dotted prefix: %s", got)
	}
}
