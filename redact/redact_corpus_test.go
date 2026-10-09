// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redact

import (
	"strings"
	"testing"
)

// TestTextReviewCorpus pins the review corpus for Text: every mustRedact row
// must lose its sensitive fragment and gain a redaction marker, and every
// mustKeep row must be returned byte-for-byte unchanged.
func TestTextReviewCorpus(t *testing.T) {
	t.Parallel()

	const jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123"
	mustRedact := []struct {
		input  string
		secret string // fragment that must not survive; empty = whole-value check only
	}{
		// Phones.
		{"9876543210", "9876543210"},
		{"call 9876543210", "9876543210"},
		{"phone 9876543210", "9876543210"},
		{"customer_phone: 9876543210", "9876543210"},
		{"phoneNumber: 9876543210", "9876543210"},
		{"contact_phone=98765 43210", "98765 43210"},
		{"phone.number: 9876543210", "9876543210"},
		{"whatsapp: 9876543210", "9876543210"},
		{"msisdn=919876543210", "919876543210"},
		{"(mobile: 9876543210)", "9876543210"},
		{"(+91 98765 43210)", "98765 43210"},
		{"+14155551234", "4155551234"},
		{"+447946095812", "7946095812"},
		{"+44 20 7946 0958", "7946 0958"},
		{"1-800-555-1234", "555-1234"},
		{"(415) 555-1234", "555-1234"},
		{"id=+919876543210", "9876543210"},
		{"qty 2 9876543210", "9876543210"},
		{"phones 9876543210 9876543211", "987654321"},
		{"0091 98765 43210", "98765 43210"},
		{`"phone":"98765 43210"`, "98765 43210"},
		// Function-call exception only for non-phone-shaped arguments.
		{"Ravi(9876543210)", "9876543210"},
		{"customer(9876543210)", "9876543210"},
		{"call(9876543210)", "9876543210"},
		{"name(98765 43210)", "98765 43210"},
		{"call(415-555-1234)", "555-1234"},
		{"call(+91 98765 43210)", "98765 43210"},
		{"f(1234567890) 415-555-1234", "555-1234"},
		// Over-long candidates joining an ID and a phone are split and judged per part.
		{"1234567890 415-555-1234", "555-1234"},
		{"order 1234567890 415-555-1234", "555-1234"},
		{"id: 1234567890 415-555-1234", "555-1234"},
		{"1234567890 +91 98765 43210", "98765 43210"},
		{"1234567890 +44 20 7946 0958", "7946 0958"},
		{"1234567890 (415) 555-1234", "555-1234"},
		{"9876543210 9876543211", "987654321"},
		{"phone: 98765 43210 98765 43211", "4321"},
		// Phone label suffixes with delimiter/camel boundaries.
		{"tel: 9876543210", "9876543210"},
		{"customer_tel: 98765 43210", "98765 43210"},
		{"customer_whatsapp 98765 43210", "98765 43210"},
		{"user_msisdn 919876543210", "919876543210"},
		{"PAID 9876543210", "9876543210"},
		// Payment cards.
		{"4111 1111 1111 1111", "4111"},
		{"4111111111111111", "4111111111111111"},
		{"card 4111 1111 1111 1111.", "4111"},
		{"card=4111111111111111-", "4111111111111111"},
		{"creditcard_4111111111111111", "4111111111111111"},
		{"my_card4111111111111111", "4111111111111111"},
		{"giftcard4111111111111111", "4111111111111111"},
		{"3782 822463 10005", "822463"},
		{`{"cardNumber":"4111111111111111"}`, "4111111111111111"},
		{"my_card-4111111111111111", "4111111111111111"},
		{"card.4111111111111111", "4111111111111111"},
		{"pan-4111111111111111", "4111111111111111"},
		{"payment 4111  1111 1111 1111", "4111"},
		{"payment 41111111 11111111", "41111111"},
		// Secrets.
		{"password=x", "=x"},
		{"pass: hunter2", "hunter2"},
		{"pass=hunter2", "hunter2"},
		{"pass: 123456", "123456"},
		{"PWD=hunter2", "hunter2"},
		{"PWD=/app", "/app"}, // documented trade-off: PWD is a password alias.
		{"cvv 123", "123"},
		{"otp=123456", "123456"},
		{"contact jane.doe@example.com", "jane.doe@example.com"},
		{"Authorization: Bearer abc.def-ghi", "abc.def-ghi"},
		{"Bearer abc.def-ghi", "abc.def-ghi"},
		{"token " + jwt, jwt},
	}
	// The non-phone part of a split candidate must survive verbatim.
	for _, input := range []string{
		"1234567890 415-555-1234", "order 1234567890 415-555-1234", "id: 1234567890 415-555-1234",
		"1234567890 +91 98765 43210", "1234567890 (415) 555-1234",
	} {
		if got := Text(input); !strings.Contains(got, "1234567890 [REDACTED_PHONE]") {
			t.Errorf("Text(%q) = %q; expected the non-phone ID to be kept", input, got)
		}
	}
	for _, row := range mustRedact {
		got := Text(row.input)
		if got == row.input || !strings.Contains(got, "[REDACTED") {
			t.Errorf("Text(%q) = %q; expected redaction", row.input, got)
			continue
		}
		if row.secret != "" && strings.Contains(got, row.secret) {
			t.Errorf("Text(%q) = %q; leaked %q", row.input, got, row.secret)
		}
	}

	mustKeep := []string{
		"dial tcp 10.12.3.45:5432",
		"peers 10.4.0.128 10.4.0.129",
		"partitions 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14",
		"v 1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18",
		"latency=0.123456789s",
		"amount=12345678.90",
		"geo 19.07601234 72.87771234",
		"2026-10-07T10:00:00Z",
		"12:34:56.123456789Z",
		"1.45.0+20261007.1234",
		"trace 550e8400-e29b-41d4-a716-446655440000",
		"timestamp=1791364800006",
		`"timestamp":1791364800006`,
		"ts=1791364800006",
		"request_ts=1791364800006",
		"start_timestamp=1791364800006",
		"requestTs=1791364800006",
		"timestamp_ms=1791364800006",
		"time=1791364800006",
		"sent_at=1791364800006",
		"created_at=1791364800006",
		"epoch 1791364800",
		"id=9876543210",
		"order_id=9876543210",
		"identifier 9876543210",
		"seq 9876543210",
		"size 9876543210",
		"build 9876543210",
		"offset 9876543210",
		"f(1234567890)",
		"id(1234567890)",
		"retry pass: 3",
		"RETRY PASS: 3",
		"FY6512345678901234",
		"bytes 123456789",
		"object 65f1a2b3c4d5e6f708091a2b",
		"HOTEL: 1234567890",
		"version +1 2 3 4 5 6 7 8",
		"metroplex.services.internal:8080",
		"fulfillment.shipments.dispatched",
		// Unlabelled separator-joined list whose digits are Luhn-valid.
		"12 18 23 41 57 64 9",
		// Over-long candidates without a phone-shaped part.
		"1234567890 1234567891",
		"id=1234567890 1234567891",
		"bytes 123456789 123456789",
		"1234 5678 9012 3456 7890",
		// '-' before digits is a card boundary only after a card label.
		"ref-4111111111111111",
	}
	for _, input := range mustKeep {
		if got := Text(input); got != input {
			t.Errorf("Text(%q) = %q; expected unchanged", input, got)
		}
	}

	const path = "https://api.example/v1/users/9876543210/orders"
	if got := SafeURL(mustParseURL(t, path)); got != "https://api.example/v1/users/[REDACTED]/orders" {
		t.Errorf("SafeURL(%q) = %q; expected phone path segment redaction", path, got)
	}
}
