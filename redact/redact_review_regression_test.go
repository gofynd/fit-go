// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redact

import (
	"net/url"
	"strings"
	"testing"
)

func TestReviewPhoneRegressionsFailClosed(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"id=+919876543210",
		"user_id: +91 98765 43210",
		"amount: +91 98765 43210",
		"version: +1 415 555 1234",
		"PAID 9876543210",
		"VALID 9876543210",
		"UID 9876543210",
		"qty 2 9876543210",
		"phones 9876543210 9876543211",
		"(98765 43210)",
		"(415-555-1234)",
		"0091 98765 43210",
		"020 7946 0958",
		"98765.43210",
		"1-800-555-1234",
		"customer_phone: 9876543210",
		"phoneNumber: 9876543210",
		"contact_phone=9876543210",
		"whatsapp: 9876543210",
		"msisdn=9876543210",
		`"phone":"98765 43210"`,
		"/v1/users/9876543210/orders",
	} {
		got := Text(input)
		if !strings.Contains(got, "[REDACTED_PHONE]") {
			t.Errorf("Text(%q) = %q; expected phone redaction", input, got)
		}
	}
}

func TestReviewPhoneFalsePositiveBoundaries(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"id=9876543210",
		"user_id: 9876543210",
		"order_id=9876543210",
		"f(1234567890)",
		"HOTEL: 1234567890",
		"version +1 2 3 4 5 6 7 8 9 0",
		"delta=+1234 5678",
	} {
		if got := Text(input); got != input {
			t.Errorf("Text(%q) = %q; expected operational value to remain", input, got)
		}
	}
}

func TestReviewCardLabelsAndNumericBoundaries(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"giftcard=4111111111111111",
		"cardpan=4111111111111111",
		"my_card4111111111111111",
		"customer_card_4111111111111111",
		"x_pan_4111111111111111",
		"card number: 4111  1111 1111-1111",
		"pan.number: 41111111 11111111",
		"cardholder: 3056 930902 5904",
		"card=4111111111111111.5",
		"payment 4111  1111 1111 1111",
		"payment 4111-1111 1111.1111",
		"payment 41111111 11111111",
		"payment 3056 930902 5904",
	} {
		got := Text(input)
		if !strings.Contains(got, "[REDACTED_CARD]") {
			t.Errorf("Text(%q) = %q; expected card redaction", input, got)
		}
	}

	for _, input := range []string{
		"card=192.168.100.120 status=1",
		"card=411111111111111111110",
		"timestamp_ms=1791364800006",
		`"time":1791364800006`,
		"sent_at=1791364800006",
		"/v1/events/1791364800006",
	} {
		if got := Text(input); got != input {
			t.Errorf("Text(%q) = %q; expected non-card numeric value to remain", input, got)
		}
	}
}

func TestSafeURLPreservesEpochEventPathsButRedactsPhones(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]string{
		"https://api.example/v1/events/1791364800006": "https://api.example/v1/events/1791364800006",
		"https://api.example/v1/users/9876543210":     "https://api.example/v1/users/[REDACTED]",
	} {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := SafeURL(parsed); got != want {
			t.Errorf("SafeURL(%q) = %q; want %q", raw, got, want)
		}
	}
}
