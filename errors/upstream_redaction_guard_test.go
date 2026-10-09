// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package errors

import (
	stderrors "errors"
	"strings"
	"testing"

	sentrylib "github.com/getsentry/sentry-go"
)

// Upstream-default redaction is a deliberate, pinned behaviour change for
// existing main users: InitSentryWithConfig keeps its API, but every event
// passes fit-go's mandatory BeforeSend sanitizer before it reaches the
// transport.
func TestUpstreamDefaultSentryRedactionContract(t *testing.T) {
	previous := Sentry
	SetSentryReporter(&sentrySdk{})
	t.Cleanup(func() { SetSentryReporter(previous) })

	transport := &mockTransport{}
	if err := InitSentryWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	}); err != nil {
		t.Fatalf("InitSentryWithConfig: %v", err)
	}

	fixture := "checkout failed for order_id=9876543210 at ts=1791364800006 " +
		"request 3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b via 10.1.2.3:5432: " +
		"password=hunter2Secret email=jane.doe@example.com phone 9876543210 " +
		"card 4111111111111111 sent Bearer eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.c2ln"
	Sentry.CaptureError(stderrors.New(fixture))
	Sentry.CaptureMessage(fixture)
	Sentry.Flush()

	events := transport.Events()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	for i, event := range events {
		text := sentryEventText(event)
		for _, secret := range []string{
			"hunter2Secret", "jane.doe@example.com", "phone 9876543210",
			"4111111111111111", "eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.c2ln",
		} {
			if strings.Contains(text, secret) {
				t.Errorf("event %d leaked %q: %s", i, secret, text)
			}
		}
		for _, kept := range []string{
			"10.1.2.3:5432", "order_id=9876543210", "ts=1791364800006",
			"3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b",
		} {
			if !strings.Contains(text, kept) {
				t.Errorf("event %d altered operational value %q: %s", i, kept, text)
			}
		}
	}
}

func sentryEventText(event *sentrylib.Event) string {
	parts := []string{event.Message}
	for _, exception := range event.Exception {
		parts = append(parts, exception.Value)
	}
	return strings.Join(parts, " | ")
}
