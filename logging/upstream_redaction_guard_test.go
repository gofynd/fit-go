// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package logging

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// Upstream-default redaction is a deliberate, pinned behaviour change for
// existing main users: logging.New keeps its API and field shape, but error
// values are rendered through redact.ErrorMessage. These fixtures are shared
// in spirit with the server and Sentry guards so all default paths agree.
const upstreamRedactionFixture = "checkout failed for order_id=9876543210 at ts=1791364800006 " +
	"request 3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b via 10.1.2.3:5432: " +
	"password=hunter2Secret email=jane.doe@example.com phone 9876543210 " +
	"card 4111111111111111 sent Bearer eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.c2ln"

var upstreamRedactionMustMask = []string{
	"hunter2Secret",
	"jane.doe@example.com",
	"phone 9876543210",
	"4111111111111111",
	"eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.c2ln",
}

var upstreamRedactionMustKeep = []string{
	"10.1.2.3:5432",
	"order_id=9876543210",
	"ts=1791364800006",
	"3f2b8c1e-9a4d-4e6f-8b7a-1c2d3e4f5a6b",
}

func TestUpstreamDefaultLoggerRedactionContract(t *testing.T) {
	for _, env := range []string{"production", "development"} {
		t.Run(env, func(t *testing.T) {
			var output bytes.Buffer
			logger, err := New(Options{Level: "info", Env: env, Output: &output})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			logger.Error("x", "error", errors.New(upstreamRedactionFixture))
			line := output.String()
			if line == "" {
				t.Fatal("default logger wrote nothing")
			}
			for _, secret := range upstreamRedactionMustMask {
				if strings.Contains(line, secret) {
					t.Errorf("default logger leaked %q: %s", secret, line)
				}
			}
			for _, operational := range upstreamRedactionMustKeep {
				if !strings.Contains(line, operational) {
					t.Errorf("default logger altered operational value %q: %s", operational, line)
				}
			}
			if !strings.Contains(line, "[REDACTED") {
				t.Errorf("default logger output carries no redaction marker: %s", line)
			}
		})
	}
}
