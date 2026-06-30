// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tracingtest provides test helpers for building an enabled tracer,
// shared across the packages that exercise the tracing hooks. Internal so it is
// never part of the public API or a production build.
package tracingtest

import (
	"context"
	"testing"

	"github.com/gofynd/fit-go/tracing"
)

// Enabled builds a real, enabled tracer (TRACING_ENABLED=true) WITHOUT touching
// global state. OTel exporters are lazy, so this is offline-safe (no collector
// needed). Use it when the code under test takes an injected *tracing.Tracer.
func Enabled(t *testing.T) *tracing.Tracer {
	t.Helper()
	t.Setenv("TRACING_ENABLED", "true")
	tr, err := tracing.New(context.Background(), tracing.DefaultOptions())
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	if !tr.IsEnabled() {
		t.Fatal("expected an enabled tracer with TRACING_ENABLED=true")
	}
	return tr
}

// EnabledGlobal is Enabled plus installing the tracer as the process global for
// the duration of the test (restored on cleanup). Use when the code under test
// reads tracing.Global() rather than an injected tracer: it sidesteps the
// sync.Once-cached-disabled global so enabled-path assertions actually run under
// `go test ./...` instead of skipping.
func EnabledGlobal(t *testing.T) *tracing.Tracer {
	t.Helper()
	tr := Enabled(t)
	t.Cleanup(tracing.SetGlobal(tr))
	return tr
}
