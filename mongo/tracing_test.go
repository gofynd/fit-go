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

package mongo

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/event"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// When tracing is off, no monitor is installed (zero overhead on the hot path).
func TestCommandMonitorFor_DisabledReturnsNil(t *testing.T) {
	if m := commandMonitorFor(nil); m != nil {
		t.Fatal("nil tracer should yield no monitor")
	}

	t.Setenv("TRACING_ENABLED", "")
	disabled, err := tracing.New(context.Background(), tracing.DefaultOptions())
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	if disabled.IsEnabled() {
		t.Skip("environment forces tracing on; disabled-path assertion not applicable")
	}
	if m := commandMonitorFor(disabled); m != nil {
		t.Fatal("disabled tracer should yield no monitor")
	}
}

// When enabled, a monitor is installed.
func TestCommandMonitorFor_EnabledReturnsMonitor(t *testing.T) {
	m := commandMonitorFor(tracingtest.Enabled(t))
	if m == nil || m.Started == nil || m.Succeeded == nil || m.Failed == nil {
		t.Fatalf("expected a fully-wired monitor, got %+v", m)
	}
}

// A command span is opened on Started and closed on Succeeded/Failed, leaving no
// in-flight spans (guards against span leaks). The failed path records the error.
func TestCommandTracer_Lifecycle(t *testing.T) {
	ct := &commandTracer{tracer: tracingtest.Enabled(t), inflight: map[int64]*tracing.Span{}}

	// Success path.
	ct.started(context.Background(), &event.CommandStartedEvent{
		CommandName:  "find",
		DatabaseName: "highbrow",
		RequestID:    1,
	})
	if got := len(ct.inflight); got != 1 {
		t.Fatalf("after started: want 1 in-flight span, got %d", got)
	}
	ct.finished(1, nil)
	if got := len(ct.inflight); got != 0 {
		t.Fatalf("after succeeded: want 0 in-flight spans, got %d (leak)", got)
	}

	// Failure path.
	ct.started(context.Background(), &event.CommandStartedEvent{
		CommandName:  "insert",
		DatabaseName: "highbrow",
		RequestID:    2,
	})
	ct.finished(2, errors.New("boom"))
	if got := len(ct.inflight); got != 0 {
		t.Fatalf("after failed: want 0 in-flight spans, got %d (leak)", got)
	}

	// Unknown completion is a safe no-op.
	ct.finished(999, nil)
}
