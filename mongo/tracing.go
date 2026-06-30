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

// tracing.go adds OpenTelemetry command-level spans to the MongoDB driver via an
// event.CommandMonitor, the Go equivalent of the
// @opentelemetry/instrumentation-mongodb auto-instrumentation that fit.js (Node)
// enabled. One client span is opened per command and closed on the matching
// succeeded/failed event. It is gated on tracing being enabled (zero overhead and
// no monitor installed when off) and NEVER records command bodies or arguments,
// honoring the platform "no PII in logs/traces" rule — only db.system,
// db.operation (command name) and db.name are attached.
package mongo

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/event"

	"github.com/gofynd/fit-go/tracing"
)

// commandTracer correlates a MongoDB CommandStarted event with its matching
// Succeeded/Failed event (keyed by RequestID, which is unique per in-flight
// command) so it can open the span on start and close it on completion.
//
// KNOWN LIMITATION (low severity): an inflight entry is removed only by a
// matching completion event. The driver's CommandMonitor pairs Started with
// Succeeded/Failed for every command in normal operation, but if a Started ever
// has no completion (connection torn down mid-command, client closed/cancelled
// in flight), that entry and its *Span are never deleted/ended. There is no TTL
// or size cap, so the map is unbounded in principle. Bounded in practice (only
// the error/teardown edge path, only when tracing is enabled). If span/heap
// growth is ever observed, add a size cap or stale-entry sweep. Mirrors the
// requestId-keyed-map design of @opentelemetry/instrumentation-mongodb. Tracked
// in OBSERVABILITY_TRACING_SENTRY_PLAN.md §3d.
type commandTracer struct {
	tracer   *tracing.Tracer
	mu       sync.Mutex
	inflight map[int64]*tracing.Span
}

// newCommandMonitor returns the command monitor for the global tracer, or nil
// when tracing is disabled (so the caller installs no monitor and the driver hot
// path is untouched).
func newCommandMonitor() *event.CommandMonitor {
	return commandMonitorFor(tracing.Global())
}

// commandMonitorFor builds a monitor bound to the given tracer. Returns nil when
// the tracer is nil or disabled. Split from newCommandMonitor so it is testable
// without mutating global tracer state.
func commandMonitorFor(tracer *tracing.Tracer) *event.CommandMonitor {
	if tracer == nil || !tracer.IsEnabled() {
		return nil
	}
	ct := &commandTracer{tracer: tracer, inflight: make(map[int64]*tracing.Span)}
	return &event.CommandMonitor{
		Started: ct.started,
		Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
			ct.finished(e.RequestID, nil)
		},
		Failed: func(_ context.Context, e *event.CommandFailedEvent) {
			ct.finished(e.RequestID, e.Failure)
		},
	}
}

// started opens a client span for a command. The started event's context carries
// the active (caller) span, so the command span links as its child.
func (ct *commandTracer) started(ctx context.Context, e *event.CommandStartedEvent) {
	_, span := ct.tracer.StartSpan(ctx, "mongodb."+e.CommandName, tracing.SpanKindClient)
	span.SetAttributes(map[string]any{
		"db.system":    "mongodb",
		"db.operation": e.CommandName,
		"db.name":      e.DatabaseName,
	})
	ct.mu.Lock()
	ct.inflight[e.RequestID] = span
	ct.mu.Unlock()
}

// finished closes (and removes) the in-flight span for reqID, recording an error
// status when cmdErr != nil. A no-op when the span is unknown (e.g. started while
// tracing was disabled, or a duplicate completion event).
func (ct *commandTracer) finished(reqID int64, cmdErr error) {
	ct.mu.Lock()
	span := ct.inflight[reqID]
	delete(ct.inflight, reqID)
	ct.mu.Unlock()
	if span == nil {
		return
	}
	if cmdErr != nil {
		span.SetStatus(tracing.StatusError, cmdErr.Error())
	} else {
		span.SetStatus(tracing.StatusOK, "")
	}
	span.End()
}
