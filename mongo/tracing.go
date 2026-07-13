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
//
// (mongo-driver v2 has no official OTel contrib instrumentation — otelmongo
// targets driver v1 — so this hand-rolled monitor is the only option here.)
package mongo

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/event"

	"github.com/gofynd/fit-go/tracing"
)

const (
	// maxInflight bounds the in-flight span map. Reaching it triggers a sweep of
	// stale entries; normal operation never approaches it.
	maxInflight = 8192
	// maxInflightAge is how long an in-flight span may live without a completion
	// event before the sweep treats it as abandoned and ends it.
	maxInflightAge = 5 * time.Minute
)

// inflightSpan is an open command span plus when it started (for the stale sweep).
type inflightSpan struct {
	span  *tracing.Span
	start time.Time
}

// commandTracer correlates a MongoDB CommandStarted event with its matching
// Succeeded/Failed event (keyed by RequestID, which is unique per in-flight
// command) so it can open the span on start and close it on completion.
//
// The driver pairs Started with Succeeded/Failed for every command in normal
// operation. If a Started ever has no completion (connection torn down
// mid-command, client closed/cancelled in flight), its entry would otherwise
// linger forever — so the map is bounded: when it reaches maxInflight, started()
// sweeps entries older than maxInflightAge, ending those orphaned spans with an
// error status. This caps memory at the cost of an O(n) sweep only on the (rare)
// leak path. Mirrors the requestId-keyed-map design of
// @opentelemetry/instrumentation-mongodb, with the leak bounded.
type commandTracer struct {
	tracer   *tracing.Tracer
	mu       sync.Mutex
	inflight map[int64]inflightSpan
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
	ct := &commandTracer{tracer: tracer, inflight: make(map[int64]inflightSpan)}
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
	ctx = tracing.ContextWithActiveGoroutine(ctx)
	_, span := ct.tracer.StartSpan(ctx, "mongodb."+e.CommandName, tracing.SpanKindClient)
	span.SetAttributes(map[string]any{
		"db.system":    "mongodb",
		"db.operation": e.CommandName,
		"db.name":      e.DatabaseName,
	})
	ct.mu.Lock()
	if len(ct.inflight) >= maxInflight {
		ct.sweepStaleLocked()
		// Hard cap: if the stale sweep freed nothing (a burst of >maxInflight
		// commands whose completion never arrived), evict the oldest so the map
		// stays bounded instead of growing while started() rescans it every call.
		for len(ct.inflight) >= maxInflight {
			ct.evictOldestLocked()
		}
	}
	ct.inflight[e.RequestID] = inflightSpan{span: span, start: time.Now()}
	ct.mu.Unlock()
}

// finished closes (and removes) the in-flight span for reqID, recording an error
// status when cmdErr != nil. A no-op when the span is unknown (e.g. started while
// tracing was disabled, or a duplicate completion event).
func (ct *commandTracer) finished(reqID int64, cmdErr error) {
	ct.mu.Lock()
	e, ok := ct.inflight[reqID]
	delete(ct.inflight, reqID)
	ct.mu.Unlock()
	if !ok {
		return
	}
	if cmdErr != nil {
		// Mongo server errors can echo document values (duplicate-key values,
		// validation failures, etc.). The command name is already a safe span
		// attribute; never copy the raw server message into telemetry.
		e.span.SetStatus(tracing.StatusError, mongoCommandFailureStatus(cmdErr))
	} else {
		e.span.SetStatus(tracing.StatusOK, "")
	}
	e.span.End()
}

func mongoCommandFailureStatus(err error) string {
	if err == nil {
		return ""
	}
	return "mongodb command failed"
}

// evictOldestLocked ends and removes the single oldest in-flight span, to enforce
// the hard size cap when the stale sweep can't free room. The caller must hold
// ct.mu. A no-op on an empty map.
func (ct *commandTracer) evictOldestLocked() {
	var oldestID int64
	var oldest time.Time
	found := false
	for id, e := range ct.inflight {
		if !found || e.start.Before(oldest) {
			oldestID, oldest, found = id, e.start, true
		}
	}
	if !found {
		return
	}
	e := ct.inflight[oldestID]
	e.span.SetStatus(tracing.StatusError, "command span evicted: in-flight cap exceeded")
	e.span.End()
	delete(ct.inflight, oldestID)
}

// sweepStaleLocked ends and removes in-flight spans older than maxInflightAge —
// orphans whose completion event never arrived. The caller must hold ct.mu.
func (ct *commandTracer) sweepStaleLocked() {
	cutoff := time.Now().Add(-maxInflightAge)
	for id, e := range ct.inflight {
		if e.start.Before(cutoff) {
			e.span.SetStatus(tracing.StatusError, "command span abandoned: no completion event")
			e.span.End()
			delete(ct.inflight, id)
		}
	}
}
