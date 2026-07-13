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

package tracing

import (
	"context"

	"github.com/gofynd/fit-go/internal/goroutinectx"
	"github.com/gofynd/fit-go/logging"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

// InjectContextIntoGoroutine stores ctx in goroutine-scoped storage so that
// loggers and other infrastructure can retrieve trace/span IDs without explicit
// context threading. Returns a cleanup function that must be deferred; nested
// calls on the same goroutine compose correctly.
//
//	cleanup := tracing.InjectContextIntoGoroutine(ctx)
//	defer cleanup()
func InjectContextIntoGoroutine(ctx context.Context) func() {
	return goroutinectx.Inject(ctx)
}

// ContextFromGoroutine retrieves the context previously stored by
// InjectContextIntoGoroutine for the current goroutine. Returns nil if none.
func ContextFromGoroutine() context.Context {
	return goroutinectx.Load()
}

// ContextWithActiveGoroutine preserves base's cancellation, deadline, and
// values while filling a missing trace parent from fit-go's goroutine-local
// active context. Explicit trace context on base always wins.
//
// This bridges legacy callbacks that run inside a fit-go boundary but still
// pass context.Background() to an instrumented dependency. It does not detach
// cancellation and does not make context implicitly cross goroutines; callers
// starting detached work must continue to pass an explicit detached context.
func ContextWithActiveGoroutine(base context.Context) context.Context {
	if base == nil {
		base = context.Background()
	}
	if hasTraceIdentity(base) {
		return base
	}

	active := ContextFromGoroutine()
	if active == nil {
		return base
	}

	base = mergeBaggage(base, active)
	activeNative := trace.SpanFromContext(active)
	activeSC := activeNative.SpanContext()
	if activeSC.IsValid() {
		base = trace.ContextWithSpan(base, activeNative)
		base = context.WithValue(base, traceIDKey, activeSC.TraceID().String())
		base = context.WithValue(base, spanIDKey, activeSC.SpanID().String())
		if privateSpan, ok := active.Value(currentSpanKey).(*Span); ok && privateSpan != nil && privateSpan.SpanID() == activeSC.SpanID().String() {
			base = context.WithValue(base, currentSpanKey, privateSpan)
		}
		return logging.ContextWithTrace(base, activeSC.TraceID().String(), activeSC.SpanID().String())
	}

	// Tracing-disabled and compatibility-only contexts can carry fit-go's
	// private span or IDs without a native OTel SpanContext.
	if privateSpan, ok := active.Value(currentSpanKey).(*Span); ok && privateSpan != nil {
		base = context.WithValue(base, currentSpanKey, privateSpan)
	}
	traceID, spanID := TraceIDFromContext(active), SpanIDFromContext(active)
	if traceID == "" && spanID == "" {
		return base
	}
	base = context.WithValue(base, traceIDKey, traceID)
	base = context.WithValue(base, spanIDKey, spanID)
	return logging.ContextWithTrace(base, traceID, spanID)
}

func hasTraceIdentity(ctx context.Context) bool {
	if trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return true
	}
	if privateSpan, ok := ctx.Value(currentSpanKey).(*Span); ok && privateSpan != nil {
		return true
	}
	return TraceIDFromContext(ctx) != "" || SpanIDFromContext(ctx) != ""
}

func mergeBaggage(base, active context.Context) context.Context {
	activeBaggage := baggage.FromContext(active)
	if activeBaggage.Len() == 0 {
		return base
	}
	baseBaggage := baggage.FromContext(base)
	if baseBaggage.Len() == 0 {
		return baggage.ContextWithBaggage(base, activeBaggage)
	}

	members := baseBaggage.Members()
	explicit := make(map[string]struct{}, len(members))
	for _, member := range members {
		explicit[member.Key()] = struct{}{}
	}
	for _, member := range activeBaggage.Members() {
		if _, exists := explicit[member.Key()]; !exists {
			members = append(members, member)
		}
	}
	merged, err := baggage.New(members...)
	if err != nil {
		return base
	}
	return baggage.ContextWithBaggage(base, merged)
}
