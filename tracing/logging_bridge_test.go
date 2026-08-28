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
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gofynd/fit-go/logging"
)

// StartSpan must bridge the span's trace/span IDs into the logging package so that
// logger.WithContext(ctx) auto-stamps trace_id/span_id — the Go equivalent of
// Node's OTel log enrichment. StartSpan always produces IDs (independent of the
// enabled/exporter state), so this is deterministic.
func TestStartSpan_BridgesTraceIDsToLogger(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.New(logging.Options{Env: "production", Level: "info", Output: &buf})
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}

	ctx, span := Global().StartSpan(context.Background(), "op", SpanKindInternal)
	if span.TraceID() == "" || span.SpanID() == "" {
		t.Fatalf("span must have IDs; got trace=%q span=%q", span.TraceID(), span.SpanID())
	}

	logger.WithContext(ctx).Info("hello")

	// Parse the last JSON line emitted.
	line := strings.TrimSpace(buf.String())
	if i := strings.LastIndexByte(line, '\n'); i >= 0 {
		line = line[i+1:]
	}
	var e struct {
		TraceID string `json:"trace_id"`
		SpanID  string `json:"span_id"`
	}
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		t.Fatalf("parse log line %q: %v", line, err)
	}

	if e.TraceID != span.TraceID() {
		t.Fatalf("log trace_id = %q, want %q", e.TraceID, span.TraceID())
	}
	if e.SpanID != span.SpanID() {
		t.Fatalf("log span_id = %q, want %q", e.SpanID, span.SpanID())
	}
}

func TestDecoratorsInstallChildAsImplicitLogContextAndRestoreParent(t *testing.T) {
	enabled := true
	tracer, err := New(context.Background(), Options{
		ServiceName:            "decorator-logging",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           tracetest.NewInMemoryExporter(),
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	restoreGlobal := SetGlobal(tracer)
	t.Cleanup(func() {
		restoreGlobal()
		_ = tracer.Shutdown(context.Background())
	})

	var buf bytes.Buffer
	logger, err := logging.New(logging.Options{Env: "production", Level: "info", Output: &buf})
	if err != nil {
		t.Fatalf("logging.New: %v", err)
	}
	parentCtx, parent := tracer.StartSpan(context.Background(), "parent", SpanKindInternal)
	defer parent.End()
	restoreParent := InjectContextIntoGoroutine(parentCtx)
	defer restoreParent()

	var firstChildID, secondChildID string
	wrapped := Decorators.Trace("decorated", func(ctx context.Context) error {
		firstChildID = SpanIDFromContext(ctx)
		if got := SpanIDFromContext(ContextFromGoroutine()); got != firstChildID {
			t.Fatalf("active decorator span = %q, want %q", got, firstChildID)
		}
		logger.Info("inside trace decorator")
		return nil
	})
	if err := wrapped(parentCtx); err != nil {
		t.Fatalf("Trace wrapper: %v", err)
	}

	withResult := TraceWithResult("decorated-result", func(ctx context.Context) (string, error) {
		secondChildID = SpanIDFromContext(ctx)
		if got := SpanIDFromContext(ContextFromGoroutine()); got != secondChildID {
			t.Fatalf("active result decorator span = %q, want %q", got, secondChildID)
		}
		logger.Info("inside result decorator")
		return "ok", nil
	})
	if result, err := withResult(parentCtx); err != nil || result != "ok" {
		t.Fatalf("TraceWithResult = (%q, %v)", result, err)
	}

	if got := SpanIDFromContext(ContextFromGoroutine()); got != parent.SpanID() {
		t.Fatalf("active context after decorators = %q, want parent %q", got, parent.SpanID())
	}
	logger.Info("after decorators")

	var records []struct {
		Message string `json:"message"`
		SpanID  string `json:"span_id"`
	}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var record struct {
			Message string `json:"message"`
			SpanID  string `json:"span_id"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log %q: %v", line, err)
		}
		records = append(records, record)
	}
	if len(records) != 3 {
		t.Fatalf("log records = %d, want 3", len(records))
	}
	for index, want := range []string{firstChildID, secondChildID, parent.SpanID()} {
		if records[index].SpanID != want {
			t.Errorf("record %d span_id = %q, want %q", index, records[index].SpanID, want)
		}
	}
	if firstChildID == parent.SpanID() || secondChildID == parent.SpanID() {
		t.Fatal("decorator did not create child spans")
	}

	panicking := Decorators.Trace("decorated-panic", func(context.Context) error {
		panic("boom")
	})
	func() {
		defer func() { _ = recover() }()
		_ = panicking(parentCtx)
	}()
	if got := SpanIDFromContext(ContextFromGoroutine()); got != parent.SpanID() {
		t.Fatalf("active context after panic = %q, want parent %q", got, parent.SpanID())
	}
}
