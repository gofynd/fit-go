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
