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

package kafka

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// TestInjectTraceHeaders_FromNativeHTTPSpan protects HTTP-to-Kafka trace
// continuity when the server span exists only in the native OTel context.
func TestInjectTraceHeaders_FromNativeHTTPSpan(t *testing.T) {
	tracingtest.EnabledGlobal(t)

	// Simulate the otelgin server span: native OTel only, no fit-go private key.
	ctx, serverSpan := otel.Tracer("otelgin").Start(context.Background(), "POST /engine/send-async")
	defer serverSpan.End()

	msg := &Message{Value: []byte(`{"payload":{}}`)}
	InjectTraceHeaders(ctx, msg)

	var tp string
	for _, h := range msg.Headers {
		if h.Key == "traceparent" {
			tp = string(h.Value)
		}
	}
	if tp == "" {
		t.Fatal("no traceparent injected from an HTTP handler context — the trace is severed " +
			"at the HTTP→Kafka boundary and the consumer will start a new trace")
	}

	// The header must carry the SERVER span's trace id, so the consumer continues
	// the request's trace rather than starting its own.
	wantTraceID := serverSpan.SpanContext().TraceID().String()
	if !strings.Contains(tp, wantTraceID) {
		t.Errorf("traceparent = %q, want it to carry the HTTP server span's trace id %s", tp, wantTraceID)
	}

	// And the consumer must actually continue that same trace.
	gotTraceID, _, _ := tracing.ExtractTraceContext(tp)
	if gotTraceID != wantTraceID {
		t.Errorf("consumer would continue trace %s, want %s (one trace, not two)", gotTraceID, wantTraceID)
	}
}
