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

	"github.com/stretchr/testify/require"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// When tracing is disabled, TracedMessageHandlerCtx is a transparent passthrough:
// the handler still runs and receives a (background) context.
func TestTracedMessageHandlerCtx_PassthroughWhenDisabled(t *testing.T) {
	called := false
	h := TracedMessageHandlerCtx(func(ctx context.Context, msg MessagePayload) error {
		called = true
		require.NotNil(t, ctx)
		require.Equal(t, "t", msg.Topic)
		return nil
	})
	require.NoError(t, h(MessagePayload{Topic: "t", Value: []byte("x")}))
	require.True(t, called)
}

// End-to-end producer→consumer trace linkage: a span injected on produce
// (ProduceCtx → InjectTraceHeaders) is continued by the consumer span
// (ConsumeCtx → TracedMessageHandlerCtx), so the handler's context carries the
// SAME trace id.
func TestProduceConsume_TraceLinkage(t *testing.T) {
	tracer := tracingtest.EnabledGlobal(t)

	// Producer side: parent span → inject traceparent into the outbound message.
	ctx, parent := tracer.StartSpan(context.Background(), "producer.publish", tracing.SpanKindProducer)
	defer parent.End()
	wantTrace := parent.TraceID()
	require.NotEmpty(t, wantTrace)

	msg := &Message{Value: []byte("payload")}
	InjectTraceHeaders(ctx, msg)

	var tp string
	for _, h := range msg.Headers {
		if h.Key == traceparentHeaderKey {
			tp = string(h.Value)
		}
	}
	require.NotEmpty(t, tp, "producer should inject a traceparent header")
	require.True(t, strings.Contains(tp, wantTrace), "traceparent must carry the producer trace id")

	// Consumer side: feed the produced headers through the ctx handler; the
	// handler's context must carry the same trace id (the link).
	var gotTrace string
	consume := TracedMessageHandlerCtx(func(hctx context.Context, _ MessagePayload) error {
		if s := tracing.SpanFromContext(hctx); s != nil {
			gotTrace = s.TraceID()
		}
		return nil
	})
	err := consume(MessagePayload{
		Topic:   "orders",
		Headers: []Header{{Key: traceparentHeaderKey, Value: []byte(tp)}},
	})
	require.NoError(t, err)
	require.Equal(t, wantTrace, gotTrace, "consumer span must continue the producer's trace")
}

// InjectTraceHeaders must REPLACE a stale traceparent on a forwarded/re-produced
// message (not append a second one), since the consumer reads the first header
// it finds. Otherwise the re-publishing span would be silently dropped.
func TestInjectTraceHeaders_ReplacesExisting(t *testing.T) {
	tracer := tracingtest.EnabledGlobal(t)

	ctx, span := tracer.StartSpan(context.Background(), "producer.republish", tracing.SpanKindProducer)
	defer span.End()
	want := span.TraceID()
	require.NotEmpty(t, want)

	stale := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	msg := &Message{
		Value:   []byte("payload"),
		Headers: []Header{{Key: traceparentHeaderKey, Value: []byte(stale)}},
	}
	InjectTraceHeaders(ctx, msg)

	var tps []string
	for _, h := range msg.Headers {
		if h.Key == traceparentHeaderKey {
			tps = append(tps, string(h.Value))
		}
	}
	require.Len(t, tps, 1, "must keep exactly one traceparent header")
	require.NotEqual(t, stale, tps[0], "stale traceparent must be overwritten")
	require.Contains(t, tps[0], want, "traceparent must carry the new span's trace id")
}
