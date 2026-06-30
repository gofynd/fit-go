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

// Kafka tracing wrappers for consumer and producer.
//
// TracedMessageHandler wraps a MessageHandler with an OpenTelemetry consumer
// span per message, extracting the W3C traceparent from Kafka headers.
//
// InjectTraceHeaders adds a traceparent header to outbound Kafka messages so
// downstream consumers can continue the trace.
package kafka

import (
	"context"
	"fmt"

	"github.com/gofynd/fit-go/tracing"
)

const traceparentHeaderKey = "traceparent"

// startConsumerSpan opens a Consumer span for a consumed message, parented to the
// trace carried in the message's traceparent header (if any). It returns the
// span-bearing context and the span, or (context.Background(), nil) when tracing
// is disabled — letting callers fast-path the passthrough.
func startConsumerSpan(msg MessagePayload) (context.Context, *tracing.Span) {
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return context.Background(), nil
	}

	ctx := context.Background()
	for _, h := range msg.Headers {
		if h.Key == traceparentHeaderKey {
			traceID, spanID, sampled := tracing.ExtractTraceContext(string(h.Value))
			if traceID != "" {
				ctx = tracing.ContextWithTrace(ctx, traceID, spanID, sampled)
			}
			break
		}
	}

	ctx, span := tracer.StartSpan(ctx, fmt.Sprintf("kafka.consume %s", msg.Topic), tracing.SpanKindConsumer)
	span.SetAttributes(map[string]any{
		"messaging.system":               "kafka",
		"messaging.destination":          msg.Topic,
		"messaging.kafka.partition":      msg.Partition,
		"messaging.kafka.message.offset": msg.Offset,
	})
	return ctx, span
}

// recordSpanResult sets OK/Error status on a consumer span from the handler error.
func recordSpanResult(span *tracing.Span, err error) {
	if err != nil {
		span.SetStatus(tracing.StatusError, err.Error())
	} else {
		span.SetStatus(tracing.StatusOK, "")
	}
}

// TracedMessageHandler wraps a MessageHandler so that each consumed message
// is processed inside an OpenTelemetry span with kind=Consumer.
// When tracing is disabled the wrapper is a transparent passthrough.
func TracedMessageHandler(handler MessageHandler) MessageHandler {
	return func(msg MessagePayload) error {
		_, span := startConsumerSpan(msg)
		if span == nil {
			return handler(msg)
		}
		defer span.End()
		err := handler(msg)
		recordSpanResult(span, err)
		return err
	}
}

// TracedMessageHandlerCtx adapts a MessageHandlerCtx to a MessageHandler, opening
// the consumer span and threading its context into the handler so consumer-side
// logs and nested spans join the producer's trace. When tracing is disabled it
// passes context.Background() (transparent passthrough). This is what ConsumeCtx
// installs.
func TracedMessageHandlerCtx(handler MessageHandlerCtx) MessageHandler {
	return func(msg MessagePayload) error {
		ctx, span := startConsumerSpan(msg)
		if span == nil {
			return handler(context.Background(), msg)
		}
		defer span.End()
		err := handler(ctx, msg)
		recordSpanResult(span, err)
		return err
	}
}

// InjectTraceHeaders sets the traceparent header on the message to the current
// span from ctx, REPLACING any existing traceparent rather than appending a
// second one. The consumer side reads the first traceparent it finds, so a
// stale header on a forwarded/re-produced message must be overwritten with this
// span's context — otherwise the new producer span would be ignored. If tracing
// is disabled or ctx has no active span, the message is left unmodified.
func InjectTraceHeaders(ctx context.Context, msg *Message) {
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return
	}

	span := tracing.SpanFromContext(ctx)
	if span == nil {
		return
	}

	traceID := span.TraceID()
	spanID := span.SpanID()
	if traceID == "" || spanID == "" {
		return
	}

	tp := tracing.FormatTraceparent(traceID, spanID, span.IsSampled())
	for i := range msg.Headers {
		if msg.Headers[i].Key == traceparentHeaderKey {
			msg.Headers[i].Value = []byte(tp)
			return
		}
	}
	msg.Headers = append(msg.Headers, Header{
		Key:   traceparentHeaderKey,
		Value: []byte(tp),
	})
}

// InjectTraceHeadersToMessages adds traceparent headers to all messages in the
// slice. Convenience wrapper for batch produce calls.
func InjectTraceHeadersToMessages(ctx context.Context, messages []Message) {
	for i := range messages {
		InjectTraceHeaders(ctx, &messages[i])
	}
}
