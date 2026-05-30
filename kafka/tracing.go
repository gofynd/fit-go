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

// TracedMessageHandler wraps a MessageHandler so that each consumed message
// is processed inside an OpenTelemetry span with kind=Consumer.
// When tracing is disabled the wrapper is a transparent passthrough.
func TracedMessageHandler(handler MessageHandler) MessageHandler {
	return func(msg MessagePayload) error {
		tracer := tracing.Global()
		if tracer == nil || !tracer.IsEnabled() {
			return handler(msg)
		}

		ctx := context.Background()

		// Extract traceparent from Kafka message headers.
		for _, h := range msg.Headers {
			if h.Key == traceparentHeaderKey {
				traceID, spanID, _ := tracing.ExtractTraceContext(string(h.Value))
				if traceID != "" {
					ctx = tracing.ContextWithTrace(ctx, traceID, spanID)
				}
				break
			}
		}

		spanName := fmt.Sprintf("kafka.consume %s", msg.Topic)
		_, span := tracer.StartSpan(ctx, spanName, tracing.SpanKindConsumer)
		defer span.End()

		span.SetAttributes(map[string]any{
			"messaging.system":               "kafka",
			"messaging.destination":          msg.Topic,
			"messaging.kafka.partition":      msg.Partition,
			"messaging.kafka.message.offset": msg.Offset,
		})

		err := handler(msg)
		if err != nil {
			span.SetStatus(tracing.StatusError, err.Error())
		} else {
			span.SetStatus(tracing.StatusOK, "")
		}
		return err
	}
}

// InjectTraceHeaders appends a traceparent header to the message's headers
// using the current span from ctx. If tracing is disabled or ctx has no
// active span, the message is returned unmodified.
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

	tp := tracing.FormatTraceparent(traceID, spanID, true)
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
