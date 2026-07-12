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

	// Extract via the GLOBAL propagator: this parses traceparent/tracestate/baggage
	// and — crucially — marks the parent span context as REMOTE. The traceclue
	// ServiceEntryPointSampler keys on exactly that to identify this service's entry
	// point, so a hand-rolled parent would defeat entry-point sampling for consumers.
	ctx := propagator().Extract(context.Background(), payloadCarrier{headers: msg.Headers})

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
		// Make the consumer span the goroutine-local active context so plain
		// logging.* calls in the handler (and its same-goroutine callees) carry
		// the trace without explicit threading.
		cleanup := tracing.InjectContextIntoGoroutine(ctx)
		defer cleanup()
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
	if msg == nil {
		return
	}
	// Inject via the GLOBAL propagator (W3C TraceContext + Baggage) rather than
	// hand-formatting traceparent: it also carries tracestate and baggage, and the
	// carrier's Set REPLACES a stale header on a re-produced message.
	//
	// The context must carry a span for the propagator to write anything. Adopt the
	// native OTel span when present (otelgin/otelgrpc put a span in the native context
	// only) — tracing.SpanFromContext handles that, and it is why a produce from an
	// HTTP handler now propagates at all.
	if tracing.SpanFromContext(ctx) == nil {
		return
	}
	propagator().Inject(ctx, messageCarrier{msg: msg})
}

// InjectTraceHeadersToMessages adds traceparent headers to all messages in the
// slice. Convenience wrapper for batch produce calls.
func InjectTraceHeadersToMessages(ctx context.Context, messages []Message) {
	for i := range messages {
		InjectTraceHeaders(ctx, &messages[i])
	}
}

// StartProducerSpan opens a SpanKindProducer span for an outbound produce and returns
// the span-bearing context (inject traceparent FROM this context, so the consumer
// parents to the PRODUCER span, not to whatever span happened to be active).
//
// Legacy traceclue's Kafka instrumentation records a producer span with messaging
// attributes and latency; fit-go's ProduceCtx previously only injected a header, so
// produce latency and failures were invisible in traces.
//
// Returns (ctx, nil) when tracing is disabled — callers must nil-check the span.
func StartProducerSpan(ctx context.Context, topic string, messageCount int) (context.Context, *tracing.Span) {
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return ctx, nil
	}
	ctx, span := tracer.StartSpan(ctx, fmt.Sprintf("kafka.produce %s", topic), tracing.SpanKindProducer)
	span.SetAttributes(map[string]any{
		"messaging.system":           "kafka",
		"messaging.destination":      topic,
		"messaging.destination_kind": "topic",
		"messaging.batch.size":       messageCount,
	})
	return ctx, span
}

// EndProducerSpan records the produce outcome and ends the span. Safe on a nil span.
func EndProducerSpan(span *tracing.Span, err error) {
	if span == nil {
		return
	}
	recordSpanResult(span, err)
	span.End()
}
