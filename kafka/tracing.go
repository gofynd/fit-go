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
// span per message, extracting the configured propagation fields from headers.
//
// InjectTraceHeaders refreshes the configured propagation fields on outbound
// Kafka messages so downstream consumers can continue the trace.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/gofynd/fit-go/redact"
	"github.com/gofynd/fit-go/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const traceparentHeaderKey = "traceparent"

// automaticProducerContext mirrors Node's active-context instrumentation for
// source-compatible raw producer methods. Cancellation is deliberately detached:
// a raw API has no cancellation contract, while its trace parent and baggage must
// still flow from the active FIT request/consumer context.
func automaticProducerContext() context.Context {
	if ctx := tracing.ContextFromGoroutine(); ctx != nil {
		return context.WithoutCancel(ctx)
	}
	return context.Background()
}

// startConsumerSpan opens a Consumer span for a consumed message, parented to the
// trace carried in the message's traceparent header (if any). It returns the
// span-bearing context and the span, or (context.Background(), nil) when tracing
// is disabled — letting callers fast-path the passthrough.
func startConsumerSpan(msg MessagePayload) (context.Context, *tracing.Span) {
	return startConsumerSpanFromContext(context.Background(), msg)
}

// startConsumerSpanFromContext preserves cancellation from base while replacing
// its trace parent with the remote context carried by the Kafka record.
func startConsumerSpanFromContext(base context.Context, msg MessagePayload) (context.Context, *tracing.Span) {
	if base == nil {
		base = context.Background()
	}
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return base, nil
	}

	// Extract via the GLOBAL propagator: this parses traceparent/tracestate/baggage
	// and — crucially — marks the parent span context as REMOTE. The traceclue
	// ServiceEntryPointSampler keys on exactly that to identify this service's entry
	// point, so a hand-rolled parent would defeat entry-point sampling for consumers.
	ctx := propagator().Extract(base, payloadCarrier{headers: msg.Headers})

	ctx, span := tracer.StartSpan(ctx, fmt.Sprintf("process %s", msg.Topic), tracing.SpanKindConsumer)
	span.SetAttributes(map[string]any{
		"messaging.system":                   "kafka",
		"messaging.destination":              msg.Topic,
		"messaging.destination.name":         msg.Topic,
		"messaging.operation.type":           "process",
		"messaging.operation.name":           "process",
		"messaging.kafka.partition":          msg.Partition,
		"messaging.destination.partition.id": fmt.Sprint(msg.Partition),
		"messaging.kafka.message.offset":     msg.Offset,
		"messaging.kafka.offset":             fmt.Sprint(msg.Offset),
	})
	return ctx, span
}

// recordSpanResult sets OK/Error status on a consumer span from the handler error.
func recordSpanResult(span *tracing.Span, err error) {
	if err != nil {
		span.SetStatus(tracing.StatusError, redact.ErrorMessage(err))
	} else {
		span.SetStatus(tracing.StatusOK, "")
	}
}

// TracedMessageHandler wraps a MessageHandler so that each consumed message
// is processed inside an OpenTelemetry span with kind=Consumer.
// When tracing is disabled the wrapper is a transparent passthrough.
func TracedMessageHandler(handler MessageHandler) MessageHandler {
	return func(msg MessagePayload) error {
		return runTracedMessageHandler(context.Background(), msg, func(_ context.Context, payload MessagePayload) error {
			return handler(payload)
		})
	}
}

// TracedMessageHandlerCtx adapts a MessageHandlerCtx to a MessageHandler, opening
// the consumer span and threading its context into the handler so consumer-side
// logs and nested spans join the producer's trace. When tracing is disabled it
// passes context.Background() (transparent passthrough). This is what ConsumeCtx
// installs.
func TracedMessageHandlerCtx(handler MessageHandlerCtx) MessageHandler {
	return func(msg MessagePayload) error {
		return runTracedMessageHandler(context.Background(), msg, handler)
	}
}

// runTracedMessageHandler is the single tracing entry point used by both raw
// and context-aware consumer APIs. Keeping it below the public adapters avoids
// wrapping a handler twice when a driver offers both surfaces.
func runTracedMessageHandler(base context.Context, msg MessagePayload, handler MessageHandlerCtx) error {
	ctx, span := startConsumerSpanFromContext(base, msg)
	cleanup := tracing.InjectContextIntoGoroutine(ctx)
	defer cleanup()
	if span == nil {
		return handler(ctx, msg)
	}
	defer span.End()
	err := handler(ctx, msg)
	recordSpanResult(span, err)
	return err
}

// InjectTraceHeaders injects every field owned by the configured global
// propagator from the current span in ctx. Before injection it removes all stale
// propagation fields case-insensitively, including duplicates and fields absent
// from the new context. Non-propagation headers are left in their original order.
// If tracing is disabled or ctx has no active span, the message is left unmodified.
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
	removePropagationHeaders(msg)
	propagator().Inject(ctx, messageCarrier{msg: msg})
}

// InjectTraceHeadersToMessages refreshes propagation headers on all messages in
// the slice. Convenience wrapper for callers that manage their own producer span.
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
	ctx, span := tracer.StartSpan(ctx, fmt.Sprintf("send %s", topic), tracing.SpanKindProducer)
	span.SetAttributes(map[string]any{
		"messaging.system":           "kafka",
		"messaging.destination":      topic,
		"messaging.destination_kind": "topic",
		"messaging.batch.size":       messageCount,
	})
	return ctx, span
}

// startProducerMessageSpan mirrors the installed KafkaJS instrumentation: every
// message gets its own producer span, even when the broker operation sends several
// messages or several topics in one batch. All message spans are siblings under the
// caller's active context; each message is injected from its own span context.
func startProducerMessageSpan(ctx context.Context, topic string, msg Message) (context.Context, *tracing.Span) {
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return ctx, nil
	}

	ctx, span := tracer.StartSpan(ctx, fmt.Sprintf("send %s", topic), tracing.SpanKindProducer)
	attrs := map[string]any{
		"messaging.system":           "kafka",
		"messaging.destination":      topic,
		"messaging.destination.name": topic,
		"messaging.destination_kind": "topic",
		"messaging.operation":        "send",
		"messaging.operation.type":   "send",
		"messaging.operation.name":   "send",
		"messaging.batch.size":       1,
	}
	if msg.Partition >= 0 {
		attrs["messaging.kafka.partition"] = msg.Partition
		attrs["messaging.destination.partition.id"] = fmt.Sprint(msg.Partition)
	}
	span.SetAttributes(attrs)
	return ctx, span
}

// startProducerMessageSpans starts and injects one span per message. The input
// TopicMessages and Message slices are not rebuilt: values, keys, partitions,
// timestamps and caller headers retain their original representation.
func startProducerMessageSpans(ctx context.Context, topicMessages []TopicMessages) []*tracing.Span {
	if ctx == nil {
		ctx = context.Background()
	}
	count := 0
	for i := range topicMessages {
		count += len(topicMessages[i].Messages)
	}
	spans := make([]*tracing.Span, 0, count)
	for i := range topicMessages {
		for j := range topicMessages[i].Messages {
			spanCtx, span := startProducerMessageSpan(ctx, topicMessages[i].Topic, topicMessages[i].Messages[j])
			if span == nil {
				continue
			}
			InjectTraceHeaders(spanCtx, &topicMessages[i].Messages[j])
			spans = append(spans, span)
		}
	}
	return spans
}

func endProducerMessageSpans(spans []*tracing.Span, err error) {
	for _, span := range spans {
		EndProducerSpan(span, err)
	}
}

// produceTopicMessagesWithTrace wraps a broker operation without changing its
// arguments or return value. Keeping the broker call in a callback makes it
// explicit that tracing may only add propagation headers; acks and delivery
// errors pass through unchanged.
func produceTopicMessagesWithTrace(
	ctx context.Context,
	topicMessages []TopicMessages,
	acks int,
	produce func([]TopicMessages, int) error,
) error {
	spans := startProducerMessageSpans(ctx, topicMessages)
	err := produce(topicMessages, acks)
	endProducerMessageSpans(spans, err)
	return err
}

// EndProducerSpan records the produce outcome and ends the span. Safe on a nil span.
func EndProducerSpan(span *tracing.Span, err error) {
	if span == nil {
		return
	}
	recordSpanResult(span, err)
	span.End()
}

// startBatchConsumerSpans follows KafkaJS eachBatch semantics. A batch receive
// span is the handler's active context; one process span is created per message
// as its child, with the message's extracted producer context recorded as a link.
// Links avoid falsely selecting the first message as the parent of a batch that
// can contain messages from independent traces.
func startBatchConsumerSpans(batch BatchPayload) (context.Context, *tracing.Span, []oteltrace.Span) {
	return startBatchConsumerSpansFromContext(context.Background(), batch)
}

func startBatchConsumerSpansFromContext(base context.Context, batch BatchPayload) (context.Context, *tracing.Span, []oteltrace.Span) {
	if base == nil {
		base = context.Background()
	}
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return base, nil, nil
	}

	ctx, receiveSpan := tracer.StartSpan(
		base,
		fmt.Sprintf("poll %s", batch.Topic),
		// KafkaJS 0.15.0 deliberately models the batch receive/poll span as
		// CLIENT; the individual message processing spans below are CONSUMER.
		tracing.SpanKindClient,
	)
	receiveSpan.SetAttributes(map[string]any{
		"messaging.system":                   "kafka",
		"messaging.destination":              batch.Topic,
		"messaging.destination.name":         batch.Topic,
		"messaging.operation":                "receive",
		"messaging.operation.type":           "receive",
		"messaging.operation.name":           "poll",
		"messaging.kafka.partition":          batch.Partition,
		"messaging.destination.partition.id": fmt.Sprint(batch.Partition),
		"messaging.batch.size":               len(batch.Messages),
		"messaging.batch.message_count":      len(batch.Messages),
	})

	processSpans := make([]oteltrace.Span, 0, len(batch.Messages))
	nativeTracer := otel.Tracer("fit.go/kafka")
	for _, msg := range batch.Messages {
		topic := msg.Topic
		if topic == "" {
			topic = batch.Topic
		}
		attrs := []attribute.KeyValue{
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation", "process"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.operation.name", "process"),
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.String("messaging.destination.partition.id", fmt.Sprint(msg.Partition)),
			attribute.Int64("messaging.kafka.message.offset", msg.Offset),
			attribute.String("messaging.kafka.offset", fmt.Sprint(msg.Offset)),
		}
		opts := []oteltrace.SpanStartOption{
			oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
			oteltrace.WithAttributes(attrs...),
		}
		extracted := propagator().Extract(context.Background(), payloadCarrier{headers: msg.Headers})
		if upstream := oteltrace.SpanContextFromContext(extracted); upstream.IsValid() {
			opts = append(opts, oteltrace.WithLinks(oteltrace.Link{SpanContext: upstream}))
		}
		_, processSpan := nativeTracer.Start(ctx, fmt.Sprintf("process %s", topic), opts...)
		processSpans = append(processSpans, processSpan)
	}
	return ctx, receiveSpan, processSpans
}

func endBatchConsumerSpans(receiveSpan *tracing.Span, processSpans []oteltrace.Span, err error) {
	if receiveSpan == nil {
		return
	}
	if err != nil {
		safeErr := errors.New(redact.ErrorMessage(err))
		receiveSpan.SetStatus(tracing.StatusError, safeErr.Error())
		for _, span := range processSpans {
			span.RecordError(safeErr)
			span.SetStatus(codes.Error, safeErr.Error())
		}
	} else {
		receiveSpan.SetStatus(tracing.StatusOK, "")
		for _, span := range processSpans {
			span.SetStatus(codes.Ok, "")
		}
	}
	for _, span := range processSpans {
		span.End()
	}
	receiveSpan.End()
}

// TracedBatchHandlerCtx adapts a BatchHandlerCtx to the existing BatchHandler
// surface while preserving KafkaJS-compatible batch span/link semantics.
func TracedBatchHandlerCtx(handler BatchHandlerCtx) BatchHandler {
	return func(batch BatchPayload) error {
		return runTracedBatchHandler(context.Background(), batch, handler)
	}
}

func runTracedBatchHandler(base context.Context, batch BatchPayload, handler BatchHandlerCtx) error {
	ctx, receiveSpan, processSpans := startBatchConsumerSpansFromContext(base, batch)
	cleanup := tracing.InjectContextIntoGoroutine(ctx)
	defer cleanup()
	if receiveSpan == nil {
		return handler(ctx, batch)
	}
	err := handler(ctx, batch)
	endBatchConsumerSpans(receiveSpan, processSpans, err)
	return err
}
