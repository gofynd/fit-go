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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofynd/fit-go/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func enabledKafkaTracer(t *testing.T) (*tracing.Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_PROPAGATORS", "tracecontext,baggage")

	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName:            "kafka-parity-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SampleRate:             1,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	require.NoError(t, err)
	restoreGlobal := tracing.SetGlobal(tracer)
	t.Cleanup(func() {
		restoreGlobal()
		require.NoError(t, tracer.Shutdown(context.Background()))
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return tracer, exporter
}

func disabledKafkaTracer(t *testing.T) {
	t.Helper()
	disabled := false
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName: "kafka-disabled-test",
		Enabled:     &disabled,
	})
	require.NoError(t, err)
	t.Cleanup(tracing.SetGlobal(tracer))
}

func propagationHeaderValues(headers []Header, key string) []string {
	var values []string
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			values = append(values, string(header.Value))
		}
	}
	return values
}

func exportedSpansNamed(exporter *tracetest.InMemoryExporter, prefix string) tracetest.SpanStubs {
	var spans tracetest.SpanStubs
	for _, span := range exporter.GetSpans() {
		if strings.HasPrefix(span.Name, prefix) {
			spans = append(spans, span)
		}
	}
	return spans
}

func spanStringAttribute(span tracetest.SpanStub, key string) string {
	for _, attr := range span.Attributes {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	return ""
}

func spanHasAttribute(span tracetest.SpanStub, key string) bool {
	for _, attr := range span.Attributes {
		if string(attr.Key) == key {
			return true
		}
	}
	return false
}

func spanByID(spans tracetest.SpanStubs, spanID string) (tracetest.SpanStub, bool) {
	for _, span := range spans {
		if span.SpanContext.SpanID().String() == spanID {
			return span, true
		}
	}
	return tracetest.SpanStub{}, false
}

func TestConfluentProducerAPIsAdoptContextWithoutDuplicateSpans(t *testing.T) {
	tracer, exporter := enabledKafkaTracer(t)
	ctx, parent := tracer.StartSpan(context.Background(), "active-handler", tracing.SpanKindServer)
	cleanup := tracing.InjectContextIntoGoroutine(ctx)

	var produced []*ckafka.Message
	driver := &fakeConfluentProducerDriver{produceFn: func(message *ckafka.Message, reports chan ckafka.Event) error {
		produced = append(produced, message)
		reports <- successfulDelivery(message, int64(len(produced)))
		return nil
	}}
	producer := newTestConfluentProducer(driver)
	require.NoError(t, producer.Produce("orders", []Message{{Value: []byte("one")}}, -1))
	require.NoError(t, producer.ProduceBatch([]TopicMessages{
		{Topic: "orders", Messages: []Message{{Value: []byte("two")}}},
		{Topic: "refunds", Messages: []Message{{Value: []byte("three")}}},
	}, -1))
	require.NoError(t, producer.ProduceCtx(ctx, "payments", []Message{{Value: []byte("four")}}, -1))
	cleanup()
	parent.End()

	spans := exportedSpansNamed(exporter, "send ")
	require.Len(t, spans, 4, "raw and context-aware records must each produce exactly one span")
	for _, span := range spans {
		require.Equal(t, parent.SpanID(), span.Parent.SpanID().String())
	}
	require.Len(t, produced, 4)
	for _, message := range produced {
		var traceparents int
		for _, header := range message.Headers {
			if strings.EqualFold(header.Key, "traceparent") {
				traceparents++
			}
		}
		require.Equal(t, 1, traceparents)
	}
}

func TestConfluentRawConsumerAPIsInstallAutomaticTracingContext(t *testing.T) {
	_, exporter := enabledKafkaTracer(t)
	stopErr := errors.New("stop")
	topic := "orders"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 4}}

	messageReads := 0
	messageDriver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		messageReads++
		if messageReads == 1 {
			return message, nil
		}
		return nil, stopErr
	}}
	messageConsumer := newTestConfluentConsumer(false, messageDriver)
	messageCalled := false
	err := messageConsumer.Consume(func(MessagePayload) error {
		messageCalled = true
		active := tracing.ContextFromGoroutine()
		require.NotNil(t, active)
		require.True(t, oteltrace.SpanContextFromContext(active).IsValid())
		return nil
	}, ConsumerOptions{})
	require.ErrorIs(t, err, stopErr)
	require.True(t, messageCalled)

	batchReads := 0
	batchDriver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		batchReads++
		switch batchReads {
		case 1:
			return message, nil
		case 2:
			return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
		default:
			return nil, stopErr
		}
	}}
	batchConsumer := newTestConfluentConsumer(false, batchDriver)
	batchCalled := false
	err = batchConsumer.ConsumeBatch(func(BatchPayload) error {
		batchCalled = true
		active := tracing.ContextFromGoroutine()
		require.NotNil(t, active)
		require.True(t, oteltrace.SpanContextFromContext(active).IsValid())
		return nil
	}, ConsumerOptions{})
	require.ErrorIs(t, err, stopErr)
	require.True(t, batchCalled)

	contextReads := 0
	contextDriver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		contextReads++
		if contextReads == 1 {
			return message, nil
		}
		return nil, stopErr
	}}
	contextConsumer := newTestConfluentConsumer(false, contextDriver)
	err = contextConsumer.ConsumeCtx(func(ctx context.Context, _ MessagePayload) error {
		require.True(t, oteltrace.SpanContextFromContext(ctx).IsValid())
		return nil
	}, ConsumerOptions{})
	require.ErrorIs(t, err, stopErr)

	require.Len(t, exportedSpansNamed(exporter, "process "), 3, "raw and context-aware handlers must each create one process span")
	require.Len(t, exportedSpansNamed(exporter, "poll "), 1)
}

func TestProduceTopicMessagesWithTrace_PerMessageSpansAndBrokerSemantics(t *testing.T) {
	tracer, exporter := enabledKafkaTracer(t)
	ctx, parent := tracer.StartSpan(context.Background(), "caller", tracing.SpanKindServer)

	timestamp := time.Date(2026, 7, 12, 10, 11, 12, 13, time.UTC)
	topicMessages := []TopicMessages{
		{
			Topic: "orders.created",
			Messages: []Message{
				{
					Key:       []byte("order-1"),
					Value:     []byte(`{"id":1}`),
					Headers:   []Header{{Key: "x-contract", Value: []byte("one")}},
					Partition: 2,
					Timestamp: timestamp,
				},
				{Key: []byte("order-2"), Value: []byte(`{"id":2}`), Partition: -1},
			},
		},
		{
			Topic: "refunds.created",
			Messages: []Message{
				{Key: []byte("refund-1"), Value: []byte(`{"id":3}`), Partition: 4},
			},
		},
	}
	deliveryErr := errors.New("broker delivery failed for user@example.com password=hunter2")
	called := false
	err := produceTopicMessagesWithTrace(ctx, topicMessages, -1, func(got []TopicMessages, acks int) error {
		called = true
		require.Equal(t, -1, acks, "acks must reach the broker unchanged")
		require.Equal(t, []byte("order-1"), got[0].Messages[0].Key)
		require.Equal(t, []byte(`{"id":1}`), got[0].Messages[0].Value)
		require.Equal(t, 2, got[0].Messages[0].Partition)
		require.Equal(t, timestamp, got[0].Messages[0].Timestamp)
		require.Equal(t, "one", string(got[0].Messages[0].Headers[0].Value))
		require.Equal(t, []byte("refund-1"), got[1].Messages[0].Key)
		require.Equal(t, []byte(`{"id":3}`), got[1].Messages[0].Value)
		return deliveryErr
	})
	require.True(t, called)
	require.ErrorIs(t, err, deliveryErr, "delivery errors must pass through unchanged")
	parent.End()

	producerSpans := exportedSpansNamed(exporter, "send ")
	require.Len(t, producerSpans, 3, "KafkaJS creates one producer span per message")
	topicCounts := map[string]int{}
	for _, span := range producerSpans {
		require.Equal(t, parent.SpanID(), span.Parent.SpanID().String(), "producer spans must be siblings under the caller")
		topicCounts[spanStringAttribute(span, "messaging.destination")]++
		require.False(t, spanHasAttribute(span, "messaging.kafka.message_key"), "Kafka keys may contain PII")
		require.Equal(t, codes.Error, span.Status.Code)
		require.Contains(t, span.Status.Description, "broker delivery failed")
		require.NotContains(t, fmt.Sprint(span), "user@example.com")
		require.NotContains(t, fmt.Sprint(span), "hunter2")
	}
	require.Equal(t, map[string]int{"orders.created": 2, "refunds.created": 1}, topicCounts)

	for topicIndex := range topicMessages {
		for messageIndex := range topicMessages[topicIndex].Messages {
			tp := propagationHeaderValues(topicMessages[topicIndex].Messages[messageIndex].Headers, "traceparent")
			require.Len(t, tp, 1)
			_, spanID, sampled := tracing.ExtractTraceContext(tp[0])
			require.True(t, sampled)
			span, found := spanByID(producerSpans, spanID)
			require.True(t, found, "message header must come from its own producer span")
			require.Equal(t, topicMessages[topicIndex].Topic, spanStringAttribute(span, "messaging.destination"))
		}
	}
}

func TestProducerInjection_ReplacesCaseInsensitiveStalePropagationFields(t *testing.T) {
	tracer, _ := enabledKafkaTracer(t)

	state, err := oteltrace.ParseTraceState("vendor=state")
	require.NoError(t, err)
	traceID, err := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	spanID, err := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)
	parent := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: oteltrace.FlagsSampled,
		TraceState: state,
		Remote:     true,
	})
	ctx := oteltrace.ContextWithRemoteSpanContext(context.Background(), parent)
	member, err := baggage.NewMember("tenant", "acme")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)
	ctx = baggage.ContextWithBaggage(ctx, bag)
	ctx, caller := tracer.StartSpan(ctx, "caller", tracing.SpanKindServer)

	msg := Message{
		Value: []byte("payload"),
		Headers: []Header{
			{Key: "x-first", Value: []byte("keep-1")},
			{Key: "TraceParent", Value: []byte("stale-1")},
			{Key: "traceparent", Value: []byte("stale-2")},
			{Key: "TRACESTATE", Value: []byte("stale-state")},
			{Key: "Baggage", Value: []byte("stale=one")},
			{Key: "baggage", Value: []byte("stale=two")},
			{Key: "x-second", Value: []byte("keep-2")},
		},
	}
	traced := []TopicMessages{{Topic: "orders", Messages: []Message{msg}}}
	spans := startProducerMessageSpans(ctx, traced)
	endProducerMessageSpans(spans, nil)
	caller.End()
	got := traced[0].Messages[0]

	require.Len(t, propagationHeaderValues(got.Headers, "traceparent"), 1)
	require.Len(t, propagationHeaderValues(got.Headers, "tracestate"), 1)
	require.Len(t, propagationHeaderValues(got.Headers, "baggage"), 1)
	require.Equal(t, "vendor=state", propagationHeaderValues(got.Headers, "tracestate")[0])
	require.Equal(t, "tenant=acme", propagationHeaderValues(got.Headers, "baggage")[0])
	require.NotContains(t, propagationHeaderValues(got.Headers, "traceparent")[0], "stale")
	require.Equal(t, "x-first", got.Headers[0].Key)
	require.Equal(t, []byte("keep-1"), got.Headers[0].Value)
	require.Equal(t, "x-second", got.Headers[1].Key)
	require.Equal(t, []byte("keep-2"), got.Headers[1].Value)
}

func TestProducerInjection_RemovesStaleFieldsAbsentFromNewContext(t *testing.T) {
	tracer, _ := enabledKafkaTracer(t)
	ctx, caller := tracer.StartSpan(context.Background(), "caller", tracing.SpanKindServer)
	msg := Message{
		Value: []byte("payload"),
		Headers: []Header{
			{Key: "BAGGAGE", Value: []byte("stale=yes")},
			{Key: "TraceState", Value: []byte("stale=state")},
			{Key: "b3", Value: []byte("stale-b3")},
			{Key: "X-B3-TraceId", Value: []byte("stale-trace")},
			{Key: "X-B3-SpanId", Value: []byte("stale-span")},
			{Key: "uber-trace-id", Value: []byte("stale-jaeger")},
			{Key: "Uberctx-Tenant", Value: []byte("stale-baggage")},
			{Key: "x-keep", Value: []byte("unchanged")},
		},
	}
	topicMessages := []TopicMessages{{Topic: "orders", Messages: []Message{msg}}}
	spans := startProducerMessageSpans(ctx, topicMessages)
	endProducerMessageSpans(spans, nil)
	caller.End()

	got := topicMessages[0].Messages[0]
	require.Empty(t, propagationHeaderValues(got.Headers, "baggage"))
	require.Empty(t, propagationHeaderValues(got.Headers, "tracestate"))
	require.Empty(t, propagationHeaderValues(got.Headers, "b3"))
	require.Empty(t, propagationHeaderValues(got.Headers, "x-b3-traceid"))
	require.Empty(t, propagationHeaderValues(got.Headers, "x-b3-spanid"))
	require.Empty(t, propagationHeaderValues(got.Headers, "uber-trace-id"))
	require.Empty(t, propagationHeaderValues(got.Headers, "uberctx-tenant"))
	require.Len(t, propagationHeaderValues(got.Headers, "traceparent"), 1)
	require.Equal(t, "x-keep", got.Headers[0].Key)
	require.Equal(t, []byte("unchanged"), got.Headers[0].Value)
}

func TestProduceTopicMessagesWithTrace_DisabledIsExactPassthrough(t *testing.T) {
	disabledKafkaTracer(t)
	timestamp := time.Date(2026, 7, 12, 1, 2, 3, 4, time.UTC)
	topicMessages := []TopicMessages{{
		Topic: "orders",
		Messages: []Message{{
			Key:       []byte("key"),
			Value:     []byte("value"),
			Headers:   []Header{{Key: "TraceParent", Value: []byte("caller-owned")}},
			Partition: 3,
			Timestamp: timestamp,
		}},
	}}
	want := []TopicMessages{{
		Topic: "orders",
		Messages: []Message{{
			Key:       []byte("key"),
			Value:     []byte("value"),
			Headers:   []Header{{Key: "TraceParent", Value: []byte("caller-owned")}},
			Partition: 3,
			Timestamp: timestamp,
		}},
	}}
	wantErr := errors.New("delivery")
	err := produceTopicMessagesWithTrace(context.Background(), topicMessages, 0, func(got []TopicMessages, acks int) error {
		require.Equal(t, 0, acks)
		require.Equal(t, want, got)
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, want, topicMessages)
}

func TestTracedMessageHandlerCtx_ContinuesTraceStateAndBaggage(t *testing.T) {
	enabledKafkaTracer(t)
	payload := MessagePayload{
		Topic: "orders",
		Value: []byte("unchanged"),
		Headers: []Header{
			{Key: "TraceParent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
			{Key: "TraceState", Value: []byte("vendor=state")},
			{Key: "Baggage", Value: []byte("tenant=acme")},
		},
	}

	called := false
	handler := TracedMessageHandlerCtx(func(ctx context.Context, got MessagePayload) error {
		called = true
		require.Equal(t, payload, got)
		require.Equal(t, "acme", baggage.FromContext(ctx).Member("tenant").Value())
		sc := oteltrace.SpanContextFromContext(ctx)
		require.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", sc.TraceID().String())
		require.Equal(t, "state", sc.TraceState().Get("vendor"))
		return nil
	})
	require.NoError(t, handler(payload))
	require.True(t, called)
}

func TestTracedBatchHandlerCtx_UsesReceiveSpanAndPerMessageLinks(t *testing.T) {
	_, exporter := enabledKafkaTracer(t)
	wantErr := errors.New("batch failed")
	batch := BatchPayload{
		Topic:     "orders",
		Partition: 1,
		Messages: []MessagePayload{
			{
				Topic: "orders", Partition: 1, Offset: 10, Key: []byte("customer-123"),
				Headers: []Header{
					{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
					{Key: "tracestate", Value: []byte("vendor=one")},
				},
			},
			{
				Topic: "orders", Partition: 1, Offset: 11, Key: []byte("customer-456"),
				Headers: []Header{
					{Key: "TraceParent", Value: []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")},
					{Key: "TraceState", Value: []byte("vendor=two")},
				},
			},
		},
		FirstOffset: 10,
		LastOffset:  11,
	}

	var receiveSpanID string
	handler := TracedBatchHandlerCtx(func(ctx context.Context, got BatchPayload) error {
		require.Equal(t, batch, got)
		sc := oteltrace.SpanContextFromContext(ctx)
		require.True(t, sc.IsValid())
		receiveSpanID = sc.SpanID().String()
		return wantErr
	})
	require.ErrorIs(t, handler(batch), wantErr)

	receiveSpans := exportedSpansNamed(exporter, "poll ")
	require.Len(t, receiveSpans, 1)
	require.Equal(t, oteltrace.SpanKindClient, receiveSpans[0].SpanKind, "KafkaJS eachBatch models poll as a client span")
	require.False(t, receiveSpans[0].Parent.IsValid(), "batch receive span must not pick the first message as parent")
	require.Equal(t, codes.Error, receiveSpans[0].Status.Code)
	processSpans := exportedSpansNamed(exporter, "process ")
	require.Len(t, processSpans, 2)
	wantTraceIDs := map[string]string{
		"4bf92f3577b34da6a3ce929d0e0e4736": "one",
		"0af7651916cd43dd8448eb211c80319c": "two",
	}
	for _, span := range processSpans {
		require.Equal(t, oteltrace.SpanKindConsumer, span.SpanKind)
		require.False(t, spanHasAttribute(span, "messaging.kafka.message_key"), "Kafka keys may contain PII")
		require.Equal(t, receiveSpanID, span.Parent.SpanID().String())
		require.Len(t, span.Links, 1)
		linked := span.Links[0].SpanContext
		require.Equal(t, wantTraceIDs[linked.TraceID().String()], linked.TraceState().Get("vendor"))
		require.Equal(t, codes.Error, span.Status.Code)
	}
}

type batchOnlyCompatibilityConsumer struct {
	payload BatchPayload
}

func (*batchOnlyCompatibilityConsumer) Connect([]TopicConfig) error { return nil }
func (*batchOnlyCompatibilityConsumer) Consume(MessageHandler, ConsumerOptions) error {
	return nil
}
func (*batchOnlyCompatibilityConsumer) ConsumeCtx(MessageHandlerCtx, ConsumerOptions) error {
	return nil
}
func (c *batchOnlyCompatibilityConsumer) ConsumeBatch(handler BatchHandler, _ ConsumerOptions) error {
	return handler(c.payload)
}
func (*batchOnlyCompatibilityConsumer) Close() error { return nil }

func TestConsumeBatchCtx_AdaptsExistingConsumerWithoutInterfaceBreak(t *testing.T) {
	disabledKafkaTracer(t)
	payload := BatchPayload{
		Topic: "orders",
		Messages: []MessagePayload{{
			Topic: "orders",
			Value: []byte("unchanged"),
		}},
	}
	consumer := &batchOnlyCompatibilityConsumer{payload: payload}
	var got BatchPayload
	err := ConsumeBatchCtx(consumer, func(ctx context.Context, batch BatchPayload) error {
		require.NotNil(t, ctx)
		require.False(t, oteltrace.SpanContextFromContext(ctx).IsValid())
		got = batch
		return nil
	}, ConsumerOptions{})
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
