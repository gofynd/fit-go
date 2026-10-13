// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gofynd/fit-go/tracing"
)

// upstreamInjectTraceHeaders is the reference oracle: upstream main
// (df96a28) kafka.InjectTraceHeaders, given the span main's
// tracing.SpanFromContext would have returned (the private fit-go span only).
func upstreamInjectTraceHeaders(enabled bool, privateSpan *tracing.Span, msg *Message) {
	if !enabled || privateSpan == nil {
		return
	}
	traceID, spanID := privateSpan.TraceID(), privateSpan.SpanID()
	if traceID == "" || spanID == "" {
		return
	}
	msg.Headers = append(msg.Headers, Header{
		Key:   traceparentHeaderKey,
		Value: []byte(tracing.FormatTraceparent(traceID, spanID, true)),
	})
}

func kafkaTracerWithSampler(t *testing.T, sampler string) *tracing.Tracer {
	t.Helper()
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("OTEL_PROPAGATORS", "tracecontext,baggage")
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	enabled := true
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:            "kafka-inject-parity-test",
		Enabled:                &enabled,
		Sampler:                sampler,
		SampleRate:             1,
		SpanExporter:           tracetest.NewInMemoryExporter(),
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
	return tracer
}

func cloneMessageHeaders(headers []Header) []Header {
	if headers == nil {
		return nil
	}
	out := make([]Header, len(headers))
	for i, h := range headers {
		out[i] = Header{Key: h.Key, Value: append([]byte(nil), h.Value...)}
	}
	return out
}

func TestInjectTraceHeadersMatchesUpstreamMain(t *testing.T) {
	tracer := kafkaTracerWithSampler(t, "always_on")
	spanCtx, span := tracer.StartSpan(context.Background(), "caller", tracing.SpanKindServer)
	defer span.End()
	member, err := baggage.NewMember("tenant", "acme")
	require.NoError(t, err)
	bag, err := baggage.New(member)
	require.NoError(t, err)
	spanCtx = baggage.ContextWithBaggage(spanCtx, bag)

	cases := []struct {
		name    string
		headers []Header
	}{
		{name: "no existing headers"},
		{name: "empty non-nil headers", headers: []Header{}},
		{name: "existing app traceparent", headers: []Header{
			{Key: "traceparent", Value: []byte("00-11111111111111111111111111111111-2222222222222222-00")},
			{Key: "x-app", Value: []byte("keep")},
		}},
		{name: "existing tracestate/baggage/b3/uber", headers: []Header{
			{Key: "tracestate", Value: []byte("vendor=state")},
			{Key: "baggage", Value: []byte("k=v")},
			{Key: "b3", Value: []byte("80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1")},
			{Key: "X-B3-TraceId", Value: []byte("80f198ee56343ba864fe8b2a57d3eff7")},
			{Key: "uber-trace-id", Value: []byte("abc:def:0:1")},
			{Key: "uberctx-user", Value: []byte("u")},
			{Key: "TraceParent", Value: []byte("mixed-case")},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Message{Value: []byte("v"), Headers: cloneMessageHeaders(c.headers)}
			want := Message{Value: []byte("v"), Headers: cloneMessageHeaders(c.headers)}
			InjectTraceHeaders(spanCtx, &got)
			upstreamInjectTraceHeaders(true, span, &want)
			require.Equal(t, want, got)
			// Exactly one header appended, sampled flag forced to 01.
			require.Len(t, got.Headers, len(c.headers)+1)
			last := got.Headers[len(got.Headers)-1]
			require.Equal(t, "traceparent", last.Key)
			require.Equal(t, "00-"+span.TraceID()+"-"+span.SpanID()+"-01", string(last.Value))
		})
	}

	t.Run("append aliases spare capacity like main", func(t *testing.T) {
		backing := make([]Header, 1, 4)
		backing[0] = Header{Key: "x-app", Value: []byte("keep")}
		msg := Message{Headers: backing}
		InjectTraceHeaders(spanCtx, &msg)
		require.Len(t, msg.Headers, 2)
		require.Same(t, &backing[0], &msg.Headers[0], "main appended in place without cloning")
		require.Equal(t, "traceparent", backing[:2][1].Key)
	})

	t.Run("InjectTraceHeadersToMessages", func(t *testing.T) {
		got := []Message{{Value: []byte("a")}, {Value: []byte("b"), Headers: []Header{{Key: "traceparent", Value: []byte("app")}}}}
		want := []Message{{Value: []byte("a")}, {Value: []byte("b"), Headers: []Header{{Key: "traceparent", Value: []byte("app")}}}}
		InjectTraceHeadersToMessages(spanCtx, got)
		for i := range want {
			upstreamInjectTraceHeaders(true, span, &want[i])
		}
		require.Equal(t, want, got)
	})

	t.Run("native OTel-only span is not adopted", func(t *testing.T) {
		nativeCtx, nativeSpan := otel.Tracer("otelgin").Start(context.Background(), "GET /x")
		defer nativeSpan.End()
		msg := Message{Headers: []Header{{Key: "x-app", Value: []byte("keep")}}}
		InjectTraceHeaders(nativeCtx, &msg)
		require.Equal(t, []Header{{Key: "x-app", Value: []byte("keep")}}, msg.Headers)
	})

	t.Run("goroutine-local context is not adopted", func(t *testing.T) {
		restore := tracing.InjectContextIntoGoroutine(spanCtx)
		defer restore()
		msg := Message{}
		InjectTraceHeaders(context.Background(), &msg)
		require.Nil(t, msg.Headers)
	})

	t.Run("no span in context", func(t *testing.T) {
		msg := Message{Headers: []Header{{Key: "traceparent", Value: []byte("app")}}}
		InjectTraceHeaders(context.Background(), &msg)
		require.Equal(t, []Header{{Key: "traceparent", Value: []byte("app")}}, msg.Headers)
	})
}

func TestInjectTraceHeadersForcesSampledFlagLikeUpstreamMain(t *testing.T) {
	tracer := kafkaTracerWithSampler(t, "always_off")
	ctx, span := tracer.StartSpan(context.Background(), "unsampled", tracing.SpanKindServer)
	defer span.End()
	require.False(t, span.IsSampled(), "test setup requires an unsampled span")
	msg := Message{}
	InjectTraceHeaders(ctx, &msg)
	want := Message{}
	upstreamInjectTraceHeaders(true, span, &want)
	require.Equal(t, want, msg)
	require.Len(t, msg.Headers, 1)
	require.Equal(t, "00-"+span.TraceID()+"-"+span.SpanID()+"-01", string(msg.Headers[0].Value))
}

func TestInjectTraceHeadersNoOpWhenTracingDisabledLikeUpstreamMain(t *testing.T) {
	tracer := kafkaTracerWithSampler(t, "always_on")
	ctx, span := tracer.StartSpan(context.Background(), "caller", tracing.SpanKindServer)
	defer span.End()
	restore := tracing.SetGlobal(nil)
	defer restore()

	headers := []Header{{Key: "traceparent", Value: []byte("app")}, {Key: "b3", Value: []byte("x")}}
	msg := Message{Headers: cloneMessageHeaders(headers)}
	InjectTraceHeaders(ctx, &msg)
	require.Equal(t, headers, msg.Headers)
	messages := []Message{{Value: []byte("a")}}
	InjectTraceHeadersToMessages(ctx, messages)
	require.Nil(t, messages[0].Headers)
}

// The automatic producer path (ProducerTraceHeadersInject) keeps the newer
// propagator semantics: stale propagation headers are replaced.
func TestProducerInjectPolicyKeepsPropagatorReplacement(t *testing.T) {
	tracer := kafkaTracerWithSampler(t, "always_on")
	ctx, span := tracer.StartSpan(context.Background(), "caller", tracing.SpanKindServer)
	defer span.End()
	topicMessages := []TopicMessages{{Topic: "orders", Messages: []Message{{
		Value: []byte("v"),
		Headers: []Header{
			{Key: "traceparent", Value: []byte("stale")},
			{Key: "uber-trace-id", Value: []byte("stale")},
			{Key: "x-app", Value: []byte("keep")},
		},
	}}}}
	spans := startProducerMessageSpansWithPolicy(ctx, topicMessages, ProducerTraceHeadersInject)
	endProducerMessageSpans(spans, nil)
	got := topicMessages[0].Messages[0].Headers
	require.Len(t, propagationHeaderValues(got, "traceparent"), 1)
	require.NotEqual(t, "stale", propagationHeaderValues(got, "traceparent")[0])
	require.Empty(t, propagationHeaderValues(got, "uber-trace-id"))
	require.Equal(t, "x-app", got[0].Key)
}
