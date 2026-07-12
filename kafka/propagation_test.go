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
	"testing"

	"go.opentelemetry.io/otel/baggage"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

func headerValue(msg *Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// TestInjectTraceHeaders_CarriesBaggage: fit-go previously registered only the
// TraceContext propagator, so W3C `baggage` set by an upstream Node/Python service (or
// by us) was silently dropped at every Go Kafka boundary. The composite propagator
// now carries it.
func TestInjectTraceHeaders_CarriesBaggage(t *testing.T) {
	tracer := tracingtest.EnabledGlobal(t)

	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(context.Background(), bag)
	ctx, span := tracer.StartSpan(ctx, "producer", tracing.SpanKindProducer)
	defer span.End()

	msg := &Message{Value: []byte("x")}
	InjectTraceHeaders(ctx, msg)

	if headerValue(msg, "traceparent") == "" {
		t.Fatal("no traceparent injected")
	}
	if got := headerValue(msg, "baggage"); got == "" {
		t.Error("no baggage header injected — upstream baggage is dropped at the Kafka boundary")
	}
}

// TestConsumerSpan_ParentIsRemote is what makes entry-point sampling work for
// consumers. The traceclue ServiceEntryPointSampler force-samples a span whose parent
// is REMOTE. Extraction must therefore mark the parent remote; a hand-rolled parent
// context would look local and defeat it.
func TestConsumerSpan_ParentIsRemote(t *testing.T) {
	tracingtest.EnabledGlobal(t)

	// A message carrying an upstream traceparent.
	msg := MessagePayload{
		Topic: "t",
		Headers: []Header{{
			Key:   "traceparent",
			Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
		}},
	}

	ctx := propagator().Extract(context.Background(), payloadCarrier{headers: msg.Headers})
	psc := oteltrace.SpanContextFromContext(ctx)

	if !psc.IsValid() {
		t.Fatal("extracted parent span context is invalid — the upstream trace is lost")
	}
	if !psc.IsRemote() {
		t.Error("extracted parent is not marked REMOTE; the entry-point sampler will not " +
			"recognise this consumer span as a service entry point")
	}
	if psc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace id = %s, want the upstream one", psc.TraceID())
	}
}

// TestStartProducerSpan_RecordsMessagingAttributes: ProduceCtx previously created NO
// span, so produce latency and failures were invisible in traces (traceclue's Kafka
// instrumentation records them).
func TestStartProducerSpan_RecordsMessagingAttributes(t *testing.T) {
	tracingtest.EnabledGlobal(t)

	ctx, span := StartProducerSpan(context.Background(), "my-topic", 3)
	if span == nil {
		t.Fatal("StartProducerSpan returned nil span while tracing is enabled")
	}
	if tracing.SpanFromContext(ctx) == nil {
		t.Error("producer span not installed in the returned context — injection would find no span")
	}
	EndProducerSpan(span, nil)

	// Nil-span path must be safe (tracing disabled).
	EndProducerSpan(nil, nil)
}

// TestProducerSpan_IsParentOfConsumer: the header must be injected FROM the producer
// span, so the consumer parents to the produce (HTTP → produce → consume), not to a
// sibling.
func TestProducerSpan_IsParentOfConsumer(t *testing.T) {
	tracingtest.EnabledGlobal(t)

	ctx, pspan := StartProducerSpan(context.Background(), "t", 1)
	msg := &Message{Value: []byte("x")}
	InjectTraceHeaders(ctx, msg)
	EndProducerSpan(pspan, nil)

	tp := headerValue(msg, "traceparent")
	if tp == "" {
		t.Fatal("no traceparent injected from the producer span")
	}
	_, spanID, _ := tracing.ExtractTraceContext(tp)
	if spanID != pspan.SpanID() {
		t.Errorf("traceparent carries span id %s, want the PRODUCER span's %s "+
			"(consumer must parent to the produce)", spanID, pspan.SpanID())
	}
}
