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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// messageCarrier adapts a Kafka *Message's headers to the OTel TextMapCarrier so the
// GLOBAL propagator can inject/extract context — instead of hand-rolling traceparent
// parsing. Using the propagator means we transparently carry everything it is
// configured with (W3C traceparent, tracestate, and baggage), and stay correct if the
// platform ever changes the propagator.
type messageCarrier struct{ msg *Message }

// Get returns the first header value with the given key.
func (c messageCarrier) Get(key string) string {
	for i := range c.msg.Headers {
		if c.msg.Headers[i].Key == key {
			return string(c.msg.Headers[i].Value)
		}
	}
	return ""
}

// Set writes a header, REPLACING any existing header with the same key rather than
// appending a second one. This matters for a forwarded/re-produced message: a stale
// traceparent must be overwritten with the current span's, or the consumer (which
// reads the first match) would continue the wrong trace.
func (c messageCarrier) Set(key, value string) {
	for i := range c.msg.Headers {
		if c.msg.Headers[i].Key == key {
			c.msg.Headers[i].Value = []byte(value)
			return
		}
	}
	c.msg.Headers = append(c.msg.Headers, Header{Key: key, Value: []byte(value)})
}

// Keys lists the header keys present on the message.
func (c messageCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for i := range c.msg.Headers {
		keys = append(keys, c.msg.Headers[i].Key)
	}
	return keys
}

// payloadCarrier is the read-only carrier for an inbound MessagePayload (the consumer
// side, whose headers are a plain slice).
type payloadCarrier struct{ headers []Header }

func (c payloadCarrier) Get(key string) string {
	for i := range c.headers {
		if c.headers[i].Key == key {
			return string(c.headers[i].Value)
		}
	}
	return ""
}

// Set is a no-op: an inbound message is never mutated.
func (c payloadCarrier) Set(string, string) {}

func (c payloadCarrier) Keys() []string {
	keys := make([]string, 0, len(c.headers))
	for i := range c.headers {
		keys = append(keys, c.headers[i].Key)
	}
	return keys
}

// propagator returns the process-global text-map propagator (TraceContext + Baggage,
// installed by tracing init).
func propagator() propagation.TextMapPropagator { return otel.GetTextMapPropagator() }
