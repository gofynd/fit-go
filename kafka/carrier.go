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
	"strings"

	"github.com/gofynd/fit-go/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// messageCarrier adapts a Kafka *Message's headers to the OTel TextMapCarrier so the
// GLOBAL propagator can inject/extract context — instead of hand-rolling traceparent
// parsing. Using the propagator means we transparently carry everything it is
// configured with (W3C traceparent, tracestate, and baggage), and stay correct if the
// platform ever changes the propagator.
type messageCarrier struct{ msg *Message }

// Get returns the first header value with the given key. Kafka header names are
// case-sensitive at the protocol level, but propagation fields are HTTP-style
// field names and must be matched case-insensitively so forwarded headers such
// as TraceParent are not ignored.
func (c messageCarrier) Get(key string) string {
	if c.msg == nil {
		return ""
	}
	for i := range c.msg.Headers {
		if strings.EqualFold(c.msg.Headers[i].Key, key) {
			return string(c.msg.Headers[i].Value)
		}
	}
	return ""
}

// Set writes exactly one canonical header, replacing the first case-insensitive
// match and removing every duplicate. This matters for forwarded/re-produced
// messages: leaving a stale TraceParent or a second baggage/tracestate field makes
// extraction order-dependent and can continue the wrong trace.
func (c messageCarrier) Set(key, value string) {
	if c.msg == nil {
		return
	}

	headers := c.msg.Headers[:0]
	replaced := false
	for _, header := range c.msg.Headers {
		if strings.EqualFold(header.Key, key) {
			if !replaced {
				header.Key = key
				header.Value = []byte(value)
				headers = append(headers, header)
				replaced = true
			}
			continue
		}
		headers = append(headers, header)
	}
	if !replaced {
		headers = append(headers, Header{Key: key, Value: []byte(value)})
	}
	c.msg.Headers = headers
}

// Keys lists the header keys present on the message.
func (c messageCarrier) Keys() []string {
	if c.msg == nil {
		return nil
	}
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
		if strings.EqualFold(c.headers[i].Key, key) {
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

// removePropagationHeaders removes every field owned by the configured global
// propagator before a fresh injection. Injectors do not call Set for absent values
// (for example, a context without baggage), so merely replacing fields in Set would
// leave stale baggage or tracestate on a forwarded message.
func removePropagationHeaders(msg *Message) {
	if msg == nil || len(msg.Headers) == 0 {
		return
	}
	// Clear every format fit-go supports, not only the currently configured one.
	// This makes propagator switches and OTEL_PROPAGATORS=none real opt-outs
	// instead of accidentally forwarding a stale B3/Jaeger/W3C parent.
	headers := msg.Headers[:0]
	for _, header := range msg.Headers {
		if !tracing.IsPropagationField(header.Key, propagator()) {
			headers = append(headers, header)
		}
	}
	msg.Headers = headers
}
