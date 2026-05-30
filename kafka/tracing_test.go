// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTracedMessageHandler_PassesThrough_WhenTracingDisabled(t *testing.T) {
	called := false
	handler := TracedMessageHandler(func(msg MessagePayload) error {
		called = true
		assert.Equal(t, "test-topic", msg.Topic)
		assert.Equal(t, []byte("hello"), msg.Value)
		return nil
	})

	err := handler(MessagePayload{
		Topic: "test-topic",
		Value: []byte("hello"),
	})

	assert.NoError(t, err)
	assert.True(t, called)
}

func TestTracedMessageHandler_PropagatesError(t *testing.T) {
	expectedErr := errors.New("handler failed")
	handler := TracedMessageHandler(func(msg MessagePayload) error {
		return expectedErr
	})

	err := handler(MessagePayload{Topic: "t"})
	assert.ErrorIs(t, err, expectedErr)
}

func TestTracedMessageHandler_ExtractsTraceparent(t *testing.T) {
	handler := TracedMessageHandler(func(msg MessagePayload) error {
		return nil
	})

	err := handler(MessagePayload{
		Topic: "traced-topic",
		Headers: []Header{
			{Key: "traceparent", Value: []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")},
		},
	})
	assert.NoError(t, err)
}

func TestInjectTraceHeaders_NoOpWhenTracingDisabled(t *testing.T) {
	msg := &Message{
		Value: []byte("test"),
	}
	InjectTraceHeaders(context.Background(), msg)
	assert.Empty(t, msg.Headers)
}

func TestInjectTraceHeadersToMessages_NoOpWhenDisabled(t *testing.T) {
	messages := []Message{
		{Value: []byte("a")},
		{Value: []byte("b")},
	}
	InjectTraceHeadersToMessages(context.Background(), messages)
	for _, m := range messages {
		assert.Empty(t, m.Headers)
	}
}

func TestTraceparentHeaderKey(t *testing.T) {
	require.Equal(t, "traceparent", traceparentHeaderKey)
}
