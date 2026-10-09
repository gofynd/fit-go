// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"errors"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type legacyIdentityHandlerError struct{ code int }

func (e *legacyIdentityHandlerError) Error() string { return "legacy handler failed" }

// oneMessageThenTimeoutDriver yields a single record, then poll timeouts (or
// a terminal error once stopAfter reads have happened).
func oneMessageThenTimeoutDriver(stopAfter int) *fakeConfluentConsumerDriver {
	topic := "orders"
	var mu sync.Mutex
	reads := 0
	return &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		mu.Lock()
		reads++
		n := reads
		mu.Unlock()
		if n == 1 {
			return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 7}}, nil
		}
		if stopAfter > 0 && n >= stopAfter {
			return nil, errors.New("stop")
		}
		return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
	}}
}

// Upstream main ConfluentConsumer.ConsumeBatch returned the handler's own
// error value (`return err`), so callers could compare identity and
// type-assert without errors.As.
func TestLegacyConsumeBatchReturnsHandlerErrorIdentity(t *testing.T) {
	handlerErr := &legacyIdentityHandlerError{code: 42}
	consumer := newTestConfluentConsumer(false, oneMessageThenTimeoutDriver(0))
	consumer.legacy = true

	got := consumer.ConsumeBatch(func(BatchPayload) error { return handlerErr }, ConsumerOptions{})
	if got != error(handlerErr) {
		t.Fatalf("ConsumeBatch() = %#v, want identical handler error %#v", got, handlerErr)
	}
	typed, ok := got.(*legacyIdentityHandlerError)
	if !ok || typed.code != 42 {
		t.Fatalf("type assertion on ConsumeBatch error failed: %T", got)
	}
}

// Upstream main returned the handler error even when Close cancelled the run
// while the handler was executing.
func TestLegacyConsumeBatchReturnsHandlerErrorWhenCloseRaces(t *testing.T) {
	handlerErr := &legacyIdentityHandlerError{code: 7}
	consumer := newTestConfluentConsumer(false, oneMessageThenTimeoutDriver(0))
	consumer.legacy = true

	got := consumer.ConsumeBatch(func(BatchPayload) error {
		if err := consumer.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
		return handlerErr
	}, ConsumerOptions{})
	if got != error(handlerErr) {
		t.Fatalf("ConsumeBatch() during Close = %#v, want identical handler error", got)
	}
}

// Upstream main's legacy Consume logged a handler error and continued; the
// handler error is never surfaced (identity is therefore not observable).
// A terminal read error is returned, and a Close racing the handler yields nil.
func TestLegacyConsumeDoesNotReturnHandlerError(t *testing.T) {
	handlerErr := &legacyIdentityHandlerError{code: 1}
	consumer := newTestConfluentConsumer(false, oneMessageThenTimeoutDriver(3))
	consumer.legacy = true
	got := consumer.Consume(func(MessagePayload) error { return handlerErr }, ConsumerOptions{})
	if got == nil || got == error(handlerErr) || errors.Is(got, handlerErr) {
		t.Fatalf("Consume() = %#v, want terminal read error unrelated to handler error", got)
	}

	raced := newTestConfluentConsumer(false, oneMessageThenTimeoutDriver(0))
	raced.legacy = true
	got = raced.Consume(func(MessagePayload) error {
		_ = raced.Close()
		return handlerErr
	}, ConsumerOptions{})
	if got != nil {
		t.Fatalf("Consume() with Close racing handler = %#v, want nil", got)
	}
}

// Advanced consumers keep the wrapped handler error (unchanged behaviour),
// still reachable through errors.As / errors.Is.
func TestAdvancedConsumeBatchKeepsWrappedHandlerError(t *testing.T) {
	handlerErr := &legacyIdentityHandlerError{code: 9}
	consumer := newTestConfluentConsumer(false, oneMessageThenTimeoutDriver(0))
	got := consumer.ConsumeBatch(func(BatchPayload) error { return handlerErr }, ConsumerOptions{})
	if got == nil || !errors.Is(got, handlerErr) {
		t.Fatalf("advanced ConsumeBatch() = %#v, want error wrapping handler error", got)
	}
	var typed *legacyIdentityHandlerError
	if !errors.As(got, &typed) || typed.code != 9 {
		t.Fatalf("errors.As failed on advanced error %T", got)
	}
}
