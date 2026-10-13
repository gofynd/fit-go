// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

type compatibilityProducer struct {
	produceCalls         int
	produceBatchCalls    int
	produceCtxCalls      int
	produceBatchCtxCalls int
}

func (*compatibilityProducer) Connect() error { return nil }
func (p *compatibilityProducer) Produce(string, []Message, int) error {
	p.produceCalls++
	return nil
}
func (p *compatibilityProducer) ProduceBatch([]TopicMessages, int) error {
	p.produceBatchCalls++
	return nil
}
func (*compatibilityProducer) Close() error { return nil }

type contextualCompatibilityProducer struct{ compatibilityProducer }

func (p *contextualCompatibilityProducer) ProduceCtx(context.Context, string, []Message, int) error {
	p.produceCtxCalls++
	return nil
}
func (p *contextualCompatibilityProducer) ProduceBatchCtx(context.Context, []TopicMessages, int) error {
	p.produceBatchCtxCalls++
	return nil
}

func TestProducerContextHelpersPreserveOldDriversAndPreferNativeContext(t *testing.T) {
	basic := &compatibilityProducer{}
	if err := ProduceCtx(basic, context.Background(), "events", nil, -1); err != nil {
		t.Fatalf("ProduceCtx fallback: %v", err)
	}
	if err := ProduceBatchCtx(basic, context.Background(), nil, -1); err != nil {
		t.Fatalf("ProduceBatchCtx fallback: %v", err)
	}
	if basic.produceCalls != 1 || basic.produceBatchCalls != 1 {
		t.Fatalf("fallback calls = (%d, %d), want (1, 1)", basic.produceCalls, basic.produceBatchCalls)
	}

	contextual := &contextualCompatibilityProducer{}
	if err := ProduceCtx(contextual, context.Background(), "events", nil, -1); err != nil {
		t.Fatalf("ProduceCtx native: %v", err)
	}
	if err := ProduceBatchCtx(contextual, context.Background(), nil, -1); err != nil {
		t.Fatalf("ProduceBatchCtx native: %v", err)
	}
	if contextual.produceCtxCalls != 1 || contextual.produceBatchCtxCalls != 1 {
		t.Fatalf("native calls = (%d, %d), want (1, 1)", contextual.produceCtxCalls, contextual.produceBatchCtxCalls)
	}
	if contextual.produceCalls != 0 || contextual.produceBatchCalls != 0 {
		t.Fatal("native context producer unexpectedly used legacy entry points")
	}
}

type compatibilityConsumer struct {
	messageOptions ConsumerOptions
	batchOptions   ConsumerOptions
}

func (*compatibilityConsumer) Connect([]TopicConfig) error { return nil }
func (c *compatibilityConsumer) Consume(handler MessageHandler, opts ConsumerOptions) error {
	c.messageOptions = opts
	return handler(MessagePayload{Topic: "events"})
}
func (c *compatibilityConsumer) ConsumeBatch(handler BatchHandler, opts ConsumerOptions) error {
	c.batchOptions = opts
	return handler(BatchPayload{Topic: "events"})
}
func (*compatibilityConsumer) Close() error { return nil }

type compatibilityClient struct {
	consumer       KafkaConsumer
	consumerConfig ConsumerConfig
}

func (*compatibilityClient) Producer(ProducerConfig) (KafkaProducer, error) { return nil, nil }
func (c *compatibilityClient) Consumer(config ConsumerConfig) (KafkaConsumer, error) {
	c.consumerConfig = config
	return c.consumer, nil
}
func (*compatibilityClient) Close() error { return nil }

type advancedCompatibilityClient struct {
	compatibilityClient
	advancedConfig ConsumerAdvancedConfig
}

func (c *advancedCompatibilityClient) ConsumerAdvanced(config ConsumerAdvancedConfig) (KafkaConsumer, error) {
	c.advancedConfig = config
	return c.consumer, nil
}

func TestAdvancedConsumerConstructionRequiresOptInDriverSupport(t *testing.T) {
	consumer := &compatibilityConsumer{}
	basicClient := &compatibilityClient{consumer: consumer}
	basicConfig := DefaultConsumerAdvancedConfig("group")
	got, err := NewConsumerAdvanced(basicClient, basicConfig)
	if err != nil || got != consumer {
		t.Fatalf("basic projection = (%T, %v), want consumer and nil", got, err)
	}
	if basicClient.consumerConfig != basicConfig.ConsumerConfig() {
		t.Fatalf("projected config = %+v, want %+v", basicClient.consumerConfig, basicConfig.ConsumerConfig())
	}

	advancedConfig := basicConfig
	advancedConfig.AutoCreateTopics = true
	if _, err := NewConsumerAdvanced(basicClient, advancedConfig); err == nil {
		t.Fatal("advanced config was silently accepted by a basic driver")
	}

	advancedClient := &advancedCompatibilityClient{compatibilityClient: compatibilityClient{consumer: consumer}}
	got, err = NewConsumerAdvanced(advancedClient, advancedConfig)
	if err != nil || got != consumer {
		t.Fatalf("advanced dispatch = (%T, %v), want consumer and nil", got, err)
	}
	if !advancedClient.advancedConfig.AutoCreateTopics {
		t.Fatal("advanced config was not dispatched to the extension")
	}
}

func TestAdvancedRunAdaptersProjectBasicControlsAndRejectSilentDegradation(t *testing.T) {
	consumer := &compatibilityConsumer{}
	autoCommit := true
	basic := ConsumerAdvancedOptions{
		AutoCommit:                     &autoCommit,
		PartitionsConsumedConcurrently: 3,
		PollTimeout:                    250 * time.Millisecond,
		MaxRecords:                     17,
	}
	if err := ConsumeAdvanced(consumer, func(MessagePayload) error { return nil }, basic); err != nil {
		t.Fatalf("ConsumeAdvanced basic projection: %v", err)
	}
	if consumer.messageOptions != basic.ConsumerOptions() {
		t.Fatalf("message options = %+v, want %+v", consumer.messageOptions, basic.ConsumerOptions())
	}
	if err := ConsumeBatchAdvanced(consumer, func(BatchPayload) error { return nil }, basic); err != nil {
		t.Fatalf("ConsumeBatchAdvanced basic projection: %v", err)
	}
	if consumer.batchOptions != basic.ConsumerOptions() {
		t.Fatalf("batch options = %+v, want %+v", consumer.batchOptions, basic.ConsumerOptions())
	}

	advanced := basic
	advanced.OffsetFinalizer = func(context.Context, MessagePayload, error, ExactOffsetCommit) error {
		return errors.New("must not run")
	}
	if err := ConsumeAdvanced(consumer, func(MessagePayload) error { return nil }, advanced); err == nil {
		t.Fatal("advanced message controls were silently ignored")
	}
	if err := ConsumeCtxAdvanced(consumer, func(context.Context, MessagePayload) error { return nil }, advanced); err == nil {
		t.Fatal("advanced context message controls were silently ignored")
	}
	if err := ConsumeBatchAdvanced(consumer, func(BatchPayload) error { return nil }, advanced); err == nil {
		t.Fatal("advanced batch controls were silently ignored")
	}
	if err := ConsumeBatchCtxAdvanced(consumer, func(context.Context, BatchPayload) error { return nil }, advanced); err == nil {
		t.Fatal("advanced context batch controls were silently ignored")
	}
}
