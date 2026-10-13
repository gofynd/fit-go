// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

// ProducerOptions configures producer delivery, tracing, partitioning, retry,
// and shutdown behavior without changing the original ProducerConfig layout.
type ProducerOptions = ProducerAdvancedConfig

// ConsumerSettings configures consumer construction and backend compatibility.
type ConsumerSettings = ConsumerAdvancedConfig

// ConsumeOptions configures one message or batch consumption run.
type ConsumeOptions = ConsumerAdvancedOptions

// KafkaClientProducerFactory is the optional producer-options extension.
type KafkaClientProducerFactory interface {
	ProducerWithOptions(ProducerOptions) (KafkaProducer, error)
}

// KafkaClientConsumerFactory is the optional consumer-settings extension.
type KafkaClientConsumerFactory interface {
	ConsumerWithSettings(ConsumerSettings) (KafkaConsumer, error)
}

// KafkaConsumerWithOptions is the optional message and batch options extension.
type KafkaConsumerWithOptions interface {
	ConsumeWithOptions(MessageHandler, ConsumeOptions) error
	ConsumeBatchWithOptions(BatchHandler, ConsumeOptions) error
}

// KafkaConsumerContextWithOptions is the optional context-aware message extension.
type KafkaConsumerContextWithOptions interface {
	ConsumeCtxWithOptions(MessageHandlerCtx, ConsumeOptions) error
}

// KafkaBatchConsumerContextWithOptions is the optional context-aware batch extension.
type KafkaBatchConsumerContextWithOptions interface {
	ConsumeBatchCtxWithOptions(BatchHandlerCtx, ConsumeOptions) error
}

// DefaultConsumerSettings returns the established defaults in the complete settings shape.
func DefaultConsumerSettings(groupID string) ConsumerSettings {
	return DefaultConsumerAdvancedConfig(groupID)
}

// NewProducer creates a producer using the complete producer option contract.
func NewProducer(client KafkaClient, options ProducerOptions) (KafkaProducer, error) {
	if factory, ok := client.(KafkaClientProducerFactory); ok {
		return factory.ProducerWithOptions(options)
	}
	return NewProducerAdvanced(client, options)
}

// NewConsumer creates a consumer using the complete consumer settings contract.
func NewConsumer(client KafkaClient, settings ConsumerSettings) (KafkaConsumer, error) {
	if factory, ok := client.(KafkaClientConsumerFactory); ok {
		return factory.ConsumerWithSettings(settings)
	}
	return NewConsumerAdvanced(client, settings)
}

// ConsumeWithOptions dispatches one message at a time using the complete run options.
func ConsumeWithOptions(consumer KafkaConsumer, handler MessageHandler, opts ConsumeOptions) error {
	if configured, ok := consumer.(KafkaConsumerWithOptions); ok {
		return configured.ConsumeWithOptions(handler, opts)
	}
	return ConsumeAdvanced(consumer, handler, opts)
}

// ConsumeCtxWithOptions is the context-aware message entry point.
func ConsumeCtxWithOptions(consumer KafkaConsumer, handler MessageHandlerCtx, opts ConsumeOptions) error {
	if configured, ok := consumer.(KafkaConsumerContextWithOptions); ok {
		return configured.ConsumeCtxWithOptions(handler, opts)
	}
	return ConsumeCtxAdvanced(consumer, handler, opts)
}

// ConsumeBatchWithOptions dispatches batches using the complete run options.
func ConsumeBatchWithOptions(consumer KafkaConsumer, handler BatchHandler, opts ConsumeOptions) error {
	if configured, ok := consumer.(KafkaConsumerWithOptions); ok {
		return configured.ConsumeBatchWithOptions(handler, opts)
	}
	return ConsumeBatchAdvanced(consumer, handler, opts)
}

// ConsumeBatchCtxWithOptions is the context-aware batch entry point.
func ConsumeBatchCtxWithOptions(consumer KafkaConsumer, handler BatchHandlerCtx, opts ConsumeOptions) error {
	if configured, ok := consumer.(KafkaBatchConsumerContextWithOptions); ok {
		return configured.ConsumeBatchCtxWithOptions(handler, opts)
	}
	return ConsumeBatchCtxAdvanced(consumer, handler, opts)
}

// ProducerWithOptions creates a Confluent producer using the complete option contract.
func (cc *ConfluentClient) ProducerWithOptions(options ProducerOptions) (KafkaProducer, error) {
	return cc.ProducerAdvanced(options)
}

// ConsumerWithSettings creates a Confluent consumer using the complete settings contract.
func (cc *ConfluentClient) ConsumerWithSettings(settings ConsumerSettings) (KafkaConsumer, error) {
	return cc.ConsumerAdvanced(settings)
}

// ConsumeWithOptions processes Confluent messages using the complete run options.
func (cc *ConfluentConsumer) ConsumeWithOptions(handler MessageHandler, opts ConsumeOptions) error {
	return cc.ConsumeAdvanced(handler, opts)
}

// ConsumeCtxWithOptions processes Confluent messages with an explicit context-aware handler.
func (cc *ConfluentConsumer) ConsumeCtxWithOptions(handler MessageHandlerCtx, opts ConsumeOptions) error {
	return cc.ConsumeCtxAdvanced(handler, opts)
}

// ConsumeBatchWithOptions processes Confluent batches using the complete run options.
func (cc *ConfluentConsumer) ConsumeBatchWithOptions(handler BatchHandler, opts ConsumeOptions) error {
	return cc.ConsumeBatchAdvanced(handler, opts)
}

// ConsumeBatchCtxWithOptions processes Confluent batches with a context-aware handler.
func (cc *ConfluentConsumer) ConsumeBatchCtxWithOptions(handler BatchHandlerCtx, opts ConsumeOptions) error {
	return cc.ConsumeBatchCtxAdvanced(handler, opts)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeWithOptions(handler MessageHandler, opts ConsumeOptions) error {
	return c.ConsumeAdvanced(handler, opts)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeCtxWithOptions(handler MessageHandlerCtx, opts ConsumeOptions) error {
	return c.ConsumeCtxAdvanced(handler, opts)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeBatchWithOptions(handler BatchHandler, opts ConsumeOptions) error {
	return c.ConsumeBatchAdvanced(handler, opts)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeBatchCtxWithOptions(handler BatchHandlerCtx, opts ConsumeOptions) error {
	return c.ConsumeBatchCtxAdvanced(handler, opts)
}
