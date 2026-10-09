// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import "testing"

type descriptiveClient struct {
	producerOptions  ProducerOptions
	consumerSettings ConsumerSettings
}

func (*descriptiveClient) Connect() error { return nil }
func (*descriptiveClient) Close() error   { return nil }
func (*descriptiveClient) Producer(ProducerConfig) (KafkaProducer, error) {
	return &descriptiveProducer{}, nil
}
func (*descriptiveClient) Consumer(ConsumerConfig) (KafkaConsumer, error) {
	return &descriptiveConsumer{}, nil
}
func (c *descriptiveClient) ProducerWithOptions(options ProducerOptions) (KafkaProducer, error) {
	c.producerOptions = options
	return &descriptiveProducer{}, nil
}
func (c *descriptiveClient) ConsumerWithSettings(settings ConsumerSettings) (KafkaConsumer, error) {
	c.consumerSettings = settings
	return &descriptiveConsumer{}, nil
}

type descriptiveProducer struct{}

func (*descriptiveProducer) Connect() error                          { return nil }
func (*descriptiveProducer) Produce(string, []Message, int) error    { return nil }
func (*descriptiveProducer) ProduceBatch([]TopicMessages, int) error { return nil }
func (*descriptiveProducer) Close() error                            { return nil }

type descriptiveConsumer struct{ usedOptions bool }

func (*descriptiveConsumer) Connect([]TopicConfig) error { return nil }
func (*descriptiveConsumer) Consume(MessageHandler, ConsumerOptions) error {
	return nil
}
func (*descriptiveConsumer) ConsumeBatch(BatchHandler, ConsumerOptions) error {
	return nil
}
func (*descriptiveConsumer) Close() error { return nil }
func (c *descriptiveConsumer) ConsumeWithOptions(MessageHandler, ConsumeOptions) error {
	c.usedOptions = true
	return nil
}
func (c *descriptiveConsumer) ConsumeBatchWithOptions(BatchHandler, ConsumeOptions) error {
	c.usedOptions = true
	return nil
}

func TestDescriptiveFactoriesDispatchWithoutAdvancedInterfaceNames(t *testing.T) {
	client := &descriptiveClient{}
	if _, err := NewProducer(client, ProducerOptions{AcksSet: true}); err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if !client.producerOptions.AcksSet {
		t.Fatal("NewProducer did not dispatch the complete options")
	}

	if _, err := NewConsumer(client, ConsumerSettings{GroupID: "group", AutoCreateTopics: true}); err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if !client.consumerSettings.AutoCreateTopics {
		t.Fatal("NewConsumer did not dispatch the complete settings")
	}
}

func TestConsumeWithOptionsUsesDescriptiveExtension(t *testing.T) {
	consumer := &descriptiveConsumer{}
	if err := ConsumeWithOptions(consumer, func(MessagePayload) error { return nil }, ConsumeOptions{CommitBeforeHandler: true}); err != nil {
		t.Fatalf("ConsumeWithOptions: %v", err)
	}
	if !consumer.usedOptions {
		t.Fatal("ConsumeWithOptions did not dispatch to the descriptive extension")
	}
}
