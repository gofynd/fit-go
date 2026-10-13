// Example 06: Kafka produce & consume
//
// The kafka package provides a confluent-kafka-go backed client with SASL/TLS
// support and OpenTelemetry trace propagation. A ConfluentClient creates
// producers and consumers that share broker configuration.
//
// Requires a reachable broker. Example env:
//
//	KAFKA_BROKER_LIST=localhost:9092
//
// Run:
//
//	go run ./examples/06-kafka
package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/gofynd/fit-go/kafka"
)

func main() {
	cfg, err := kafka.ConfigFromEnv()
	if err != nil {
		log.Fatalf("kafka config: %v", err)
	}
	cfg.ClientID = "demo-app"
	client, err := kafka.NewConfluentClient(cfg)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer client.Close()

	produce(context.Background(), client)
	consume(client)
}

func produce(ctx context.Context, client *kafka.ConfluentClient) {
	producer, err := client.Producer(kafka.ProducerConfig{})
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	if err := producer.Connect(); err != nil {
		log.Printf("producer connect: %v", err)
		return
	}

	// This original Producer constructor uses its configured acknowledgements
	// (the default is -1: all in-sync replicas), not the helper's per-call acks.
	// ProducerWithOptions is the opt-in path for per-call acknowledgement changes.
	// ProduceCtx creates producer spans and injects trace headers when tracing is
	// enabled; raw Produce on this original constructor remains untraced.
	err = kafka.ProduceCtx(producer, ctx, "orders", []kafka.Message{
		{Key: []byte("o-1"), Value: []byte(`{"id":"o-1","total":42}`)},
		{Key: []byte("o-2"), Value: []byte(`{"id":"o-2","total":99}`)},
	}, -1)
	if err != nil {
		log.Printf("produce: %v", err)
		return
	}
	log.Print("produced 2 messages to 'orders'")
}

func consume(client *kafka.ConfluentClient) {
	consumer, err := client.Consumer(kafka.ConsumerConfig{
		GroupID:    "demo-consumers",
		AutoCommit: true,
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	if err := consumer.Connect([]kafka.TopicConfig{
		{Topic: "orders", FromBeginning: true},
	}); err != nil {
		log.Printf("consumer connect: %v", err)
		if closeErr := consumer.Close(); closeErr != nil {
			log.Printf("consumer close: %v", closeErr)
		}
		return
	}

	// The original Consumer logs handler errors and continues. An external timer
	// requests shutdown after five seconds, even if the topic has no messages.
	// Close is synchronous; broker shutdown can take additional time.
	handler := func(_ context.Context, msg kafka.MessagePayload) error {
		log.Printf("consumed topic=%s partition=%d offset=%d",
			msg.Topic, msg.Partition, msg.Offset)
		return nil
	}

	if err := consumeFor(consumer, handler, 5*time.Second); err != nil {
		log.Printf("consume ended: %v", err)
	}
}

// consumeFor requests shutdown after the demo window or when Consume returns,
// whichever comes first. It keeps Consume synchronous and closes exactly once.
func consumeFor(consumer kafka.KafkaConsumer, handler kafka.MessageHandlerCtx, window time.Duration) error {
	var closeOnce sync.Once
	var closeErr error
	closeConsumer := func() {
		closeOnce.Do(func() { closeErr = consumer.Close() })
	}
	defer closeConsumer()
	timer := time.AfterFunc(window, closeConsumer)
	defer timer.Stop()

	consumeErr := kafka.ConsumeCtx(consumer, handler, kafka.ConsumerOptions{})
	closeConsumer()
	return errors.Join(consumeErr, closeErr)
}
