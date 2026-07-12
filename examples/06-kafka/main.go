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
	"fmt"
	"log"
	"time"

	"github.com/gofynd/fit-go/kafka"
)

func main() {
	client, err := kafka.NewConfluentClient(&kafka.Config{
		Brokers:  []string{"localhost:9092"},
		ClientID: "demo-app",
	})
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

	// acks: -1 = all in-sync replicas, 1 = leader only, 0 = fire-and-forget.
	// Context-aware methods are canonical: every message gets a producer span
	// and carries traceparent, tracestate and baggage when tracing is enabled.
	err = producer.ProduceCtx(ctx, "orders", []kafka.Message{
		{Key: []byte("o-1"), Value: []byte(`{"id":"o-1","total":42}`)},
		{Key: []byte("o-2"), Value: []byte(`{"id":"o-2","total":99}`)},
	}, -1)
	if err != nil {
		log.Printf("produce: %v", err)
		return
	}
	fmt.Println("produced 2 messages to 'orders'")
}

func consume(client *kafka.ConfluentClient) {
	consumer, err := client.Consumer(kafka.ConsumerConfig{
		GroupID:    "demo-consumers",
		AutoCommit: true,
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Connect([]kafka.TopicConfig{
		{Topic: "orders", FromBeginning: true},
	}); err != nil {
		log.Printf("consumer connect: %v", err)
		return
	}

	// Consume blocks, dispatching each message to the handler. Here we stop
	// after a short demo window by returning an error from the handler.
	deadline := time.Now().Add(5 * time.Second)
	handler := func(_ context.Context, msg kafka.MessagePayload) error {
		fmt.Printf("consumed topic=%s partition=%d offset=%d value=%s\n",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		if time.Now().After(deadline) {
			return fmt.Errorf("demo window elapsed")
		}
		return nil
	}

	if err := consumer.ConsumeCtx(handler, kafka.ConsumerOptions{}); err != nil {
		log.Printf("consume ended: %v", err)
	}
}
