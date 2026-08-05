package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	confluentKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/gofynd/fit-go/logging"
)

// TestKafkaJSCompatibleUnresolvedFinalizerMultiRecordLive proves the exact
// compatibility behavior against a disposable broker. Two same-partition
// records are present before the first poll (and MaxRecords is two), so the
// failed N and following N+1 are returned in one PollRecords batch. The opt-in
// must stop that batch, replay N, then deliver N+1. The default-false case pins
// the existing behavior for every consumer that does not request compatibility.
func TestKafkaJSCompatibleUnresolvedFinalizerMultiRecordLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	cases := []struct {
		name      string
		redeliver bool
		want      []int64
	}{
		{name: "opt_in_replays_failed_N_before_N_plus_1", redeliver: true, want: []int64{0, 0, 1}},
		{name: "default_false_preserves_existing_fetch_position", want: []int64{0, 1}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			runKafkaJSUnresolvedMultiRecordFixture(t, broker, test.redeliver, test.want)
		})
	}
}

func runKafkaJSUnresolvedMultiRecordFixture(t *testing.T, broker string, redeliver bool, want []int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "fit-go-unresolved-redelivery-" + suffix
	group := "fit-go-unresolved-redelivery-group-" + suffix

	admin, err := confluentKafka.NewAdminClient(&confluentKafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	results, err := admin.CreateTopics(ctx, []confluentKafka.TopicSpecification{{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}})
	if err != nil || len(results) != 1 || (results[0].Error.Code() != confluentKafka.ErrNoError && results[0].Error.Code() != confluentKafka.ErrTopicAlreadyExists) {
		t.Fatalf("create topic: results=%#v err=%v", results, err)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Value: []byte("zero")},
		&kgo.Record{Topic: topic, Value: []byte("one")},
	).FirstErr(); err != nil {
		t.Fatalf("produce fixture: %v", err)
	}

	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	consumerConfig := DefaultConsumerConfig(group)
	consumerConfig.Backend = ConsumerBackendKafkaJSCompatible
	consumerConfig.AutoCommit = false
	consumer, err := newKafkaJSCompatibleConsumer([]string{broker}, &Config{ClientID: "fit-go-redelivery-test"}, consumerConfig, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = consumer.Close() }()

	var deliveriesMu sync.Mutex
	deliveries := make([]int64, 0, len(want))
	completed := make(chan struct{}, 1)
	runDone := make(chan error, 1)
	handlerFailure := errors.New("caught task failure")
	manualCommit := false
	failedZero := false
	go func() {
		runDone <- consumer.ConsumeCtx(func(_ context.Context, payload MessagePayload) error {
			if payload.Offset < 0 || payload.Offset > 1 || string(payload.Value) != []string{"zero", "one"}[payload.Offset] {
				return fmt.Errorf("unexpected payload offset=%d value=%q", payload.Offset, payload.Value)
			}
			deliveriesMu.Lock()
			deliveries = append(deliveries, payload.Offset)
			isFirstZero := payload.Offset == 0 && !failedZero
			if isFirstZero {
				failedZero = true
			}
			if len(deliveries) == len(want) {
				select {
				case completed <- struct{}{}:
				default:
				}
			}
			deliveriesMu.Unlock()
			if isFirstZero {
				return handlerFailure
			}
			return nil
		}, ConsumerOptions{
			AutoCommit: &manualCommit,
			OffsetFinalizer: func(_ context.Context, _ MessagePayload, _ error, _ ExactOffsetCommit) error {
				return nil
			},
			ResolveAfterSuccessfulFinalizer: true,
			RedeliverUnresolvedFinalizer:    redeliver,
			PollTimeout:                     100 * time.Millisecond,
			MaxRecords:                      2,
		})
	}()

	select {
	case <-completed:
	case err := <-runDone:
		t.Fatalf("consumer ended before expected deliveries: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for same-process delivery sequence")
	}
	waitForKafkaJSCommittedOffset(t, ctx, broker, group, topic, 2)
	deliveriesMu.Lock()
	got := append([]int64(nil), deliveries...)
	deliveriesMu.Unlock()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("deliveries = %v, want %v", got, want)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("consumer shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func waitForKafkaJSCommittedOffset(t *testing.T, ctx context.Context, broker, group, topic string, want int64) {
	t.Helper()
	reader, err := confluentKafka.NewConsumer(&confluentKafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           group,
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for {
		partitions, commitErr := reader.Committed([]confluentKafka.TopicPartition{{Topic: &topic, Partition: 0}}, 2000)
		if commitErr == nil && len(partitions) == 1 && int64(partitions[0].Offset) == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("committed offset did not reach %d: partitions=%#v err=%v", want, partitions, commitErr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
