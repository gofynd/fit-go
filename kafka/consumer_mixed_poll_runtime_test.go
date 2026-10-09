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

// Package errors provides Sentry error reporting integration for the fit.go framework.

package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofynd/fit-go/logging"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// This test also runs with an in-memory Kafka protocol broker through the
// temporary kfake overlay used for PR verification; kfake is not a dependency
// of the library. The records and offset state come from a real franz client.
func TestKafkaJSMixedTransientPollMultiPartitionLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	for _, batch := range []bool{false, true} {
		for _, mode := range []string{"no_commit", "committed", "automatic"} {
			t.Run(fmt.Sprintf("batch_%t/%s", batch, mode), func(t *testing.T) {
				runKafkaJSMixedPollBrokerFixture(t, broker, batch, mode)
			})
		}
	}
}

type kafkaJSMixedPollReadyHook struct {
	once  sync.Once
	ready chan struct{}
}

func (h *kafkaJSMixedPollReadyHook) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	// A successful Fetch response follows the initial group offset lookup.
	if key == 1 && e2e.Err() == nil {
		h.once.Do(func() { close(h.ready) })
	}
}

type kafkaJSMixedPollBrokerClient struct {
	*kgo.Client
	inject   atomic.Bool
	observed chan kgo.Fetches
}

func (c *kafkaJSMixedPollBrokerClient) PollRecords(ctx context.Context, limit int) kgo.Fetches {
	fetches := c.Client.PollRecords(ctx, limit)
	if fetches.NumRecords() == 0 || !c.inject.CompareAndSwap(true, false) {
		return fetches
	}
	// Gather the four already-produced records across both partitions before
	// attaching the supported group-error notification. Every returned record
	// has really advanced the driver's cursor; none has reached a handler.
	for fetches.NumRecords() < 4 && ctx.Err() == nil {
		fetches = append(fetches, c.Client.PollRecords(ctx, 4-fetches.NumRecords())...)
	}
	c.observed <- fetches
	return append(fetches, kgo.NewErrFetch(&kgo.ErrGroupSession{Err: kerr.RebalanceInProgress})...)
}

func runKafkaJSMixedPollBrokerFixture(t *testing.T, broker string, batch bool, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic, group := "fit-go-mixed-poll-"+suffix, "fit-go-mixed-group-"+suffix
	createKafkaJSRuntimeTopic(t, ctx, broker, topic, 2)
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	// Historical records must stay skipped for a new FromBeginning=false group.
	if err := producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Partition: 0, Value: []byte("history")},
		&kgo.Record{Topic: topic, Partition: 1, Value: []byte("history")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if mode == "committed" {
		committer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
		if err != nil {
			t.Fatal(err)
		}
		for partition := int32(0); partition < 2; partition++ {
			if err := commitKafkaJSExact(ctx, committer, &kgo.Record{Topic: topic, Partition: partition}, 1, false); err != nil {
				committer.Close()
				t.Fatal(err)
			}
		}
		committer.Close()
	}
	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	settings := DefaultConsumerAdvancedConfig(group)
	settings.AutoCommit = mode == "automatic"
	settings.AutoCommitInterval = 100 * time.Millisecond
	consumer := &franzKafkaJS2CompatConsumer{brokers: []string{broker}, fitCfg: &Config{ClientID: "mixed-poll-review"}, config: settings, logger: logger}
	ready := &kafkaJSMixedPollReadyHook{ready: make(chan struct{})}
	observed := make(chan kgo.Fetches, 1)
	var constructed atomic.Int32
	var realClient *kafkaJSMixedPollBrokerClient
	consumer.newClient = func(opts ...kgo.Opt) (franzKafkaJS2CompatClient, error) {
		real, err := kgo.NewClient(append(opts, kgo.WithHooks(ready))...)
		if err != nil {
			return nil, err
		}
		constructed.Add(1)
		realClient = &kafkaJSMixedPollBrokerClient{Client: real, observed: observed}
		realClient.inject.Store(true)
		return realClient, nil
	}
	if err := consumer.Connect([]TopicConfig{{Topic: topic, FromBeginning: false}}); err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	handled := make(chan MessagePayload, 16)
	message := func(_ context.Context, payload MessagePayload) error { handled <- payload; return nil }
	opts := ConsumerAdvancedOptions{PollTimeout: time.Second, MaxRecords: 4}
	run := func() error { return consumer.ConsumeCtxAdvanced(message, opts) }
	if batch {
		run = func() error {
			return consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, payload BatchPayload) error {
				for _, record := range payload.Messages {
					if err := message(ctx, record); err != nil {
						return err
					}
				}
				return nil
			}, opts)
		}
	}
	done := make(chan error, 1)
	go func() { done <- run() }()
	select {
	case <-ready.ready:
	case <-ctx.Done():
		t.Fatal("initial group offset lookup never reached Fetch")
	}
	var pending []*kgo.Record
	for partition := int32(0); partition < 2; partition++ {
		for i := 0; i < 2; i++ {
			pending = append(pending, &kgo.Record{Topic: topic, Partition: partition, Value: []byte("pending")})
		}
	}
	if err := producer.ProduceSync(ctx, pending...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !IsTransientConsumerError(err) {
			t.Fatalf("mixed poll error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("mixed poll did not end the run")
	}
	fetched := <-observed
	if fetched.NumRecords() != 4 || len(handled) != 0 || constructed.Load() != 1 || consumer.client != realClient {
		t.Fatalf("mixed poll: records=%d handled=%d clients=%d", fetched.NumRecords(), len(handled), constructed.Load())
	}
	for _, record := range fetched.Records() {
		if record.Offset < 1 {
			t.Fatal("FromBeginning=false replayed historical records")
		}
	}
	// Reading the marked state is a control for automatic commits:
	// fetched-but-unhandled records must not be marked as resolved.
	if mode == "automatic" {
		for _, offsets := range realClient.MarkedOffsets() {
			for _, offset := range offsets {
				if offset.Offset > 1 {
					t.Fatalf("mixed poll marked unprocessed offset %d", offset.Offset)
				}
			}
		}
	}
	go func() { done <- run() }()
	seen := map[int]map[int64]bool{0: {}, 1: {}}
	for count := 0; count < 4; count++ {
		select {
		case payload := <-handled:
			if payload.Offset < 1 || payload.Offset > 2 || seen[payload.Partition][payload.Offset] {
				t.Fatalf("retry skipped/duplicated historical boundary: partition=%d offset=%d", payload.Partition, payload.Offset)
			}
			seen[payload.Partition][payload.Offset] = true
		case <-ctx.Done():
			t.Fatalf("retry lost fetched records: seen=%v", seen)
		}
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal("retry failed to shut down")
	}
}
