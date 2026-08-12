// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofynd/fit-go/tracing"
	"github.com/stretchr/testify/require"
)

func TestDeprecatedKafkaJSPartitionerAliasMatchesCanonicalValue(t *testing.T) {
	require.Equal(t, ProducerPartitionerKafkaJSCompatible, ProducerPartitionerKafkaJSLegacy)
}

func TestProducerConfigKafkaJSCompatibilityIsExplicitAndExact(t *testing.T) {
	client, err := NewConfluentClient(&Config{
		Brokers:     []string{"broker:9092"},
		ClientID:    "legacy-main-server",
		Compression: CompressionLZ4,
	})
	require.NoError(t, err)

	producer, err := client.Producer(ProducerConfig{
		Acks:              -1,
		AcksSet:           true,
		MaxRetries:        10,
		RetryBackoff:      2 * time.Second,
		RetryBackoffMax:   2 * time.Second,
		Partitioner:       ProducerPartitionerKafkaJSCompatible,
		TraceHeaderPolicy: ProducerTraceHeadersPreserve,
	})
	require.NoError(t, err)
	cp := producer.(*ConfluentProducer)

	partitioner, err := cp.configMap.Get("partitioner", "")
	require.NoError(t, err)
	require.Equal(t, "murmur2_random", partitioner)
	retryBackoff, err := cp.configMap.Get("retry.backoff.ms", 0)
	require.NoError(t, err)
	require.Equal(t, 2000, retryBackoff)
	retryBackoffMax, err := cp.configMap.Get("retry.backoff.max.ms", 0)
	require.NoError(t, err)
	require.Equal(t, 2000, retryBackoffMax)
	retries, err := cp.configMap.Get("message.send.max.retries", 0)
	require.NoError(t, err)
	require.Equal(t, 10, retries)
	require.Equal(t, ProducerTraceHeadersPreserve, cp.traceHeaders)
}

func TestProducerConfigCompatibilityDefaultsRemainUnchanged(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"broker:9092"}})
	require.NoError(t, err)

	producer, err := client.Producer(ProducerConfig{})
	require.NoError(t, err)
	cp := producer.(*ConfluentProducer)

	partitioner, err := cp.configMap.Get("partitioner", nil)
	require.NoError(t, err)
	require.Nil(t, partitioner)
	retryBackoffMax, err := cp.configMap.Get("retry.backoff.max.ms", nil)
	require.NoError(t, err)
	require.Nil(t, retryBackoffMax)
	require.Equal(t, ProducerTraceHeadersInject, cp.traceHeaders)
}

func TestProducerConfigRejectsUnknownCompatibilityPolicies(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"broker:9092"}})
	require.NoError(t, err)

	_, err = client.Producer(ProducerConfig{Partitioner: ProducerPartitioner("unknown")})
	require.EqualError(t, err, `kafka/confluent: unsupported producer partitioner "unknown"`)
	_, err = client.Producer(ProducerConfig{TraceHeaderPolicy: ProducerTraceHeaderPolicy(255)})
	require.EqualError(t, err, "kafka/confluent: unsupported producer trace header policy 255")
}

func TestKafkaJSLegacyPartitionerKnownSevenPartitionFixture(t *testing.T) {
	cluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	require.NoError(t, cluster.CreateTopic("application-events-v2", 7, 1))

	client, err := NewConfluentClient(&Config{
		Brokers:  []string{cluster.BootstrapServers()},
		ClientID: "kafkajs-partitioner-fixture",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	producerAPI, err := client.Producer(ProducerConfig{
		Acks:        -1,
		AcksSet:     true,
		Partitioner: ProducerPartitionerKafkaJSCompatible,
	})
	require.NoError(t, err)
	producer := producerAPI.(*ConfluentProducer)
	require.NoError(t, producer.Connect())
	t.Cleanup(func() { require.NoError(t, producer.Close()) })

	metadata, err := producer.ProduceWithMetadata("application-events-v2", []Message{{
		Key:       []byte("66aa11bb22cc33dd44ee5501"),
		Value:     []byte(`{"fixture":"application-v2"}`),
		Partition: -1,
	}}, -1)
	require.NoError(t, err)
	require.Len(t, metadata, 1)
	require.Equal(t, 6, metadata[0].Partition, "must match KafkaJS 2.2.4 on a seven-partition topic")
}

func TestKafkaJSLegacyPartitionerKeylessRoundRobinOnBroker(t *testing.T) {
	cluster, err := ckafka.NewMockCluster(1)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	require.NoError(t, cluster.CreateTopic("keyless-events", 3, 1))

	client, err := NewConfluentClient(&Config{
		Brokers:  []string{cluster.BootstrapServers()},
		ClientID: "kafkajs-keyless-partitioner-fixture",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	producerAPI, err := client.Producer(ProducerConfig{
		Acks:        -1,
		AcksSet:     true,
		Partitioner: ProducerPartitionerKafkaJSCompatible,
	})
	require.NoError(t, err)
	producer := producerAPI.(*ConfluentProducer)
	producer.partitionSeed = func() (uint32, error) { return 0, nil }
	require.NoError(t, producer.Connect())
	t.Cleanup(func() { require.NoError(t, producer.Close()) })

	metadata, err := producer.ProduceWithMetadata("keyless-events", []Message{
		{Value: []byte("first"), Partition: -1},
		{Value: []byte("second"), Partition: -1},
		{Value: []byte("third"), Partition: -1},
	}, -1)
	require.NoError(t, err)
	require.Len(t, metadata, 3)
	partitions := make(map[int]struct{}, len(metadata))
	for _, item := range metadata {
		partitions[item.Partition] = struct{}{}
	}
	require.Equal(t, map[int]struct{}{0: {}, 1: {}, 2: {}}, partitions,
		"seed zero must cycle through every available partition exactly once")
}

func TestProducerTraceHeaderPreservePolicyKeepsSpansAndExactHeaders(t *testing.T) {
	tracer, exporter := enabledKafkaTracer(t)
	ctx, parent := tracer.StartSpan(context.Background(), "legacy-save", tracing.SpanKindInternal)

	var produced []*ckafka.Message
	driver := &fakeConfluentProducerDriver{produceFn: func(message *ckafka.Message, reports chan ckafka.Event) error {
		produced = append(produced, message)
		reports <- successfulDelivery(message, int64(len(produced)))
		return nil
	}}
	producer := newTestConfluentProducer(driver)
	producer.traceHeaders = ProducerTraceHeadersPreserve

	require.NoError(t, producer.ProduceCtx(ctx, "fynd-json-application-events-v2", []Message{
		{Value: []byte("no-headers"), Partition: -1},
		{
			Value:     []byte("caller-header"),
			Partition: -1,
			Headers:   []Header{{Key: "TraceParent", Value: []byte("caller-owned")}},
		},
	}, -1))
	parent.End()

	require.Len(t, produced, 2)
	require.Empty(t, produced[0].Headers, "Legacy KafkaJS production-order TraceClue record had no automatic headers")
	require.Equal(t, []ckafka.Header{{Key: "TraceParent", Value: []byte("caller-owned")}}, produced[1].Headers)
	producerSpans := exportedSpansNamed(exporter, "send ")
	require.Len(t, producerSpans, 2, "header suppression must not suppress producer observability")
	for _, span := range producerSpans {
		require.Equal(t, parent.SpanID(), span.Parent.SpanID().String())
	}
}
