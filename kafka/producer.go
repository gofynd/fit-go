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

// Package kafka provides Kafka message types for producing messages.
package kafka

import "time"

// ProducerPartitioner selects the driver partitioning strategy for records
// without an explicit partition. The zero value preserves the driver's
// existing default.
type ProducerPartitioner string

const (
	// ProducerPartitionerDefault preserves librdkafka's configured/default
	// partitioner.
	ProducerPartitionerDefault ProducerPartitioner = ""
	// ProducerPartitionerKafkaJSLegacy selects librdkafka's Java-compatible
	// murmur2_random partitioner. For keyed records this matches KafkaJS 2.2.4's
	// legacy/default murmur2 selection. Keyless KafkaJS round-robin behavior is
	// intentionally outside this option's compatibility guarantee.
	ProducerPartitionerKafkaJSLegacy ProducerPartitioner = "kafkajs-legacy"
)

// ProducerTraceHeaderPolicy controls automatic trace propagation performed by
// fit-go producer entry points. The zero value preserves the existing behavior.
type ProducerTraceHeaderPolicy uint8

const (
	// ProducerTraceHeadersInject creates producer spans and injects configured
	// propagation headers into each record.
	ProducerTraceHeadersInject ProducerTraceHeaderPolicy = iota
	// ProducerTraceHeadersPreserve creates the same producer spans but leaves
	// caller-provided record headers byte-for-byte unchanged.
	ProducerTraceHeadersPreserve
)

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

// Header is a single Kafka message header (key-value pair).
type Header struct {
	Key   string
	Value []byte
}

// Message represents a single Kafka message to be produced.
// Mirrors the Message interface used.
type Message struct {
	// Key is the optional partition key. When set, messages with the same
	// key are routed to the same partition.
	Key []byte

	// Value is the message payload.
	Value []byte

	// Headers are optional key-value metadata attached to the message.
	Headers []Header

	// Partition is an optional explicit partition override. A negative value
	// means the partitioner selects the partition automatically.
	Partition int

	// Timestamp is the message timestamp. Zero value means the broker
	// assigns a timestamp.
	Timestamp time.Time
}

// TopicMessages groups messages destined for a single topic.
// Used by ProduceBatch to send to multiple topics in one call.
// Mirrors the TopicMessages interface.
type TopicMessages struct {
	Topic    string
	Messages []Message
}

// RecordMetadata is returned once per topic/partition acknowledged by a produce
// request, matching KafkaJS-style grouped delivery metadata.
type RecordMetadata struct {
	// Topic and Offset are kept for Go callers that want typed access. They are
	// not serialized because legacy fit.js/KafkaJS exposes topicName/baseOffset
	// in HTTP responses.
	Topic  string `json:"-"`
	Offset int64  `json:"-"`

	TopicName      string `json:"topicName"`
	Partition      int    `json:"partition"`
	ErrorCode      int    `json:"errorCode"`
	BaseOffset     string `json:"baseOffset,omitempty"`
	LogAppendTime  string `json:"logAppendTime,omitempty"`
	LogStartOffset string `json:"logStartOffset,omitempty"`
}

// ---------------------------------------------------------------------------
// Producer configuration
// ---------------------------------------------------------------------------

// ProducerConfig holds settings for a Kafka producer.
// Mirrors the ProducerConfig options.
type ProducerConfig struct {
	// Acks is the default acknowledgement level:
	// 0 = fire-and-forget, 1 = leader only, -1 = all ISR (default).
	Acks int
	// AcksSet distinguishes an explicit fire-and-forget value (Acks=0) from
	// the zero value of ProducerConfig, which inherits the safe default (-1).
	// Non-zero Acks values remain implicitly set for backward compatibility.
	AcksSet bool

	// IdempotentProducer enables exactly-once semantics when true.
	IdempotentProducer bool

	// Timeout is the maximum time to wait for a produce request.
	Timeout time.Duration

	// Compression overrides the client-level compression setting.
	// Zero value inherits from the Client.
	Compression CompressionType

	// MaxRetries is the number of times a failed produce request is retried.
	MaxRetries int

	// RetryBackoff is the delay between retries.
	RetryBackoff time.Duration

	// RetryBackoffMax is the maximum retry delay. It is separate from
	// RetryBackoff because librdkafka defaults retry.backoff.max.ms to one
	// second, which otherwise caps larger explicitly configured initial delays.
	// Zero preserves librdkafka's default.
	RetryBackoffMax time.Duration

	// Partitioner optionally selects a compatibility partitioner. Zero preserves
	// the existing librdkafka default.
	Partitioner ProducerPartitioner

	// TraceHeaderPolicy controls only automatic record-header injection. Producer
	// spans are still created when headers are preserved. Zero preserves fit-go's
	// existing automatic injection behavior.
	TraceHeaderPolicy ProducerTraceHeaderPolicy
}
