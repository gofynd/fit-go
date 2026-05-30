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

// RecordMetadata is returned for each message after a successful produce.
type RecordMetadata struct {
	Topic     string
	Partition int
	Offset    int64
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
}
