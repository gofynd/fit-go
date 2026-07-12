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

// Package kafka provides Kafka consumer types and configuration.
package kafka

import (
	"context"
	"time"
)

// ---------------------------------------------------------------------------
// Handler types
// ---------------------------------------------------------------------------

// MessagePayload is the payload delivered to a MessageHandler for each
// individual message. It mirrors the EachMessagePayload.
type MessagePayload struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// BatchPayload is the payload delivered to a BatchHandler for each batch.
// It mirrors the EachBatchPayload.
type BatchPayload struct {
	Topic     string
	Partition int
	Messages  []MessagePayload

	// FirstOffset and LastOffset are the offset range of the batch,
	// populated automatically from Messages for logging convenience.
	FirstOffset int64
	LastOffset  int64
}

// MessageHandler processes a single consumed message.
// Return an error to signal processing failure (the consumer will handle
// retry/dead-letter based on its configuration).
type MessageHandler func(payload MessagePayload) error

// MessageHandlerCtx is like MessageHandler but receives a context.Context that
// carries the per-message consumer trace span. ConsumeCtx extracts the W3C
// traceparent from the message headers, opens the consumer span, and passes the
// span-bearing context here — so handler logs and any downstream DB/HTTP spans
// join the producer's trace. Use this instead of MessageHandler when you want
// consumer-side trace correlation.
type MessageHandlerCtx func(ctx context.Context, payload MessagePayload) error

// BatchHandler processes a batch of consumed messages.
// Return an error to signal processing failure.
type BatchHandler func(payload BatchPayload) error

// BatchHandlerCtx is the context-aware batch handler. The context carries the
// batch receive span; each message's extracted producer context is represented
// as a link on its corresponding process span, matching KafkaJS eachBatch
// instrumentation without falsely parenting the batch to its first message.
type BatchHandlerCtx func(ctx context.Context, payload BatchPayload) error

// ---------------------------------------------------------------------------
// Topic configuration
// ---------------------------------------------------------------------------

// TopicConfig describes a topic subscription. It mirrors the
// ConsumerSubscribeTopics interface.
type TopicConfig struct {
	// Topic is the Kafka topic name.
	Topic string

	// FromBeginning controls whether the consumer starts from the earliest
	// offset (true) or the latest (false) when no committed offset exists.
	FromBeginning bool
}

// ---------------------------------------------------------------------------
// Consumer configuration
// ---------------------------------------------------------------------------

// ConsumerConfig holds settings for a Kafka consumer group.
// Mirrors the ConsumerConfig options.
type ConsumerConfig struct {
	// GroupID is the consumer group identifier (required).
	GroupID string

	// SessionTimeout is the timeout for detecting consumer failures within
	// the group. Default: 30s.
	SessionTimeout time.Duration

	// HeartbeatInterval is how often the consumer sends heartbeats to the
	// broker. Must be less than SessionTimeout. Default: 3s.
	HeartbeatInterval time.Duration

	// RebalanceTimeout is the maximum time allowed for a rebalance.
	// Default: 60s.
	RebalanceTimeout time.Duration

	// MaxBytesPerPartition is the maximum amount of data per partition the
	// broker returns in a fetch request. Default: 1MB.
	MaxBytesPerPartition int

	// MinBytes is the minimum amount of data the broker should return for
	// a fetch request. Default: 1 byte.
	MinBytes int

	// MaxBytes is the maximum amount of data the broker returns for a fetch
	// request across all partitions. Default: 10MB.
	MaxBytes int

	// MaxWaitTime is the maximum time the broker waits before responding to
	// a fetch request if MinBytes is not yet satisfied. Default: 5s.
	MaxWaitTime time.Duration

	// RetryBackoff is the delay before retrying a failed fetch. Default: 100ms.
	RetryBackoff time.Duration

	// AutoCommit controls automatic offset committing. DefaultConsumerConfig sets
	// this to true; a literal ConsumerConfig{} leaves it false, so callers that build
	// configs manually should set this explicitly when matching legacy behavior.
	AutoCommit bool

	// AutoCommitInterval is the interval between automatic offset commits.
	// Default: 5s.
	AutoCommitInterval time.Duration

	// MaxPollInterval is the maximum delay between consumer poll invocations.
	// If the consumer does not poll within this interval, the broker considers
	// it dead and triggers a rebalance. Default: 5m (300s).
	// Mirrors Kafka's max.poll.interval.ms.
	MaxPollInterval time.Duration

	// AutoCreateTopics, when true, best-effort creates any subscribed topics that
	// don't yet exist (broker defaults) before subscribing. Default false to match
	// legacy fit.js (subscribe-only) and avoid silently creating mistyped topics —
	// opt in only where the broker's auto-create is off and topics aren't
	// provisioned out of band.
	AutoCreateTopics bool

	// OnPartitionsAssigned, if set, is invoked after partitions are assigned to
	// this consumer during a group rebalance (visibility / app hook). Optional.
	OnPartitionsAssigned func([]PartitionAssignment)

	// OnPartitionsRevoked, if set, is invoked before partitions are revoked from
	// this consumer during a group rebalance. Optional.
	OnPartitionsRevoked func([]PartitionAssignment)
}

// PartitionAssignment identifies a single topic-partition assigned to or revoked
// from a consumer during a rebalance.
type PartitionAssignment struct {
	Topic     string
	Partition int32
}

// DefaultConsumerConfig returns a ConsumerConfig with sensible defaults that
// match the defaults used.
func DefaultConsumerConfig(groupID string) ConsumerConfig {
	return ConsumerConfig{
		GroupID:              groupID,
		SessionTimeout:       30 * time.Second,
		HeartbeatInterval:    3 * time.Second,
		RebalanceTimeout:     60 * time.Second,
		MaxBytesPerPartition: 1 << 20, // 1 MB
		MinBytes:             1,
		MaxBytes:             10 << 20, // 10 MB
		MaxWaitTime:          5 * time.Second,
		RetryBackoff:         100 * time.Millisecond,
		AutoCommit:           true,
		AutoCommitInterval:   5 * time.Second,
		MaxPollInterval:      5 * time.Minute,
	}
}

// ConsumerOptions holds per-run options passed to Consume/ConsumeBatch.
// Mirrors the ConsumerRunConfig.
type ConsumerOptions struct {
	// AutoCommit requests the offset mode for this run. nil means use the
	// ConsumerConfig value. The Confluent driver disables automatic offset storage,
	// so either value resolves offsets only after a successful handler. When the
	// construction-time mode is automatic, its configured interval is retained;
	// the opposite run-time mode falls back to a synchronous commit.
	AutoCommit *bool

	// CommitBeforeHandler commits the consumed offset before invoking the message
	// handler when manual commits are enabled. This is an explicit opt-in for
	// legacy at-most-once consumers whose handlers perform non-idempotent external
	// side effects. It is ignored when auto-commit is enabled.
	CommitBeforeHandler bool

	// PartitionsConsumedConcurrently is the requested number of partitions
	// processed concurrently. Default: 1 (sequential). The Confluent driver keeps
	// records from one topic-partition ordered and runs independent partition groups
	// in parallel up to this limit.
	PartitionsConsumedConcurrently int

	// PollTimeout is how long each poll call waits for new messages before
	// returning an empty batch. Default: 0 means 100ms for message mode and 1s
	// for batch mode.
	PollTimeout time.Duration

	// MaxRecords limits the number of records returned per poll. Default: 0
	// means use the driver default.
	MaxRecords int
}
