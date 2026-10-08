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
	"errors"
	"sort"
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

// ExactOffsetCommit commits the supplied offset exactly. Unlike CommitMessage,
// it does not advance the value to the next offset. An OffsetFinalizer receives
// one callback per message and may invoke it at most once.
type ExactOffsetCommit func(offset int64) error

// TransientConsumerError identifies a broker transport or retriable Kafka
// protocol failure from a running consumer. Callers may use
// IsTransientConsumerError to restart the consume loop without treating a
// temporary broker outage as a fatal application error.
//
// The concrete cause remains available through errors.Is/errors.As. Handler
// errors are never converted to this type: the marker is reserved for
// consumer transport boundaries owned by fit-go.
type TransientConsumerError struct {
	cause error
}

func (e *TransientConsumerError) Error() string { return e.cause.Error() }
func (e *TransientConsumerError) Unwrap() error { return e.cause }

type consumerHandlerError struct {
	message string
	cause   error
	failed  *consumerRecordPosition
}

// consumerRewindError records the earliest offset that must be retried when a
// non-handler processing step (for example an offset finalizer or a
// post-handler commit) fails. It is deliberately internal: callers still see
// and can inspect the original error through errors.Is/errors.As.
type consumerRewindError struct {
	cause  error
	failed consumerRecordPosition
}

type consumerRecordPosition struct {
	topic       string
	partition   int32
	offset      int64
	leaderEpoch int32
}

func (e *consumerHandlerError) Error() string {
	if e.message == "" {
		return e.cause.Error()
	}
	return e.message + ": " + e.cause.Error()
}
func (e *consumerHandlerError) Unwrap() error { return e.cause }

func (e *consumerHandlerError) consumerRewindPositions() []consumerRecordPosition {
	if e.failed == nil {
		// A handler failure after CommitBeforeHandler has no work to rewind. The
		// empty, but authoritative, plan prevents the retry path from falling
		// back to the handler's already committed position.
		return nil
	}
	return []consumerRecordPosition{*e.failed}
}

func (e *consumerRewindError) Error() string { return e.cause.Error() }
func (e *consumerRewindError) Unwrap() error { return e.cause }
func (e *consumerRewindError) consumerRewindPositions() []consumerRecordPosition {
	return []consumerRecordPosition{e.failed}
}

func newConsumerHandlerError(message string, cause error) error {
	if cause == nil {
		return nil
	}
	return &consumerHandlerError{message: message, cause: cause}
}

func newConsumerHandlerErrorAt(message string, cause error, position consumerRecordPosition) error {
	if cause == nil {
		return nil
	}
	return &consumerHandlerError{message: message, cause: cause, failed: &position}
}

func isConsumerHandlerError(err error) bool {
	var handlerErr *consumerHandlerError
	return errors.As(err, &handlerErr)
}

func consumerHandlerPosition(err error) (consumerRecordPosition, bool) {
	var handlerErr *consumerHandlerError
	if !errors.As(err, &handlerErr) || handlerErr.failed == nil {
		return consumerRecordPosition{}, false
	}
	return *handlerErr.failed, true
}

// withConsumerRewindPosition attaches an exact retry boundary unless an inner
// layer has already made an authoritative rewind decision. This distinction is
// important for CommitBeforeHandler, whose correct decision is "rewind
// nothing" after the pre-handler commit succeeds.
func withConsumerRewindPosition(err error, position consumerRecordPosition) error {
	if err == nil {
		return nil
	}
	if _, decided := consumerRewindPositions(err); decided {
		return err
	}
	return &consumerRewindError{cause: err, failed: position}
}

// consumerRewindPositions returns the earliest requested offset per partition
// from a single error or an errors.Join tree. decided is true even when the
// authoritative plan is empty.
func consumerRewindPositions(err error) (positions []consumerRecordPosition, decided bool) {
	if err == nil {
		return nil, false
	}
	type rewindPlanner interface {
		consumerRewindPositions() []consumerRecordPosition
	}
	type multiUnwrapper interface {
		Unwrap() []error
	}
	type singleUnwrapper interface {
		Unwrap() error
	}

	add := func(position consumerRecordPosition) {
		for i := range positions {
			if positions[i].topic == position.topic && positions[i].partition == position.partition {
				if position.offset < positions[i].offset {
					positions[i] = position
				}
				return
			}
		}
		positions = append(positions, position)
	}
	var walk func(error)
	walk = func(current error) {
		if current == nil {
			return
		}
		if plan, ok := current.(rewindPlanner); ok {
			decided = true
			for _, position := range plan.consumerRewindPositions() {
				add(position)
			}
			return
		}
		if joined, ok := current.(multiUnwrapper); ok {
			for _, child := range joined.Unwrap() {
				walk(child)
			}
			return
		}
		if wrapped, ok := current.(singleUnwrapper); ok {
			walk(wrapped.Unwrap())
		}
	}
	walk(err)
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].topic != positions[j].topic {
			return positions[i].topic < positions[j].topic
		}
		return positions[i].partition < positions[j].partition
	})
	return positions, decided
}

// NewTransientConsumerError marks cause as a retryable running-consumer
// transport failure. Driver adapters should call this only after classifying
// the underlying network or Kafka protocol error. A nil cause remains nil and
// an already-marked error is returned unchanged.
func NewTransientConsumerError(cause error) error {
	if cause == nil || IsTransientConsumerError(cause) {
		return cause
	}
	return &TransientConsumerError{cause: cause}
}

// IsTransientConsumerError reports whether err contains a fit-go consumer
// transport failure that is safe for an application to retry. It deliberately
// relies on a typed boundary rather than error-message matching.
func IsTransientConsumerError(err error) bool {
	var transient *TransientConsumerError
	return errors.As(err, &transient)
}

// OffsetFinalizer owns an advanced manual-offset boundary after a message
// handler returns. handlerErr is the unmodified handler result. The finalizer
// may skip the commit, await commit(exactOffset), perform post-commit work, and
// then return the outcome whose precedence the legacy runtime requires.
// The supplied context is the same message-span context passed to ConsumeCtx's
// handler and remains active until the finalizer and any requested resolution
// complete.
//
// This hook exists for compatibility consumers whose behavior cannot be
// represented by automatic commit, commit-after-success or CommitBeforeHandler.
// New consumers should normally use the standard modes.
type OffsetFinalizer func(ctx context.Context, payload MessagePayload, handlerErr error, commit ExactOffsetCommit) error

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

// ConsumerConfig is the original comparable consumer-group contract.
type ConsumerConfig struct {
	GroupID              string
	SessionTimeout       time.Duration
	HeartbeatInterval    time.Duration
	RebalanceTimeout     time.Duration
	MaxBytesPerPartition int
	MinBytes             int
	MaxBytes             int
	MaxWaitTime          time.Duration
	RetryBackoff         time.Duration
	AutoCommit           bool
	AutoCommitInterval   time.Duration
	MaxPollInterval      time.Duration
}

// ConsumerAdvancedConfig adds migration-only backend, topic-creation,
// rebalance-hook, and shutdown controls without changing ConsumerConfig.
// Deprecated: use ConsumerSettings.
type ConsumerAdvancedConfig struct {
	// GroupID is the consumer group identifier (required).
	GroupID string

	// Backend selects the consumer implementation. The zero value retains the
	// production Confluent/librdkafka backend. FranzKafkaJS2Compat is an explicit
	// compatibility backend whose group protocol name matches KafkaJS 2.x's
	// literal "RoundRobinAssigner", allowing old and new members to overlap in
	// one group during a rolling cutover. All members in such a mixed group must
	// subscribe to the same topic set. franz-go and KafkaJS intentionally differ
	// in how their round-robin assigners treat heterogeneous subscriptions, so a
	// group with per-member topic interests is outside this compatibility mode.
	Backend ConsumerBackend

	// PartitionAssignmentStrategy selects the group protocol assignor advertised
	// to Kafka. Empty preserves the driver default. Librdkafka's "roundrobin"
	// protocol is not wire-compatible with KafkaJS 2.x's literal
	// "RoundRobinAssigner" protocol name; use ConsumerBackendFranzKafkaJS2Compat
	// for a mixed-runtime rolling cutover.
	PartitionAssignmentStrategy string

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

	// MaxPollInterval is the maximum delay between consumer poll invocations for
	// the Confluent/librdkafka backend. If the consumer does not poll within this
	// interval, librdkafka leaves the group and triggers a rebalance. Default: 5m
	// (300s). Mirrors Kafka's max.poll.interval.ms.
	//
	// The KafkaJS-compatible franz-go backend intentionally does not apply this
	// option: neither franz-go nor legacy KafkaJS exposes librdkafka's local
	// max-poll watchdog. That backend uses background heartbeats together with
	// SessionTimeout and RebalanceTimeout instead. MaxPollInterval must never be
	// translated to RebalanceTimeout because that would change JoinGroup wire
	// behavior and make mixed KafkaJS/franz-go rebalances wait longer.
	MaxPollInterval time.Duration

	// AutoCreateTopics, when true, best-effort creates any subscribed topics that
	// don't yet exist (broker defaults) before subscribing. Default false to match
	// legacy fit.js (subscribe-only) and avoid silently creating mistyped topics —
	// opt in only where the broker's auto-create is off and topics aren't
	// provisioned out of band.
	AutoCreateTopics bool

	// OnPartitionsAssigned, if set, is invoked after partitions are assigned to
	// this consumer during a group rebalance (visibility / app hook). Optional.
	// Do not call the consumer's Close method synchronously from this legacy hook;
	// use OnPartitionsAssignedWithLifecycle when the hook may request shutdown.
	OnPartitionsAssigned func([]PartitionAssignment)
	// OnPartitionsAssignedWithLifecycle is the shutdown-safe form of
	// OnPartitionsAssigned. lifecycle.Close requests shutdown without waiting for
	// the callback itself; an external consumer Close still waits for completion.
	OnPartitionsAssignedWithLifecycle func([]PartitionAssignment, RebalanceLifecycle)

	// OnPartitionsRevoked, if set, is invoked before partitions are revoked from
	// this consumer during a group rebalance. Optional. When automatic commits
	// are configured, fit-go performs the franz-go final marked-offset commit
	// before invoking this hook. Do not call the consumer's Close method
	// synchronously from this legacy hook; use the lifecycle form below.
	OnPartitionsRevoked func([]PartitionAssignment)
	// OnPartitionsRevokedWithLifecycle is the shutdown-safe form of
	// OnPartitionsRevoked.
	OnPartitionsRevokedWithLifecycle func([]PartitionAssignment, RebalanceLifecycle)

	// OnPartitionsLost is franz-go specific and reports assignments that were
	// lost without a safe final-commit boundary. It never triggers fit-go's final
	// revoke commit. Use the lifecycle form when the hook may request shutdown.
	OnPartitionsLost              func([]PartitionAssignment)
	OnPartitionsLostWithLifecycle func([]PartitionAssignment, RebalanceLifecycle)

	// ShutdownPolicy selects how the KafkaJS-compatible backend stops an active
	// consume run. The zero value preserves fit-go's existing behavior: cancel
	// the in-flight handler context, wait for it to return, then close the group
	// member. ConsumerShutdownDrainInFlight instead stops new polling while
	// allowing records already returned by PollRecords to finish their handler,
	// finalizer, and offset-commit boundary before the client is closed.
	//
	// This option is ignored by the Confluent backend. Drain mode is deliberately
	// opt-in because handlers that do not have a bounded completion path can make
	// Close wait indefinitely, just as an awaited KafkaJS disconnect can.
	ShutdownPolicy ConsumerShutdownPolicy
}

func advancedConsumerConfig(config ConsumerConfig) ConsumerAdvancedConfig {
	return ConsumerAdvancedConfig{
		GroupID: config.GroupID, SessionTimeout: config.SessionTimeout,
		HeartbeatInterval: config.HeartbeatInterval, RebalanceTimeout: config.RebalanceTimeout,
		MaxBytesPerPartition: config.MaxBytesPerPartition, MinBytes: config.MinBytes,
		MaxBytes: config.MaxBytes, MaxWaitTime: config.MaxWaitTime,
		RetryBackoff: config.RetryBackoff, AutoCommit: config.AutoCommit,
		AutoCommitInterval: config.AutoCommitInterval, MaxPollInterval: config.MaxPollInterval,
	}
}

// ConsumerConfig projects the advanced config onto the original contract.
func (config ConsumerAdvancedConfig) ConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		GroupID: config.GroupID, SessionTimeout: config.SessionTimeout,
		HeartbeatInterval: config.HeartbeatInterval, RebalanceTimeout: config.RebalanceTimeout,
		MaxBytesPerPartition: config.MaxBytesPerPartition, MinBytes: config.MinBytes,
		MaxBytes: config.MaxBytes, MaxWaitTime: config.MaxWaitTime,
		RetryBackoff: config.RetryBackoff, AutoCommit: config.AutoCommit,
		AutoCommitInterval: config.AutoCommitInterval, MaxPollInterval: config.MaxPollInterval,
	}
}

func (config ConsumerAdvancedConfig) hasAdvancedControls() bool {
	return config.Backend != ConsumerBackendConfluent ||
		config.PartitionAssignmentStrategy != "" || config.AutoCreateTopics ||
		config.OnPartitionsAssigned != nil || config.OnPartitionsAssignedWithLifecycle != nil ||
		config.OnPartitionsRevoked != nil || config.OnPartitionsRevokedWithLifecycle != nil ||
		config.OnPartitionsLost != nil || config.OnPartitionsLostWithLifecycle != nil ||
		config.ShutdownPolicy != ConsumerShutdownCancelInFlight
}

// RebalanceLifecycle is valid only for the duration of an advanced partition
// callback. Close requests consumer shutdown without waiting for that callback
// to return. This explicit scope avoids goroutine-identity heuristics: external
// calls to KafkaConsumer.Close remain synchronous.
type RebalanceLifecycle interface {
	Close() error
}

type rebalanceLifecycle struct {
	close func() error
}

func (l rebalanceLifecycle) Close() error {
	if l.close == nil {
		return nil
	}
	return l.close()
}

// ConsumerBackend selects a fit-go Kafka consumer implementation. Producers
// are unaffected: selecting a consumer backend never changes the client's
// producer driver or wire behavior.
type ConsumerBackend uint8

const (
	ConsumerBackendConfluent ConsumerBackend = iota
	ConsumerBackendFranzKafkaJS2Compat
)

// ConsumerShutdownPolicy controls the active-run shutdown boundary of the
// franz-go backend's KafkaJS 2.x compatibility consumer. Existing consumers
// retain cancel-in-flight semantics unless they explicitly opt in to draining.
type ConsumerShutdownPolicy uint8

const (
	// ConsumerShutdownCancelInFlight cancels the handler context before waiting
	// for the active run to return. This is the backward-compatible default.
	ConsumerShutdownCancelInFlight ConsumerShutdownPolicy = iota

	// ConsumerShutdownDrainInFlight stops new polling/admission, lets the current
	// polled records finish all handler/finalizer/commit work, and only then closes
	// the consumer. Handlers must therefore have their own bounded completion.
	ConsumerShutdownDrainInFlight
)

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

// DefaultConsumerAdvancedConfig returns the established consumer defaults in
// the advanced configuration shape.
// Deprecated: use DefaultConsumerSettings.
func DefaultConsumerAdvancedConfig(groupID string) ConsumerAdvancedConfig {
	return advancedConsumerConfig(DefaultConsumerConfig(groupID))
}

// ConsumerOptions is the original comparable per-run contract.
type ConsumerOptions struct {
	AutoCommit                     *bool
	PartitionsConsumedConcurrently int
	PollTimeout                    time.Duration
	MaxRecords                     int
}

// ConsumerAdvancedOptions adds migration-only commit/finalizer controls
// without changing ConsumerOptions.
// Deprecated: use ConsumeOptions.
type ConsumerAdvancedOptions struct {
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

	// OffsetFinalizer is an opt-in exact manual-offset boundary for message-mode
	// consumers. It runs once after every handler return, including failures, and
	// may invoke its exact-offset commit callback at most once. It requires manual
	// commit and cannot be combined with CommitBeforeHandler. Batch consumption
	// rejects this option.
	OffsetFinalizer OffsetFinalizer

	// ResolveAfterSuccessfulFinalizer performs a second, synchronous broker
	// commit of the next offset after the finalizer and handler succeed. Despite
	// its name, it does not merely resolve local consumer state and is not exact
	// KafkaJS finalizer parity: some legacy consumers commit only the physical
	// current offset.
	// It is ignored unless OffsetFinalizer is set.
	ResolveAfterSuccessfulFinalizer bool

	// NullOffsetCommitMetadata clears the Kafka offset-commit metadata on both
	// commits owned by OffsetFinalizer compatibility mode. KafkaJS commits null
	// metadata for its explicit current-offset commit and its subsequent resolved
	// N+1 commit, while franz-go and librdkafka attach the member id by default. This
	// is deliberately opt-in so existing fit-go consumers keep their driver-native
	// metadata. It requires OffsetFinalizer.
	NullOffsetCommitMetadata bool

	// RedeliverUnresolvedFinalizer rewinds the KafkaJS-compatible consumer to the
	// current physical record when OffsetFinalizer succeeds after a handler
	// failure without resolving that record. KafkaJS eachBatch immediately polls
	// such an unresolved offset again in the same process; franz-go otherwise
	// retains its already-advanced fetch position until a group rejoin. This is a
	// narrow migration compatibility option, ignored by other consumer backends
	// and unless OffsetFinalizer is configured.
	RedeliverUnresolvedFinalizer bool

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

func advancedConsumerOptions(opts ConsumerOptions) ConsumerAdvancedOptions {
	return ConsumerAdvancedOptions{
		AutoCommit:                     opts.AutoCommit,
		PartitionsConsumedConcurrently: opts.PartitionsConsumedConcurrently,
		PollTimeout:                    opts.PollTimeout,
		MaxRecords:                     opts.MaxRecords,
	}
}

// ConsumerOptions projects advanced run options onto the original contract.
func (opts ConsumerAdvancedOptions) ConsumerOptions() ConsumerOptions {
	return ConsumerOptions{
		AutoCommit:                     opts.AutoCommit,
		PartitionsConsumedConcurrently: opts.PartitionsConsumedConcurrently,
		PollTimeout:                    opts.PollTimeout,
		MaxRecords:                     opts.MaxRecords,
	}
}

func (opts ConsumerAdvancedOptions) hasAdvancedControls() bool {
	return opts.CommitBeforeHandler || opts.OffsetFinalizer != nil ||
		opts.ResolveAfterSuccessfulFinalizer || opts.NullOffsetCommitMetadata ||
		opts.RedeliverUnresolvedFinalizer
}
