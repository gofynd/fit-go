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

// Package kafka - confluent.go provides a real Kafka driver implementation using
// confluent-kafka-go (librdkafka wrapper). It satisfies the KafkaClient,
// KafkaProducer, and KafkaConsumer interfaces defined in kafka.go.
//
// This is the production driver for the fit Kafka integration, built on
// confluent-kafka-go (librdkafka) for performance and stability.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofynd/fit-go/logging"
)

// ---------------------------------------------------------------------------
// Compile-time interface checks
// ---------------------------------------------------------------------------

var (
	_ KafkaClient   = (*ConfluentClient)(nil)
	_ KafkaProducer = (*ConfluentProducer)(nil)
	_ KafkaConsumer = (*ConfluentConsumer)(nil)
)

// ---------------------------------------------------------------------------
// ConfluentClient
// ---------------------------------------------------------------------------

// ConfluentClient implements KafkaClient using the confluent-kafka-go library.
// It holds the resolved configuration and broker list, creating producers and
// consumers on demand.
type ConfluentClient struct {
	brokers []string
	fitCfg  *Config
	baseCfg *ckafka.ConfigMap
	logger  *logging.Logger

	mu     sync.Mutex
	closed bool
}

// NewConfluentClient creates a real Kafka client backed by confluent-kafka-go.
// It builds a ckafka.ConfigMap from the fit Config, including SASL, TLS,
// compression, and client ID settings. The client is ready to create producers
// and consumers but does not open any connections until Producer() or
// Consumer() is called.
func NewConfluentClient(cfg *Config) (*ConfluentClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("kafka/confluent: config must not be nil")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka/confluent: no brokers configured")
	}

	baseCfg, err := buildConfluentConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka/confluent: failed to build config: %w", err)
	}

	logger := cfg.Logger
	if logger == nil {
		l, lerr := logging.New(logging.Options{Level: "info"})
		if lerr != nil {
			return nil, fmt.Errorf("kafka/confluent: failed to create logger: %w", lerr)
		}
		logger = l
	}

	logger.Info("kafka/confluent: client created",
		"brokers", strings.Join(cfg.Brokers, ","),
		"clientId", cfg.ClientID,
		"compression", cfg.Compression,
	)

	return &ConfluentClient{
		brokers: cfg.Brokers,
		fitCfg:  cfg,
		baseCfg: baseCfg,
		logger:  logger,
	}, nil
}

// Producer creates a ConfluentProducer that satisfies the KafkaProducer
// interface. The producer is not connected until Connect() is called on it.
func (cc *ConfluentClient) Producer(config ProducerConfig) (KafkaProducer, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil, fmt.Errorf("kafka/confluent: client is closed")
	}

	// Clone the base config for producer-specific overrides.
	pCfg := cloneConfigMap(cc.baseCfg)

	// Apply producer-specific settings.
	if config.Acks != 0 {
		switch config.Acks {
		case -1:
			_ = pCfg.SetKey("acks", "all")
		default:
			_ = pCfg.SetKey("acks", fmt.Sprintf("%d", config.Acks))
		}
	}

	if config.Compression != CompressionNone {
		_ = pCfg.SetKey("compression.type", mapCompressionToString(config.Compression))
	}

	if config.IdempotentProducer {
		_ = pCfg.SetKey("enable.idempotence", true)
		_ = pCfg.SetKey("acks", "all")
		_ = pCfg.SetKey("max.in.flight.requests.per.connection", 1)
	}

	if config.Timeout > 0 {
		_ = pCfg.SetKey("request.timeout.ms", int(config.Timeout.Milliseconds()))
	}

	if config.MaxRetries > 0 {
		_ = pCfg.SetKey("message.send.max.retries", config.MaxRetries)
	}

	if config.RetryBackoff > 0 {
		_ = pCfg.SetKey("retry.backoff.ms", int(config.RetryBackoff.Milliseconds()))
	}

	return &ConfluentProducer{
		configMap: pCfg,
		logger:    cc.logger,
		brokers:   cc.brokers,
	}, nil
}

// Consumer creates a ConfluentConsumer that satisfies the KafkaConsumer
// interface. The consumer is not connected until Connect() is called on it.
func (cc *ConfluentClient) Consumer(config ConsumerConfig) (KafkaConsumer, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil, fmt.Errorf("kafka/confluent: client is closed")
	}

	if config.GroupID == "" {
		return nil, fmt.Errorf("kafka/confluent: consumer group ID is required")
	}

	// Clone the base config for consumer-specific overrides.
	cCfg := cloneConfigMap(cc.baseCfg)

	// The base config carries producer-only defaults (acks, compression) shared
	// with the producer path; drop them here so librdkafka doesn't log CONFWARN
	// about producer properties being ignored on a consumer instance.
	delete(*cCfg, "acks")
	delete(*cCfg, "compression.type")

	_ = cCfg.SetKey("group.id", config.GroupID)

	if config.SessionTimeout > 0 {
		_ = cCfg.SetKey("session.timeout.ms", int(config.SessionTimeout.Milliseconds()))
	}
	if config.HeartbeatInterval > 0 {
		_ = cCfg.SetKey("heartbeat.interval.ms", int(config.HeartbeatInterval.Milliseconds()))
	}
	if config.RebalanceTimeout > 0 {
		_ = cCfg.SetKey("max.poll.interval.ms", int(config.RebalanceTimeout.Milliseconds()))
	}
	if config.MaxBytesPerPartition > 0 {
		_ = cCfg.SetKey("max.partition.fetch.bytes", config.MaxBytesPerPartition)
	}
	if config.MinBytes > 0 {
		_ = cCfg.SetKey("fetch.min.bytes", config.MinBytes)
	}
	if config.MaxBytes > 0 {
		_ = cCfg.SetKey("fetch.max.bytes", config.MaxBytes)
	}
	if config.MaxWaitTime > 0 {
		_ = cCfg.SetKey("fetch.wait.max.ms", int(config.MaxWaitTime.Milliseconds()))
	}

	// Auto-commit settings.
	_ = cCfg.SetKey("enable.auto.commit", config.AutoCommit)
	if config.AutoCommitInterval > 0 {
		_ = cCfg.SetKey("auto.commit.interval.ms", int(config.AutoCommitInterval.Milliseconds()))
	}

	return &ConfluentConsumer{
		configMap: cCfg,
		groupID:   config.GroupID,
		config:    config,
		logger:    cc.logger,
	}, nil
}

// Close shuts down the confluent client and releases all resources.
func (cc *ConfluentClient) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil
	}
	cc.closed = true

	cc.logger.Info("kafka/confluent: client closed")
	return nil
}

// ---------------------------------------------------------------------------
// ConfluentProducer
// ---------------------------------------------------------------------------

// ConfluentProducer implements KafkaProducer using confluent-kafka-go's
// Producer. It sends messages synchronously by waiting for delivery reports.
type ConfluentProducer struct {
	configMap *ckafka.ConfigMap
	logger    *logging.Logger
	brokers   []string

	mu       sync.Mutex
	producer *ckafka.Producer
	closed   bool
}

// Connect establishes the confluent Producer connection to the brokers.
func (cp *ConfluentProducer) Connect() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.producer != nil {
		return nil
	}

	producer, err := ckafka.NewProducer(cp.configMap)
	if err != nil {
		return fmt.Errorf("kafka/confluent: producer connect failed: %w", err)
	}

	cp.producer = producer
	cp.logger.Info("kafka/confluent: producer connected",
		"brokers", strings.Join(cp.brokers, ","),
	)
	return nil
}

// Produce sends messages to a single topic. The acks parameter is configured
// at the producer level in confluent-kafka-go via the ConfigMap.
func (cp *ConfluentProducer) Produce(topic string, messages []Message, acks int) error {
	cp.mu.Lock()
	producer := cp.producer
	cp.mu.Unlock()

	if producer == nil {
		return fmt.Errorf("kafka/confluent: producer not connected")
	}

	deliveryChan := make(chan ckafka.Event, len(messages))
	defer close(deliveryChan)

	for _, msg := range messages {
		km := buildConfluentMessage(topic, msg)
		if err := producer.Produce(km, deliveryChan); err != nil {
			return fmt.Errorf("kafka/confluent: produce to %s failed: %w", topic, err)
		}
	}

	// Wait for all delivery reports.
	for i := 0; i < len(messages); i++ {
		e := <-deliveryChan
		m := e.(*ckafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("kafka/confluent: produce to %s failed: %w", topic, m.TopicPartition.Error)
		}
	}

	return nil
}

// ProduceBatch sends messages to multiple topics in one call.
func (cp *ConfluentProducer) ProduceBatch(topicMessages []TopicMessages, acks int) error {
	cp.mu.Lock()
	producer := cp.producer
	cp.mu.Unlock()

	if producer == nil {
		return fmt.Errorf("kafka/confluent: producer not connected")
	}

	totalMessages := 0
	for _, tm := range topicMessages {
		totalMessages += len(tm.Messages)
	}

	if totalMessages == 0 {
		return nil
	}

	deliveryChan := make(chan ckafka.Event, totalMessages)
	defer close(deliveryChan)

	for _, tm := range topicMessages {
		for _, msg := range tm.Messages {
			km := buildConfluentMessage(tm.Topic, msg)
			if err := producer.Produce(km, deliveryChan); err != nil {
				return fmt.Errorf("kafka/confluent: batch produce failed: %w", err)
			}
		}
	}

	// Wait for all delivery reports.
	for i := 0; i < totalMessages; i++ {
		e := <-deliveryChan
		m := e.(*ckafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("kafka/confluent: batch produce failed: %w", m.TopicPartition.Error)
		}
	}

	return nil
}

// ProduceCtx injects the active span's traceparent into each message's headers
// (when tracing is enabled and ctx carries a span), then delegates to Produce.
// Existing Produce callers are unaffected; this is the trace-propagating variant.
func (cp *ConfluentProducer) ProduceCtx(ctx context.Context, topic string, messages []Message, acks int) error {
	InjectTraceHeadersToMessages(ctx, messages)
	return cp.Produce(topic, messages, acks)
}

// ProduceBatchCtx is ProduceBatch with per-message traceparent injection.
func (cp *ConfluentProducer) ProduceBatchCtx(ctx context.Context, topicMessages []TopicMessages, acks int) error {
	for i := range topicMessages {
		InjectTraceHeadersToMessages(ctx, topicMessages[i].Messages)
	}
	return cp.ProduceBatch(topicMessages, acks)
}

// Close disconnects the producer gracefully by flushing pending messages.
func (cp *ConfluentProducer) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.producer == nil || cp.closed {
		return nil
	}

	cp.closed = true

	// Flush outstanding messages with a 15-second timeout.
	cp.producer.Flush(15 * 1000)
	cp.producer.Close()

	cp.logger.Info("kafka/confluent: producer closed")
	return nil
}

// ---------------------------------------------------------------------------
// ConfluentConsumer
// ---------------------------------------------------------------------------

// ConfluentConsumer implements KafkaConsumer using confluent-kafka-go's
// Consumer. It manages topic subscriptions, message dispatch, and graceful
// shutdown.
type ConfluentConsumer struct {
	configMap *ckafka.ConfigMap
	groupID   string
	config    ConsumerConfig
	logger    *logging.Logger

	mu       sync.Mutex
	consumer *ckafka.Consumer
	topics   []string
	cancelFn context.CancelFunc
	closed   bool
}

// Connect subscribes to the given topics by creating a confluent Consumer.
// Messages are not consumed until Consume() or ConsumeBatch() is called.
func (cc *ConfluentConsumer) Connect(topics []TopicConfig) error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.consumer != nil {
		return nil
	}

	// Set initial offset based on FromBeginning.
	if len(topics) > 0 && topics[0].FromBeginning {
		_ = cc.configMap.SetKey("auto.offset.reset", "earliest")
	} else {
		_ = cc.configMap.SetKey("auto.offset.reset", "latest")
	}

	consumer, err := ckafka.NewConsumer(cc.configMap)
	if err != nil {
		return fmt.Errorf("kafka/confluent: consumer group connect failed: %w", err)
	}

	// Extract topic names for subscription.
	names := make([]string, len(topics))
	for i, t := range topics {
		names[i] = t.Topic
	}

	// Opt-in only (default off = legacy fit.js subscribe-only behaviour): when
	// AutoCreateTopics is set, best-effort create any missing topics before
	// subscribing. Never blocks startup — failures are logged and we fall through
	// to subscribe.
	if cc.config.AutoCreateTopics {
		cc.ensureTopics(names)
	}

	// Subscribe with a rebalance callback for partition-assignment visibility and
	// optional app hooks. Once a RebalanceCb is supplied we own assign/unassign
	// (eager protocol), which the callback handles.
	if err := consumer.SubscribeTopics(names, cc.rebalanceCb); err != nil {
		consumer.Close()
		return fmt.Errorf("kafka/confluent: subscribe failed: %w", err)
	}

	cc.consumer = consumer
	cc.topics = names

	cc.logger.Info("kafka/confluent: consumer connected",
		"groupId", cc.groupID,
		"topics", strings.Join(names, ","),
	)
	return nil
}

// ensureTopics best-effort creates any of the given topics that don't yet exist,
// using the broker's default partitions/replication. All failures are logged and
// swallowed — topic creation must never block consumer startup.
func (cc *ConfluentConsumer) ensureTopics(names []string) {
	admin, err := ckafka.NewAdminClient(cloneConfigMap(cc.configMap))
	if err != nil {
		cc.logger.Warn("kafka/confluent: topic auto-create skipped (admin client)", "error", err.Error())
		return
	}
	defer admin.Close()

	specs := make([]ckafka.TopicSpecification, len(names))
	for i, n := range names {
		// -1 => use the broker's default num.partitions / replication factor.
		specs[i] = ckafka.TopicSpecification{Topic: n, NumPartitions: -1, ReplicationFactor: -1}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	results, err := admin.CreateTopics(ctx, specs)
	if err != nil {
		cc.logger.Warn("kafka/confluent: topic auto-create failed", "error", err.Error())
		return
	}
	for _, r := range results {
		switch r.Error.Code() {
		case ckafka.ErrNoError:
			cc.logger.Info("kafka/confluent: topic created", "topic", r.Topic)
		case ckafka.ErrTopicAlreadyExists:
			// already present — nothing to do
		default:
			cc.logger.Warn("kafka/confluent: topic create result", "topic", r.Topic, "error", r.Error.String())
		}
	}
}

// rebalanceCb drives partition assignment/revocation for the subscription.
// Supplying a RebalanceCb means we own assign/unassign (eager protocol). It logs
// the partition set and invokes the optional ConsumerConfig hooks.
func (cc *ConfluentConsumer) rebalanceCb(consumer *ckafka.Consumer, event ckafka.Event) error {
	// Use incremental (un)assign under the cooperative-sticky protocol and the
	// wholesale variants under the eager protocol (librdkafka default). Mixing
	// them corrupts the assignment, so branch on the negotiated protocol rather
	// than assuming eager.
	cooperative := consumer.GetRebalanceProtocol() == "COOPERATIVE"
	switch e := event.(type) {
	case ckafka.AssignedPartitions:
		var err error
		if cooperative {
			err = consumer.IncrementalAssign(e.Partitions)
		} else {
			err = consumer.Assign(e.Partitions)
		}
		if err != nil {
			// Surface it, don't swallow: returning nil tells librdkafka the rebalance
			// succeeded with no partitions assigned — a silent zombie consumer that
			// polls forever and reads nothing.
			cc.logger.Error("kafka/confluent: partition assign failed", "groupId", cc.groupID, "error", err.Error())
			return err
		}
		cc.logger.Info("kafka/confluent: partitions assigned",
			"groupId", cc.groupID, "partitions", formatPartitions(e.Partitions))
		if cc.config.OnPartitionsAssigned != nil {
			cc.config.OnPartitionsAssigned(toPartitionAssignments(e.Partitions))
		}
	case ckafka.RevokedPartitions:
		cc.logger.Info("kafka/confluent: partitions revoked",
			"groupId", cc.groupID, "partitions", formatPartitions(e.Partitions))
		if cc.config.OnPartitionsRevoked != nil {
			cc.config.OnPartitionsRevoked(toPartitionAssignments(e.Partitions))
		}
		var err error
		if cooperative {
			err = consumer.IncrementalUnassign(e.Partitions)
		} else {
			err = consumer.Unassign()
		}
		if err != nil {
			cc.logger.Error("kafka/confluent: partition unassign failed", "groupId", cc.groupID, "error", err.Error())
			return err
		}
	}
	return nil
}

// formatPartitions renders partitions as "topic[partition]" for logs.
func formatPartitions(parts []ckafka.TopicPartition) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := ""
		if p.Topic != nil {
			t = *p.Topic
		}
		out = append(out, fmt.Sprintf("%s[%d]", t, p.Partition))
	}
	return out
}

// toPartitionAssignments maps confluent partitions to the public type.
func toPartitionAssignments(parts []ckafka.TopicPartition) []PartitionAssignment {
	out := make([]PartitionAssignment, 0, len(parts))
	for _, p := range parts {
		t := ""
		if p.Topic != nil {
			t = *p.Topic
		}
		out = append(out, PartitionAssignment{Topic: t, Partition: p.Partition})
	}
	return out
}

// Consume processes messages one at a time via the handler. It blocks until
// the consumer is closed or an unrecoverable error occurs.
func (cc *ConfluentConsumer) Consume(handler MessageHandler, opts ConsumerOptions) error {
	cc.mu.Lock()
	consumer := cc.consumer
	cc.mu.Unlock()

	if consumer == nil {
		return fmt.Errorf("kafka/confluent: consumer not connected")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cc.mu.Lock()
	cc.cancelFn = cancel
	cc.mu.Unlock()
	defer cancel()

	isAutoCommit := resolveAutoCommit(cc.config.AutoCommit, opts.AutoCommit)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msg, err := consumer.ReadMessage(100 * time.Millisecond)
		if err != nil {
			// Timeout is expected when no messages are available.
			if kafkaErr, ok := err.(ckafka.Error); ok && kafkaErr.Code() == ckafka.ErrTimedOut {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("kafka/confluent: consume error: %w", err)
		}

		payload := mapConfluentToPayload(msg)

		if !isAutoCommit && opts.CommitBeforeHandler {
			if _, err := consumer.CommitMessage(msg); err != nil {
				cc.logger.Error("kafka/confluent: pre-handler commit failed",
					"topic", *msg.TopicPartition.Topic,
					"partition", msg.TopicPartition.Partition,
					"offset", msg.TopicPartition.Offset,
					"error", err,
				)
				return fmt.Errorf("kafka/confluent: pre-handler commit failed: %w", err)
			}
		}

		if err := handler(payload); err != nil {
			cc.logger.Error("kafka/confluent: message handler error",
				"topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", msg.TopicPartition.Offset,
				"error", err,
			)
			continue
		}

		if !isAutoCommit && !opts.CommitBeforeHandler {
			if _, err := consumer.CommitMessage(msg); err != nil {
				cc.logger.Error("kafka/confluent: commit failed",
					"topic", *msg.TopicPartition.Topic,
					"partition", msg.TopicPartition.Partition,
					"offset", msg.TopicPartition.Offset,
					"error", err,
				)
			}
		}
	}
}

// ConsumeCtx is Consume with a context-aware handler. It reuses the exact same
// consume loop, wrapping the handler so each message opens a Consumer span
// (parented to the producer's traceparent header) and the span context is threaded
// into the handler. Transparent passthrough when tracing is disabled.
func (cc *ConfluentConsumer) ConsumeCtx(handler MessageHandlerCtx, opts ConsumerOptions) error {
	return cc.Consume(TracedMessageHandlerCtx(handler), opts)
}

// ConsumeBatch processes messages in batches via the handler. It collects
// messages and delivers them as BatchPayload to the handler.
func (cc *ConfluentConsumer) ConsumeBatch(handler BatchHandler, opts ConsumerOptions) error {
	cc.mu.Lock()
	consumer := cc.consumer
	cc.mu.Unlock()

	if consumer == nil {
		return fmt.Errorf("kafka/confluent: consumer not connected")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cc.mu.Lock()
	cc.cancelFn = cancel
	cc.mu.Unlock()
	defer cancel()

	isAutoCommit := resolveAutoCommit(cc.config.AutoCommit, opts.AutoCommit)

	// Default batch size.
	const batchSize = 100
	const batchTimeout = 1 * time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		var batch []MessagePayload
		var lastMsg *ckafka.Message
		var batchTopic string
		var batchPartition int32

		deadline := time.Now().Add(batchTimeout)

		for len(batch) < batchSize && time.Now().Before(deadline) {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}

			msg, err := consumer.ReadMessage(remaining)
			if err != nil {
				if kafkaErr, ok := err.(ckafka.Error); ok && kafkaErr.Code() == ckafka.ErrTimedOut {
					break
				}
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("kafka/confluent: consume batch error: %w", err)
			}

			payload := mapConfluentToPayload(msg)
			batch = append(batch, payload)
			lastMsg = msg

			if len(batch) == 1 {
				batchTopic = payload.Topic
				batchPartition = int32(payload.Partition)
			}
		}

		if len(batch) == 0 {
			continue
		}

		batchPayload := BatchPayload{
			Topic:       batchTopic,
			Partition:   int(batchPartition),
			Messages:    batch,
			FirstOffset: batch[0].Offset,
			LastOffset:  batch[len(batch)-1].Offset,
		}

		if !isAutoCommit && opts.CommitBeforeHandler && lastMsg != nil {
			if _, err := consumer.CommitMessage(lastMsg); err != nil {
				cc.logger.Error("kafka/confluent: pre-handler batch commit failed",
					"topic", batchTopic,
					"partition", batchPartition,
					"firstOffset", batchPayload.FirstOffset,
					"lastOffset", batchPayload.LastOffset,
					"error", err,
				)
				return fmt.Errorf("kafka/confluent: pre-handler batch commit failed: %w", err)
			}
		}

		if err := handler(batchPayload); err != nil {
			cc.logger.Error("kafka/confluent: batch handler error",
				"topic", batchTopic,
				"partition", batchPartition,
				"firstOffset", batchPayload.FirstOffset,
				"lastOffset", batchPayload.LastOffset,
				"error", err,
			)
			return err
		}

		if !isAutoCommit && !opts.CommitBeforeHandler && lastMsg != nil {
			if _, err := consumer.CommitMessage(lastMsg); err != nil {
				cc.logger.Error("kafka/confluent: batch commit failed",
					"error", err,
				)
			}
		}
	}
}

// Close disconnects the consumer gracefully, cancelling any active
// consumption loop and leaving the consumer group.
func (cc *ConfluentConsumer) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil
	}
	cc.closed = true

	// Cancel the consumption context.
	if cc.cancelFn != nil {
		cc.cancelFn()
	}

	if cc.consumer != nil {
		if err := cc.consumer.Close(); err != nil {
			return fmt.Errorf("kafka/confluent: consumer close failed: %w", err)
		}
	}

	cc.logger.Info("kafka/confluent: consumer closed",
		"groupId", cc.groupID,
	)
	return nil
}

// ---------------------------------------------------------------------------
// InitDefault convenience function
// ---------------------------------------------------------------------------

// InitDefault creates a Client with a real confluent-kafka-go driver, fully
// connected. Go equivalent It resolves
// configuration, creates the client, and wires up the confluent driver so that
// Producer() and Consumer() calls on the returned Client use real Kafka
// connections.
//
// Usage:
//
//	client, err := kafka.InitDefault(nil) // resolve config from env
//	if err != nil { ... }
//	defer client.Driver.Close()
func InitDefault(cfg *Config) (*Client, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}

	driver, err := NewConfluentClient(client.Config)
	if err != nil {
		return nil, fmt.Errorf("kafka: confluent driver init failed: %w", err)
	}

	client.Driver = driver
	client.Logger.Info("kafka: initialized with confluent-kafka-go driver")
	return client, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// buildConfluentConfig translates a fit Config into a ckafka.ConfigMap.
func buildConfluentConfig(cfg *Config) (*ckafka.ConfigMap, error) {
	cm := &ckafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Brokers, ","),
	}

	// Client identity.
	if cfg.ClientID != "" {
		_ = cm.SetKey("client.id", cfg.ClientID)
	}

	// Producer defaults: acks=all.
	_ = cm.SetKey("acks", "all")

	// Compression: LZ4 by default.
	_ = cm.SetKey("compression.type", mapCompressionToString(cfg.Compression))

	// SASL configuration.
	if cfg.SASL != nil {
		_ = cm.SetKey("security.protocol", securityProtocol(cfg))
		_ = cm.SetKey("sasl.username", cfg.SASL.Username)
		_ = cm.SetKey("sasl.password", cfg.SASL.Password)
		_ = cm.SetKey("sasl.mechanism", mapSASLMechanismToString(cfg.SASL.Mechanism))
	} else if cfg.TLS != nil {
		_ = cm.SetKey("security.protocol", "SSL")
	}

	// TLS configuration.
	if cfg.TLS != nil {
		if cfg.TLS.CAFile != "" {
			_ = cm.SetKey("ssl.ca.location", cfg.TLS.CAFile)
		}
		if cfg.TLS.CertFile != "" {
			_ = cm.SetKey("ssl.certificate.location", cfg.TLS.CertFile)
		}
		if cfg.TLS.KeyFile != "" {
			_ = cm.SetKey("ssl.key.location", cfg.TLS.KeyFile)
		}
		// Match rejectUnauthorized: false
		_ = cm.SetKey("enable.ssl.certificate.verification", false)
	}

	return cm, nil
}

// securityProtocol determines the security.protocol value based on SASL and
// TLS config presence.
func securityProtocol(cfg *Config) string {
	if cfg.SASL != nil && cfg.TLS != nil {
		return "SASL_SSL"
	}
	if cfg.SASL != nil {
		return "SASL_PLAINTEXT"
	}
	if cfg.TLS != nil {
		return "SSL"
	}
	return "PLAINTEXT"
}

// mapCompressionToString converts a fit CompressionType to a librdkafka
// compression.type string.
func mapCompressionToString(ct CompressionType) string {
	switch ct {
	case CompressionGZIP:
		return "gzip"
	case CompressionSnappy:
		return "snappy"
	case CompressionLZ4:
		return "lz4"
	case CompressionZSTD:
		return "zstd"
	default:
		return "none"
	}
}

// mapSASLMechanismToString converts a string SASL mechanism name to the
// librdkafka sasl.mechanism value.
func mapSASLMechanismToString(mechanism string) string {
	switch strings.ToUpper(mechanism) {
	case "SCRAM-SHA-256":
		return "SCRAM-SHA-256"
	case "SCRAM-SHA-512":
		return "SCRAM-SHA-512"
	default:
		return "PLAIN"
	}
}

// buildConfluentMessage converts a fit Message to a ckafka.Message.
func buildConfluentMessage(topic string, msg Message) *ckafka.Message {
	km := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: ckafka.PartitionAny,
		},
		Value: msg.Value,
	}

	if len(msg.Key) > 0 {
		km.Key = msg.Key
	}

	if msg.Partition >= 0 {
		km.TopicPartition.Partition = int32(msg.Partition)
	}

	if !msg.Timestamp.IsZero() {
		km.Timestamp = msg.Timestamp
	}

	// Map headers.
	if len(msg.Headers) > 0 {
		headers := make([]ckafka.Header, len(msg.Headers))
		for i, h := range msg.Headers {
			headers[i] = ckafka.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
		km.Headers = headers
	}

	return km
}

// mapConfluentToPayload converts a ckafka.Message to a fit MessagePayload.
func mapConfluentToPayload(msg *ckafka.Message) MessagePayload {
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	payload := MessagePayload{
		Topic:     topic,
		Partition: int(msg.TopicPartition.Partition),
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Timestamp,
	}

	if len(msg.Headers) > 0 {
		headers := make([]Header, len(msg.Headers))
		for i, h := range msg.Headers {
			headers[i] = Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
		payload.Headers = headers
	}

	return payload
}

// cloneConfigMap creates a copy of a ckafka.ConfigMap. Since ConfigMap is a
// map[string]ConfigValue, we iterate and copy each key.
func cloneConfigMap(src *ckafka.ConfigMap) *ckafka.ConfigMap {
	clone := ckafka.ConfigMap{}
	for k, v := range *src {
		clone[k] = v
	}
	return &clone
}

// resolveAutoCommit determines the effective auto-commit setting. The
// per-run override (from ConsumerOptions) takes precedence over the
// consumer config default.
func resolveAutoCommit(configDefault bool, override *bool) bool {
	if override != nil {
		return *override
	}
	return configDefault
}
