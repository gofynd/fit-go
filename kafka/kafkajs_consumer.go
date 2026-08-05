package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/gofynd/fit-go/logging"
)

var (
	_ KafkaConsumer         = (*kafkaJSCompatibleConsumer)(nil)
	_ KafkaBatchConsumerCtx = (*kafkaJSCompatibleConsumer)(nil)
)

// kafkaJSRoundRobinBalancer delegates the standard round-robin plan and wire
// metadata to franz-go, changing only the protocol identifier. KafkaJS 2.x
// advertises the literal "RoundRobinAssigner" rather than the conventional
// "roundrobin" used by librdkafka, so the latter cannot coexist in the group.
type kafkaJSRoundRobinBalancer struct{ kgo.GroupBalancer }

func (kafkaJSRoundRobinBalancer) ProtocolName() string { return "RoundRobinAssigner" }

type kafkaJSCompatibleConsumer struct {
	brokers []string
	fitCfg  *Config
	config  ConsumerConfig
	logger  *logging.Logger

	mu        sync.Mutex
	client    *kgo.Client
	topics    []TopicConfig
	cancelRun context.CancelFunc
	runDone   chan struct{}
	closed    bool
	closeErr  error
}

func newKafkaJSCompatibleConsumer(
	brokers []string,
	fitCfg *Config,
	config ConsumerConfig,
	logger *logging.Logger,
) (KafkaConsumer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka/kafkajs: no brokers configured")
	}
	if fitCfg == nil {
		return nil, fmt.Errorf("kafka/kafkajs: client config is required")
	}
	if config.PartitionAssignmentStrategy != "" {
		return nil, fmt.Errorf("kafka/kafkajs: PartitionAssignmentStrategy must be empty when the KafkaJS-compatible backend is selected")
	}
	return &kafkaJSCompatibleConsumer{
		brokers: append([]string(nil), brokers...),
		fitCfg:  fitCfg,
		config:  config,
		logger:  logger,
	}, nil
}

func (c *kafkaJSCompatibleConsumer) Connect(topics []TopicConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("kafka/kafkajs: consumer is closed")
	}
	if c.client != nil {
		return nil
	}
	if len(topics) == 0 {
		return fmt.Errorf("kafka/kafkajs: at least one topic is required")
	}
	names := make([]string, len(topics))
	for i, topic := range topics {
		if strings.TrimSpace(topic.Topic) == "" {
			return fmt.Errorf("kafka/kafkajs: topic name is required")
		}
		names[i] = topic.Topic
	}
	opts, err := c.clientOptions(topics)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka/kafkajs: consumer group connect failed: %w", err)
	}
	c.client = client
	c.topics = append([]TopicConfig(nil), topics...)
	c.logger.Info("kafka/kafkajs: consumer connected",
		"groupId", c.config.GroupID,
		"topics", strings.Join(names, ","),
		"protocol", "RoundRobinAssigner",
	)
	return nil
}

func (c *kafkaJSCompatibleConsumer) clientOptions(topics []TopicConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.brokers...),
		kgo.ClientID(c.fitCfg.ClientID),
		kgo.ConsumerGroup(c.config.GroupID),
		kgo.ConsumeTopics(topicNames(topics)...),
		kgo.Balancers(kafkaJSRoundRobinBalancer{kgo.RoundRobinBalancer()}),
		kgo.BlockRebalanceOnPoll(),
	}
	// Manual consumers must disable franz-go's background auto-commit loop.
	// AutoCommitMarks only changes which offsets that loop commits; on its own
	// it does not turn the loop off and can advance a polled-but-unhandled
	// record. Promotions uses OffsetFinalizer and exact synchronous commits, so
	// this option is required for at-least-once parity with KafkaJS eachBatch.
	if !c.config.AutoCommit {
		opts = append(opts, kgo.DisableAutoCommit())
	} else {
		opts = append(opts, kgo.AutoCommitMarks())
	}
	if topics[0].FromBeginning {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if c.config.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(c.config.SessionTimeout))
	}
	if c.config.HeartbeatInterval > 0 {
		opts = append(opts, kgo.HeartbeatInterval(c.config.HeartbeatInterval))
	}
	if c.config.RebalanceTimeout > 0 {
		opts = append(opts, kgo.RebalanceTimeout(c.config.RebalanceTimeout))
	}
	if c.config.MaxBytesPerPartition > 0 {
		opts = append(opts, kgo.FetchMaxPartitionBytes(int32(c.config.MaxBytesPerPartition)))
	}
	if c.config.MinBytes > 0 {
		opts = append(opts, kgo.FetchMinBytes(int32(c.config.MinBytes)))
	}
	if c.config.MaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(int32(c.config.MaxBytes)))
	}
	if c.config.MaxWaitTime > 0 {
		opts = append(opts, kgo.FetchMaxWait(c.config.MaxWaitTime))
	}
	if c.config.RetryBackoff > 0 {
		opts = append(opts, kgo.RetryBackoffFn(func(int) time.Duration { return c.config.RetryBackoff }))
	}
	if c.config.AutoCommitInterval > 0 {
		opts = append(opts, kgo.AutoCommitInterval(c.config.AutoCommitInterval))
	}
	if c.config.AutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	if c.fitCfg.TLS != nil {
		tlsConfig, err := c.fitCfg.TLS.BuildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("kafka/kafkajs: TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if c.fitCfg.SASL != nil {
		mechanism, err := kafkaJSCompatibleSASL(c.fitCfg.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}
	opts = append(opts,
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			parts := flattenAssignments(assigned)
			c.logger.Info("kafka/kafkajs: partitions assigned", "groupId", c.config.GroupID, "partitions", formatAssignments(parts))
			if c.config.OnPartitionsAssigned != nil {
				c.config.OnPartitionsAssigned(parts)
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			parts := flattenAssignments(revoked)
			c.logger.Info("kafka/kafkajs: partitions revoked", "groupId", c.config.GroupID, "partitions", formatAssignments(parts))
			if c.config.OnPartitionsRevoked != nil {
				c.config.OnPartitionsRevoked(parts)
			}
		}),
	)
	return opts, nil
}

func topicNames(topics []TopicConfig) []string {
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = topic.Topic
	}
	return names
}

func kafkaJSCompatibleSASL(config *SASLConfig) (sasl.Mechanism, error) {
	authPlain := plain.Auth{User: config.Username, Pass: config.Password}
	switch strings.ToUpper(strings.TrimSpace(config.Mechanism)) {
	case "", "PLAIN":
		return authPlain.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: config.Username, Pass: config.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: config.Username, Pass: config.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("kafka/kafkajs: unsupported SASL mechanism %q", config.Mechanism)
	}
}

func flattenAssignments(assignments map[string][]int32) []PartitionAssignment {
	result := make([]PartitionAssignment, 0)
	for topic, partitions := range assignments {
		for _, partition := range partitions {
			result = append(result, PartitionAssignment{Topic: topic, Partition: partition})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Topic == result[j].Topic {
			return result[i].Partition < result[j].Partition
		}
		return result[i].Topic < result[j].Topic
	})
	return result
}

func formatAssignments(assignments []PartitionAssignment) []string {
	result := make([]string, len(assignments))
	for i, assignment := range assignments {
		result[i] = fmt.Sprintf("%s[%d]", assignment.Topic, assignment.Partition)
	}
	return result
}

func (c *kafkaJSCompatibleConsumer) Consume(handler MessageHandler, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: message handler is nil")
	}
	return c.consumeMessages(func(ctx context.Context, payload MessagePayload) error {
		return runTracedMessageHandler(ctx, payload, func(_ context.Context, traced MessagePayload) error { return handler(traced) })
	}, opts)
}

func (c *kafkaJSCompatibleConsumer) ConsumeCtx(handler MessageHandlerCtx, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: message handler is nil")
	}
	return c.consumeMessages(func(ctx context.Context, payload MessagePayload) error {
		return runTracedMessageHandler(ctx, payload, handler)
	}, opts)
}

type kafkaJSMessageHandler func(context.Context, MessagePayload) error

func (c *kafkaJSCompatibleConsumer) consumeMessages(handler kafkaJSMessageHandler, opts ConsumerOptions) error {
	isAutoCommit, pollTimeout, concurrency, err := validateConfluentConsumerOptions(c.config.AutoCommit, opts, 100*time.Millisecond)
	if err != nil {
		return err
	}
	client, ctx, finish, err := c.beginRun()
	if err != nil {
		return err
	}
	defer finish()
	maxRecords := opts.MaxRecords
	if maxRecords <= 0 {
		maxRecords = concurrency
	}
	for {
		fetches, err := pollKafkaJSRecords(ctx, client, pollTimeout, maxRecords)
		if err != nil {
			return c.prepareTransientRunRetry(client, err)
		}
		if ctx.Err() != nil {
			return nil
		}
		groups := groupKafkaJSRecords(fetches.Records())
		if err = runKafkaJSRecordGroups(ctx, groups, concurrency, func(group []*kgo.Record) error {
			for _, record := range group {
				if err := c.processRecord(ctx, client, record, handler, isAutoCommit, opts); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			client.AllowRebalance()
			return c.prepareTransientRunRetry(client, err)
		}
		client.AllowRebalance()
	}
}

func pollKafkaJSRecords(ctx context.Context, client *kgo.Client, timeout time.Duration, maxRecords int) (kgo.Fetches, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fetches := client.PollRecords(pollCtx, maxRecords)
	if ctx.Err() != nil {
		return fetches, nil
	}
	for _, fetchErr := range fetches.Errors() {
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}
		return fetches, classifyKafkaJSTransientConsumerError(
			fmt.Errorf("kafka/kafkajs: consume error: %w", fetchErr.Err),
		)
	}
	return fetches, nil
}

func groupKafkaJSRecords(records []*kgo.Record) [][]*kgo.Record {
	index := make(map[string]int)
	groups := make([][]*kgo.Record, 0)
	for _, record := range records {
		key := fmt.Sprintf("%s\x00%d", record.Topic, record.Partition)
		groupIndex, found := index[key]
		if !found {
			groupIndex = len(groups)
			index[key] = groupIndex
			groups = append(groups, nil)
		}
		groups[groupIndex] = append(groups[groupIndex], record)
	}
	return groups
}

func runKafkaJSRecordGroups(ctx context.Context, groups [][]*kgo.Record, concurrency int, process func([]*kgo.Record) error) error {
	if len(groups) == 0 {
		return nil
	}
	if concurrency < 1 {
		concurrency = 1
	}
	sem := make(chan struct{}, concurrency)
	errs := make(chan error, len(groups))
	var wg sync.WaitGroup
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if err := process(group); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func (c *kafkaJSCompatibleConsumer) processRecord(
	ctx context.Context,
	client *kgo.Client,
	record *kgo.Record,
	handler kafkaJSMessageHandler,
	isAutoCommit bool,
	opts ConsumerOptions,
) error {
	payload := kafkaJSPayload(record)
	if !isAutoCommit && opts.CommitBeforeHandler {
		if err := client.CommitRecords(ctx, record); err != nil {
			return classifyKafkaJSTransientConsumerError(
				fmt.Errorf("kafka/kafkajs: pre-handler commit failed: %w", err),
			)
		}
	}
	handlerErr := handler(ctx, payload)
	if opts.OffsetFinalizer != nil {
		commitCalled := false
		commitExact := func(exact int64) error {
			if commitCalled {
				return fmt.Errorf("kafka/kafkajs: exact offset commit callback called more than once")
			}
			commitCalled = true
			return commitKafkaJSExact(ctx, client, c.config.GroupID, record, exact, opts.NullOffsetCommitMetadata)
		}
		finalizerErr := opts.OffsetFinalizer(ctx, payload, handlerErr, commitExact)
		if finalizerErr != nil {
			return finalizerErr
		}
		if handlerErr == nil && opts.ResolveAfterSuccessfulFinalizer {
			return resolveKafkaJSRecord(ctx, client, c.config.GroupID, record, isAutoCommit, opts.NullOffsetCommitMetadata)
		}
		return nil
	}
	if handlerErr != nil {
		return fmt.Errorf("kafka/kafkajs: message handler failed: %w", handlerErr)
	}
	if !isAutoCommit && opts.CommitBeforeHandler {
		return nil
	}
	return resolveKafkaJSRecord(ctx, client, c.config.GroupID, record, isAutoCommit, false)
}

func kafkaJSPayload(record *kgo.Record) MessagePayload {
	headers := make([]Header, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return MessagePayload{
		Topic: record.Topic, Partition: int(record.Partition), Offset: record.Offset,
		Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...),
		Headers: headers, Timestamp: record.Timestamp,
	}
}

func resolveKafkaJSRecord(
	ctx context.Context,
	client *kgo.Client,
	groupID string,
	record *kgo.Record,
	auto bool,
	nullMetadata bool,
) error {
	if auto {
		client.MarkCommitRecords(record)
		return nil
	}
	if nullMetadata {
		return commitKafkaJSExact(ctx, client, groupID, record, record.Offset+1, true)
	}
	if err := client.CommitRecords(ctx, record); err != nil {
		return classifyKafkaJSTransientConsumerError(
			fmt.Errorf("kafka/kafkajs: post-handler commit failed: %w", err),
		)
	}
	return nil
}

func commitKafkaJSExact(
	ctx context.Context,
	client *kgo.Client,
	groupID string,
	record *kgo.Record,
	exact int64,
	nullMetadata bool,
) error {
	if nullMetadata {
		memberID, generation := client.GroupMetadata()
		request := newKafkaJSOffsetCommitRequest(groupID, memberID, generation, record, exact)
		response, err := request.RequestWith(ctx, client)
		if err != nil {
			return classifyKafkaJSTransientConsumerError(
				fmt.Errorf("kafka/kafkajs: exact offset commit failed: %w", err),
			)
		}
		if err := kafkaJSOffsetCommitResponseError(response); err != nil {
			return classifyKafkaJSTransientConsumerError(
				fmt.Errorf("kafka/kafkajs: exact offset commit failed: %w", err),
			)
		}
		return nil
	}
	offsets := map[string]map[int32]kgo.EpochOffset{
		record.Topic: {record.Partition: {Epoch: -1, Offset: exact}},
	}
	var commitErr error
	client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
		if commitErr == nil && response != nil {
			for _, topic := range response.Topics {
				for _, partition := range topic.Partitions {
					if partition.ErrorCode != 0 {
						commitErr = fmt.Errorf("broker rejected exact offset commit with error code %d", partition.ErrorCode)
						return
					}
				}
			}
		}
	})
	if commitErr != nil {
		return classifyKafkaJSTransientConsumerError(
			fmt.Errorf("kafka/kafkajs: exact offset commit failed: %w", commitErr),
		)
	}
	return nil
}

func newKafkaJSOffsetCommitRequest(
	groupID string,
	memberID string,
	generation int32,
	record *kgo.Record,
	offset int64,
) *kmsg.OffsetCommitRequest {
	request := kmsg.NewPtrOffsetCommitRequest()
	request.Group = groupID
	request.Generation = generation
	request.MemberID = memberID
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = record.Topic
	partition := kmsg.NewOffsetCommitRequestTopicPartition()
	partition.Partition = record.Partition
	partition.Offset = offset
	partition.LeaderEpoch = -1
	partition.Metadata = nil
	topic.Partitions = append(topic.Partitions, partition)
	request.Topics = append(request.Topics, topic)
	return request
}

func kafkaJSOffsetCommitResponseError(response *kmsg.OffsetCommitResponse) error {
	if response == nil {
		return fmt.Errorf("broker returned an empty offset commit response")
	}
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				return fmt.Errorf("broker rejected exact offset commit with error code %d", partition.ErrorCode)
			}
		}
	}
	return nil
}

// classifyKafkaJSTransientConsumerError turns only transport and Kafka
// protocol retry classes into the exported typed boundary. Permanent broker
// responses (authorization, invalid topic/config, oversized records) retain
// their original error identity and continue to fail fast in callers.
func classifyKafkaJSTransientConsumerError(err error) error {
	if err == nil || IsTransientConsumerError(err) || errors.Is(err, context.Canceled) {
		return err
	}
	if kerr.IsRetriable(err) || errors.Is(err, context.DeadlineExceeded) {
		return NewTransientConsumerError(err)
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return NewTransientConsumerError(err)
	}
	return err
}

func (c *kafkaJSCompatibleConsumer) ConsumeBatch(handler BatchHandler, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: batch handler is nil")
	}
	return c.consumeBatches(func(ctx context.Context, payload BatchPayload) error {
		return runTracedBatchHandler(ctx, payload, func(_ context.Context, traced BatchPayload) error { return handler(traced) })
	}, opts)
}

func (c *kafkaJSCompatibleConsumer) ConsumeBatchCtx(handler BatchHandlerCtx, opts ConsumerOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: batch handler is nil")
	}
	return c.consumeBatches(func(ctx context.Context, payload BatchPayload) error {
		return runTracedBatchHandler(ctx, payload, handler)
	}, opts)
}

type kafkaJSBatchHandler func(context.Context, BatchPayload) error

func (c *kafkaJSCompatibleConsumer) consumeBatches(handler kafkaJSBatchHandler, opts ConsumerOptions) error {
	if opts.OffsetFinalizer != nil {
		return fmt.Errorf("kafka/kafkajs: OffsetFinalizer is supported only for message consumption")
	}
	isAutoCommit, pollTimeout, concurrency, err := validateConfluentConsumerOptions(c.config.AutoCommit, opts, time.Second)
	if err != nil {
		return err
	}
	client, ctx, finish, err := c.beginRun()
	if err != nil {
		return err
	}
	defer finish()
	maxRecords := opts.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 100
	}
	for {
		fetches, err := pollKafkaJSRecords(ctx, client, pollTimeout, maxRecords)
		if err != nil {
			return c.prepareTransientRunRetry(client, err)
		}
		if ctx.Err() != nil {
			return nil
		}
		groups := groupKafkaJSRecords(fetches.Records())
		if err = runKafkaJSRecordGroups(ctx, groups, concurrency, func(group []*kgo.Record) error {
			if len(group) == 0 {
				return nil
			}
			messages := make([]MessagePayload, len(group))
			for i, record := range group {
				messages[i] = kafkaJSPayload(record)
			}
			last := group[len(group)-1]
			if !isAutoCommit && opts.CommitBeforeHandler {
				if err := client.CommitRecords(ctx, last); err != nil {
					return classifyKafkaJSTransientConsumerError(
						fmt.Errorf("kafka/kafkajs: pre-handler batch commit failed: %w", err),
					)
				}
			}
			payload := BatchPayload{Topic: last.Topic, Partition: int(last.Partition), Messages: messages, FirstOffset: group[0].Offset, LastOffset: last.Offset}
			if err := handler(ctx, payload); err != nil {
				return fmt.Errorf("kafka/kafkajs: batch handler failed: %w", err)
			}
			if !isAutoCommit && opts.CommitBeforeHandler {
				return nil
			}
			return resolveKafkaJSRecord(ctx, client, c.config.GroupID, last, isAutoCommit, false)
		}); err != nil {
			client.AllowRebalance()
			return c.prepareTransientRunRetry(client, err)
		}
		client.AllowRebalance()
	}
}

func (c *kafkaJSCompatibleConsumer) beginRun() (*kgo.Client, context.Context, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, nil, fmt.Errorf("kafka/kafkajs: consumer is closed")
	}
	if c.client == nil {
		if len(c.topics) == 0 {
			c.mu.Unlock()
			return nil, nil, nil, fmt.Errorf("kafka/kafkajs: consumer is not connected")
		}
		opts, err := c.clientOptions(c.topics)
		if err != nil {
			c.mu.Unlock()
			return nil, nil, nil, fmt.Errorf("kafka/kafkajs: rebuild consumer options: %w", err)
		}
		client, err := kgo.NewClient(opts...)
		if err != nil {
			c.mu.Unlock()
			return nil, nil, nil, fmt.Errorf("kafka/kafkajs: reconnect consumer group: %w", err)
		}
		c.client = client
		c.logger.Info("kafka/kafkajs: consumer reconnected after transient run failure",
			"groupId", c.config.GroupID,
			"topics", strings.Join(topicNames(c.topics), ","),
			"protocol", "RoundRobinAssigner",
		)
	}
	if c.runDone != nil {
		c.mu.Unlock()
		return nil, nil, nil, fmt.Errorf("kafka/kafkajs: consumer is already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.cancelRun = cancel
	c.runDone = done
	client := c.client
	c.mu.Unlock()
	finish := func() {
		c.mu.Lock()
		if c.runDone == done {
			c.cancelRun = nil
			c.runDone = nil
			close(done)
		}
		c.mu.Unlock()
	}
	return client, ctx, finish, nil
}

// prepareTransientRunRetry discards the current franz-go client only for the
// exported transient boundary. Polling advances franz-go's in-memory fetch
// position before the handler's synchronous offset commit. Re-entering Consume
// on that same client after a broker outage would therefore skip the failed
// record even though the broker has no committed next offset. KafkaJS restarts
// its runner from the group commit in this situation. Closing this client and
// lazily rebuilding it in beginRun gives the caller the same replay boundary
// without retrying permanent protocol or handler failures.
func (c *kafkaJSCompatibleConsumer) prepareTransientRunRetry(runClient *kgo.Client, err error) error {
	if !IsTransientConsumerError(err) || runClient == nil {
		return err
	}

	c.mu.Lock()
	if c.closed || c.client != runClient {
		c.mu.Unlock()
		return err
	}
	c.client = nil
	c.mu.Unlock()

	// Close stops heartbeats and releases the old assignment before the outer
	// retry starts a new group member. kgo.Close is safe without a live broker
	// and prevents a stale member from delaying recovery by a session timeout.
	runClient.Close()
	return err
}

func (c *kafkaJSCompatibleConsumer) Close() error {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		return err
	}
	c.closed = true
	client, cancel, done := c.client, c.cancelRun, c.runDone
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if client != nil {
		client.Close()
	}
	if done != nil {
		<-done
	}
	c.logger.Info("kafka/kafkajs: consumer closed", "groupId", c.config.GroupID)
	return nil
}
