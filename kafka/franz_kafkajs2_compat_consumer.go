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
	"github.com/gofynd/fit-go/redact"
)

var (
	_ KafkaConsumer         = (*franzKafkaJS2CompatConsumer)(nil)
	_ KafkaBatchConsumerCtx = (*franzKafkaJS2CompatConsumer)(nil)
)

// kafkaJSRoundRobinBalancer delegates the standard round-robin plan and wire
// metadata to franz-go, changing only the protocol identifier. KafkaJS 2.x
// advertises the literal "RoundRobinAssigner" rather than the conventional
// "roundrobin" used by librdkafka, so the latter cannot coexist in the group.
type kafkaJSRoundRobinBalancer struct{ kgo.GroupBalancer }

func (kafkaJSRoundRobinBalancer) ProtocolName() string { return "RoundRobinAssigner" }

// franzKafkaJS2CompatClient is the narrow franz-go surface used by the
// compatibility loop. Keeping the boundary explicit makes shutdown ordering
// testable without a broker while *kgo.Client remains the only production
// implementation.
type franzKafkaJS2CompatClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	AllowRebalance()
	CommitRecords(context.Context, ...*kgo.Record) error
	MarkCommitRecords(...*kgo.Record)
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	CommitOffsetsSync(
		context.Context,
		map[string]map[int32]kgo.EpochOffset,
		func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
	)
	CloseAllowingRebalance()
}

type franzKafkaJS2CompatConsumer struct {
	brokers   []string
	fitCfg    *Config
	config    ConsumerAdvancedConfig
	logger    *logging.Logger
	newClient func(...kgo.Opt) (franzKafkaJS2CompatClient, error)

	mu        sync.Mutex
	client    franzKafkaJS2CompatClient
	topics    []TopicConfig
	cancelRun context.CancelFunc
	stopPoll  context.CancelFunc
	runDone   chan struct{}
	closed    bool
	closeErr  error
	closeDone chan struct{}

	// pendingRecovery outlives an assignment and even a rebuilt driver. SetOffsets
	// only rewinds the current member; franz-go clears that state on ownership
	// loss. Keep known unresolved boundaries until processing succeeds or a
	// broker-committed offset makes the boundary durable on the next assignment.
	// All access is protected by mu.
	pendingRecovery []consumerRecordPosition

	// closeRequested is closed as soon as Close (or a lifecycle close request)
	// starts, so legacy rebalance hooks stop blocking the franz-go callback.
	closeRequested chan struct{}
}

func newFranzKafkaJS2CompatConsumer(
	brokers []string,
	fitCfg *Config,
	config ConsumerAdvancedConfig,
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
	if !validConsumerShutdownPolicy(config.ShutdownPolicy) {
		return nil, fmt.Errorf("kafka/kafkajs: unsupported shutdown policy %d", config.ShutdownPolicy)
	}
	return &franzKafkaJS2CompatConsumer{
		brokers: append([]string(nil), brokers...),
		fitCfg:  fitCfg,
		config:  config,
		logger:  logger,
	}, nil
}

func validConsumerShutdownPolicy(policy ConsumerShutdownPolicy) bool {
	return policy == ConsumerShutdownCancelInFlight || policy == ConsumerShutdownDrainInFlight
}

func (c *franzKafkaJS2CompatConsumer) Connect(topics []TopicConfig) error {
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
	if err := validateUniformTopicStart(topics); err != nil {
		return fmt.Errorf("kafka/kafkajs: connect: %w", err)
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
	client, err := c.createClient(opts...)
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

func (c *franzKafkaJS2CompatConsumer) createClient(opts ...kgo.Opt) (franzKafkaJS2CompatClient, error) {
	if c.newClient != nil {
		return c.newClient(opts...)
	}
	return kgo.NewClient(opts...)
}

func (c *franzKafkaJS2CompatConsumer) clientOptions(topics []TopicConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.brokers...),
		kgo.ClientID(c.fitCfg.ClientID),
		kgo.ConsumerGroup(c.config.GroupID),
		kgo.ConsumeTopics(topicNames(topics)...),
		kgo.Balancers(kafkaJSRoundRobinBalancer{kgo.RoundRobinBalancer()}),
		kgo.BlockRebalanceOnPoll(),
		// KafkaJS consumers default to read_committed. Retain transaction
		// markers internally so their offsets can advance without exposing the
		// marker records to application handlers.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
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
		kgo.AdjustFetchOffsetsFn(c.adjustRecoveryOffsets),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			parts := flattenAssignments(assigned)
			c.logger.Info("kafka/kafkajs: partitions assigned", "groupId", c.config.GroupID, "partitions", formatAssignments(parts))
			c.invokePartitionsAssigned(parts)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
			c.handlePartitionsRevoked(ctx, client, revoked)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			c.handlePartitionsLost(lost)
		}),
	)
	return opts, nil
}

type franzKafkaJS2RebalanceCommitter interface {
	CommitMarkedOffsets(context.Context) error
}

func (c *franzKafkaJS2CompatConsumer) handlePartitionsRevoked(
	ctx context.Context,
	client franzKafkaJS2RebalanceCommitter,
	revoked map[string][]int32,
) {
	parts := flattenAssignments(revoked)
	c.logger.Info("kafka/kafkajs: partitions revoked", "groupId", c.config.GroupID, "partitions", formatAssignments(parts))
	// Installing a custom revoke hook replaces franz-go's default final
	// auto-commit. Restore that boundary before the application observes the
	// revoke; MarkCommitRecords ensures only successfully handled records move.
	if c.config.AutoCommit && client != nil {
		if err := client.CommitMarkedOffsets(ctx); err != nil {
			c.logger.Error(
				"kafka/kafkajs: final revoke commit failed",
				"groupId", c.config.GroupID,
				"error", redact.ErrorMessage(err),
			)
		}
	}
	c.invokePartitionsRevoked(parts)
}

func (c *franzKafkaJS2CompatConsumer) handlePartitionsLost(lost map[string][]int32) {
	parts := flattenAssignments(lost)
	c.logger.Warn("kafka/kafkajs: partitions lost", "groupId", c.config.GroupID, "partitions", formatAssignments(parts))
	hasLostHook := c.config.OnPartitionsLost != nil || c.config.OnPartitionsLostWithLifecycle != nil
	if c.config.OnPartitionsLost != nil {
		if !awaitConsumerRebalanceHook(c.closeRequestSignal(), func() { c.config.OnPartitionsLost(parts) }) {
			return
		}
	}
	if c.config.OnPartitionsLostWithLifecycle != nil {
		c.config.OnPartitionsLostWithLifecycle(parts, rebalanceLifecycle{close: c.requestCloseFromRebalance})
	}
	// franz-go invokes the revoke callback for lost partitions when no distinct
	// lost callback is configured. Preserve that lifecycle notification without
	// performing the final revoke commit, which is unsafe after ownership loss.
	if !hasLostHook {
		c.invokePartitionsRevoked(parts)
	}
}

func (c *franzKafkaJS2CompatConsumer) invokePartitionsAssigned(parts []PartitionAssignment) {
	if c.config.OnPartitionsAssigned != nil {
		if !awaitConsumerRebalanceHook(c.closeRequestSignal(), func() { c.config.OnPartitionsAssigned(parts) }) {
			return
		}
	}
	if c.config.OnPartitionsAssignedWithLifecycle != nil {
		c.config.OnPartitionsAssignedWithLifecycle(parts, rebalanceLifecycle{close: c.requestCloseFromRebalance})
	}
}

func (c *franzKafkaJS2CompatConsumer) invokePartitionsRevoked(parts []PartitionAssignment) {
	if c.config.OnPartitionsRevoked != nil {
		if !awaitConsumerRebalanceHook(c.closeRequestSignal(), func() { c.config.OnPartitionsRevoked(parts) }) {
			return
		}
	}
	if c.config.OnPartitionsRevokedWithLifecycle != nil {
		c.config.OnPartitionsRevokedWithLifecycle(parts, rebalanceLifecycle{close: c.requestCloseFromRebalance})
	}
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

func (c *franzKafkaJS2CompatConsumer) Consume(handler MessageHandler, opts ConsumerOptions) error {
	return c.ConsumeAdvanced(handler, advancedConsumerOptions(opts))
}

func (c *franzKafkaJS2CompatConsumer) ConsumeAdvanced(handler MessageHandler, opts ConsumerAdvancedOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: message handler is nil")
	}
	handlerWithContext := func(_ context.Context, payload MessagePayload) error {
		return handler(payload)
	}
	if opts.OffsetFinalizer != nil {
		return c.consumeMessages(handlerWithContext, opts)
	}
	return c.consumeMessages(func(ctx context.Context, payload MessagePayload) error {
		return runTracedMessageHandler(ctx, payload, handlerWithContext)
	}, opts)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeCtx(handler MessageHandlerCtx, opts ConsumerOptions) error {
	return c.ConsumeCtxAdvanced(handler, advancedConsumerOptions(opts))
}

func (c *franzKafkaJS2CompatConsumer) ConsumeCtxAdvanced(handler MessageHandlerCtx, opts ConsumerAdvancedOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: message handler is nil")
	}
	if opts.OffsetFinalizer != nil {
		return c.consumeMessages(kafkaJSMessageHandler(handler), opts)
	}
	return c.consumeMessages(func(ctx context.Context, payload MessagePayload) error {
		return runTracedMessageHandler(ctx, payload, handler)
	}, opts)
}

type kafkaJSMessageHandler func(context.Context, MessagePayload) error

var errKafkaJSUnresolvedRecordRewound = errors.New("kafka/kafkajs: unresolved record rewound")

func (c *franzKafkaJS2CompatConsumer) consumeMessages(handler kafkaJSMessageHandler, opts ConsumerAdvancedOptions) error {
	isAutoCommit, pollTimeout, concurrency, err := validateConfluentConsumerOptions(c.config.AutoCommit, opts, 100*time.Millisecond)
	if err != nil {
		return err
	}
	client, runCtx, pollCtx, finish, err := c.beginRun()
	if err != nil {
		return err
	}
	defer finish()
	// Every successful PollRecords call blocks group rebalances until the
	// current records have reached their handler/offset boundary. Release that
	// gate on every return path before finish signals Close that the run ended.
	defer client.AllowRebalance()
	maxRecords := opts.MaxRecords
	if maxRecords <= 0 {
		maxRecords = concurrency
	}
	for {
		fetches, err := pollKafkaJSRecords(pollCtx, client, pollTimeout, maxRecords)
		if err != nil {
			return c.prepareTransientRunRetry(client, err)
		}
		if runCtx.Err() != nil || (pollCtx.Err() != nil && fetches.NumRecords() == 0) {
			return nil
		}
		groups := groupKafkaJSRecords(fetches.Records())
		if err = runKafkaJSRecordGroups(runCtx, groups, concurrency, func(group []*kgo.Record) error {
			for index, record := range group {
				if err := c.processRecord(runCtx, client, record, handler, isAutoCommit, opts); err != nil {
					if errors.Is(err, errKafkaJSUnresolvedRecordRewound) {
						return err
					}
					if !isAutoCommit && opts.CommitBeforeHandler && isConsumerHandlerError(err) {
						// The failed record was committed before its handler ran and is
						// intentionally not redelivered (at-most-once). Later fetched
						// records of this partition were never started: rewind to the
						// first of them so they are not skipped.
						if index+1 < len(group) {
							return &consumerRewindError{cause: err, failed: kafkaJSRecordPosition(group[index+1])}
						}
						return err
					}
					return withConsumerRewindPosition(err, kafkaJSRecordPosition(record))
				}
				c.completeRecoveryPosition(kafkaJSRecordPosition(record))
			}
			return nil
		}); errors.Is(err, errKafkaJSUnresolvedRecordRewound) {
			// SetOffsets below has restored the failed record's physical offset.
			// End this poll batch so records after it are not observed before its
			// KafkaJS-style marker redelivery.
			client.AllowRebalance()
			if pollCtx.Err() != nil {
				return nil
			}
			continue
		} else if err != nil {
			if isKafkaJSRunCancellation(runCtx, err) {
				// Shutdown cancellation deliberately leaves the current record
				// unresolved so the next group member can replay it.
				client.AllowRebalance()
				return nil
			}
			err = c.prepareTransientRunRetry(client, err)
			client.AllowRebalance()
			return err
		}
		client.AllowRebalance()
		if pollCtx.Err() != nil {
			return nil
		}
	}
}

func pollKafkaJSRecords(ctx context.Context, client franzKafkaJS2CompatClient, timeout time.Duration, maxRecords int) (kgo.Fetches, error) {
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
		err := classifyKafkaJSTransientConsumerError(
			fmt.Errorf("kafka/kafkajs: consume error: %w", fetchErr.Err),
		)
		if IsTransientConsumerError(err) && fetches.NumRecords() > 0 {
			// PollRecords advances every returned partition even when a sibling
			// fetch or group notification failed. None of these records reached
			// application code. Preserve their exact retry boundaries while the
			// poll's rebalance gate still protects the active assignment; rebuilding
			// a group with no committed offset and FromBeginning=false loses them.
			positions := make([]consumerRecordPosition, 0, fetches.NumRecords())
			fetches.EachRecord(func(record *kgo.Record) {
				positions = append(positions, kafkaJSRecordPosition(record))
			})
			err = &kafkaJSPollRewindError{cause: err, positions: positions}
		}
		return fetches, err
	}
	return fetches, nil
}

type kafkaJSPollRewindError struct {
	cause     error
	positions []consumerRecordPosition
}

func (e *kafkaJSPollRewindError) Error() string { return e.cause.Error() }
func (e *kafkaJSPollRewindError) Unwrap() error { return e.cause }
func (e *kafkaJSPollRewindError) consumerRewindPositions() []consumerRecordPosition {
	return e.positions
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
	var failures []error
	rewound := false
	for err := range errs {
		if errors.Is(err, errKafkaJSUnresolvedRecordRewound) {
			rewound = true
			continue
		}
		failures = append(failures, err)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	if rewound {
		return errKafkaJSUnresolvedRecordRewound
	}
	return nil
}

func (c *franzKafkaJS2CompatConsumer) processRecord(
	ctx context.Context,
	client franzKafkaJS2CompatClient,
	record *kgo.Record,
	handler kafkaJSMessageHandler,
	isAutoCommit bool,
	opts ConsumerAdvancedOptions,
) error {
	if record.Attrs.IsControl() {
		return resolveKafkaJSRecord(ctx, client, record, isAutoCommit, c.config.AutoCommit, opts.NullOffsetCommitMetadata)
	}
	var cleanup func()
	ctx, cleanup = enterConsumerCallback(ctx, c)
	defer cleanup()
	payload := kafkaJSPayload(record)
	if !isAutoCommit && opts.CommitBeforeHandler {
		if err := client.CommitRecords(ctx, record); err != nil {
			return classifyKafkaJSTransientConsumerError(
				fmt.Errorf("kafka/kafkajs: pre-handler commit failed: %w", err),
			)
		}
		// A previously failed commit may have left a retry boundary. Once this
		// pre-handler commit succeeds, at-most-once must never replay the record,
		// including when its handler fails while the member loses ownership.
		c.completeRecoveryPosition(kafkaJSRecordPosition(record))
	}
	if opts.OffsetFinalizer != nil {
		return runTracedMessageLifecycle(ctx, payload, MessageHandlerCtx(handler), func(messageCtx context.Context, handlerErr error) error {
			commitCalled := false
			commitExact := func(exact int64) error {
				if commitCalled {
					return fmt.Errorf("kafka/kafkajs: exact offset commit callback called more than once")
				}
				commitCalled = true
				return commitKafkaJSExact(messageCtx, client, record, exact, opts.NullOffsetCommitMetadata)
			}
			finalizerErr := opts.OffsetFinalizer(messageCtx, payload, handlerErr, commitExact)
			if finalizerErr != nil {
				if handlerErr != nil && errors.Is(finalizerErr, handlerErr) {
					return newConsumerHandlerErrorAt("kafka/kafkajs: message handler failed", finalizerErr, consumerRecordPosition{
						topic: record.Topic, partition: record.Partition, offset: record.Offset, leaderEpoch: record.LeaderEpoch,
					})
				}
				return finalizerErr
			}
			if handlerErr == nil && opts.ResolveAfterSuccessfulFinalizer {
				return resolveKafkaJSRecord(messageCtx, client, record, isAutoCommit, c.config.AutoCommit, opts.NullOffsetCommitMetadata)
			}
			if handlerErr != nil && opts.RedeliverUnresolvedFinalizer {
				c.rememberRecoveryPositions([]consumerRecordPosition{kafkaJSRecordPosition(record)})
				client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
					record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}},
				})
				return errKafkaJSUnresolvedRecordRewound
			}
			return nil
		})
	}

	handlerErr := handler(ctx, payload)
	if handlerErr != nil {
		if !isAutoCommit && opts.CommitBeforeHandler {
			return newConsumerHandlerError("kafka/kafkajs: message handler failed", handlerErr)
		}
		return newConsumerHandlerErrorAt("kafka/kafkajs: message handler failed", handlerErr, consumerRecordPosition{
			topic: record.Topic, partition: record.Partition, offset: record.Offset, leaderEpoch: record.LeaderEpoch,
		})
	}
	if !isAutoCommit && opts.CommitBeforeHandler {
		return nil
	}
	return resolveKafkaJSRecord(ctx, client, record, isAutoCommit, c.config.AutoCommit, false)
}

func kafkaJSPayload(record *kgo.Record) MessagePayload {
	headers := make([]Header, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = Header{Key: header.Key, Value: cloneNullableBytes(header.Value)}
	}
	return MessagePayload{
		Topic: record.Topic, Partition: int(record.Partition), Offset: record.Offset,
		Key: cloneNullableBytes(record.Key), Value: cloneNullableBytes(record.Value),
		Headers: headers, Timestamp: record.Timestamp,
	}
}

func kafkaJSRecordPosition(record *kgo.Record) consumerRecordPosition {
	if record == nil {
		return consumerRecordPosition{}
	}
	return consumerRecordPosition{
		topic: record.Topic, partition: record.Partition, offset: record.Offset, leaderEpoch: record.LeaderEpoch,
	}
}

func cloneNullableBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func resolveKafkaJSRecord(
	ctx context.Context,
	client franzKafkaJS2CompatClient,
	record *kgo.Record,
	auto bool,
	backgroundAutoCommit bool,
	nullMetadata bool,
) error {
	if auto && backgroundAutoCommit {
		client.MarkCommitRecords(record)
		return nil
	}
	if nullMetadata {
		return commitKafkaJSExact(ctx, client, record, record.Offset+1, true)
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
	client franzKafkaJS2CompatClient,
	record *kgo.Record,
	exact int64,
	nullMetadata bool,
) error {
	if nullMetadata {
		ctx = kgo.PreCommitFnContext(ctx, clearKafkaJSOffsetCommitMetadata)
	}
	offsets := map[string]map[int32]kgo.EpochOffset{
		record.Topic: {record.Partition: {Epoch: -1, Offset: exact}},
	}
	var commitErr error
	client.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, response *kmsg.OffsetCommitResponse, err error) {
		commitErr = err
		if commitErr == nil {
			commitErr = kafkaJSOffsetCommitResponseError(response)
		}
	})
	if commitErr != nil {
		return classifyKafkaJSTransientConsumerError(
			fmt.Errorf("kafka/kafkajs: exact offset commit failed: %w", commitErr),
		)
	}
	return nil
}

// clearKafkaJSOffsetCommitMetadata is installed through franz-go's supported
// pre-commit hook instead of issuing a raw OffsetCommitRequest. This preserves
// KafkaJS's null metadata while retaining franz-go's coordinator routing,
// serialized commit ordering, topic-ID population, and v9 fallback for brokers
// where OffsetCommit v10 cannot safely address the topic by ID.
func clearKafkaJSOffsetCommitMetadata(request *kmsg.OffsetCommitRequest) error {
	if request == nil {
		return fmt.Errorf("kafka/kafkajs: offset commit request is nil")
	}
	for topicIndex := range request.Topics {
		for partitionIndex := range request.Topics[topicIndex].Partitions {
			request.Topics[topicIndex].Partitions[partitionIndex].Metadata = nil
		}
	}
	return nil
}

func kafkaJSOffsetCommitResponseError(response *kmsg.OffsetCommitResponse) error {
	if response == nil {
		return fmt.Errorf("broker returned an empty offset commit response")
	}
	for _, topic := range response.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				return fmt.Errorf(
					"broker rejected exact offset commit for topic %q partition %d: %w",
					topic.Topic,
					partition.Partition,
					kerr.ErrorForCode(partition.ErrorCode),
				)
			}
		}
	}
	return nil
}

// classifyKafkaJSTransientConsumerError turns only transport and Kafka
// consumer-run recovery classes into the exported typed boundary. In addition
// to errors Kafka marks retriable for the same request, KafkaJS explicitly
// rejoins the group for UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, and
// REBALANCE_IN_PROGRESS. Those responses are non-retriable at the individual
// request level, but recoverable after replacing the stale group member.
// Permanent broker responses (authorization, invalid topic/config, oversized
// records) retain their original error identity and continue to fail fast in
// callers.
func classifyKafkaJSTransientConsumerError(err error) error {
	if err == nil || IsTransientConsumerError(err) || errors.Is(err, context.Canceled) {
		return err
	}
	if kerr.IsRetriable(err) || isKafkaJSGroupRejoinError(err) || errors.Is(err, context.DeadlineExceeded) {
		return NewTransientConsumerError(err)
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return NewTransientConsumerError(err)
	}
	return err
}

// isKafkaJSGroupRejoinError mirrors KafkaJS' consumer runner rather than
// Kafka's per-request retriable flag. Reissuing the failed request with the
// same member identity cannot succeed; franz-go's group management rejoins the
// group, or a no-record run failure rebuilds the client from the committed
// offset. Keep this list deliberately narrow so authorization and
// configuration failures are never hidden behind a reconnect loop.
func isKafkaJSGroupRejoinError(err error) bool {
	return errors.Is(err, kerr.UnknownMemberID) ||
		errors.Is(err, kerr.IllegalGeneration) ||
		errors.Is(err, kerr.RebalanceInProgress)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeBatch(handler BatchHandler, opts ConsumerOptions) error {
	return c.ConsumeBatchAdvanced(handler, advancedConsumerOptions(opts))
}

func (c *franzKafkaJS2CompatConsumer) ConsumeBatchAdvanced(handler BatchHandler, opts ConsumerAdvancedOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: batch handler is nil")
	}
	return c.consumeBatches(func(ctx context.Context, payload BatchPayload) error {
		return runTracedBatchHandler(ctx, payload, func(_ context.Context, traced BatchPayload) error { return handler(traced) })
	}, opts)
}

func (c *franzKafkaJS2CompatConsumer) ConsumeBatchCtx(handler BatchHandlerCtx, opts ConsumerOptions) error {
	return c.ConsumeBatchCtxAdvanced(handler, advancedConsumerOptions(opts))
}

func (c *franzKafkaJS2CompatConsumer) ConsumeBatchCtxAdvanced(handler BatchHandlerCtx, opts ConsumerAdvancedOptions) error {
	if handler == nil {
		return fmt.Errorf("kafka/kafkajs: batch handler is nil")
	}
	return c.consumeBatches(func(ctx context.Context, payload BatchPayload) error {
		return runTracedBatchHandler(ctx, payload, handler)
	}, opts)
}

type kafkaJSBatchHandler func(context.Context, BatchPayload) error

func (c *franzKafkaJS2CompatConsumer) consumeBatches(handler kafkaJSBatchHandler, opts ConsumerAdvancedOptions) error {
	if opts.OffsetFinalizer != nil {
		return fmt.Errorf("kafka/kafkajs: OffsetFinalizer is supported only for message consumption")
	}
	isAutoCommit, pollTimeout, concurrency, err := validateConfluentConsumerOptions(c.config.AutoCommit, opts, time.Second)
	if err != nil {
		return err
	}
	client, runCtx, pollCtx, finish, err := c.beginRun()
	if err != nil {
		return err
	}
	defer finish()
	defer client.AllowRebalance()
	maxRecords := opts.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 100
	}
	for {
		fetches, err := pollKafkaJSRecords(pollCtx, client, pollTimeout, maxRecords)
		if err != nil {
			return c.prepareTransientRunRetry(client, err)
		}
		if runCtx.Err() != nil || (pollCtx.Err() != nil && fetches.NumRecords() == 0) {
			return nil
		}
		groups := groupKafkaJSRecords(fetches.Records())
		if err = runKafkaJSRecordGroups(runCtx, groups, concurrency, func(group []*kgo.Record) error {
			if len(group) == 0 {
				return nil
			}
			lastFetched := group[len(group)-1]
			visible := make([]*kgo.Record, 0, len(group))
			for _, record := range group {
				if !record.Attrs.IsControl() {
					visible = append(visible, record)
				}
			}
			if len(visible) == 0 {
				resolutionErr := withConsumerRewindPosition(
					resolveKafkaJSRecord(runCtx, client, lastFetched, isAutoCommit, c.config.AutoCommit, false),
					kafkaJSRecordPosition(lastFetched),
				)
				if resolutionErr == nil {
					c.completeRecoveryPosition(kafkaJSRecordPosition(lastFetched))
				}
				return resolutionErr
			}
			firstPosition := kafkaJSRecordPosition(visible[0])
			messages := make([]MessagePayload, len(visible))
			for i, record := range visible {
				messages[i] = kafkaJSPayload(record)
			}
			if !isAutoCommit && opts.CommitBeforeHandler {
				if err := client.CommitRecords(runCtx, lastFetched); err != nil {
					return withConsumerRewindPosition(classifyKafkaJSTransientConsumerError(
						fmt.Errorf("kafka/kafkajs: pre-handler batch commit failed: %w", err),
					), firstPosition)
				}
				c.completeRecoveryPosition(kafkaJSRecordPosition(lastFetched))
			}
			lastVisible := visible[len(visible)-1]
			payload := BatchPayload{
				Topic:       lastVisible.Topic,
				Partition:   int(lastVisible.Partition),
				Messages:    messages,
				FirstOffset: visible[0].Offset,
				LastOffset:  lastVisible.Offset,
			}
			handlerErr := func() error {
				callbackCtx, cleanup := enterConsumerCallback(runCtx, c)
				defer cleanup()
				return handler(callbackCtx, payload)
			}()
			if handlerErr != nil {
				if !isAutoCommit && opts.CommitBeforeHandler {
					return newConsumerHandlerError("kafka/kafkajs: batch handler failed", handlerErr)
				}
				return newConsumerHandlerErrorAt("kafka/kafkajs: batch handler failed", handlerErr, firstPosition)
			}
			if !isAutoCommit && opts.CommitBeforeHandler {
				return nil
			}
			resolutionErr := withConsumerRewindPosition(
				resolveKafkaJSRecord(runCtx, client, lastFetched, isAutoCommit, c.config.AutoCommit, false),
				firstPosition,
			)
			if resolutionErr == nil {
				c.completeRecoveryPosition(kafkaJSRecordPosition(lastFetched))
			}
			return resolutionErr
		}); err != nil {
			if isKafkaJSRunCancellation(runCtx, err) {
				// Do not turn an expected shutdown cancellation into a pod
				// failure. The uncommitted batch remains replayable.
				client.AllowRebalance()
				return nil
			}
			err = c.prepareTransientRunRetry(client, err)
			client.AllowRebalance()
			return err
		}
		client.AllowRebalance()
		if pollCtx.Err() != nil {
			return nil
		}
	}
}

// isKafkaJSRunCancellation distinguishes an expected shutdown error from a
// real handler, finalizer, or commit failure that merely happened at the same
// time as shutdown. Only an error wrapping the run context's own terminal
// error is suppressed; unrelated processing failures must remain visible.
func isKafkaJSRunCancellation(ctx context.Context, err error) bool {
	if ctx == nil || err == nil {
		return false
	}
	runErr := ctx.Err()
	return runErr != nil && errors.Is(err, runErr)
}

func (c *franzKafkaJS2CompatConsumer) beginRun() (franzKafkaJS2CompatClient, context.Context, context.Context, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, nil, nil, fmt.Errorf("kafka/kafkajs: consumer is closed")
	}
	if !validConsumerShutdownPolicy(c.config.ShutdownPolicy) {
		c.mu.Unlock()
		return nil, nil, nil, nil, fmt.Errorf("kafka/kafkajs: unsupported shutdown policy %d", c.config.ShutdownPolicy)
	}
	if c.client == nil {
		if len(c.topics) == 0 {
			c.mu.Unlock()
			return nil, nil, nil, nil, fmt.Errorf("kafka/kafkajs: consumer is not connected")
		}
		opts, err := c.clientOptions(c.topics)
		if err != nil {
			c.mu.Unlock()
			return nil, nil, nil, nil, fmt.Errorf("kafka/kafkajs: rebuild consumer options: %w", err)
		}
		client, err := c.createClient(opts...)
		if err != nil {
			c.mu.Unlock()
			return nil, nil, nil, nil, fmt.Errorf("kafka/kafkajs: reconnect consumer group: %w", err)
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
		return nil, nil, nil, nil, fmt.Errorf("kafka/kafkajs: consumer is already running")
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	pollCtx, stopPoll := context.WithCancel(runCtx)
	done := make(chan struct{})
	c.cancelRun = cancelRun
	c.stopPoll = stopPoll
	c.runDone = done
	client := c.client
	c.mu.Unlock()
	finish := func() {
		stopPoll()
		cancelRun()
		c.mu.Lock()
		if c.runDone == done {
			c.cancelRun = nil
			c.stopPoll = nil
			c.runDone = nil
			close(done)
		}
		c.mu.Unlock()
	}
	return client, runCtx, pollCtx, finish, nil
}

// prepareTransientRunRetry applies the complete rewind plan collected for a
// failed poll wave. Handler, finalizer, commit, and mixed poll failures retain
// the group member and rewind every affected partition. Transport/group failures
// without a known record boundary still discard the client and rebuild from the
// broker's committed boundary. CommitBeforeHandler failures carry an explicit
// empty plan because their offsets were committed before user code ran.
func (c *franzKafkaJS2CompatConsumer) prepareTransientRunRetry(runClient franzKafkaJS2CompatClient, err error) error {
	positions, rewindDecided := consumerRewindPositions(err)
	if (!IsTransientConsumerError(err) && !rewindDecided) || runClient == nil {
		return err
	}
	if rewindDecided {
		if len(positions) > 0 {
			c.rememberRecoveryPositions(positions)
			offsets := make(map[string]map[int32]kgo.EpochOffset)
			for _, failed := range positions {
				partitions := offsets[failed.topic]
				if partitions == nil {
					partitions = make(map[int32]kgo.EpochOffset)
					offsets[failed.topic] = partitions
				}
				partitions[failed.partition] = kgo.EpochOffset{Epoch: failed.leaderEpoch, Offset: failed.offset}
			}
			runClient.SetOffsets(offsets)
		}
		return err
	}

	c.mu.Lock()
	if c.closed || c.client != runClient {
		c.mu.Unlock()
		return err
	}
	c.client = nil
	c.mu.Unlock()

	// A PollRecords call may still hold BlockRebalanceOnPoll's gate on this
	// early error path. Allowing the rebalance while closing prevents recovery
	// from deadlocking before the outer retry can create a new group member.
	runClient.CloseAllowingRebalance()
	return err
}

func (c *franzKafkaJS2CompatConsumer) rememberRecoveryPositions(positions []consumerRecordPosition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.pendingRecovery = mergeConsumerRewindPositions(c.pendingRecovery, positions)
	}
}

func (c *franzKafkaJS2CompatConsumer) completeRecoveryPosition(completed consumerRecordPosition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := c.pendingRecovery[:0]
	for _, pending := range c.pendingRecovery {
		if pending.topic != completed.topic || pending.partition != completed.partition || pending.offset > completed.offset {
			kept = append(kept, pending)
		}
	}
	c.pendingRecovery = kept
}

// adjustRecoveryOffsets runs after the broker's OffsetFetch, before franz-go
// starts fetching the newly assigned partitions. Only partitions present in
// offsets belong to this assignment. Preserve any real committed boundary;
// otherwise a remembered exact position replaces the symbolic earliest/latest
// fallback. A newer owner may have committed beyond our failure in the meantime.
func (c *franzKafkaJS2CompatConsumer) adjustRecoveryOffsets(
	ctx context.Context,
	offsets map[string]map[int32]kgo.Offset,
) (map[string]map[int32]kgo.Offset, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kept := c.pendingRecovery[:0]
	for _, pending := range c.pendingRecovery {
		partitions := offsets[pending.topic]
		current, assigned := partitions[pending.partition]
		if assigned {
			committed := current.EpochOffset().Offset
			if committed >= 0 {
				// The coordinator already provides durable replay, or another
				// owner has legitimately completed this record. Never rewind it.
				if committed >= pending.offset {
					continue
				}
			} else {
				partitions[pending.partition] = kgo.NewOffset().At(pending.offset).WithEpoch(pending.leaderEpoch)
			}
		}
		kept = append(kept, pending)
	}
	c.pendingRecovery = kept
	return offsets, nil
}

func (c *franzKafkaJS2CompatConsumer) Close() error {
	callbackClose := isCurrentConsumerCallback(c)
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		if callbackClose {
			return nil
		}
		if done != nil {
			<-done
			c.mu.Lock()
			err := c.closeErr
			c.mu.Unlock()
			return err
		}
		return nil
	}
	c.closed = true
	c.signalCloseRequestedLocked()
	closeDone := make(chan struct{})
	c.closeDone = closeDone
	client, cancel, stopPoll, runDone := c.client, c.cancelRun, c.stopPoll, c.runDone
	shutdownPolicy := c.config.ShutdownPolicy
	c.mu.Unlock()
	if callbackClose {
		go c.finishClose(client, cancel, stopPoll, runDone, shutdownPolicy, closeDone)
		return nil
	}
	return c.finishClose(client, cancel, stopPoll, runDone, shutdownPolicy, closeDone)
}

func (c *franzKafkaJS2CompatConsumer) requestCloseFromRebalance() error {
	c.mu.Lock()
	if c.closeDone != nil {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.signalCloseRequestedLocked()
	closeDone := make(chan struct{})
	c.closeDone = closeDone
	client, cancel, stopPoll, runDone := c.client, c.cancelRun, c.stopPoll, c.runDone
	shutdownPolicy := c.config.ShutdownPolicy
	c.mu.Unlock()

	go c.finishClose(client, cancel, stopPoll, runDone, shutdownPolicy, closeDone)
	return nil
}

func (c *franzKafkaJS2CompatConsumer) finishClose(
	client franzKafkaJS2CompatClient,
	cancel context.CancelFunc,
	stopPoll context.CancelFunc,
	runDone chan struct{},
	shutdownPolicy ConsumerShutdownPolicy,
	closeDone chan struct{},
) error {

	if shutdownPolicy == ConsumerShutdownDrainInFlight && stopPoll != nil {
		// PollRecords uses a child context, so stopping admission does not cancel
		// a handler/finalizer/commit sequence that already owns runCtx.
		stopPoll()
	} else if cancel != nil {
		// Preserve the original zero-value behavior for every existing caller.
		cancel()
	}
	// Both consume loops release BlockRebalanceOnPoll before finish signals
	// runDone. Drain mode therefore retains the assignment through the final
	// offset boundary, while default mode waits for cancellation cleanup.
	if runDone != nil {
		<-runDone
	}
	// Drain mode intentionally keeps runCtx live until admitted work completes.
	// Cancel it now to release context resources before closing the client.
	if shutdownPolicy == ConsumerShutdownDrainInFlight && cancel != nil {
		cancel()
	}
	if client != nil {
		client.CloseAllowingRebalance()
	}
	if c.logger != nil {
		c.logger.Info("kafka/kafkajs: consumer closed", "groupId", c.config.GroupID)
	}

	c.mu.Lock()
	err := c.closeErr
	close(closeDone)
	c.mu.Unlock()
	return err
}

func (c *franzKafkaJS2CompatConsumer) closeRequestSignal() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeRequested == nil {
		c.closeRequested = make(chan struct{})
		if c.closed {
			close(c.closeRequested)
		}
	}
	return c.closeRequested
}

func (c *franzKafkaJS2CompatConsumer) signalCloseRequestedLocked() {
	if c.closeRequested == nil {
		c.closeRequested = make(chan struct{})
	}
	select {
	case <-c.closeRequested:
	default:
		close(c.closeRequested)
	}
}
