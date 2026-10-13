// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sortRemediationPositions(positions []consumerRecordPosition) {
	sort.Slice(positions, func(i, j int) bool {
		if positions[i].topic != positions[j].topic {
			return positions[i].topic < positions[j].topic
		}
		return positions[i].partition < positions[j].partition
	})
}

func remediationSeekPositions(partitions []ckafka.TopicPartition) []consumerRecordPosition {
	positions := make([]consumerRecordPosition, 0, len(partitions))
	for _, partition := range partitions {
		positions = append(positions, consumerRecordPosition{
			topic: confluentTopicName(partition.Topic), partition: partition.Partition, offset: int64(partition.Offset),
		})
	}
	sortRemediationPositions(positions)
	return positions
}

func remediationFetches(groups ...[]*kgo.Record) kgo.Fetches {
	topics := make(map[string][]kgo.FetchPartition)
	order := make([]string, 0)
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		topic := group[0].Topic
		if _, ok := topics[topic]; !ok {
			order = append(order, topic)
		}
		topics[topic] = append(topics[topic], kgo.FetchPartition{Partition: group[0].Partition, Records: group})
	}
	fetch := kgo.Fetch{}
	for _, topic := range order {
		fetch.Topics = append(fetch.Topics, kgo.FetchTopic{Topic: topic, Partitions: topics[topic]})
	}
	return kgo.Fetches{fetch}
}

// confluentMessageSource serves a fixed message list from a fake driver and
// honours SeekPartitions like librdkafka: a seek moves a partition's fetch
// position and discards already-served-but-unprocessed records.
type confluentMessageSource struct {
	mu       sync.Mutex
	topic    string
	logEnd   map[int32]int64
	position map[int32]int64
	order    []int32
	next     int
	served   int
}

func (s *confluentMessageSource) servedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served
}

func newConfluentMessageSource(topic string, logEnd map[int32]int64) *confluentMessageSource {
	source := &confluentMessageSource{topic: topic, logEnd: logEnd, position: make(map[int32]int64)}
	for partition := range logEnd {
		source.order = append(source.order, partition)
	}
	sort.Slice(source.order, func(i, j int) bool { return source.order[i] < source.order[j] })
	return source
}

func (s *confluentMessageSource) read(timeout time.Duration) (*ckafka.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Serve partitions round-robin so a single wave mixes partitions.
	for range s.order {
		partition := s.order[s.next%len(s.order)]
		s.next++
		if s.position[partition] < s.logEnd[partition] {
			offset := s.position[partition]
			s.position[partition]++
			s.served++
			topic := s.topic
			return &ckafka.Message{TopicPartition: ckafka.TopicPartition{
				Topic: &topic, Partition: partition, Offset: ckafka.Offset(offset),
			}}, nil
		}
	}
	if timeout > 0 {
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
		s.mu.Lock()
	}
	return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
}

func (s *confluentMessageSource) seek(partitions []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, partition := range partitions {
		s.position[partition.Partition] = int64(partition.Offset)
	}
	return partitions, nil
}

type remediationDeliveries struct {
	mu        sync.Mutex
	delivered map[int32][]int64
	committed map[int32]int64
}

func newRemediationDeliveries() *remediationDeliveries {
	return &remediationDeliveries{delivered: make(map[int32][]int64), committed: make(map[int32]int64)}
}

func (d *remediationDeliveries) deliver(partition int32, offset int64) {
	d.mu.Lock()
	d.delivered[partition] = append(d.delivered[partition], offset)
	d.mu.Unlock()
}

func (d *remediationDeliveries) commit(partition int32, next int64) {
	d.mu.Lock()
	if next > d.committed[partition] {
		d.committed[partition] = next
	}
	d.mu.Unlock()
}

func (d *remediationDeliveries) snapshot() (map[int32][]int64, map[int32]int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delivered := make(map[int32][]int64, len(d.delivered))
	for partition, offsets := range d.delivered {
		delivered[partition] = append([]int64(nil), offsets...)
	}
	committed := make(map[int32]int64, len(d.committed))
	for partition, offset := range d.committed {
		committed[partition] = offset
	}
	return delivered, committed
}

var errRemediationInjected = errors.New("injected handler failure")

// ---------------------------------------------------------------------------
// K-a: CommitBeforeHandler message mode keeps later fetched records replayable
// ---------------------------------------------------------------------------

func TestRemediationConfluentCommitBeforeHandlerRewindsToFirstUnstartedRecord(t *testing.T) {
	topic := "orders"
	var messages []*ckafka.Message
	for _, offset := range []int64{0, 1, 2, 3} {
		messages = append(messages, &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: ckafka.Offset(offset)}})
	}
	for _, offset := range []int64{10, 11} {
		messages = append(messages, &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: ckafka.Offset(offset)}})
	}
	readIndex := 0
	var delivered []string
	var mu sync.Mutex
	driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		if readIndex >= len(messages) {
			return nil, errors.New("unexpected read after failed wave")
		}
		message := messages[readIndex]
		readIndex++
		return message, nil
	}}
	consumer := newTestConfluentConsumer(false, driver)
	err := consumer.ConsumeAdvanced(func(payload MessagePayload) error {
		mu.Lock()
		delivered = append(delivered, fmt.Sprintf("%d@%d", payload.Partition, payload.Offset))
		mu.Unlock()
		if (payload.Partition == 0 && payload.Offset == 1) || (payload.Partition == 1 && payload.Offset == 11) {
			return errRemediationInjected
		}
		return nil
	}, ConsumerAdvancedOptions{
		AutoCommit:                     boolPointer(false),
		CommitBeforeHandler:            true,
		PartitionsConsumedConcurrently: 2,
		MaxRecords:                     len(messages),
	})
	if !errors.Is(err, errRemediationInjected) {
		t.Fatalf("consume error = %v, want injected handler error", err)
	}
	seeks := driver.seeks()
	if len(seeks) != 1 {
		t.Fatalf("seek calls = %#v, want one wave rewind", seeks)
	}
	// p0@1 was committed before its handler and is not replayed; p0@2 is the
	// first unstarted record. p1@11 was the last fetched record: nothing to rewind.
	want := []consumerRecordPosition{{topic: topic, partition: 0, offset: 2}}
	if got := remediationSeekPositions(seeks[0]); !reflect.DeepEqual(got, want) {
		t.Fatalf("rewound positions = %#v, want %#v", got, want)
	}
	sort.Strings(delivered)
	if wantDelivered := []string{"0@0", "0@1", "1@10", "1@11"}; !reflect.DeepEqual(delivered, wantDelivered) {
		t.Fatalf("delivered = %v, want %v", delivered, wantDelivered)
	}
	if commits, _, _ := driver.operationCalls(); commits != 4 {
		t.Fatalf("pre-handler commits = %d, want 4", commits)
	}
}

func TestRemediationConfluentCommitBeforeHandlerNoLossAcrossRetries(t *testing.T) {
	cases := []struct {
		name        string
		partitions  map[int32]int64
		failAt      map[int32]int64
		concurrency int
	}{
		{name: "single partition", partitions: map[int32]int64{0: 4}, failAt: map[int32]int64{0: 1}, concurrency: 1},
		{name: "two partitions concurrently", partitions: map[int32]int64{0: 4, 1: 4}, failAt: map[int32]int64{0: 1, 1: 2}, concurrency: 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			source := newConfluentMessageSource("orders", test.partitions)
			deliveries := newRemediationDeliveries()
			driver := &fakeConfluentConsumerDriver{
				readFn: source.read,
				seekFn: source.seek,
				commitFn: func(message *ckafka.Message) ([]ckafka.TopicPartition, error) {
					deliveries.commit(message.TopicPartition.Partition, int64(message.TopicPartition.Offset)+1)
					return nil, nil
				},
			}
			consumer := newTestConfluentConsumer(false, driver)
			failed := make(map[int32]bool)
			var failedMu sync.Mutex
			handler := func(payload MessagePayload) error {
				partition := int32(payload.Partition)
				deliveries.deliver(partition, payload.Offset)
				failedMu.Lock()
				defer failedMu.Unlock()
				if at, ok := test.failAt[partition]; ok && at == payload.Offset && !failed[partition] {
					failed[partition] = true
					return errRemediationInjected
				}
				return nil
			}
			opts := ConsumerAdvancedOptions{
				AutoCommit:                     boolPointer(false),
				CommitBeforeHandler:            true,
				PartitionsConsumedConcurrently: test.concurrency,
				MaxRecords:                     8,
			}
			for attempt := 0; attempt < 5; attempt++ {
				err := runConfluentUntilIdle(t, consumer, source.servedCount, handler, opts)
				if err == nil {
					break
				}
				if !errors.Is(err, errRemediationInjected) {
					t.Fatalf("attempt %d: consume error = %v", attempt, err)
				}
			}
			delivered, committed := deliveries.snapshot()
			for partition, end := range test.partitions {
				want := make([]int64, 0, end)
				for offset := int64(0); offset < end; offset++ {
					want = append(want, offset)
				}
				if !reflect.DeepEqual(delivered[partition], want) {
					t.Fatalf("partition %d delivered = %v, want %v (failed record once, no loss)", partition, delivered[partition], want)
				}
				if committed[partition] != end {
					t.Fatalf("partition %d committed = %d, want %d", partition, committed[partition], end)
				}
			}
		})
	}
}

// runConfluentUntilIdle runs one Consume call and closes the run once reads go
// idle. It returns the Consume error, or nil if the run drained without error.
func runConfluentUntilIdle(
	t *testing.T,
	consumer *ConfluentConsumer,
	progress func() int,
	handler MessageHandler,
	opts ConsumerAdvancedOptions,
) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- consumer.ConsumeAdvanced(handler, opts) }()
	idleSince := time.Now()
	lastReads := -1
	for {
		select {
		case err := <-result:
			return err
		case <-time.After(10 * time.Millisecond):
		}
		reads := progress()
		if reads != lastReads {
			lastReads = reads
			idleSince = time.Now()
			continue
		}
		if time.Since(idleSince) > 50*time.Millisecond {
			consumer.mu.Lock()
			cancel := consumer.cancelFn
			consumer.mu.Unlock()
			if cancel != nil {
				cancel()
			}
			select {
			case err := <-result:
				return err
			case <-time.After(2 * time.Second):
				t.Fatal("Consume did not stop after cancellation")
			}
		}
	}
}

func TestRemediationFranzCommitBeforeHandlerRewindsToFirstUnstartedRecord(t *testing.T) {
	groups := [][]*kgo.Record{
		{
			{Topic: "orders", Partition: 0, Offset: 0},
			{Topic: "orders", Partition: 0, Offset: 1},
			{Topic: "orders", Partition: 0, Offset: 2},
			{Topic: "orders", Partition: 0, Offset: 3},
		},
		{
			{Topic: "orders", Partition: 1, Offset: 10},
			{Topic: "orders", Partition: 1, Offset: 11},
		},
	}
	polls := 0
	client := &fakeFranzKafkaJS2CompatClient{pollFn: func(context.Context, int) kgo.Fetches {
		polls++
		if polls == 1 {
			return remediationFetches(groups...)
		}
		return kgo.NewErrFetch(errors.New("unexpected second poll"))
	}}
	consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	var mu sync.Mutex
	var delivered []string
	err := consumer.ConsumeAdvanced(func(payload MessagePayload) error {
		mu.Lock()
		delivered = append(delivered, fmt.Sprintf("%d@%d", payload.Partition, payload.Offset))
		mu.Unlock()
		if (payload.Partition == 0 && payload.Offset == 1) || (payload.Partition == 1 && payload.Offset == 11) {
			return errRemediationInjected
		}
		return nil
	}, ConsumerAdvancedOptions{
		CommitBeforeHandler:            true,
		PartitionsConsumedConcurrently: 2,
		MaxRecords:                     6,
	})
	if !errors.Is(err, errRemediationInjected) {
		t.Fatalf("consume error = %v, want injected handler error", err)
	}
	got := client.rewoundOffsets()
	sortRemediationPositions(got)
	want := []consumerRecordPosition{{topic: "orders", partition: 0, offset: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rewound positions = %#v, want %#v", got, want)
	}
	sort.Strings(delivered)
	if wantDelivered := []string{"0@0", "0@1", "1@10", "1@11"}; !reflect.DeepEqual(delivered, wantDelivered) {
		t.Fatalf("delivered = %v, want %v", delivered, wantDelivered)
	}
	if _, closes, _, _ := client.snapshot(); closes != 0 {
		t.Fatalf("commit-before handler failure rebuilt client; closes=%d", closes)
	}
}

func TestRemediationFranzCommitBeforeHandlerNoLossAcrossRetries(t *testing.T) {
	cases := []struct {
		name        string
		partitions  map[int32]int64
		failAt      map[int32]int64
		concurrency int
	}{
		{name: "single partition", partitions: map[int32]int64{0: 4}, failAt: map[int32]int64{0: 1}, concurrency: 1},
		{name: "two partitions concurrently", partitions: map[int32]int64{0: 4, 1: 4}, failAt: map[int32]int64{0: 1, 1: 2}, concurrency: 2},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			deliveries := newRemediationDeliveries()
			position := make(map[int32]int64)
			appliedRewinds := 0
			client := &fakeFranzKafkaJS2CompatClient{}
			client.pollFn = func(ctx context.Context, _ int) kgo.Fetches {
				// Model franz-go: SetOffsets moves the partition fetch position.
				rewinds := client.rewoundOffsets()
				for _, rewind := range rewinds[appliedRewinds:] {
					position[rewind.partition] = rewind.offset
				}
				appliedRewinds = len(rewinds)
				var groups [][]*kgo.Record
				for partition := int32(0); partition < int32(len(test.partitions)); partition++ {
					var group []*kgo.Record
					for offset := position[partition]; offset < test.partitions[partition]; offset++ {
						group = append(group, &kgo.Record{Topic: "orders", Partition: partition, Offset: offset})
					}
					position[partition] = test.partitions[partition]
					if len(group) > 0 {
						groups = append(groups, group)
					}
				}
				if len(groups) == 0 {
					<-ctx.Done()
					return kgo.Fetches{}
				}
				return remediationFetches(groups...)
			}
			client.commitRecordsFn = func(_ context.Context, records ...*kgo.Record) error {
				for _, record := range records {
					deliveries.commit(record.Partition, record.Offset+1)
				}
				return nil
			}
			consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			failed := make(map[int32]bool)
			var failedMu sync.Mutex
			var remaining atomic.Int64
			for _, end := range test.partitions {
				remaining.Add(end)
			}
			remaining.Add(int64(len(test.failAt))) // failed records are delivered once more as "seen"
			done := make(chan struct{})
			var doneOnce sync.Once
			seen := make(map[string]bool)
			handler := func(payload MessagePayload) error {
				partition := int32(payload.Partition)
				deliveries.deliver(partition, payload.Offset)
				failedMu.Lock()
				defer failedMu.Unlock()
				key := fmt.Sprintf("%d@%d", partition, payload.Offset)
				if !seen[key] {
					seen[key] = true
					if remaining.Add(-1) == int64(len(test.failAt)) {
						doneOnce.Do(func() { close(done) })
					}
				}
				if at, ok := test.failAt[partition]; ok && at == payload.Offset && !failed[partition] {
					failed[partition] = true
					return errRemediationInjected
				}
				return nil
			}
			opts := ConsumerAdvancedOptions{
				CommitBeforeHandler:            true,
				PartitionsConsumedConcurrently: test.concurrency,
				MaxRecords:                     8,
			}
			result := make(chan error, 1)
			go func() {
				for {
					err := consumer.ConsumeAdvanced(handler, opts)
					if err == nil || !errors.Is(err, errRemediationInjected) {
						result <- err
						return
					}
				}
			}()
			select {
			case <-done:
			case err := <-result:
				t.Fatalf("consume loop ended early: %v", err)
			case <-time.After(5 * time.Second):
				delivered, _ := deliveries.snapshot()
				t.Fatalf("records were lost after CommitBeforeHandler failure; delivered=%v", delivered)
			}
			if err := consumer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := <-result; err != nil {
				t.Fatalf("Consume after Close = %v, want nil", err)
			}
			delivered, committed := deliveries.snapshot()
			for partition, end := range test.partitions {
				want := make([]int64, 0, end)
				for offset := int64(0); offset < end; offset++ {
					want = append(want, offset)
				}
				if !reflect.DeepEqual(delivered[partition], want) {
					t.Fatalf("partition %d delivered = %v, want %v (failed record once, no loss)", partition, delivered[partition], want)
				}
				if committed[partition] != end {
					t.Fatalf("partition %d committed = %d, want %d", partition, committed[partition], end)
				}
			}
		})
	}
}

// TestRemediationConfluentCommitBeforeHandlerMockCluster exercises the real
// librdkafka seek path: a CommitBeforeHandler failure must not skip later
// records that were already fetched in the same wave.
func TestRemediationConfluentCommitBeforeHandlerMockCluster(t *testing.T) {
	cases := []struct {
		name        string
		partitions  int
		failAt      map[int32]int64
		concurrency int
	}{
		{name: "single partition", partitions: 1, failAt: map[int32]int64{0: 1}, concurrency: 1},
		{name: "two partitions concurrently", partitions: 2, failAt: map[int32]int64{0: 1, 1: 2}, concurrency: 2},
	}
	const recordsPerPartition = 4
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cluster, err := ckafka.NewMockCluster(1)
			if err != nil {
				t.Fatalf("NewMockCluster: %v", err)
			}
			t.Cleanup(cluster.Close)
			topic := "cbh-orders"
			if err := cluster.CreateTopic(topic, test.partitions, 1); err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			produceRemediationMockRecords(t, cluster.BootstrapServers(), topic, test.partitions, recordsPerPartition)

			client, err := NewConfluentClient(&Config{Brokers: []string{cluster.BootstrapServers()}, ClientID: "cbh-mock"})
			if err != nil {
				t.Fatalf("NewConfluentClient: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })
			group := "cbh-group-" + strings.ReplaceAll(test.name, " ", "-")
			config := DefaultConsumerAdvancedConfig(group)
			config.AutoCommit = false
			config.MaxWaitTime = 100 * time.Millisecond
			consumerAPI, err := client.ConsumerAdvanced(config)
			if err != nil {
				t.Fatalf("ConsumerAdvanced: %v", err)
			}
			if err := consumerAPI.Connect([]TopicConfig{{Topic: topic, FromBeginning: true}}); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			consumer := consumerAPI.(*ConfluentConsumer)

			deliveries := newRemediationDeliveries()
			failed := make(map[int32]bool)
			var mu sync.Mutex
			last := make(map[int32]bool)
			done := make(chan struct{})
			var doneOnce sync.Once
			handler := func(payload MessagePayload) error {
				partition := int32(payload.Partition)
				deliveries.deliver(partition, payload.Offset)
				mu.Lock()
				defer mu.Unlock()
				if payload.Offset == recordsPerPartition-1 {
					last[partition] = true
					if len(last) == test.partitions {
						doneOnce.Do(func() { close(done) })
					}
				}
				if at, ok := test.failAt[partition]; ok && at == payload.Offset && !failed[partition] {
					failed[partition] = true
					return errRemediationInjected
				}
				return nil
			}
			opts := ConsumerAdvancedOptions{
				AutoCommit:                     boolPointer(false),
				CommitBeforeHandler:            true,
				PartitionsConsumedConcurrently: test.concurrency,
				MaxRecords:                     test.partitions * recordsPerPartition,
			}
			result := make(chan error, 1)
			go func() {
				for {
					err := consumer.ConsumeAdvanced(handler, opts)
					if err == nil || !errors.Is(err, errRemediationInjected) {
						result <- err
						return
					}
				}
			}()
			select {
			case <-done:
			case err := <-result:
				t.Fatalf("consume loop ended early: %v", err)
			case <-time.After(30 * time.Second):
				delivered, _ := deliveries.snapshot()
				t.Fatalf("records were lost after CommitBeforeHandler failure; delivered=%v", delivered)
			}
			if err := consumer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := <-result; err != nil {
				t.Fatalf("Consume after Close = %v, want nil", err)
			}
			delivered, _ := deliveries.snapshot()
			want := []int64{0, 1, 2, 3}
			for partition := int32(0); partition < int32(test.partitions); partition++ {
				if !reflect.DeepEqual(delivered[partition], want) {
					t.Fatalf("partition %d delivered = %v, want %v", partition, delivered[partition], want)
				}
			}
			assertRemediationMockCommitted(t, cluster.BootstrapServers(), group, topic, test.partitions, recordsPerPartition)
		})
	}
}

func produceRemediationMockRecords(t *testing.T, bootstrap, topic string, partitions, perPartition int) {
	t.Helper()
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()
	deliveries := make(chan ckafka.Event, partitions*perPartition)
	for partition := 0; partition < partitions; partition++ {
		for i := 0; i < perPartition; i++ {
			if err := producer.Produce(&ckafka.Message{
				TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
				Value:          []byte(fmt.Sprintf("p%d-%d", partition, i)),
			}, deliveries); err != nil {
				t.Fatalf("Produce: %v", err)
			}
		}
	}
	for i := 0; i < partitions*perPartition; i++ {
		select {
		case event := <-deliveries:
			if message, ok := event.(*ckafka.Message); ok && message.TopicPartition.Error != nil {
				t.Fatalf("delivery failed: %v", message.TopicPartition.Error)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for mock-cluster delivery")
		}
	}
}

func assertRemediationMockCommitted(t *testing.T, bootstrap, group, topic string, partitions int, want int64) {
	t.Helper()
	checker, err := ckafka.NewConsumer(&ckafka.ConfigMap{"bootstrap.servers": bootstrap, "group.id": group})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer checker.Close()
	requested := make([]ckafka.TopicPartition, partitions)
	for i := range requested {
		requested[i] = ckafka.TopicPartition{Topic: &topic, Partition: int32(i)}
	}
	committed, err := checker.Committed(requested, 10000)
	if err != nil {
		t.Fatalf("Committed: %v", err)
	}
	for _, partition := range committed {
		if int64(partition.Offset) != want {
			t.Fatalf("committed %s[%d] = %v, want %d", topic, partition.Partition, partition.Offset, want)
		}
	}
}

// ---------------------------------------------------------------------------
// K-c/K-g: Confluent rewinds seek only owned partitions and fail closed
// ---------------------------------------------------------------------------

func TestRemediationConfluentRevocationDuringWaveSeeksOnlyOwnedPartitions(t *testing.T) {
	topic := "orders"
	messages := []*ckafka.Message{
		{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 5}},
		{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 9}},
	}
	var readMu sync.Mutex
	readIndex := 0
	driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
		readMu.Lock()
		defer readMu.Unlock()
		if readIndex >= len(messages) {
			return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
		}
		message := messages[readIndex]
		readIndex++
		return message, nil
	}}
	consumer := newTestConfluentConsumer(false, driver)
	err := consumer.ConsumeAdvanced(func(payload MessagePayload) error {
		if payload.Partition == 1 {
			// Simulate the rebalance callback revoking partition 1 while its
			// handler is in flight.
			consumer.clearPendingRewindsForPartitions([]ckafka.TopicPartition{{Topic: &topic, Partition: 1}})
		}
		return errRemediationInjected
	}, ConsumerAdvancedOptions{PartitionsConsumedConcurrently: 2, MaxRecords: 2})
	if !errors.Is(err, errRemediationInjected) {
		t.Fatalf("consume error = %v, want injected handler error", err)
	}
	seeks := driver.seeks()
	if len(seeks) != 1 {
		t.Fatalf("seek calls = %#v, want one seek", seeks)
	}
	want := []consumerRecordPosition{{topic: topic, partition: 0, offset: 5}}
	if got := remediationSeekPositions(seeks[0]); !reflect.DeepEqual(got, want) {
		t.Fatalf("seek positions = %#v, want only owned %#v", got, want)
	}
	if len(consumer.pendingRewinds) != 0 {
		t.Fatalf("pending rewinds = %#v, want none", consumer.pendingRewinds)
	}
	if commits, _, closes := driver.operationCalls(); commits != 0 || closes != 0 {
		t.Fatalf("commits=%d closes=%d, want no commit of failed records and no rebuild", commits, closes)
	}

	// The consumer is not stuck: the next run reads again.
	before := len(driver.reads())
	result := make(chan error, 1)
	go func() {
		result <- consumer.ConsumeAdvanced(func(MessagePayload) error { return nil }, ConsumerAdvancedOptions{})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(driver.reads()) == before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(driver.reads()) == before {
		t.Fatal("consumer did not resume reading after revoked-partition failure")
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("second Consume = %v, want nil", err)
	}
}

type assignmentAwareConfluentDriver struct {
	*fakeConfluentConsumerDriver
	assignment []ckafka.TopicPartition
}

func (d *assignmentAwareConfluentDriver) Assignment() ([]ckafka.TopicPartition, error) {
	return d.assignment, nil
}

func TestRemediationConfluentRewindSkipsPartitionsOutsideCurrentAssignment(t *testing.T) {
	topic := "orders"
	inner := &fakeConfluentConsumerDriver{}
	driver := &assignmentAwareConfluentDriver{
		fakeConfluentConsumerDriver: inner,
		assignment:                  []ckafka.TopicPartition{{Topic: &topic, Partition: 0}},
	}
	consumer := newTestConfluentConsumer(false, driver)
	err := errors.Join(
		withConsumerRewindPosition(errors.New("owned failed"), consumerRecordPosition{topic: topic, partition: 0, offset: 3}),
		withConsumerRewindPosition(errors.New("unowned failed"), consumerRecordPosition{topic: topic, partition: 1, offset: 8}),
	)
	if got := consumer.prepareHandlerRunRetry(driver, err); got != err {
		t.Fatalf("prepare error = %v, want original", got)
	}
	seeks := inner.seeks()
	want := []consumerRecordPosition{{topic: topic, partition: 0, offset: 3}}
	if len(seeks) != 1 || !reflect.DeepEqual(remediationSeekPositions(seeks[0]), want) {
		t.Fatalf("seeks = %#v, want only %#v", seeks, want)
	}
}

func TestRemediationConfluentPersistentSeekFailureFailsClosed(t *testing.T) {
	topic := "orders"
	seekErr := errors.New("partition seek rejected")
	var seekOK atomic.Bool
	var mu sync.Mutex
	served := false
	driver := &fakeConfluentConsumerDriver{
		readFn: func(time.Duration) (*ckafka.Message, error) {
			mu.Lock()
			defer mu.Unlock()
			if !served {
				served = true
				return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 5}}, nil
			}
			return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
		},
		seekFn: func(partitions []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
			if seekOK.Load() {
				return partitions, nil
			}
			return nil, seekErr
		},
	}
	consumer := newTestConfluentConsumer(false, driver)
	err := consumer.ConsumeAdvanced(func(MessagePayload) error { return errRemediationInjected }, ConsumerAdvancedOptions{})
	if !errors.Is(err, errRemediationInjected) {
		t.Fatalf("first Consume = %v, want handler error", err)
	}
	if consumer.consumer != driver {
		t.Fatal("failed seek rebuilt the consumer (would resume from auto.offset.reset)")
	}
	if want := []consumerRecordPosition{{topic: topic, partition: 0, offset: 5}}; !reflect.DeepEqual(consumer.pendingRewinds, want) {
		t.Fatalf("pending rewinds = %#v, want %#v", consumer.pendingRewinds, want)
	}

	readsBefore := len(driver.reads())
	for attempt := 0; attempt < 3; attempt++ {
		err = consumer.ConsumeAdvanced(func(MessagePayload) error {
			t.Error("handler ran while exact rewind was pending")
			return nil
		}, ConsumerAdvancedOptions{})
		if err == nil || !errors.Is(err, seekErr) || !strings.Contains(err.Error(), "pending exact handler offset rewind failed") {
			t.Fatalf("retry %d Consume = %v, want pending seek failure", attempt, err)
		}
	}
	if reads := len(driver.reads()); reads != readsBefore {
		t.Fatalf("reads while rewind pending = %d, want %d", reads, readsBefore)
	}
	if commits, stores, closes := driver.operationCalls(); commits != 0 || stores != 0 || closes != 0 {
		t.Fatalf("commits=%d stores=%d closes=%d; nothing may be committed past the unprocessed record and no rebuild", commits, stores, closes)
	}

	// Once the seek succeeds the run proceeds normally.
	seekOK.Store(true)
	result := make(chan error, 1)
	go func() {
		result <- consumer.ConsumeAdvanced(func(MessagePayload) error { return nil }, ConsumerAdvancedOptions{})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(driver.reads()) == readsBefore && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(driver.reads()) == readsBefore {
		t.Fatal("consumer did not resume after the pending seek succeeded")
	}
	if len(consumer.pendingRewindsSnapshot()) != 0 {
		t.Fatalf("pending rewinds after successful seek = %#v", consumer.pendingRewindsSnapshot())
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("Consume after recovery = %v", err)
	}
}

func (cc *ConfluentConsumer) pendingRewindsSnapshot() []consumerRecordPosition {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return append([]consumerRecordPosition(nil), cc.pendingRewinds...)
}

func TestRemediationConfluentRevocationPrunesPendingRewinds(t *testing.T) {
	orders := "orders"
	payments := "payments"
	consumer := newTestConfluentConsumer(false, &fakeConfluentConsumerDriver{})
	consumer.pendingRewinds = []consumerRecordPosition{
		{topic: orders, partition: 0, offset: 8},
		{topic: orders, partition: 1, offset: 13},
		{topic: payments, partition: 0, offset: 21},
	}
	consumer.clearPendingRewindsForPartitions([]ckafka.TopicPartition{
		{Topic: &orders, Partition: 0},
		{Topic: &payments, Partition: 0},
	})
	want := []consumerRecordPosition{{topic: orders, partition: 1, offset: 13}}
	if !reflect.DeepEqual(consumer.pendingRewinds, want) {
		t.Fatalf("pending rewinds after revocation = %#v, want %#v", consumer.pendingRewinds, want)
	}
	// Reassignment makes the partition eligible for exact rewinds again.
	consumer.markRewindPartitionsAssigned([]ckafka.TopicPartition{{Topic: &orders, Partition: 0}})
	if got := consumer.rewindPositionsStillOwned(nil, []consumerRecordPosition{{topic: orders, partition: 0, offset: 1}}); len(got) != 1 {
		t.Fatalf("reassigned partition filtered out: %#v", got)
	}
}

func TestRemediationConfluentRevocationRacingFailedSeekDoesNotStick(t *testing.T) {
	topic := "orders"
	var consumer *ConfluentConsumer
	driver := &fakeConfluentConsumerDriver{seekFn: func([]ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
		consumer.clearPendingRewindsForPartitions([]ckafka.TopicPartition{{Topic: &topic, Partition: 2}})
		return nil, errors.New("partition became unassigned")
	}}
	consumer = newTestConfluentConsumer(false, driver)
	err := newConsumerHandlerErrorAt("handler failed", errors.New("boom"), consumerRecordPosition{topic: topic, partition: 2, offset: 17})
	if got := consumer.prepareHandlerRunRetry(driver, err); got != err {
		t.Fatalf("prepare error = %v, want original handler error", got)
	}
	if len(consumer.pendingRewinds) != 0 {
		t.Fatalf("pending rewinds after seek/revoke race = %#v, want none", consumer.pendingRewinds)
	}
	if err := consumer.applyPendingRewinds(driver); err != nil {
		t.Fatalf("applyPendingRewinds = %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// K-f: legacy rebalance hooks and Close
// ---------------------------------------------------------------------------

func TestRemediationAwaitHookStillRunsWhenCloseAlreadyRequested(t *testing.T) {
	closeRequested := make(chan struct{})
	close(closeRequested)
	called := make(chan struct{})
	if completed := awaitConsumerRebalanceHook(closeRequested, func() { close(called) }); completed {
		t.Fatal("hook reported completion after shutdown started; later lifecycle hooks must stay suppressed")
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("legacy hook was skipped during Close")
	}
	if !awaitConsumerRebalanceHook(nil, func() {}) {
		t.Fatal("hook without close signal must complete synchronously")
	}
}

func TestRemediationConfluentLegacyHooksCanCloseConsumer(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*ConfluentConsumer, chan<- error, *atomic.Bool)
		invoke    func(*ConfluentConsumer)
	}{
		{
			name: "assigned",
			configure: func(consumer *ConfluentConsumer, returned chan<- error, lifecycleCalled *atomic.Bool) {
				consumer.config.OnPartitionsAssigned = func([]PartitionAssignment) { returned <- consumer.Close() }
				consumer.config.OnPartitionsAssignedWithLifecycle = func([]PartitionAssignment, RebalanceLifecycle) { lifecycleCalled.Store(true) }
			},
			invoke: func(consumer *ConfluentConsumer) {
				consumer.invokePartitionsAssigned([]PartitionAssignment{{Topic: "orders", Partition: 0}})
			},
		},
		{
			name: "revoked",
			configure: func(consumer *ConfluentConsumer, returned chan<- error, lifecycleCalled *atomic.Bool) {
				consumer.config.OnPartitionsRevoked = func([]PartitionAssignment) { returned <- consumer.Close() }
				consumer.config.OnPartitionsRevokedWithLifecycle = func([]PartitionAssignment, RebalanceLifecycle) { lifecycleCalled.Store(true) }
			},
			invoke: func(consumer *ConfluentConsumer) {
				consumer.invokePartitionsRevoked([]PartitionAssignment{{Topic: "orders", Partition: 0}})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &fakeConfluentConsumerDriver{}
			runDone := make(chan struct{})
			consumer := newTestConfluentConsumer(false, driver)
			consumer.runDone = runDone
			closeReturned := make(chan error, 1)
			var lifecycleCalled atomic.Bool
			test.configure(consumer, closeReturned, &lifecycleCalled)

			callbackDone := make(chan struct{})
			go func() {
				test.invoke(consumer)
				close(callbackDone)
			}()
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("legacy rebalance callback deadlocked after hook called Close")
			}
			if lifecycleCalled.Load() {
				t.Fatal("lifecycle hook ran after legacy hook requested shutdown")
			}
			close(runDone)
			select {
			case err := <-closeReturned:
				if err != nil {
					t.Fatalf("Close from legacy hook: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close from legacy hook did not finish after run drained")
			}
			if _, _, closes := driver.operationCalls(); closes != 1 {
				t.Fatalf("driver closes = %d, want 1", closes)
			}
		})
	}
}

func TestRemediationConfluentExternalCloseDuringSlowLegacyHook(t *testing.T) {
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	t.Cleanup(func() { close(releaseHook) })
	consumer.config.OnPartitionsAssigned = func([]PartitionAssignment) {
		close(hookStarted)
		<-releaseHook
	}
	callbackDone := make(chan struct{})
	go func() {
		consumer.invokePartitionsAssigned([]PartitionAssignment{{Topic: "orders", Partition: 0}})
		close(callbackDone)
	}()
	<-hookStarted
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- consumer.Close() }()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("external Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("external Close blocked on a slow legacy hook")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("driver callback was not released by external Close")
	}
}

func TestRemediationConfluentLegacyRevokeHookRunsOnExternalClose(t *testing.T) {
	var consumer *ConfluentConsumer
	var hookCalls atomic.Int32
	hookCloseErr := make(chan error, 1)
	driver := &fakeConfluentConsumerDriver{closeFn: func() error {
		// librdkafka emits the final revoke from inside consumer Close.
		consumer.invokePartitionsRevoked([]PartitionAssignment{{Topic: "orders", Partition: 0}})
		return nil
	}}
	consumer = newTestConfluentConsumer(false, driver)
	consumer.config.OnPartitionsRevoked = func([]PartitionAssignment) {
		hookCalls.Add(1)
		// A legacy hook calling Close during an external Close must not deadlock.
		hookCloseErr <- consumer.Close()
	}
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- consumer.Close() }()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("external Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("external Close deadlocked on the legacy revoke hook")
	}
	select {
	case err := <-hookCloseErr:
		if err != nil {
			t.Fatalf("Close from revoke hook: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy revoke hook did not run on external Close")
	}
	if calls := hookCalls.Load(); calls != 1 {
		t.Fatalf("revoke hook calls = %d, want 1", calls)
	}
}

func TestRemediationConfluentMockClusterLegacyHookCloses(t *testing.T) {
	cluster, err := ckafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("NewMockCluster: %v", err)
	}
	t.Cleanup(cluster.Close)
	topic := "hook-orders"
	if err := cluster.CreateTopic(topic, 1, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	client, err := NewConfluentClient(&Config{Brokers: []string{cluster.BootstrapServers()}, ClientID: "hook-mock"})
	if err != nil {
		t.Fatalf("NewConfluentClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	t.Run("assigned hook calls Close", func(t *testing.T) {
		var consumerAPI KafkaConsumer
		hookClosed := make(chan error, 1)
		var revokeCalls atomic.Int32
		config := DefaultConsumerAdvancedConfig("hook-close-group")
		config.OnPartitionsAssigned = func([]PartitionAssignment) { hookClosed <- consumerAPI.Close() }
		config.OnPartitionsRevoked = func([]PartitionAssignment) { revokeCalls.Add(1) }
		consumerAPI, err = client.ConsumerAdvanced(config)
		if err != nil {
			t.Fatalf("ConsumerAdvanced: %v", err)
		}
		if err := consumerAPI.Connect([]TopicConfig{{Topic: topic}}); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		result := make(chan error, 1)
		go func() { result <- consumerAPI.Consume(func(MessagePayload) error { return nil }, ConsumerOptions{}) }()
		select {
		case err := <-hookClosed:
			if err != nil {
				t.Fatalf("Close from hook: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("legacy assigned hook calling Close deadlocked")
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Consume = %v, want nil after hook Close", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Consume did not return after hook Close")
		}
	})

	t.Run("external Close during slow hook and revoke hook runs", func(t *testing.T) {
		hookStarted := make(chan struct{})
		releaseHook := make(chan struct{})
		t.Cleanup(func() { close(releaseHook) })
		revoked := make(chan struct{}, 4)
		var startedOnce sync.Once
		config := DefaultConsumerAdvancedConfig("hook-slow-group")
		config.OnPartitionsAssigned = func([]PartitionAssignment) {
			startedOnce.Do(func() { close(hookStarted) })
			<-releaseHook
		}
		config.OnPartitionsRevoked = func([]PartitionAssignment) { revoked <- struct{}{} }
		consumerAPI, err := client.ConsumerAdvanced(config)
		if err != nil {
			t.Fatalf("ConsumerAdvanced: %v", err)
		}
		if err := consumerAPI.Connect([]TopicConfig{{Topic: topic}}); err != nil {
			t.Fatalf("Connect: %v", err)
		}
		result := make(chan error, 1)
		go func() { result <- consumerAPI.Consume(func(MessagePayload) error { return nil }, ConsumerOptions{}) }()
		select {
		case <-hookStarted:
		case <-time.After(30 * time.Second):
			t.Fatal("partitions were never assigned")
		}
		closeReturned := make(chan error, 1)
		go func() { closeReturned <- consumerAPI.Close() }()
		select {
		case err := <-closeReturned:
			if err != nil {
				t.Fatalf("external Close: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("external Close blocked on slow legacy hook")
		}
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Consume = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Consume did not return after external Close")
		}
		select {
		case <-revoked:
		case <-time.After(5 * time.Second):
			t.Fatal("legacy revoke hook did not run on external Close")
		}
	})
}

func TestRemediationFranzLegacyHooksCanCloseConsumer(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*franzKafkaJS2CompatConsumer, chan<- error, *atomic.Bool)
		invoke    func(*franzKafkaJS2CompatConsumer)
	}{
		{
			name: "assigned",
			configure: func(consumer *franzKafkaJS2CompatConsumer, returned chan<- error, lifecycleCalled *atomic.Bool) {
				consumer.config.OnPartitionsAssigned = func([]PartitionAssignment) { returned <- consumer.Close() }
				consumer.config.OnPartitionsAssignedWithLifecycle = func([]PartitionAssignment, RebalanceLifecycle) { lifecycleCalled.Store(true) }
			},
			invoke: func(consumer *franzKafkaJS2CompatConsumer) {
				consumer.invokePartitionsAssigned([]PartitionAssignment{{Topic: "orders", Partition: 0}})
			},
		},
		{
			name: "revoked",
			configure: func(consumer *franzKafkaJS2CompatConsumer, returned chan<- error, lifecycleCalled *atomic.Bool) {
				consumer.config.OnPartitionsRevoked = func([]PartitionAssignment) { returned <- consumer.Close() }
				consumer.config.OnPartitionsRevokedWithLifecycle = func([]PartitionAssignment, RebalanceLifecycle) { lifecycleCalled.Store(true) }
			},
			invoke: func(consumer *franzKafkaJS2CompatConsumer) {
				consumer.handlePartitionsRevoked(context.Background(), nil, map[string][]int32{"orders": {0}})
			},
		},
		{
			name: "lost",
			configure: func(consumer *franzKafkaJS2CompatConsumer, returned chan<- error, lifecycleCalled *atomic.Bool) {
				consumer.config.OnPartitionsLost = func([]PartitionAssignment) { returned <- consumer.Close() }
				consumer.config.OnPartitionsLostWithLifecycle = func([]PartitionAssignment, RebalanceLifecycle) { lifecycleCalled.Store(true) }
			},
			invoke: func(consumer *franzKafkaJS2CompatConsumer) {
				consumer.handlePartitionsLost(map[string][]int32{"orders": {0}})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeFranzKafkaJS2CompatClient{}
			runDone := make(chan struct{})
			consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			consumer.runDone = runDone
			closeReturned := make(chan error, 1)
			var lifecycleCalled atomic.Bool
			test.configure(consumer, closeReturned, &lifecycleCalled)

			callbackDone := make(chan struct{})
			go func() {
				test.invoke(consumer)
				close(callbackDone)
			}()
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("legacy rebalance callback deadlocked after hook called Close")
			}
			if lifecycleCalled.Load() {
				t.Fatal("lifecycle hook ran after legacy hook requested shutdown")
			}
			close(runDone)
			select {
			case err := <-closeReturned:
				if err != nil {
					t.Fatalf("Close from legacy hook: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close from legacy hook did not finish after run drained")
			}
			if _, closes, _, _ := client.snapshot(); closes != 1 {
				t.Fatalf("client closes = %d, want 1", closes)
			}
		})
	}
}

func TestRemediationFranzExternalCloseDuringSlowLegacyHook(t *testing.T) {
	client := &fakeFranzKafkaJS2CompatClient{}
	consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	t.Cleanup(func() { close(releaseHook) })
	consumer.config.OnPartitionsAssigned = func([]PartitionAssignment) {
		close(hookStarted)
		<-releaseHook
	}
	callbackDone := make(chan struct{})
	go func() {
		consumer.invokePartitionsAssigned([]PartitionAssignment{{Topic: "orders", Partition: 0}})
		close(callbackDone)
	}()
	<-hookStarted
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- consumer.Close() }()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("external Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("external Close blocked on a slow legacy hook")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("franz callback was not released by external Close")
	}
}

// revokingFranzClient models franz-go invoking the revoke callback from inside
// CloseAllowingRebalance.
type revokingFranzClient struct {
	*fakeFranzKafkaJS2CompatClient
	onClose func()
}

func (c *revokingFranzClient) CloseAllowingRebalance() {
	c.fakeFranzKafkaJS2CompatClient.CloseAllowingRebalance()
	if c.onClose != nil {
		c.onClose()
	}
}

func TestRemediationFranzLegacyRevokeHookRunsOnExternalClose(t *testing.T) {
	client := &revokingFranzClient{fakeFranzKafkaJS2CompatClient: &fakeFranzKafkaJS2CompatClient{}}
	consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	client.onClose = func() {
		consumer.handlePartitionsRevoked(context.Background(), nil, map[string][]int32{"orders": {0}})
	}
	var hookCalls atomic.Int32
	hookCloseErr := make(chan error, 1)
	consumer.config.OnPartitionsRevoked = func([]PartitionAssignment) {
		hookCalls.Add(1)
		hookCloseErr <- consumer.Close()
	}
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- consumer.Close() }()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("external Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("external Close deadlocked on the legacy revoke hook")
	}
	select {
	case err := <-hookCloseErr:
		if err != nil {
			t.Fatalf("Close from revoke hook: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy revoke hook did not run on external Close")
	}
	if calls := hookCalls.Load(); calls != 1 {
		t.Fatalf("revoke hook calls = %d, want 1", calls)
	}
}

// ---------------------------------------------------------------------------
// K-h: Confluent rejects franz-only lost hooks
// ---------------------------------------------------------------------------

func TestRemediationConfluentRejectsPartitionsLostHooks(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"broker:9092"}})
	if err != nil {
		t.Fatalf("NewConfluentClient error = %v", err)
	}
	defer client.Close()
	configs := []ConsumerAdvancedConfig{
		{GroupID: "group", OnPartitionsLost: func([]PartitionAssignment) {}},
		{GroupID: "group", OnPartitionsLostWithLifecycle: func([]PartitionAssignment, RebalanceLifecycle) {}},
	}
	for _, config := range configs {
		if _, err := client.ConsumerAdvanced(config); err == nil || !strings.Contains(err.Error(), "require the franz KafkaJS-compatible backend") {
			t.Fatalf("ConsumerAdvanced error = %v, want unsupported lost-hook error", err)
		}
		if _, err := NewConsumer(client, config); err == nil {
			t.Fatal("NewConsumer accepted a Confluent OnPartitionsLost hook")
		}
	}
	// The franz backend continues to support lost hooks.
	franzConfig := ConsumerAdvancedConfig{
		GroupID: "group", Backend: ConsumerBackendFranzKafkaJS2Compat,
		OnPartitionsLost: func([]PartitionAssignment) {},
	}
	if _, err := client.ConsumerAdvanced(franzConfig); err != nil {
		t.Fatalf("franz backend rejected OnPartitionsLost: %v", err)
	}
}
