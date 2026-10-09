// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaJSRecoveryAssignmentRespectsOwnershipAndCommittedOffsets(t *testing.T) {
	for _, fallback := range []kgo.Offset{kgo.NewOffset().AtEnd(), kgo.NewOffset().AtStart()} {
		c := &franzKafkaJS2CompatConsumer{}
		positions := []consumerRecordPosition{
			{topic: "orders", partition: 0, offset: 8, leaderEpoch: 3},
			{topic: "orders", partition: 1, offset: 11, leaderEpoch: 4},
			{topic: "orders", partition: 2, offset: 15, leaderEpoch: 5},
			{topic: "orders", partition: 3, offset: 19, leaderEpoch: 6},
			{topic: "other-owner", partition: 0, offset: 24},
		}
		c.rememberRecoveryPositions(positions)
		c.rememberRecoveryPositions([]consumerRecordPosition{{topic: "orders", partition: 0, offset: 9}})
		offsets := map[string]map[int32]kgo.Offset{"orders": {
			0: fallback, 1: kgo.NewOffset().At(5), 2: kgo.NewOffset().At(15), 3: kgo.NewOffset().At(30),
		}}
		got, err := c.adjustRecoveryOffsets(context.Background(), offsets)
		if err != nil {
			t.Fatal(err)
		}
		for partition, want := range map[int32]int64{0: 8, 1: 5, 2: 15, 3: 30} {
			if offset := got["orders"][partition].EpochOffset().Offset; offset != want {
				t.Fatalf("partition %d: offset=%d want=%d", partition, offset, want)
			}
		}
		if got["orders"][0].EpochOffset().Epoch != 3 || len(got) != 1 || len(got["orders"]) != 4 {
			t.Fatalf("adjustment lost epoch or added unowned partition: %v", got)
		}
		wantPending := []consumerRecordPosition{positions[0], positions[1], positions[4]}
		if !reflect.DeepEqual(c.pendingRecovery, wantPending) {
			t.Fatalf("pending=%v want=%v", c.pendingRecovery, wantPending)
		}
		// A partition that moved to another owner is untouched until it is ours
		// again; its new owner's higher durable commit wins over our old failure.
		got, err = c.adjustRecoveryOffsets(context.Background(), map[string]map[int32]kgo.Offset{
			"other-owner": {0: kgo.NewOffset().At(25)},
		})
		if err != nil || got["other-owner"][0].EpochOffset().Offset != 25 || len(c.pendingRecovery) != 2 {
			t.Fatalf("returned ownership: offsets=%v pending=%v err=%v", got, c.pendingRecovery, err)
		}
	}
}

func TestKafkaJSRecoverySurvivesClientReplacementAndCancelledAssignment(t *testing.T) {
	client := &fakeFranzKafkaJS2CompatClient{}
	c := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	position := consumerRecordPosition{topic: "orders", partition: 0, offset: 8, leaderEpoch: 3}
	failed := withConsumerRewindPosition(NewTransientConsumerError(errors.New("handler failed")), position)
	c.prepareTransientRunRetry(client, failed)
	// A subsequent error without a record boundary replaces the SDK driver;
	// the unresolved boundary belongs to the consumer, not that driver.
	c.prepareTransientRunRetry(client, NewTransientConsumerError(kerr.UnknownMemberID))
	if c.client != nil {
		t.Fatal("no-boundary failure did not discard the client")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	offsets := map[string]map[int32]kgo.Offset{"orders": {0: kgo.NewOffset().AtEnd()}}
	if _, err := c.adjustRecoveryOffsets(ctx, offsets); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled adjustment=%v", err)
	}
	if offsets["orders"][0].EpochOffset().Offset != -1 || !reflect.DeepEqual(c.pendingRecovery, []consumerRecordPosition{position}) {
		t.Fatal("cancelled assignment mutated the fallback or discarded recovery")
	}
	got, err := c.adjustRecoveryOffsets(context.Background(), offsets)
	if err != nil || got["orders"][0].EpochOffset().Offset != 8 {
		t.Fatalf("replacement assignment=%v err=%v", got, err)
	}
}

func TestKafkaJSRecoveryClearedOnlyByCompletedPartition(t *testing.T) {
	c := &franzKafkaJS2CompatConsumer{}
	positions := []consumerRecordPosition{
		{topic: "orders", partition: 0, offset: 8},
		{topic: "orders", partition: 1, offset: 11},
		{topic: "audit", partition: 0, offset: 15},
	}
	c.rememberRecoveryPositions(positions)
	c.completeRecoveryPosition(consumerRecordPosition{topic: "orders", partition: 0, offset: 7})
	if !reflect.DeepEqual(c.pendingRecovery, positions) {
		t.Fatal("earlier completion cleared an unprocessed record")
	}
	c.completeRecoveryPosition(consumerRecordPosition{topic: "orders", partition: 0, offset: 8})
	if !reflect.DeepEqual(c.pendingRecovery, positions[1:]) {
		t.Fatalf("completion affected sibling partition/topic: %v", c.pendingRecovery)
	}
}

func TestKafkaJSRecoveryPreHandlerCommitRemovesOldBoundary(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "message"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			client := &fakeFranzKafkaJS2CompatClient{}
			c := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 8}
			c.rememberRecoveryPositions([]consumerRecordPosition{kafkaJSRecordPosition(record)})
			want := errors.New("failed after durable pre-handler commit")
			opts := ConsumerAdvancedOptions{CommitBeforeHandler: true}
			var err error
			if batch {
				client.pollFn = func(context.Context, int) kgo.Fetches { return kafkaJSTestFetch(record) }
				err = c.ConsumeBatchCtxAdvanced(func(context.Context, BatchPayload) error { return want }, opts)
			} else {
				err = c.processRecord(context.Background(), client, record, func(context.Context, MessagePayload) error { return want }, false, opts)
			}
			if !errors.Is(err, want) || len(c.pendingRecovery) != 0 {
				t.Fatalf("pre-handler commit: err=%v pending=%v", err, c.pendingRecovery)
			}
			positions, decided := consumerRewindPositions(err)
			if !decided || len(positions) != 0 {
				t.Fatalf("pre-committed failure must have empty rewind: %v decided=%v", positions, decided)
			}
		})
	}
}

func TestKafkaJSRecoverySuccessfulRetryClearsManualAndAutomaticBoundaries(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, automatic := range []bool{false, true} {
			client := &fakeFranzKafkaJS2CompatClient{}
			c := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			c.config.AutoCommit = automatic
			record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 8}
			c.rememberRecoveryPositions([]consumerRecordPosition{kafkaJSRecordPosition(record)})
			stop := errors.New("stop after retry")
			polls := 0
			client.pollFn = func(context.Context, int) kgo.Fetches {
				polls++
				if polls == 1 {
					return kafkaJSTestFetch(record)
				}
				return kgo.NewErrFetch(stop)
			}
			var err error
			if batch {
				err = c.ConsumeBatchCtxAdvanced(func(context.Context, BatchPayload) error { return nil }, ConsumerAdvancedOptions{})
			} else {
				err = c.ConsumeCtxAdvanced(func(context.Context, MessagePayload) error { return nil }, ConsumerAdvancedOptions{})
			}
			if !errors.Is(err, stop) || len(c.pendingRecovery) != 0 {
				t.Fatalf("batch=%t automatic=%t err=%v pending=%v", batch, automatic, err, c.pendingRecovery)
			}
			_, _, marked, exact := client.snapshot()
			if automatic && !reflect.DeepEqual(marked, []int64{9}) || !automatic && !reflect.DeepEqual(exact, []int64{9}) {
				t.Fatalf("batch=%t automatic=%t marked=%v exact=%v", batch, automatic, marked, exact)
			}
		}
	}
}

func TestKafkaJSRecoveryUnresolvedFinalizerRetainsBoundary(t *testing.T) {
	client := &fakeFranzKafkaJS2CompatClient{}
	c := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 8, LeaderEpoch: 3}
	err := c.processRecord(context.Background(), client, record,
		func(context.Context, MessagePayload) error { return errors.New("handler failed") }, false,
		ConsumerAdvancedOptions{
			OffsetFinalizer:              func(context.Context, MessagePayload, error, ExactOffsetCommit) error { return nil },
			RedeliverUnresolvedFinalizer: true,
		})
	if !errors.Is(err, errKafkaJSUnresolvedRecordRewound) {
		t.Fatalf("unresolved finalizer=%v", err)
	}
	got, err := c.adjustRecoveryOffsets(context.Background(), map[string]map[int32]kgo.Offset{"orders": {0: kgo.NewOffset().AtEnd()}})
	if err != nil || got["orders"][0].EpochOffset().Offset != 8 || len(c.pendingRecovery) != 1 {
		t.Fatalf("unresolved finalizer lost retry boundary: offsets=%v pending=%v err=%v", got, c.pendingRecovery, err)
	}
	_, _, marked, exact := client.snapshot()
	if len(marked) != 0 || len(exact) != 0 {
		t.Fatalf("unresolved record was resolved: marked=%v exact=%v", marked, exact)
	}
}
