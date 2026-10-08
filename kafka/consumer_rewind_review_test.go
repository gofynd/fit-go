// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReviewConsumerRewindPlanKeepsEarliestOffsetPerPartition(t *testing.T) {
	t.Parallel()

	errA := errors.New("partition zero failed")
	errB := errors.New("partition one failed")
	joined := errors.Join(
		withConsumerRewindPosition(errA, consumerRecordPosition{topic: "orders", partition: 0, offset: 9}),
		withConsumerRewindPosition(errB, consumerRecordPosition{topic: "orders", partition: 1, offset: 4}),
		withConsumerRewindPosition(errors.New("earlier retry"), consumerRecordPosition{topic: "orders", partition: 0, offset: 7}),
	)

	positions, decided := consumerRewindPositions(joined)
	if !decided {
		t.Fatal("joined processing failures did not produce an authoritative rewind plan")
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i].partition < positions[j].partition })
	want := []consumerRecordPosition{
		{topic: "orders", partition: 0, offset: 7},
		{topic: "orders", partition: 1, offset: 4},
	}
	if !reflect.DeepEqual(positions, want) {
		t.Fatalf("rewind positions = %#v, want %#v", positions, want)
	}
	if !errors.Is(joined, errA) || !errors.Is(joined, errB) {
		t.Fatalf("rewind wrappers lost original causes: %v", joined)
	}
}

func TestReviewKafkaJSWaveDoesNotLetRewoundSentinelHideSiblingFailure(t *testing.T) {
	t.Parallel()

	siblingErr := errors.New("sibling finalizer failed")
	groups := [][]*kgo.Record{
		{{Topic: "orders", Partition: 0, Offset: 1}},
		{{Topic: "orders", Partition: 1, Offset: 2}},
	}
	err := runKafkaJSRecordGroups(context.Background(), groups, 2, func(group []*kgo.Record) error {
		if group[0].Partition == 0 {
			return errKafkaJSUnresolvedRecordRewound
		}
		return withConsumerRewindPosition(siblingErr, kafkaJSRecordPosition(group[0]))
	})
	if !errors.Is(err, siblingErr) {
		t.Fatalf("wave error = %v, want sibling failure", err)
	}
	if errors.Is(err, errKafkaJSUnresolvedRecordRewound) {
		t.Fatalf("rewound sentinel hid a sibling failure: %v", err)
	}
	positions, decided := consumerRewindPositions(err)
	if !decided || len(positions) != 1 || positions[0].partition != 1 || positions[0].offset != 2 {
		t.Fatalf("wave rewind plan = %#v, decided=%v; want orders[1]@2", positions, decided)
	}
}

func TestReviewFranzRewindsEveryFailedPartitionInWave(t *testing.T) {
	t.Parallel()

	client := &fakeFranzKafkaJS2CompatClient{}
	consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	wantErr := errors.New("wave failed")
	err := errors.Join(
		withConsumerRewindPosition(wantErr, consumerRecordPosition{topic: "orders", partition: 0, offset: 3, leaderEpoch: 1}),
		withConsumerRewindPosition(errors.New("finalizer failed"), consumerRecordPosition{topic: "orders", partition: 1, offset: 8, leaderEpoch: 2}),
	)
	if got := consumer.prepareTransientRunRetry(client, err); got != err {
		t.Fatalf("prepare error = %v, want original wave error", got)
	}
	positions := client.rewoundOffsets()
	sort.Slice(positions, func(i, j int) bool { return positions[i].partition < positions[j].partition })
	want := []consumerRecordPosition{
		{topic: "orders", partition: 0, offset: 3, leaderEpoch: 1},
		{topic: "orders", partition: 1, offset: 8, leaderEpoch: 2},
	}
	if !reflect.DeepEqual(positions, want) {
		t.Fatalf("rewound offsets = %#v, want %#v", positions, want)
	}
}

func TestReviewCommitBeforeHandlerFailureDoesNotRewindFranz(t *testing.T) {
	t.Parallel()

	client := &fakeFranzKafkaJS2CompatClient{}
	consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
	record := &kgo.Record{Topic: "orders", Partition: 0, Offset: 12}
	wantErr := errors.New("handler failed after pre-commit")
	err := consumer.processRecord(
		context.Background(), client, record,
		func(context.Context, MessagePayload) error { return wantErr },
		false,
		ConsumerAdvancedOptions{CommitBeforeHandler: true},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("process error = %v, want handler cause", err)
	}
	positions, decided := consumerRewindPositions(err)
	if !decided || len(positions) != 0 {
		t.Fatalf("commit-before rewind plan = %#v, decided=%v; want authoritative empty plan", positions, decided)
	}
	if got := consumer.prepareTransientRunRetry(client, err); got != err {
		t.Fatalf("prepare error = %v, want original handler error", got)
	}
	if got := client.rewoundOffsets(); len(got) != 0 {
		t.Fatalf("commit-before handler failure rewound offsets: %#v", got)
	}
	if _, closes, _, _ := client.snapshot(); closes != 0 {
		t.Fatalf("commit-before handler failure rebuilt client; closes=%d", closes)
	}
}

func TestReviewConfluentRewindsEveryFailedPartitionInWave(t *testing.T) {
	t.Parallel()

	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)
	err := errors.Join(
		withConsumerRewindPosition(errors.New("partition zero failed"), consumerRecordPosition{topic: "orders", partition: 0, offset: 3}),
		withConsumerRewindPosition(errors.New("partition one failed"), consumerRecordPosition{topic: "orders", partition: 1, offset: 8}),
	)
	if got := consumer.prepareHandlerRunRetry(driver, err); got != err {
		t.Fatalf("prepare error = %v, want original wave error", got)
	}
	seeks := driver.seeks()
	if len(seeks) != 1 || len(seeks[0]) != 2 {
		t.Fatalf("seek calls = %#v, want one two-partition seek", seeks)
	}
	sort.Slice(seeks[0], func(i, j int) bool { return seeks[0][i].Partition < seeks[0][j].Partition })
	for i, wantOffset := range []ckafka.Offset{3, 8} {
		if seeks[0][i].Topic == nil || *seeks[0][i].Topic != "orders" || seeks[0][i].Partition != int32(i) || seeks[0][i].Offset != wantOffset {
			t.Fatalf("seek[%d] = %#v, want orders[%d]@%d", i, seeks[0][i], i, wantOffset)
		}
	}
}

func TestReviewCommitBeforeHandlerFailureDoesNotRewindConfluent(t *testing.T) {
	t.Parallel()

	topic := "orders"
	message := &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 12}}
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)
	group := groupConfluentBatchMessages([]*ckafka.Message{message})[0]
	wantErr := errors.New("handler failed after pre-commit")
	err := consumer.processMessageGroup(
		context.Background(), driver, group,
		func(context.Context, MessagePayload) error { return wantErr },
		false,
		ConsumerAdvancedOptions{CommitBeforeHandler: true},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("process error = %v, want handler cause", err)
	}
	positions, decided := consumerRewindPositions(err)
	if !decided || len(positions) != 0 {
		t.Fatalf("commit-before rewind plan = %#v, decided=%v; want authoritative empty plan", positions, decided)
	}
	if got := consumer.prepareHandlerRunRetry(driver, err); got != err {
		t.Fatalf("prepare error = %v, want original handler error", got)
	}
	if seeks := driver.seeks(); len(seeks) != 0 {
		t.Fatalf("commit-before handler failure sought offsets: %#v", seeks)
	}
	_, _, closes := driver.operationCalls()
	if closes != 0 {
		t.Fatalf("commit-before handler failure rebuilt driver; closes=%d", closes)
	}
}

func TestReviewConfluentPartitionWaveDoesNotCancelSuccessfulSibling(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first partition failed")
	secondErr := errors.New("second partition failed")
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	groups := []confluentBatchGroup{{payload: BatchPayload{Partition: 0}}, {payload: BatchPayload{Partition: 1}}}
	done := make(chan error, 1)
	go func() {
		done <- runConfluentPartitionGroups(context.Background(), groups, 2, func(ctx context.Context, group confluentBatchGroup) error {
			started <- struct{}{}
			<-release
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if group.payload.Partition == 0 {
				return firstErr
			}
			return secondErr
		})
	}()
	<-started
	<-started
	close(release)
	err := <-done
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("partition wave error = %v, want both sibling failures", err)
	}
}
