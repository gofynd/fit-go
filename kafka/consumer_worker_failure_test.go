// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerRunCancellationRequiresEveryJoinedCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	applicationErr := errors.New("independent application failure")
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"cancellation", context.Canceled, true},
		{"wrapped cancellation", fmt.Errorf("handler: %w", context.Canceled), true},
		{"joined cancellations", errors.Join(context.Canceled, fmt.Errorf("cleanup: %w", context.Canceled)), true},
		{"application failure", applicationErr, false},
		{"mixed failures", errors.Join(context.Canceled, applicationErr), false},
		{"wrapped mixed failures", fmt.Errorf("finalizer: %w", errors.Join(context.Canceled, applicationErr)), false},
		{"worker abort", errConsumerWorkerAborted, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := isConsumerRunCancellation(ctx, test.err); got != test.want {
				t.Fatalf("cancellation=%v, want %v", got, test.want)
			}
			if isConsumerRunCancellation(context.Background(), test.err) {
				t.Fatal("an active run suppressed a failure")
			}
		})
	}
}

func TestConsumerShutdownKeepsIndependentHandlerErrors(t *testing.T) {
	for _, backend := range []string{"franz", "confluent"} {
		for _, batch := range []bool{false, true} {
			for _, joined := range []bool{false, true} {
				name := fmt.Sprintf("%s/batch=%v/joined=%v", backend, batch, joined)
				t.Run(name, func(t *testing.T) {
					applicationErr := errors.New("independent database update failed")
					entered := make(chan struct{})
					handle := func(ctx context.Context) error {
						close(entered)
						<-ctx.Done()
						if joined {
							return errors.Join(ctx.Err(), applicationErr)
						}
						return applicationErr
					}
					var run func() error
					var closeConsumer func() error
					if backend == "franz" {
						client := &fakeFranzKafkaJS2CompatClient{pollFn: func(context.Context, int) kgo.Fetches {
							return kafkaJSTestFetch(&kgo.Record{Topic: "orders", Partition: 0, Offset: 8})
						}}
						consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
						closeConsumer = consumer.Close
						run = func() error {
							if batch {
								return consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, _ BatchPayload) error { return handle(ctx) }, ConsumerAdvancedOptions{MaxRecords: 1})
							}
							return consumer.ConsumeCtxAdvanced(func(ctx context.Context, _ MessagePayload) error { return handle(ctx) }, ConsumerAdvancedOptions{MaxRecords: 1})
						}
					} else {
						topic := "orders"
						driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
							return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 8}}, nil
						}}
						consumer := newTestConfluentConsumer(false, driver)
						closeConsumer = consumer.Close
						run = func() error {
							if batch {
								return consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, _ BatchPayload) error { return handle(ctx) }, ConsumerAdvancedOptions{MaxRecords: 1})
							}
							return consumer.ConsumeCtxAdvanced(func(ctx context.Context, _ MessagePayload) error { return handle(ctx) }, ConsumerAdvancedOptions{MaxRecords: 1})
						}
					}
					result := make(chan error, 1)
					go func() { result <- run() }()
					<-entered
					closed := make(chan error, 1)
					go func() { closed <- closeConsumer() }()
					var err error
					select {
					case err = <-result:
					case <-time.After(time.Second):
						t.Fatal("Consume did not exit")
					}
					select {
					case closeErr := <-closed:
						if closeErr != nil {
							t.Fatalf("Close: %v", closeErr)
						}
					case <-time.After(time.Second):
						t.Fatal("Close did not exit")
					}
					if !errors.Is(err, applicationErr) {
						t.Fatalf("lost original handler cause: %v", err)
					}
				})
			}
		}
	}
}

func TestFranzShutdownKeepsIndependentPartitionFailure(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			applicationErr := errors.New("independent database update failed")
			client := &fakeFranzKafkaJS2CompatClient{pollFn: func(context.Context, int) kgo.Fetches {
				return append(kafkaJSTestFetch(&kgo.Record{Topic: "orders", Partition: 0, Offset: 8}), kafkaJSTestFetch(&kgo.Record{Topic: "orders", Partition: 1, Offset: 8})...)
			}}
			consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			var entered sync.WaitGroup
			entered.Add(2)
			handle := func(ctx context.Context, partition int) error {
				entered.Done()
				<-ctx.Done()
				if partition == 0 {
					return ctx.Err()
				}
				return applicationErr
			}
			result := make(chan error, 1)
			go func() {
				opts := ConsumerAdvancedOptions{MaxRecords: 2, PartitionsConsumedConcurrently: 2}
				if batch {
					result <- consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, p BatchPayload) error { return handle(ctx, p.Partition) }, opts)
				} else {
					result <- consumer.ConsumeCtxAdvanced(func(ctx context.Context, p MessagePayload) error { return handle(ctx, p.Partition) }, opts)
				}
			}()
			entered.Wait()
			closed := make(chan error, 1)
			go func() { closed <- consumer.Close() }()
			var err error
			select {
			case err = <-result:
			case <-time.After(time.Second):
				t.Fatal("Consume did not exit")
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("Close did not exit")
			}
			if !errors.Is(err, applicationErr) {
				t.Fatalf("sibling application failure lost: %v", err)
			}
		})
	}
}

func TestFranzWorkerGoexitPreservesExactRetryBoundary(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, mode := range []string{"manual", "automatic", "commit_before_handler"} {
			for _, stage := range []string{"handler", "finalizer", "commit"} {
				if batch && stage == "finalizer" || mode != "manual" && stage == "finalizer" {
					continue
				}
				if mode == "automatic" && stage == "commit" {
					continue // Automatic groups mark records instead of synchronous commits.
				}
				t.Run(fmt.Sprintf("batch=%v/%s/%s", batch, mode, stage), func(t *testing.T) {
					records := []*kgo.Record{{Topic: "orders", Partition: 0, Offset: 7}, {Topic: "orders", Partition: 0, Offset: 8}, {Topic: "orders", Partition: 0, Offset: 9}}
					stopErr := errors.New("probe complete")
					client := &fakeFranzKafkaJS2CompatClient{}
					polls := 0
					client.pollFn = func(context.Context, int) kgo.Fetches {
						polls++
						if polls > 2 {
							return kgo.NewErrFetch(stopErr)
						}
						var fetches kgo.Fetches
						for _, record := range records {
							if polls == 2 {
								eligible := false
								for _, position := range client.rewoundOffsets() {
									if record.Offset >= position.offset {
										eligible = true
									}
								}
								if !eligible {
									continue
								}
							}
							fetches = append(fetches, kafkaJSTestFetch(record)...)
						}
						return fetches
					}
					consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
					consumer.config.AutoCommit = mode == "automatic"
					opts := ConsumerAdvancedOptions{MaxRecords: 3, CommitBeforeHandler: mode == "commit_before_handler"}
					aborted := false
					var handled []int64
					handle := func(_ context.Context, offset int64) error {
						handled = append(handled, offset)
						if stage == "handler" && !aborted && (batch || offset == 8) {
							aborted = true
							runtime.Goexit()
						}
						return nil
					}
					if stage == "finalizer" {
						opts.OffsetFinalizer = func(_ context.Context, p MessagePayload, _ error, commit ExactOffsetCommit) error {
							if !aborted && p.Offset == 8 {
								aborted = true
								runtime.Goexit()
							}
							return commit(p.Offset + 1)
						}
					}
					if stage == "commit" {
						client.commitRecordsFn = func(_ context.Context, committed ...*kgo.Record) error {
							for _, record := range committed {
								if !aborted && (batch || record.Offset == 8) {
									aborted = true
									runtime.Goexit()
								}
								client.mu.Lock()
								client.exactOffsets = append(client.exactOffsets, record.Offset+1)
								client.mu.Unlock()
							}
							return nil
						}
					}
					run := func() error {
						if batch {
							return consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, p BatchPayload) error { return handle(ctx, p.FirstOffset) }, opts)
						}
						return consumer.ConsumeCtxAdvanced(func(ctx context.Context, p MessagePayload) error { return handle(ctx, p.Offset) }, opts)
					}
					if err := run(); !errors.Is(err, errConsumerWorkerAborted) {
						t.Fatalf("worker abort returned %v", err)
					}
					if polls != 1 {
						t.Fatalf("continued polling after aborted worker: polls=%d", polls)
					}
					wantOffset := int64(8)
					if batch {
						wantOffset = 7
					}
					committedAbort := mode == "commit_before_handler" && stage == "handler"
					if committedAbort && !batch {
						wantOffset = 9
					}
					positions := client.rewoundOffsets()
					if committedAbort && batch {
						if len(positions) != 0 {
							t.Fatalf("rewound precommitted batch: %v", positions)
						}
					} else if len(positions) != 1 || positions[0].offset != wantOffset {
						t.Fatalf("rewind=%v, want offset %d", positions, wantOffset)
					}
					if err := run(); !errors.Is(err, stopErr) {
						t.Fatalf("retry: %v", err)
					}
					if polls != 3 {
						t.Fatalf("retry did not poll to completion: polls=%d", polls)
					}
					if committedAbort && !batch && !reflect.DeepEqual(handled, []int64{7, 8, 9}) {
						t.Fatalf("precommitted record redelivered: %v", handled)
					}
				})
			}
		}
	}
}

func TestFranzWorkerGoexitDoesNotLoseOrRewindSuccessfulSibling(t *testing.T) {
	for _, bothAbort := range []bool{false, true} {
		t.Run(fmt.Sprintf("both_abort=%v", bothAbort), func(t *testing.T) {
			records := []*kgo.Record{{Topic: "orders", Partition: 0, Offset: 8}, {Topic: "orders", Partition: 1, Offset: 12}}
			client := &fakeFranzKafkaJS2CompatClient{}
			stopErr := errors.New("probe complete")
			polls := 0
			client.pollFn = func(context.Context, int) kgo.Fetches {
				polls++
				if polls > 2 {
					return kgo.NewErrFetch(stopErr)
				}
				var fetches kgo.Fetches
				for _, record := range records {
					if polls == 2 {
						found := false
						for _, position := range client.rewoundOffsets() {
							if record.Partition == position.partition {
								found = true
							}
						}
						if !found {
							continue
						}
					}
					fetches = append(fetches, kafkaJSTestFetch(record)...)
				}
				return fetches
			}
			consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			firstRun := true
			run := func() error {
				return consumer.ConsumeCtxAdvanced(func(_ context.Context, payload MessagePayload) error {
					if firstRun && (payload.Partition == 0 || bothAbort) {
						runtime.Goexit()
					}
					return nil
				}, ConsumerAdvancedOptions{MaxRecords: 2, PartitionsConsumedConcurrently: 2})
			}
			if err := run(); !errors.Is(err, errConsumerWorkerAborted) {
				t.Fatalf("abort: %v", err)
			}
			positions := client.rewoundOffsets()
			wantCount := 1
			if bothAbort {
				wantCount = 2
			}
			if len(positions) != wantCount {
				t.Fatalf("lost abort or rewound successful sibling: %v", positions)
			}
			firstRun = false
			if err := run(); !errors.Is(err, stopErr) {
				t.Fatalf("retry: %v", err)
			}
			_, _, _, commits := client.snapshot()
			if len(commits) != 2 {
				t.Fatalf("retry lost or duplicated committed records: %v", commits)
			}
		})
	}
}

func TestLegacyConfluentGoexitStillRunsOnCallingGoroutine(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			topic := "orders"
			driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
				return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Offset: 8}}, nil
			}}
			consumer := newTestConfluentConsumer(false, driver)
			consumer.legacy = true
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				if batch {
					_ = consumer.consumeBatches(func(context.Context, BatchPayload) error { runtime.Goexit(); return nil }, ConsumerAdvancedOptions{MaxRecords: 1})
				} else {
					_ = consumer.Consume(func(MessagePayload) error { runtime.Goexit(); return nil }, ConsumerOptions{})
				}
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("legacy handler did not exit its caller")
			}
			if returned {
				t.Fatal("legacy Goexit was converted to a worker result")
			}
		})
	}
}

func TestConfluentWorkerGoexitAtCommitOrFinalizerFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		batch     bool
		before    bool
		finalizer bool
	}{
		{name: "message post-handler commit"},
		{name: "message pre-handler commit", before: true},
		{name: "message finalizer", finalizer: true},
		{name: "batch post-handler commit", batch: true},
		{name: "batch pre-handler commit", batch: true, before: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			topic := "orders"
			position := int64(8)
			stopErr := errors.New("probe complete")
			driver := &fakeConfluentConsumerDriver{}
			driver.readFn = func(time.Duration) (*ckafka.Message, error) {
				if position > 9 {
					return nil, stopErr
				}
				offset := position
				position++
				return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: ckafka.Offset(offset)}}, nil
			}
			driver.seekFn = func(parts []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
				position = int64(parts[0].Offset)
				return parts, nil
			}
			aborted := false
			consumer := newTestConfluentConsumer(false, driver)
			opts := ConsumerAdvancedOptions{MaxRecords: 2, CommitBeforeHandler: test.before}
			if test.finalizer {
				opts.OffsetFinalizer = func(_ context.Context, p MessagePayload, _ error, commit ExactOffsetCommit) error {
					if !aborted {
						aborted = true
						runtime.Goexit()
					}
					return commit(p.Offset + 1)
				}
			} else {
				driver.commitFn = func(*ckafka.Message) ([]ckafka.TopicPartition, error) {
					if !aborted {
						aborted = true
						runtime.Goexit()
					}
					return nil, nil
				}
			}
			run := func() error {
				if test.batch {
					return consumer.ConsumeBatchCtxAdvanced(func(context.Context, BatchPayload) error { return nil }, opts)
				}
				return consumer.ConsumeCtxAdvanced(func(context.Context, MessagePayload) error { return nil }, opts)
			}
			if err := run(); !errors.Is(err, errConsumerWorkerAborted) {
				t.Fatalf("abort: %v", err)
			}
			seeks := driver.seeks()
			if len(seeks) != 1 || seeks[0][0].Offset != 8 {
				t.Fatalf("unconfirmed boundary was skipped: %v", seeks)
			}
			opts.MaxRecords = 1
			if err := run(); !errors.Is(err, stopErr) {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}

func TestAdvancedConfluentPartialBatchPollRecoveryDoesNotReadUntilSeekSucceeds(t *testing.T) {
	topic := "orders"
	position := int64(8)
	reads, seeks := 0, 0
	pollErr := ckafka.NewError(ckafka.ErrTransport, "transport failed", false)
	seekErr := errors.New("seek temporarily unavailable")
	stopErr := errors.New("probe complete")
	driver := &fakeConfluentConsumerDriver{}
	driver.readFn = func(time.Duration) (*ckafka.Message, error) {
		reads++
		if reads == 2 {
			return nil, pollErr
		}
		if position > 9 {
			return nil, stopErr
		}
		offset := position
		position++
		return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Offset: ckafka.Offset(offset)}}, nil
	}
	driver.seekFn = func(parts []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
		seeks++
		if seeks < 3 {
			return nil, seekErr
		}
		position = int64(parts[0].Offset)
		return parts, nil
	}
	consumer := newTestConfluentConsumer(false, driver)
	opts := ConsumerAdvancedOptions{MaxRecords: 2}
	var handled []int64
	run := func() error {
		return consumer.ConsumeBatchCtxAdvanced(func(_ context.Context, payload BatchPayload) error {
			for _, message := range payload.Messages {
				handled = append(handled, message.Offset)
			}
			return nil
		}, opts)
	}
	if err := run(); !errors.Is(err, pollErr) {
		t.Fatalf("original poll cause was changed: %v", err)
	}
	if reads != 2 || seeks != 1 || len(handled) != 0 {
		t.Fatalf("initial recovery: reads=%d seeks=%d handled=%v", reads, seeks, handled)
	}
	if err := run(); !errors.Is(err, seekErr) {
		t.Fatalf("pending seek cause was changed: %v", err)
	}
	if reads != 2 || seeks != 2 || len(handled) != 0 {
		t.Fatalf("read past failed recovery: reads=%d seeks=%d handled=%v", reads, seeks, handled)
	}
	opts.MaxRecords = 1
	if err := run(); !errors.Is(err, stopErr) {
		t.Fatalf("successful recovery: %v", err)
	}
	if !reflect.DeepEqual(handled, []int64{8, 9}) {
		t.Fatalf("recovery skipped or duplicated records: %v", handled)
	}
}

func TestConfluentWorkerGoexitPreservesExactRetryBoundary(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/commit_before=%v", batch, before), func(t *testing.T) {
				topic := "orders"
				position := int64(7)
				stopErr := errors.New("probe complete")
				driver := &fakeConfluentConsumerDriver{}
				driver.readFn = func(time.Duration) (*ckafka.Message, error) {
					if position > 9 {
						return nil, stopErr
					}
					offset := position
					position++
					return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: ckafka.Offset(offset)}}, nil
				}
				driver.seekFn = func(parts []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
					position = int64(parts[0].Offset)
					return parts, nil
				}
				consumer := newTestConfluentConsumer(false, driver)
				opts := ConsumerAdvancedOptions{MaxRecords: 3, CommitBeforeHandler: before}
				aborted := false
				var handled []int64
				handle := func(offset int64) error {
					handled = append(handled, offset)
					if !aborted && (batch || offset == 8) {
						aborted = true
						runtime.Goexit()
					}
					return nil
				}
				run := func() error {
					if batch {
						return consumer.ConsumeBatchCtxAdvanced(func(_ context.Context, p BatchPayload) error { return handle(p.FirstOffset) }, opts)
					}
					return consumer.ConsumeCtxAdvanced(func(_ context.Context, p MessagePayload) error { return handle(p.Offset) }, opts)
				}
				if err := run(); !errors.Is(err, errConsumerWorkerAborted) {
					t.Fatalf("worker abort returned %v", err)
				}
				seeks := driver.seeks()
				wantOffset := int64(8)
				if batch {
					wantOffset = 7
				}
				if before && !batch {
					wantOffset = 9
				}
				if before && batch {
					if len(seeks) != 0 {
						t.Fatalf("precommitted batch replayed: %v", seeks)
					}
				} else if len(seeks) != 1 || int64(seeks[0][0].Offset) != wantOffset {
					t.Fatalf("seeks=%v, want %d", seeks, wantOffset)
				}
				// Complete a smaller retry batch without a poll failure interrupting it.
				opts.MaxRecords = 1
				if err := run(); !errors.Is(err, stopErr) {
					t.Fatalf("retry: %v", err)
				}
				if before && !batch && !reflect.DeepEqual(handled, []int64{7, 8, 9}) {
					t.Fatalf("precommitted record redelivered: %v", handled)
				}
			})
		}
	}
}

func TestAdvancedConfluentPartialBatchPollFailureRewindsEveryOwnedPartition(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%v/commit_before=%v", legacy, before), func(t *testing.T) {
				topic := "orders"
				records := []*ckafka.Message{
					{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 8}},
					{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 12}},
					{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 9}},
				}
				pollErr := ckafka.NewError(ckafka.ErrTransport, "transport failed", false)
				stopErr := errors.New("probe complete")
				queue := append([]*ckafka.Message(nil), records...)
				first := true
				driver := &fakeConfluentConsumerDriver{}
				driver.readFn = func(time.Duration) (*ckafka.Message, error) {
					if len(queue) > 0 {
						record := queue[0]
						queue = queue[1:]
						return record, nil
					}
					if first {
						first = false
						return nil, pollErr
					}
					return nil, stopErr
				}
				driver.seekFn = func(parts []ckafka.TopicPartition) ([]ckafka.TopicPartition, error) {
					for _, record := range records {
						for _, part := range parts {
							if part.Partition == record.TopicPartition.Partition && record.TopicPartition.Offset >= part.Offset {
								queue = append(queue, record)
							}
						}
					}
					return parts, nil
				}
				consumer := newTestConfluentConsumer(false, driver)
				consumer.legacy = legacy
				opts := ConsumerAdvancedOptions{MaxRecords: 4, CommitBeforeHandler: before}
				var handled []int64
				run := func() error {
					return consumer.consumeBatches(func(_ context.Context, p BatchPayload) error {
						for _, record := range p.Messages {
							handled = append(handled, record.Offset)
						}
						return nil
					}, opts)
				}
				if err := run(); !errors.Is(err, pollErr) {
					t.Fatalf("first poll: %v", err)
				}
				if len(handled) != 0 {
					t.Fatalf("failed poll dispatched %v", handled)
				}
				seeks := driver.seeks()
				if legacy {
					if len(seeks) != 0 {
						t.Fatalf("changed original-main batch behavior: %v", seeks)
					}
					return
				}
				if len(seeks) != 1 || len(seeks[0]) != 2 {
					t.Fatalf("incomplete rewind plan: %v", seeks)
				}
				opts.MaxRecords = 3
				if err := run(); !errors.Is(err, stopErr) {
					t.Fatalf("retry: %v", err)
				}
				if len(handled) != 3 {
					t.Fatalf("retry lost fetched records: %v", handled)
				}
			})
		}
	}
}
