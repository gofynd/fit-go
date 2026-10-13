// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofynd/fit-go/tracing"
	"github.com/twmb/franz-go/pkg/kgo"
)

const callbackCloseTestTimeout = 2 * time.Second

func TestAdvancedConfluentCallbackCloseDoesNotDeadlock(t *testing.T) {
	tests := []struct {
		name string
		run  func(*ConfluentConsumer, chan<- error) error
	}{
		{
			name: "message handler",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(func(MessagePayload) error {
					closed <- consumer.Close()
					return nil
				}, ConsumerAdvancedOptions{})
			},
		},
		{
			name: "batch handler",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeBatchAdvanced(func(BatchPayload) error {
					closed <- consumer.Close()
					return nil
				}, ConsumerAdvancedOptions{MaxRecords: 1})
			},
		},
		{
			name: "public traced message handler",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(TracedMessageHandler(func(MessagePayload) error {
					closed <- consumer.Close()
					return nil
				}), ConsumerAdvancedOptions{})
			},
		},
		{
			name: "public traced context message handler",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(TracedMessageHandlerCtx(func(context.Context, MessagePayload) error {
					closed <- consumer.Close()
					return nil
				}), ConsumerAdvancedOptions{})
			},
		},
		{
			name: "public traced context batch handler",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeBatchAdvanced(TracedBatchHandlerCtx(func(context.Context, BatchPayload) error {
					closed <- consumer.Close()
					return nil
				}), ConsumerAdvancedOptions{MaxRecords: 1})
			},
		},
		{
			name: "replaced active tracing context",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(func(MessagePayload) error {
					cleanup := tracing.InjectContextIntoGoroutine(context.Background())
					defer cleanup()
					closed <- consumer.Close()
					return nil
				}, ConsumerAdvancedOptions{})
			},
		},
		{
			name: "offset finalizer",
			run: func(consumer *ConfluentConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(func(MessagePayload) error { return nil }, ConsumerAdvancedOptions{
					OffsetFinalizer: func(context.Context, MessagePayload, error, ExactOffsetCommit) error {
						closed <- consumer.Close()
						return nil
					},
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			topic := "orders"
			var readMu sync.Mutex
			read := false
			driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
				readMu.Lock()
				defer readMu.Unlock()
				if !read {
					read = true
					return &ckafka.Message{TopicPartition: ckafka.TopicPartition{
						Topic: &topic, Partition: 0, Offset: 1,
					}}, nil
				}
				return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
			}}
			consumer := newTestConfluentConsumer(false, driver)
			callbackClose := make(chan error, 1)
			consumeDone := make(chan error, 1)
			go func() { consumeDone <- test.run(consumer, callbackClose) }()

			select {
			case err := <-callbackClose:
				if err != nil {
					t.Fatalf("callback Close error = %v", err)
				}
			case <-time.After(callbackCloseTestTimeout):
				t.Fatal("callback Close deadlocked")
			}
			select {
			case err := <-consumeDone:
				if err != nil {
					t.Fatalf("consume error = %v", err)
				}
			case <-time.After(callbackCloseTestTimeout):
				t.Fatal("consume did not finish after callback Close")
			}
			if err := consumer.Close(); err != nil {
				t.Fatalf("await asynchronous Close: %v", err)
			}
			_, _, closes := driver.operationCalls()
			if closes != 1 {
				t.Fatalf("driver closes = %d, want 1", closes)
			}
		})
	}
}

func TestAdvancedFranzCallbackCloseDoesNotDeadlock(t *testing.T) {
	tests := []struct {
		name string
		run  func(*franzKafkaJS2CompatConsumer, chan<- error) error
	}{
		{
			name: "message handler",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(func(MessagePayload) error {
					closed <- consumer.Close()
					return nil
				}, ConsumerAdvancedOptions{})
			},
		},
		{
			name: "batch handler",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeBatchAdvanced(func(BatchPayload) error {
					closed <- consumer.Close()
					return nil
				}, ConsumerAdvancedOptions{MaxRecords: 1})
			},
		},
		{
			name: "public traced message handler",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(TracedMessageHandler(func(MessagePayload) error {
					closed <- consumer.Close()
					return nil
				}), ConsumerAdvancedOptions{})
			},
		},
		{
			name: "public traced context message handler",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(TracedMessageHandlerCtx(func(context.Context, MessagePayload) error {
					closed <- consumer.Close()
					return nil
				}), ConsumerAdvancedOptions{})
			},
		},
		{
			name: "public traced context batch handler",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeBatchAdvanced(TracedBatchHandlerCtx(func(context.Context, BatchPayload) error {
					closed <- consumer.Close()
					return nil
				}), ConsumerAdvancedOptions{MaxRecords: 1})
			},
		},
		{
			name: "replaced active tracing context",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(func(MessagePayload) error {
					cleanup := tracing.InjectContextIntoGoroutine(context.Background())
					defer cleanup()
					closed <- consumer.Close()
					return nil
				}, ConsumerAdvancedOptions{})
			},
		},
		{
			name: "offset finalizer",
			run: func(consumer *franzKafkaJS2CompatConsumer, closed chan<- error) error {
				return consumer.ConsumeAdvanced(func(MessagePayload) error { return nil }, ConsumerAdvancedOptions{
					OffsetFinalizer: func(context.Context, MessagePayload, error, ExactOffsetCommit) error {
						closed <- consumer.Close()
						return nil
					},
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pollMu sync.Mutex
			polled := false
			client := &fakeFranzKafkaJS2CompatClient{pollFn: func(ctx context.Context, _ int) kgo.Fetches {
				pollMu.Lock()
				if !polled {
					polled = true
					pollMu.Unlock()
					return kafkaJSTestFetch(&kgo.Record{Topic: "orders", Partition: 0, Offset: 1})
				}
				pollMu.Unlock()
				<-ctx.Done()
				return kgo.NewErrFetch(ctx.Err())
			}}
			consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
			callbackClose := make(chan error, 1)
			consumeDone := make(chan error, 1)
			go func() { consumeDone <- test.run(consumer, callbackClose) }()

			select {
			case err := <-callbackClose:
				if err != nil {
					t.Fatalf("callback Close error = %v", err)
				}
			case <-time.After(callbackCloseTestTimeout):
				t.Fatal("callback Close deadlocked")
			}
			select {
			case err := <-consumeDone:
				if err != nil {
					t.Fatalf("consume error = %v", err)
				}
			case <-time.After(callbackCloseTestTimeout):
				t.Fatal("consume did not finish after callback Close")
			}
			if err := consumer.Close(); err != nil {
				t.Fatalf("await asynchronous Close: %v", err)
			}
			_, closes, _, _ := client.snapshot()
			if closes != 1 {
				t.Fatalf("client closes = %d, want 1", closes)
			}
		})
	}
}

func TestAdvancedConsumerExternalCloseRemainsSynchronous(t *testing.T) {
	t.Run("confluent", func(t *testing.T) {
		topic := "orders"
		read := false
		driver := &fakeConfluentConsumerDriver{readFn: func(time.Duration) (*ckafka.Message, error) {
			if !read {
				read = true
				return &ckafka.Message{TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1}}, nil
			}
			return nil, ckafka.NewError(ckafka.ErrTimedOut, "timeout", false)
		}}
		consumer := newTestConfluentConsumer(false, driver)
		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		consumeDone := make(chan error, 1)
		go func() {
			consumeDone <- consumer.Consume(func(MessagePayload) error {
				close(handlerStarted)
				<-releaseHandler
				return nil
			}, ConsumerOptions{})
		}()
		<-handlerStarted
		closeDone := make(chan error, 1)
		go func() { closeDone <- consumer.Close() }()
		assertCloseStillWaiting(t, closeDone)
		close(releaseHandler)
		awaitCloseAndConsume(t, closeDone, consumeDone)
	})

	t.Run("franz", func(t *testing.T) {
		client := &fakeFranzKafkaJS2CompatClient{pollFn: func(context.Context, int) kgo.Fetches {
			return kafkaJSTestFetch(&kgo.Record{Topic: "orders", Partition: 0, Offset: 1})
		}}
		consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		consumeDone := make(chan error, 1)
		go func() {
			consumeDone <- consumer.Consume(func(MessagePayload) error {
				close(handlerStarted)
				<-releaseHandler
				return nil
			}, ConsumerOptions{MaxRecords: 1})
		}()
		<-handlerStarted
		closeDone := make(chan error, 1)
		go func() { closeDone <- consumer.Close() }()
		assertCloseStillWaiting(t, closeDone)
		close(releaseHandler)
		awaitCloseAndConsume(t, closeDone, consumeDone)
	})
}

func assertCloseStillWaiting(t *testing.T, closeDone <-chan error) {
	t.Helper()
	select {
	case err := <-closeDone:
		t.Fatalf("external Close returned before callback completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func awaitCloseAndConsume(t *testing.T, closeDone, consumeDone <-chan error) {
	t.Helper()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close error = %v", err)
		}
	case <-time.After(callbackCloseTestTimeout):
		t.Fatal("external Close did not finish")
	}
	select {
	case err := <-consumeDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("consume error = %v", err)
		}
	case <-time.After(callbackCloseTestTimeout):
		t.Fatal("consume did not finish")
	}
}

func TestAdvancedConfluentSkipsMessageAndBatchDispatchAfterPartitionRevoke(t *testing.T) {
	topic := "orders"
	driver := &fakeConfluentConsumerDriver{}
	consumer := newTestConfluentConsumer(false, driver)
	partition := ckafka.TopicPartition{Topic: &topic, Partition: 3}
	consumer.markPartitionsAssigned([]ckafka.TopicPartition{partition}, false)
	message := &ckafka.Message{TopicPartition: partition}
	group := groupConfluentBatchMessagesWithAssignments(
		[]*ckafka.Message{message},
		[]confluentAssignmentSnapshot{consumer.assignmentSnapshot(message)},
	)[0]

	messageCalls := 0
	if err := consumer.processAssignedMessageGroup(context.Background(), driver, group, func(context.Context, MessagePayload) error {
		messageCalls++
		return nil
	}, true, ConsumerAdvancedOptions{}); err != nil {
		t.Fatalf("owned dispatch: %v", err)
	}
	batchCalls := 0
	if err := consumer.processAssignedBatchGroup(context.Background(), driver, group, func(context.Context, BatchPayload) error {
		batchCalls++
		return nil
	}, true, ConsumerAdvancedOptions{}); err != nil {
		t.Fatalf("owned batch dispatch: %v", err)
	}
	consumer.markPartitionsRevoked([]ckafka.TopicPartition{partition}, false)
	// Reassignment makes ownership true again, but the record gathered under the
	// old generation must still not reach the handler.
	consumer.markPartitionsAssigned([]ckafka.TopicPartition{partition}, false)
	if err := consumer.processAssignedMessageGroup(context.Background(), driver, group, func(context.Context, MessagePayload) error {
		messageCalls++
		return nil
	}, true, ConsumerAdvancedOptions{}); err != nil {
		t.Fatalf("revoked dispatch: %v", err)
	}
	if err := consumer.processAssignedBatchGroup(context.Background(), driver, group, func(context.Context, BatchPayload) error {
		batchCalls++
		return nil
	}, true, ConsumerAdvancedOptions{}); err != nil {
		t.Fatalf("revoked batch dispatch: %v", err)
	}
	if messageCalls != 1 {
		t.Fatalf("message handler calls = %d, want only the owned dispatch", messageCalls)
	}
	if batchCalls != 1 {
		t.Fatalf("batch handler calls = %d, want only the owned dispatch", batchCalls)
	}
	if consumer.assignmentGeneration != 3 {
		t.Fatalf("assignment generation = %d, want 3", consumer.assignmentGeneration)
	}
}

func TestAdvancedConsumersRejectMixedTopicStartSettings(t *testing.T) {
	topics := []TopicConfig{{Topic: "earliest", FromBeginning: true}, {Topic: "latest", FromBeginning: false}}

	t.Run("confluent advanced", func(t *testing.T) {
		consumer := newTestConfluentConsumer(false, nil)
		consumer.configMap = &ckafka.ConfigMap{}
		created := 0
		consumer.newConsumer = func(*ckafka.ConfigMap) (confluentConsumerConnectDriver, error) {
			created++
			return &fakeConfluentConsumerDriver{}, nil
		}
		err := consumer.Connect(topics)
		if err == nil || !strings.Contains(err.Error(), "mixed FromBeginning") {
			t.Fatalf("Connect error = %v, want mixed setting rejection", err)
		}
		if created != 0 {
			t.Fatalf("drivers created = %d, want 0", created)
		}
	})

	t.Run("franz advanced", func(t *testing.T) {
		consumer := newFranzKafkaJS2LifecycleTestConsumer(t, nil, ConsumerShutdownCancelInFlight)
		consumer.brokers = []string{"localhost:9092"}
		consumer.fitCfg = &Config{ClientID: "mixed-start-test"}
		created := 0
		consumer.newClient = func(...kgo.Opt) (franzKafkaJS2CompatClient, error) {
			created++
			return &fakeFranzKafkaJS2CompatClient{}, nil
		}
		err := consumer.Connect(topics)
		if err == nil || !strings.Contains(err.Error(), "mixed FromBeginning") {
			t.Fatalf("Connect error = %v, want mixed setting rejection", err)
		}
		if created != 0 {
			t.Fatalf("clients created = %d, want 0", created)
		}
	})

	t.Run("confluent legacy keeps first topic behavior", func(t *testing.T) {
		consumer := newTestConfluentConsumer(false, nil)
		consumer.legacy = true
		consumer.configMap = &ckafka.ConfigMap{}
		consumer.newConsumer = func(*ckafka.ConfigMap) (confluentConsumerConnectDriver, error) {
			return &fakeConfluentConsumerDriver{}, nil
		}
		if err := consumer.Connect(topics); err != nil {
			t.Fatalf("legacy Connect rejected mixed settings: %v", err)
		}
		value, err := consumer.configMap.Get("auto.offset.reset", nil)
		if err != nil || value != "earliest" {
			t.Fatalf("legacy auto.offset.reset = %v, %v; want earliest", value, err)
		}
	})
}
