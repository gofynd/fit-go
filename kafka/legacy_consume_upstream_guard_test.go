// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/gofynd/fit-go/logging"
)

// Upstream main's client.Consumer/Consume manual-commit contract: a handler
// error is logged and the record skipped without a commit; the next successful
// record commits past it, so the failed record is not redelivered. The logged
// error text is redacted (deliberate default-path change).
func TestUpstreamLegacyConsumeLogsSkipsAndCommitsPastHandlerError(t *testing.T) {
	topic := "orders"
	var mu sync.Mutex
	reads := 0
	var committed []ckafka.Offset
	driver := &fakeConfluentConsumerDriver{
		readFn: func(time.Duration) (*ckafka.Message, error) {
			mu.Lock()
			defer mu.Unlock()
			reads++
			if reads <= 2 {
				return &ckafka.Message{
					TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 3, Offset: ckafka.Offset(9 + reads)},
					Value:          []byte{byte(reads)},
				}, nil
			}
			return nil, errors.New("stop")
		},
		commitFn: func(message *ckafka.Message) ([]ckafka.TopicPartition, error) {
			mu.Lock()
			committed = append(committed, message.TopicPartition.Offset)
			mu.Unlock()
			return nil, nil
		},
	}
	var logs bytes.Buffer
	logger, err := logging.New(logging.Options{Level: "info", Env: "production", Output: &logs})
	if err != nil {
		t.Fatal(err)
	}
	consumer := newTestConfluentConsumer(false, driver)
	consumer.legacy = true
	consumer.logger = logger

	var handled []int64
	err = consumer.Consume(func(payload MessagePayload) error {
		handled = append(handled, payload.Offset)
		if payload.Offset == 10 {
			return errors.New("handler failed password=hunter2Secret")
		}
		return nil
	}, ConsumerOptions{})
	if err == nil || !strings.Contains(err.Error(), "stop") {
		t.Fatalf("Consume() error = %v, want terminal read error", err)
	}
	if len(handled) != 2 || handled[0] != 10 || handled[1] != 11 {
		t.Fatalf("handled offsets = %v, want [10 11]", handled)
	}
	mu.Lock()
	gotCommits := append([]ckafka.Offset(nil), committed...)
	mu.Unlock()
	if len(gotCommits) != 1 || gotCommits[0] != 11 {
		t.Fatalf("CommitMessage offsets = %v, want only the later successful record [11]", gotCommits)
	}
	if _, stores, _ := driver.operationCalls(); stores != 0 {
		t.Fatalf("legacy manual path called StoreMessage %d times", stores)
	}
	if seeks := driver.seeks(); len(seeks) != 0 {
		t.Fatalf("legacy path rewound after handler error: %v", seeks)
	}
	out := logs.String()
	if !strings.Contains(out, "kafka/confluent: message handler error") {
		t.Fatalf("handler failure was not logged: %s", out)
	}
	if strings.Contains(out, "hunter2Secret") {
		t.Fatalf("handler failure log leaked a secret: %s", out)
	}
}
