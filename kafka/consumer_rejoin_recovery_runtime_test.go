// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofynd/fit-go/logging"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Only a disposable broker is supported: this fixture creates unique topics
// and groups and deliberately removes its own member through Kafka LeaveGroup.
// There is no synthetic PollRecords error or dependency on a mock-broker API.
func TestKafkaJSRecoveryAfterRealOwnershipLossLive(t *testing.T) {
	broker := strings.TrimSpace(os.Getenv("FIT_GO_KAFKA_RUNTIME_BROKER"))
	if broker == "" {
		t.Skip("set FIT_GO_KAFKA_RUNTIME_BROKER to a disposable Kafka broker")
	}
	for _, batch := range []bool{false, true} {
		for _, mode := range []string{"uncommitted_latest", "committed_latest", "uncommitted_beginning", "automatic_latest", "before_latest", "higher_committed_latest"} {
			t.Run(fmt.Sprintf("batch_%t/%s", batch, mode), func(t *testing.T) {
				runKafkaJSOwnershipLossBrokerFixture(t, broker, batch, mode)
			})
		}
	}
}

func runKafkaJSOwnershipLossBrokerFixture(t *testing.T, broker string, batch bool, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic, group := "fit-go-rejoin-"+suffix, "fit-go-rejoin-group-"+suffix
	createKafkaJSRuntimeTopic(t, ctx, broker, topic, 1)
	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	committer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer committer.Close()
	produce := func(value string) {
		t.Helper()
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte(value)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce("history")
	if mode == "committed_latest" {
		if err := commitKafkaJSExact(ctx, committer, &kgo.Record{Topic: topic, Partition: 0}, 1, false); err != nil {
			t.Fatal(err)
		}
	}
	logger, err := logging.New(logging.Options{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	settings := DefaultConsumerAdvancedConfig(group)
	settings.AutoCommit = mode == "automatic_latest"
	settings.AutoCommitInterval = 100 * time.Millisecond
	settings.SessionTimeout = 10 * time.Second
	settings.HeartbeatInterval = 200 * time.Millisecond
	lost := make(chan struct{})
	var lostOnce sync.Once
	settings.OnPartitionsLost = func([]PartitionAssignment) { lostOnce.Do(func() { close(lost) }) }
	c := &franzKafkaJS2CompatConsumer{brokers: []string{broker}, fitCfg: &Config{ClientID: "ownership-loss-regression"}, config: settings, logger: logger}
	ready := &kafkaJSMixedPollReadyHook{ready: make(chan struct{})}
	c.newClient = func(opts ...kgo.Opt) (franzKafkaJS2CompatClient, error) {
		return kgo.NewClient(append(opts, kgo.WithHooks(ready))...)
	}
	beginning := mode == "uncommitted_beginning"
	if err := c.Connect([]TopicConfig{{Topic: topic, FromBeginning: beginning}}); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// The original client is only used for its thread-safe GroupMetadata; a
	// later no-record group error is allowed to rebuild the SDK client.
	original := c.client.(*kgo.Client)
	failedOffset := int64(1)
	if beginning {
		failedOffset = 0
	}
	var first atomic.Bool
	first.Store(true)
	handled := make(chan MessagePayload, 8)
	wantFailure := errors.New("real handler failed while its member was removed")
	handler := func(_ context.Context, payload MessagePayload) error {
		if !first.CompareAndSwap(true, false) {
			handled <- payload
			return nil
		}
		if payload.Offset != failedOffset {
			return fmt.Errorf("initial offset=%d want=%d", payload.Offset, failedOffset)
		}
		member, _ := original.GroupMetadata()
		request := kmsg.NewPtrLeaveGroupRequest()
		request.Group, request.MemberID = group, member
		request.Members = []kmsg.LeaveGroupRequestMember{{MemberID: member}}
		response, err := request.RequestWith(ctx, producer)
		if err != nil {
			return fmt.Errorf("remove member: %w", err)
		}
		if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
			return err
		}
		for _, member := range response.Members {
			if err := kerr.ErrorForCode(member.ErrorCode); err != nil {
				return err
			}
		}
		if mode == "higher_committed_latest" {
			if err := commitKafkaJSExact(ctx, committer, &kgo.Record{Topic: topic, Partition: 0}, failedOffset+1, false); err != nil {
				return err
			}
		}
		return wantFailure
	}
	opts := ConsumerAdvancedOptions{PollTimeout: time.Second, MaxRecords: 1, CommitBeforeHandler: mode == "before_latest"}
	run := func() error { return c.ConsumeCtxAdvanced(handler, opts) }
	if batch {
		run = func() error {
			return c.ConsumeBatchCtxAdvanced(func(ctx context.Context, payload BatchPayload) error {
				return handler(ctx, payload.Messages[0])
			}, opts)
		}
	}
	done := make(chan error, 1)
	go func() { done <- run() }()
	if !beginning {
		select {
		case <-ready.ready:
		case <-ctx.Done():
			t.Fatal("initial offset lookup never reached Fetch")
		}
		produce("failed")
	}
	select {
	case err := <-done:
		if !errors.Is(err, wantFailure) {
			t.Fatalf("handler failure=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("handler did not fail")
	}
	select {
	case <-lost:
	case <-ctx.Done():
		t.Fatal("broker removal did not cause real ownership loss")
	}
	produce("after-loss")
	go func() {
		for {
			err := run()
			if !IsTransientConsumerError(err) {
				done <- err
				return
			}
			select {
			case <-ctx.Done():
				done <- ctx.Err()
				return
			default:
			}
		}
	}()
	wantFirst := failedOffset
	if mode == "before_latest" || mode == "higher_committed_latest" {
		wantFirst++
	}
	select {
	case payload := <-handled:
		if payload.Offset != wantFirst {
			t.Fatalf("after ownership loss: first=%d want=%d (failed=%d)", payload.Offset, wantFirst, failedOffset)
		}
	case <-ctx.Done():
		t.Fatal("ownership-loss retry lost the failed record")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("retry shutdown=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("retry did not shut down")
	}
}
