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

// Package errors provides Sentry error reporting integration for the fit.go framework.

package kafka

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaJSMixedTransientPollReplaysEveryFetchedPartition(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, mode := range []string{"manual", "automatic", "commit_before_handler"} {
			name := "message/" + mode
			if batch {
				name = "batch/" + mode
			}
			t.Run(name, func(t *testing.T) {
				records := []*kgo.Record{
					{Topic: "orders", Partition: 1, Offset: 8, LeaderEpoch: 3},
					{Topic: "orders", Partition: 0, Offset: 4, LeaderEpoch: 2},
					{Topic: "orders", Partition: 1, Offset: 9, LeaderEpoch: 3},
					{Topic: "orders", Partition: 0, Offset: 5, LeaderEpoch: 2},
					{Topic: "audit", Partition: 0, Offset: 11, LeaderEpoch: 4},
				}
				client := &fakeFranzKafkaJS2CompatClient{}
				stop := errors.New("test poll stopped")
				polls := 0
				client.pollFn = func(context.Context, int) kgo.Fetches {
					polls++
					if polls > 2 {
						return kgo.NewErrFetch(stop)
					}
					var fetches kgo.Fetches
					for _, record := range records {
						if polls == 2 {
							// Model the driver's fetched position: only records at or
							// after the exact recovery boundary can be returned again.
							found := false
							for _, position := range client.rewoundOffsets() {
								if position.topic == record.Topic && position.partition == record.Partition && record.Offset >= position.offset {
									found = true
								}
							}
							if !found {
								continue
							}
						}
						fetches = append(fetches, kafkaJSTestFetch(record)...)
					}
					if polls == 1 {
						fetches = append(fetches, kgo.NewErrFetch(&kgo.ErrGroupSession{Err: kerr.RebalanceInProgress})...)
					}
					return fetches
				}
				consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
				consumer.config.AutoCommit = mode == "automatic"
				opts := ConsumerAdvancedOptions{MaxRecords: len(records), CommitBeforeHandler: mode == "commit_before_handler"}
				var handled []consumerRecordPosition
				message := func(_ context.Context, payload MessagePayload) error {
					handled = append(handled, consumerRecordPosition{topic: payload.Topic, partition: int32(payload.Partition), offset: payload.Offset})
					return nil
				}
				run := func() error { return consumer.ConsumeCtxAdvanced(message, opts) }
				if batch {
					run = func() error {
						return consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, payload BatchPayload) error {
							for _, record := range payload.Messages {
								if err := message(ctx, record); err != nil {
									return err
								}
							}
							return nil
						}, opts)
					}
				}
				if err := run(); !IsTransientConsumerError(err) || !errors.Is(err, kerr.RebalanceInProgress) {
					t.Fatalf("mixed poll error = %v", err)
				}
				allow, closed, marked, exact := client.snapshot()
				if len(handled) != 0 || len(marked) != 0 || len(exact) != 0 || closed != 0 || allow == 0 || consumer.client != client {
					t.Fatalf("mixed poll dispatched/committed/discarded records: handled=%v marked=%v exact=%v closed=%d released=%d", handled, marked, exact, closed, allow)
				}
				wantRewinds := []consumerRecordPosition{
					{topic: "audit", partition: 0, offset: 11, leaderEpoch: 4},
					{topic: "orders", partition: 0, offset: 4, leaderEpoch: 2},
					{topic: "orders", partition: 1, offset: 8, leaderEpoch: 3},
				}
				gotRewinds := client.rewoundOffsets()
				sort.Slice(gotRewinds, func(i, j int) bool {
					if gotRewinds[i].topic != gotRewinds[j].topic {
						return gotRewinds[i].topic < gotRewinds[j].topic
					}
					return gotRewinds[i].partition < gotRewinds[j].partition
				})
				if !reflect.DeepEqual(gotRewinds, wantRewinds) {
					t.Fatalf("rewinds = %v, want %v", gotRewinds, wantRewinds)
				}
				if err := run(); !errors.Is(err, stop) {
					t.Fatalf("retry error = %v", err)
				}
				if len(handled) != len(records) {
					t.Fatalf("retry handled %v, want every fetched record", handled)
				}
				wantHandled := make([]consumerRecordPosition, len(records))
				for i, record := range records {
					wantHandled[i] = consumerRecordPosition{topic: record.Topic, partition: record.Partition, offset: record.Offset}
				}
				less := func(positions []consumerRecordPosition, i, j int) bool {
					if positions[i].topic != positions[j].topic {
						return positions[i].topic < positions[j].topic
					}
					if positions[i].partition != positions[j].partition {
						return positions[i].partition < positions[j].partition
					}
					return positions[i].offset < positions[j].offset
				}
				sort.Slice(handled, func(i, j int) bool { return less(handled, i, j) })
				sort.Slice(wantHandled, func(i, j int) bool { return less(wantHandled, i, j) })
				if !reflect.DeepEqual(handled, wantHandled) {
					t.Fatalf("retry handled %v, want %v", handled, wantHandled)
				}
				_, _, marked, exact = client.snapshot()
				wantOffsets := []int64{6, 10, 12}
				if !batch {
					wantOffsets = []int64{5, 6, 9, 10, 12}
				}
				committed := exact
				if mode == "automatic" {
					committed = marked
					if len(exact) != 0 {
						t.Fatalf("automatic mode committed synchronously: %v", exact)
					}
				} else if len(marked) != 0 {
					t.Fatalf("manual mode marked automatic offsets: %v", marked)
				}
				sort.Slice(committed, func(i, j int) bool { return committed[i] < committed[j] })
				if !reflect.DeepEqual(committed, wantOffsets) {
					t.Fatalf("commit boundaries = %v, want %v", committed, wantOffsets)
				}
			})
		}
	}
}

func TestKafkaJSPollRecoveryKeepsNoRecordAndCancellationSemantics(t *testing.T) {
	for _, test := range []struct {
		name    string
		cause   error
		cancel  bool
		wantErr bool
	}{
		{name: "no_record_transient", cause: kerr.RebalanceInProgress, wantErr: true},
		{name: "poll_timeout_with_record", cause: context.DeadlineExceeded},
		{name: "poll_cancellation_with_record", cause: context.Canceled},
		{name: "parent_cancel_with_record_and_group_error", cause: kerr.RebalanceInProgress, cancel: true},
		{name: "permanent_error_with_record", cause: kerr.TopicAuthorizationFailed, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &fakeFranzKafkaJS2CompatClient{pollFn: func(context.Context, int) kgo.Fetches {
				fetches := kgo.NewErrFetch(test.cause)
				if test.name != "no_record_transient" {
					fetches = append(fetches, kafkaJSTestFetch(&kgo.Record{Topic: "orders", Offset: 7})...)
				}
				if test.cancel {
					cancel()
				}
				return fetches
			}}
			_, err := pollKafkaJSRecords(ctx, client, time.Second, 10)
			if (err != nil) != test.wantErr {
				t.Fatalf("poll error = %v", err)
			}
			if _, decided := consumerRewindPositions(err); decided {
				t.Fatalf("unexpected rewind plan for %v", err)
			}
		})
	}
}

func TestKafkaJSPayloadPreservesNullableBytesAndOwnsCopies(t *testing.T) {
	for _, value := range [][]byte{nil, {}, []byte("payload")} {
		payload := kafkaJSPayload(&kgo.Record{Key: value, Value: value, Headers: []kgo.RecordHeader{{Key: "h", Value: value}}})
		for _, got := range [][]byte{payload.Key, payload.Value, payload.Headers[0].Value} {
			if (got == nil) != (value == nil) || !bytes.Equal(got, value) {
				t.Fatalf("cloned bytes = %#v, want %#v", got, value)
			}
		}
		if len(value) > 0 {
			payload.Key[0], payload.Value[0], payload.Headers[0].Value[0] = 'k', 'v', 'h'
			if string(value) != "payload" || string(payload.Key) != "kayload" || string(payload.Value) != "vayload" || string(payload.Headers[0].Value) != "hayload" {
				t.Fatal("payload bytes alias the driver record or another field")
			}
		}
	}
}

func TestKafkaJSMessageAndBatchDispatchPreserveEmptyValueAndHeaders(t *testing.T) {
	for _, batch := range []bool{false, true} {
		client := &fakeFranzKafkaJS2CompatClient{}
		stop := errors.New("dispatch probe complete")
		poll := 0
		client.pollFn = func(context.Context, int) kgo.Fetches {
			poll++
			if poll > 1 {
				return kgo.NewErrFetch(stop)
			}
			return kafkaJSTestFetch(&kgo.Record{Topic: "orders", Value: []byte{}, Headers: []kgo.RecordHeader{{Key: "null"}, {Key: "empty", Value: []byte{}}, {Key: "present", Value: []byte("v")}}})
		}
		consumer := newFranzKafkaJS2LifecycleTestConsumer(t, client, ConsumerShutdownCancelInFlight)
		called := 0
		handler := func(_ context.Context, payload MessagePayload) error {
			called++
			if payload.Value == nil || len(payload.Value) != 0 || payload.Headers[0].Value != nil || payload.Headers[1].Value == nil || len(payload.Headers[1].Value) != 0 || string(payload.Headers[2].Value) != "v" {
				t.Errorf("dispatch collapsed nullable bytes: %#v", payload)
			}
			return nil
		}
		var err error
		if batch {
			err = consumer.ConsumeBatchCtxAdvanced(func(ctx context.Context, payload BatchPayload) error { return handler(ctx, payload.Messages[0]) }, ConsumerAdvancedOptions{})
		} else {
			err = consumer.ConsumeCtxAdvanced(handler, ConsumerAdvancedOptions{})
		}
		if !errors.Is(err, stop) || called != 1 {
			t.Fatalf("dispatch err=%v calls=%d", err, called)
		}
	}
}
