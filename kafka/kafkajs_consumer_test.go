package kafka

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/gofynd/fit-go/logging"
)

func TestKafkaJSTransientConsumerErrorClassification(t *testing.T) {
	networkCause := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	tests := []struct {
		name      string
		err       error
		transient bool
	}{
		{name: "dial failure", err: fmt.Errorf("post-handler commit failed: %w", networkCause), transient: true},
		{name: "retriable protocol failure", err: fmt.Errorf("commit failed: %w", kerr.CoordinatorNotAvailable), transient: true},
		{name: "permanent protocol failure", err: fmt.Errorf("commit failed: %w", kerr.GroupAuthorizationFailed), transient: false},
		{name: "handler failure", err: errors.New("validation failed"), transient: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyKafkaJSTransientConsumerError(tc.err)
			isTransient := IsTransientConsumerError(got)
			if isTransient != tc.transient {
				t.Fatalf("IsTransientConsumerError(%v) = %v, want %v", got, isTransient, tc.transient)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("classified error no longer unwraps to original: %v", got)
			}
		})
	}
}

func TestKafkaJSRoundRobinBalancerChangesOnlyProtocolName(t *testing.T) {
	base := kgo.RoundRobinBalancer()
	candidate := kafkaJSRoundRobinBalancer{GroupBalancer: base}
	if got := candidate.ProtocolName(); got != "RoundRobinAssigner" {
		t.Fatalf("protocol name = %q", got)
	}
	interests := []string{"discounts"}
	assignment := map[string][]int32{"discounts": {0, 1}}
	if got, want := candidate.JoinGroupMetadata(interests, assignment, 7), base.JoinGroupMetadata(interests, assignment, 7); !reflect.DeepEqual(got, want) {
		t.Fatal("KafkaJS wrapper changed standard consumer-group metadata")
	}
	if candidate.IsCooperative() != base.IsCooperative() {
		t.Fatal("KafkaJS wrapper changed eager/cooperative semantics")
	}
}

func TestKafkaJSRoundRobinBalancerMatchesMultiMemberMultiPartitionPlan(t *testing.T) {
	base := kgo.RoundRobinBalancer()
	candidate := kafkaJSRoundRobinBalancer{GroupBalancer: base}
	members := []kmsg.JoinGroupResponseMember{
		{MemberID: "legacy-kafkajs", ProtocolMetadata: base.JoinGroupMetadata([]string{"discount-a", "discount-b"}, nil, 1)},
		{MemberID: "metroplex-go", ProtocolMetadata: candidate.JoinGroupMetadata([]string{"discount-a", "discount-b"}, nil, 1)},
	}
	baseBalancer, baseTopics, err := base.MemberBalancer(members)
	if err != nil {
		t.Fatal(err)
	}
	candidateBalancer, candidateTopics, err := candidate.MemberBalancer(members)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(candidateTopics, baseTopics) {
		t.Fatalf("topics = %#v, want %#v", candidateTopics, baseTopics)
	}
	partitions := map[string]int32{"discount-a": 3, "discount-b": 2}
	basePlan, err := baseBalancer.(interface {
		BalanceOrError(map[string]int32) (kgo.IntoSyncAssignment, error)
	}).BalanceOrError(partitions)
	if err != nil {
		t.Fatal(err)
	}
	candidatePlan, err := candidateBalancer.(interface {
		BalanceOrError(map[string]int32) (kgo.IntoSyncAssignment, error)
	}).BalanceOrError(partitions)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := candidatePlan.IntoSyncAssignment(), basePlan.IntoSyncAssignment(); !reflect.DeepEqual(got, want) {
		t.Fatalf("assignment = %#v, want %#v", got, want)
	}
}

func TestKafkaJSCompatibleManualConsumerDisablesBackgroundAutoCommit(t *testing.T) {
	logger, err := logging.New(logging.Options{Level: "info"})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &kafkaJSCompatibleConsumer{
		brokers: []string{"127.0.0.1:1"},
		fitCfg:  &Config{ClientID: "test"},
		config:  ConsumerConfig{GroupID: "group", AutoCommit: false},
		logger:  logger,
	}
	opts, err := consumer.clientOptions([]TopicConfig{{Topic: "discounts"}})
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if disabled, ok := client.OptValue(kgo.DisableAutoCommit).(bool); !ok || !disabled {
		t.Fatalf("DisableAutoCommit = %#v; manual mode could background-commit an unhandled record", client.OptValue(kgo.DisableAutoCommit))
	}
}

func TestConfluentClientSelectsKafkaJSConsumerOnlyWhenExplicit(t *testing.T) {
	client, err := NewConfluentClient(&Config{Brokers: []string{"127.0.0.1:9092"}, ClientID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	legacyCompatible, err := client.Consumer(ConsumerConfig{
		GroupID: "group",
		Backend: ConsumerBackendKafkaJSCompatible,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := legacyCompatible.(*kafkaJSCompatibleConsumer); !ok {
		t.Fatalf("explicit backend produced %T", legacyCompatible)
	}

	standard, err := client.Consumer(ConsumerConfig{GroupID: "group"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := standard.(*ConfluentConsumer); !ok {
		t.Fatalf("zero-value backend produced %T", standard)
	}

	if _, err = client.Consumer(ConsumerConfig{GroupID: "group", Backend: ConsumerBackend(255)}); err == nil {
		t.Fatal("unknown backend was accepted")
	}
}

func TestKafkaJSCompatibleSASLRejectsUnknownMechanism(t *testing.T) {
	if _, err := kafkaJSCompatibleSASL(&SASLConfig{Mechanism: "GSSAPI"}); err == nil {
		t.Fatal("unsupported SASL mechanism was accepted")
	}
	for _, mechanism := range []string{"", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		if _, err := kafkaJSCompatibleSASL(&SASLConfig{Mechanism: mechanism, Username: "u", Password: "p"}); err != nil {
			t.Fatalf("mechanism %q: %v", mechanism, err)
		}
	}
}

func TestFlattenAssignmentsIsStable(t *testing.T) {
	got := flattenAssignments(map[string][]int32{"z": {2, 0}, "a": {3, 1}})
	want := []PartitionAssignment{{Topic: "a", Partition: 1}, {Topic: "a", Partition: 3}, {Topic: "z", Partition: 0}, {Topic: "z", Partition: 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assignments = %#v", got)
	}
}
