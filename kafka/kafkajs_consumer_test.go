package kafka

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

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
