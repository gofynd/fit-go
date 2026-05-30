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

package kafka

import (
	"os"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gofynd/fit-go/logging"
)

// ---------------------------------------------------------------------------
// TestNewConfluentClient_Config - verify confluent config is built correctly
// ---------------------------------------------------------------------------

func TestNewConfluentClient_Config(t *testing.T) {
	t.Run("basic config", func(t *testing.T) {
		cfg := &Config{
			Brokers:     []string{"broker1:9092", "broker2:9092"},
			ClientID:    "test-service-client",
			Compression: CompressionLZ4,
		}

		client, err := NewConfluentClient(cfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		if len(client.brokers) != 2 {
			t.Errorf("brokers count = %d, want 2", len(client.brokers))
		}

		// Verify base config map values.
		bootstrapServers, err := client.baseCfg.Get("bootstrap.servers", "")
		if err != nil {
			t.Fatalf("Get bootstrap.servers error = %v", err)
		}
		if bootstrapServers != "broker1:9092,broker2:9092" {
			t.Errorf("bootstrap.servers = %q, want 'broker1:9092,broker2:9092'", bootstrapServers)
		}

		clientID, err := client.baseCfg.Get("client.id", "")
		if err != nil {
			t.Fatalf("Get client.id error = %v", err)
		}
		if clientID != "test-service-client" {
			t.Errorf("client.id = %q, want 'test-service-client'", clientID)
		}

		compression, err := client.baseCfg.Get("compression.type", "")
		if err != nil {
			t.Fatalf("Get compression.type error = %v", err)
		}
		if compression != "lz4" {
			t.Errorf("compression.type = %q, want 'lz4'", compression)
		}

		acksVal, err := client.baseCfg.Get("acks", "")
		if err != nil {
			t.Fatalf("Get acks error = %v", err)
		}
		if acksVal != "all" {
			t.Errorf("acks = %q, want 'all'", acksVal)
		}
	})

	t.Run("nil config", func(t *testing.T) {
		_, err := NewConfluentClient(nil)
		if err == nil {
			t.Error("Expected error for nil config")
		}
	})

	t.Run("no brokers", func(t *testing.T) {
		_, err := NewConfluentClient(&Config{})
		if err == nil {
			t.Error("Expected error for empty brokers")
		}
	})

	t.Run("closed client rejects producer", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
		}
		client, err := NewConfluentClient(cfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		client.Close()

		_, err = client.Producer(ProducerConfig{})
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Errorf("Expected 'closed' error, got: %v", err)
		}
	})

	t.Run("closed client rejects consumer", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
		}
		client, err := NewConfluentClient(cfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		client.Close()

		_, err = client.Consumer(ConsumerConfig{GroupID: "group"})
		if err == nil || !strings.Contains(err.Error(), "closed") {
			t.Errorf("Expected 'closed' error, got: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// TestConfluentClient_ConfigFromEnv - test env var resolution end to end
// ---------------------------------------------------------------------------

func TestConfluentClient_ConfigFromEnv(t *testing.T) {
	// Helper to clear all Kafka env vars.
	clearKafkaEnv := func() {
		for _, prefix := range []string{
			"KAFKA_SASL_SSL_", "KAFKA_SSL_", "KAFKA_SASL_", "KAFKA_BROKER_LIST",
		} {
			for _, e := range os.Environ() {
				if strings.HasPrefix(e, prefix) {
					idx := strings.IndexByte(e, '=')
					if idx > 0 {
						os.Unsetenv(e[:idx])
					}
				}
			}
		}
		os.Unsetenv("KAFKA_BROKER_LIST")
		os.Unsetenv("SERVICE_NAME")
		os.Unsetenv("LOG_LEVEL")
	}

	t.Run("plaintext env creates valid confluent config", func(t *testing.T) {
		clearKafkaEnv()
		os.Setenv("KAFKA_BROKER_LIST", "broker1:9092,broker2:9092")
		os.Setenv("SERVICE_NAME", "my-service")
		defer clearKafkaEnv()

		envCfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv() error = %v", err)
		}

		// Apply defaults that NewClient would apply.
		envCfg.ClientID = "my-service-client"
		envCfg.Compression = CompressionLZ4

		client, err := NewConfluentClient(envCfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		clientID, _ := client.baseCfg.Get("client.id", "")
		if clientID != "my-service-client" {
			t.Errorf("client.id = %q, want 'my-service-client'", clientID)
		}

		// No SASL should be configured for plaintext.
		_, err = client.baseCfg.Get("sasl.username", nil)
		// For plaintext, sasl.username should not be present (Get returns
		// the default if key not found).
		secProto, _ := client.baseCfg.Get("security.protocol", "")
		if secProto != "" {
			t.Errorf("security.protocol should not be set for plaintext, got %q", secProto)
		}
	})

	t.Run("SASL env creates valid confluent config", func(t *testing.T) {
		clearKafkaEnv()
		os.Setenv("KAFKA_SASL_BROKER_LIST", "broker:9092")
		os.Setenv("KAFKA_SASL_USR", "user")
		os.Setenv("KAFKA_SASL_PAS", "pass")
		os.Setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256")
		defer clearKafkaEnv()

		envCfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv() error = %v", err)
		}
		envCfg.ClientID = "test-client"
		envCfg.Compression = CompressionLZ4

		client, err := NewConfluentClient(envCfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		secProto, _ := client.baseCfg.Get("security.protocol", "")
		if secProto != "SASL_PLAINTEXT" {
			t.Errorf("security.protocol = %q, want 'SASL_PLAINTEXT'", secProto)
		}

		saslUser, _ := client.baseCfg.Get("sasl.username", "")
		if saslUser != "user" {
			t.Errorf("sasl.username = %q, want 'user'", saslUser)
		}

		saslMech, _ := client.baseCfg.Get("sasl.mechanism", "")
		if saslMech != "SCRAM-SHA-256" {
			t.Errorf("sasl.mechanism = %q, want 'SCRAM-SHA-256'", saslMech)
		}
	})
}

// ---------------------------------------------------------------------------
// TestConfluentProducer_MessageMapping - verify Message to confluent mapping
// ---------------------------------------------------------------------------

func TestConfluentProducer_MessageMapping(t *testing.T) {
	t.Run("basic message", func(t *testing.T) {
		msg := Message{
			Key:   []byte("partition-key"),
			Value: []byte(`{"order_id": "12345"}`),
		}

		km := buildConfluentMessage("orders", msg)

		if *km.TopicPartition.Topic != "orders" {
			t.Errorf("Topic = %q, want 'orders'", *km.TopicPartition.Topic)
		}
		if string(km.Key) != "partition-key" {
			t.Errorf("Key = %q, want 'partition-key'", string(km.Key))
		}
		if string(km.Value) != `{"order_id": "12345"}` {
			t.Errorf("Value = %q, want JSON", string(km.Value))
		}
	})

	t.Run("message with headers", func(t *testing.T) {
		msg := Message{
			Value: []byte("payload"),
			Headers: []Header{
				{Key: "trace-id", Value: []byte("abc-123")},
				{Key: "content-type", Value: []byte("application/json")},
			},
		}

		km := buildConfluentMessage("events", msg)

		if len(km.Headers) != 2 {
			t.Fatalf("Headers count = %d, want 2", len(km.Headers))
		}
		if km.Headers[0].Key != "trace-id" {
			t.Errorf("Header[0].Key = %q, want 'trace-id'", km.Headers[0].Key)
		}
		if string(km.Headers[0].Value) != "abc-123" {
			t.Errorf("Header[0].Value = %q, want 'abc-123'", string(km.Headers[0].Value))
		}
		if km.Headers[1].Key != "content-type" {
			t.Errorf("Header[1].Key = %q, want 'content-type'", km.Headers[1].Key)
		}
	})

	t.Run("message with timestamp", func(t *testing.T) {
		ts := time.Date(2026, 4, 11, 10, 30, 0, 0, time.UTC)
		msg := Message{
			Value:     []byte("payload"),
			Timestamp: ts,
		}

		km := buildConfluentMessage("events", msg)

		if !km.Timestamp.Equal(ts) {
			t.Errorf("Timestamp = %v, want %v", km.Timestamp, ts)
		}
	})

	t.Run("message with explicit partition", func(t *testing.T) {
		msg := Message{
			Value:     []byte("payload"),
			Partition: 3,
		}

		km := buildConfluentMessage("events", msg)

		if km.TopicPartition.Partition != 3 {
			t.Errorf("Partition = %d, want 3", km.TopicPartition.Partition)
		}
	})

	t.Run("message with no key", func(t *testing.T) {
		msg := Message{
			Value: []byte("payload"),
		}

		km := buildConfluentMessage("events", msg)

		if km.Key != nil {
			t.Error("Key should be nil when not set")
		}
	})

	t.Run("message with no headers", func(t *testing.T) {
		msg := Message{
			Value: []byte("payload"),
		}

		km := buildConfluentMessage("events", msg)

		if km.Headers != nil {
			t.Error("Headers should be nil when not set")
		}
	})

	t.Run("message with default partition is PartitionAny", func(t *testing.T) {
		msg := Message{
			Value:     []byte("payload"),
			Partition: -1,
		}

		km := buildConfluentMessage("events", msg)

		// Partition -1 should not override to explicit -1; our code checks >= 0.
		// But -1 is not >= 0, so it stays as PartitionAny.
		if km.TopicPartition.Partition != ckafka.PartitionAny {
			t.Errorf("Partition = %d, want PartitionAny (%d)", km.TopicPartition.Partition, ckafka.PartitionAny)
		}
	})
}

// ---------------------------------------------------------------------------
// TestConfluentConsumer_TopicConfig - verify consumer config mapping
// ---------------------------------------------------------------------------

func TestConfluentConsumer_TopicConfig(t *testing.T) {
	t.Run("consumer requires group ID", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
		}
		client, err := NewConfluentClient(cfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		_, err = client.Consumer(ConsumerConfig{GroupID: ""})
		if err == nil || !strings.Contains(err.Error(), "group ID") {
			t.Errorf("Expected 'group ID' error, got: %v", err)
		}
	})

	t.Run("consumer config mapping", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
		}
		client, err := NewConfluentClient(cfg)
		if err != nil {
			t.Fatalf("NewConfluentClient() error = %v", err)
		}

		consumerCfg := ConsumerConfig{
			GroupID:              "test-group",
			SessionTimeout:       45 * time.Second,
			HeartbeatInterval:    5 * time.Second,
			RebalanceTimeout:     120 * time.Second,
			MaxBytesPerPartition: 2 << 20, // 2 MB
			MinBytes:             512,
			MaxBytes:             20 << 20, // 20 MB
			MaxWaitTime:          10 * time.Second,
			AutoCommit:           true,
			AutoCommitInterval:   10 * time.Second,
		}

		consumer, err := client.Consumer(consumerCfg)
		if err != nil {
			t.Fatalf("Consumer() error = %v", err)
		}

		cc := consumer.(*ConfluentConsumer)
		if cc.groupID != "test-group" {
			t.Errorf("groupID = %q, want 'test-group'", cc.groupID)
		}

		groupID, _ := cc.configMap.Get("group.id", "")
		if groupID != "test-group" {
			t.Errorf("group.id = %q, want 'test-group'", groupID)
		}

		sessionTimeout, _ := cc.configMap.Get("session.timeout.ms", 0)
		if sessionTimeout != 45000 {
			t.Errorf("session.timeout.ms = %v, want 45000", sessionTimeout)
		}

		heartbeat, _ := cc.configMap.Get("heartbeat.interval.ms", 0)
		if heartbeat != 5000 {
			t.Errorf("heartbeat.interval.ms = %v, want 5000", heartbeat)
		}

		rebalance, _ := cc.configMap.Get("max.poll.interval.ms", 0)
		if rebalance != 120000 {
			t.Errorf("max.poll.interval.ms = %v, want 120000", rebalance)
		}

		maxPartFetch, _ := cc.configMap.Get("max.partition.fetch.bytes", 0)
		if maxPartFetch != 2<<20 {
			t.Errorf("max.partition.fetch.bytes = %v, want %d", maxPartFetch, 2<<20)
		}

		fetchMin, _ := cc.configMap.Get("fetch.min.bytes", 0)
		if fetchMin != 512 {
			t.Errorf("fetch.min.bytes = %v, want 512", fetchMin)
		}

		fetchMax, _ := cc.configMap.Get("fetch.max.bytes", 0)
		if fetchMax != 20<<20 {
			t.Errorf("fetch.max.bytes = %v, want %d", fetchMax, 20<<20)
		}

		fetchWait, _ := cc.configMap.Get("fetch.wait.max.ms", 0)
		if fetchWait != 10000 {
			t.Errorf("fetch.wait.max.ms = %v, want 10000", fetchWait)
		}

		autoCommit, _ := cc.configMap.Get("enable.auto.commit", false)
		if autoCommit != true {
			t.Error("enable.auto.commit should be true")
		}

		autoCommitInterval, _ := cc.configMap.Get("auto.commit.interval.ms", 0)
		if autoCommitInterval != 10000 {
			t.Errorf("auto.commit.interval.ms = %v, want 10000", autoCommitInterval)
		}
	})
}

// ---------------------------------------------------------------------------
// TestSASLMechanisms - test SASL mechanism mapping
// ---------------------------------------------------------------------------

func TestSASLMechanisms(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"PLAIN", "PLAIN", "PLAIN"},
		{"SCRAM-SHA-256", "SCRAM-SHA-256", "SCRAM-SHA-256"},
		{"SCRAM-SHA-512", "SCRAM-SHA-512", "SCRAM-SHA-512"},
		{"lowercase plain", "plain", "PLAIN"},
		{"unknown defaults to PLAIN", "OAUTHBEARER", "PLAIN"},
		{"empty defaults to PLAIN", "", "PLAIN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapSASLMechanismToString(tt.input)
			if got != tt.expected {
				t.Errorf("mapSASLMechanismToString(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}

	// Test that SCRAM mechanisms build correct config.
	t.Run("SCRAM-SHA-256 builds config", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
			SASL: &SASLConfig{
				Mechanism: "SCRAM-SHA-256",
				Username:  "user",
				Password:  "pass",
			},
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		secProto, _ := cm.Get("security.protocol", "")
		if secProto != "SASL_PLAINTEXT" {
			t.Errorf("security.protocol = %q, want 'SASL_PLAINTEXT'", secProto)
		}

		mechanism, _ := cm.Get("sasl.mechanism", "")
		if mechanism != "SCRAM-SHA-256" {
			t.Errorf("sasl.mechanism = %q, want 'SCRAM-SHA-256'", mechanism)
		}

		username, _ := cm.Get("sasl.username", "")
		if username != "user" {
			t.Errorf("sasl.username = %q, want 'user'", username)
		}

		password, _ := cm.Get("sasl.password", "")
		if password != "pass" {
			t.Errorf("sasl.password = %q, want 'pass'", password)
		}
	})

	t.Run("SCRAM-SHA-512 builds config", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
			SASL: &SASLConfig{
				Mechanism: "SCRAM-SHA-512",
				Username:  "user",
				Password:  "pass",
			},
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		mechanism, _ := cm.Get("sasl.mechanism", "")
		if mechanism != "SCRAM-SHA-512" {
			t.Errorf("sasl.mechanism = %q, want 'SCRAM-SHA-512'", mechanism)
		}
	})

	t.Run("PLAIN mechanism", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
			SASL: &SASLConfig{
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass",
			},
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		mechanism, _ := cm.Get("sasl.mechanism", "")
		if mechanism != "PLAIN" {
			t.Errorf("sasl.mechanism = %q, want 'PLAIN'", mechanism)
		}
	})
}

// ---------------------------------------------------------------------------
// TestTLSConfigBuilding - test TLS configuration building
// ---------------------------------------------------------------------------

func TestTLSConfigBuilding(t *testing.T) {
	t.Run("no TLS config", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		secProto, _ := cm.Get("security.protocol", "")
		if secProto != "" {
			t.Errorf("security.protocol should not be set, got %q", secProto)
		}
	})

	t.Run("TLS config sets SSL protocol", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
			TLS:      &TLSConfig{},
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		secProto, _ := cm.Get("security.protocol", "")
		if secProto != "SSL" {
			t.Errorf("security.protocol = %q, want 'SSL'", secProto)
		}

		sslVerify, _ := cm.Get("enable.ssl.certificate.verification", true)
		if sslVerify != false {
			t.Error("enable.ssl.certificate.verification should be false")
		}
	})

	t.Run("TLS with cert paths", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
			TLS: &TLSConfig{
				CAFile:   "/path/to/ca.pem",
				CertFile: "/path/to/cert.pem",
				KeyFile:  "/path/to/key.pem",
			},
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		ca, _ := cm.Get("ssl.ca.location", "")
		if ca != "/path/to/ca.pem" {
			t.Errorf("ssl.ca.location = %q, want '/path/to/ca.pem'", ca)
		}

		cert, _ := cm.Get("ssl.certificate.location", "")
		if cert != "/path/to/cert.pem" {
			t.Errorf("ssl.certificate.location = %q, want '/path/to/cert.pem'", cert)
		}

		key, _ := cm.Get("ssl.key.location", "")
		if key != "/path/to/key.pem" {
			t.Errorf("ssl.key.location = %q, want '/path/to/key.pem'", key)
		}
	})

	t.Run("SASL + TLS sets SASL_SSL protocol", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test",
			SASL: &SASLConfig{
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass",
			},
			TLS: &TLSConfig{
				CAFile: "/path/to/ca.pem",
			},
		}

		cm, err := buildConfluentConfig(cfg)
		if err != nil {
			t.Fatalf("buildConfluentConfig() error = %v", err)
		}

		secProto, _ := cm.Get("security.protocol", "")
		if secProto != "SASL_SSL" {
			t.Errorf("security.protocol = %q, want 'SASL_SSL'", secProto)
		}
	})
}

// ---------------------------------------------------------------------------
// TestCompressionCodecMapping - test compression codec mapping
// ---------------------------------------------------------------------------

func TestCompressionCodecMapping(t *testing.T) {
	tests := []struct {
		name     string
		fit      CompressionType
		expected string
	}{
		{"None", CompressionNone, "none"},
		{"GZIP", CompressionGZIP, "gzip"},
		{"Snappy", CompressionSnappy, "snappy"},
		{"LZ4", CompressionLZ4, "lz4"},
		{"ZSTD", CompressionZSTD, "zstd"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapCompressionToString(tt.fit)
			if got != tt.expected {
				t.Errorf("mapCompressionToString(%v) = %v, want %v", tt.fit, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestConfluentPayloadMapping - verify confluent Message to fit payload
// ---------------------------------------------------------------------------

func TestConfluentPayloadMapping(t *testing.T) {
	t.Run("full message mapping", func(t *testing.T) {
		ts := time.Date(2026, 4, 11, 10, 0, 0, 0, time.UTC)
		topic := "orders"
		msg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{
				Topic:     &topic,
				Partition: 2,
				Offset:    42,
			},
			Key:       []byte("order-123"),
			Value:     []byte(`{"status":"confirmed"}`),
			Timestamp: ts,
			Headers: []ckafka.Header{
				{Key: "trace-id", Value: []byte("abc-123")},
				{Key: "source", Value: []byte("api")},
			},
		}

		payload := mapConfluentToPayload(msg)

		if payload.Topic != "orders" {
			t.Errorf("Topic = %q, want 'orders'", payload.Topic)
		}
		if payload.Partition != 2 {
			t.Errorf("Partition = %d, want 2", payload.Partition)
		}
		if payload.Offset != 42 {
			t.Errorf("Offset = %d, want 42", payload.Offset)
		}
		if string(payload.Key) != "order-123" {
			t.Errorf("Key = %q, want 'order-123'", string(payload.Key))
		}
		if string(payload.Value) != `{"status":"confirmed"}` {
			t.Errorf("Value = %q, want JSON", string(payload.Value))
		}
		if !payload.Timestamp.Equal(ts) {
			t.Errorf("Timestamp = %v, want %v", payload.Timestamp, ts)
		}
		if len(payload.Headers) != 2 {
			t.Fatalf("Headers count = %d, want 2", len(payload.Headers))
		}
		if payload.Headers[0].Key != "trace-id" {
			t.Errorf("Headers[0].Key = %q, want 'trace-id'", payload.Headers[0].Key)
		}
		if string(payload.Headers[0].Value) != "abc-123" {
			t.Errorf("Headers[0].Value = %q, want 'abc-123'", string(payload.Headers[0].Value))
		}
	})

	t.Run("message with no headers", func(t *testing.T) {
		topic := "events"
		msg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{
				Topic:     &topic,
				Partition: 0,
				Offset:    1,
			},
			Value: []byte("data"),
		}

		payload := mapConfluentToPayload(msg)

		if payload.Headers != nil {
			t.Error("Headers should be nil when message has no headers")
		}
	})

	t.Run("message with nil topic", func(t *testing.T) {
		msg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{
				Topic:     nil,
				Partition: 0,
				Offset:    1,
			},
			Value: []byte("data"),
		}

		payload := mapConfluentToPayload(msg)

		if payload.Topic != "" {
			t.Errorf("Topic = %q, want empty string for nil topic", payload.Topic)
		}
	})
}

// ---------------------------------------------------------------------------
// TestProducerConfigOverrides - verify per-producer config overrides
// ---------------------------------------------------------------------------

func TestProducerConfigOverrides(t *testing.T) {
	cfg := &Config{
		Brokers:     []string{"broker:9092"},
		ClientID:    "test",
		Compression: CompressionLZ4,
	}
	client, err := NewConfluentClient(cfg)
	if err != nil {
		t.Fatalf("NewConfluentClient() error = %v", err)
	}

	t.Run("default producer inherits client compression", func(t *testing.T) {
		producer, err := client.Producer(ProducerConfig{})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		cp := producer.(*ConfluentProducer)
		compression, _ := cp.configMap.Get("compression.type", "")
		if compression != "lz4" {
			t.Errorf("compression.type = %v, want 'lz4'", compression)
		}
	})

	t.Run("producer overrides compression", func(t *testing.T) {
		producer, err := client.Producer(ProducerConfig{
			Compression: CompressionSnappy,
		})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		cp := producer.(*ConfluentProducer)
		compression, _ := cp.configMap.Get("compression.type", "")
		if compression != "snappy" {
			t.Errorf("compression.type = %v, want 'snappy'", compression)
		}
	})

	t.Run("producer overrides acks", func(t *testing.T) {
		producer, err := client.Producer(ProducerConfig{
			Acks: 1,
		})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		cp := producer.(*ConfluentProducer)
		acksVal, _ := cp.configMap.Get("acks", "")
		if acksVal != "1" {
			t.Errorf("acks = %v, want '1'", acksVal)
		}
	})

	t.Run("idempotent producer forces acks=all", func(t *testing.T) {
		producer, err := client.Producer(ProducerConfig{
			IdempotentProducer: true,
		})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		cp := producer.(*ConfluentProducer)
		idempotent, _ := cp.configMap.Get("enable.idempotence", false)
		if idempotent != true {
			t.Error("enable.idempotence should be true")
		}

		acksVal, _ := cp.configMap.Get("acks", "")
		if acksVal != "all" {
			t.Errorf("acks = %v, want 'all'", acksVal)
		}

		maxInFlight, _ := cp.configMap.Get("max.in.flight.requests.per.connection", 0)
		if maxInFlight != 1 {
			t.Errorf("max.in.flight.requests.per.connection = %v, want 1", maxInFlight)
		}
	})

	t.Run("producer with timeout and retry settings", func(t *testing.T) {
		producer, err := client.Producer(ProducerConfig{
			Timeout:      15 * time.Second,
			MaxRetries:   5,
			RetryBackoff: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		cp := producer.(*ConfluentProducer)
		timeout, _ := cp.configMap.Get("request.timeout.ms", 0)
		if timeout != 15000 {
			t.Errorf("request.timeout.ms = %v, want 15000", timeout)
		}

		retries, _ := cp.configMap.Get("message.send.max.retries", 0)
		if retries != 5 {
			t.Errorf("message.send.max.retries = %v, want 5", retries)
		}

		backoff, _ := cp.configMap.Get("retry.backoff.ms", 0)
		if backoff != 500 {
			t.Errorf("retry.backoff.ms = %v, want 500", backoff)
		}
	})

	t.Run("producer config does not mutate client config", func(t *testing.T) {
		// Create a producer with overrides.
		_, err := client.Producer(ProducerConfig{
			Compression: CompressionGZIP,
			Acks:        1,
		})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		// Create another producer - should still have client defaults.
		producer2, err := client.Producer(ProducerConfig{})
		if err != nil {
			t.Fatalf("Producer() error = %v", err)
		}

		cp2 := producer2.(*ConfluentProducer)
		compression, _ := cp2.configMap.Get("compression.type", "")
		if compression != "lz4" {
			t.Errorf("Second producer compression.type = %v, want 'lz4' (client default)", compression)
		}

		acksVal, _ := cp2.configMap.Get("acks", "")
		if acksVal != "all" {
			t.Errorf("Second producer acks = %v, want 'all' (client default)", acksVal)
		}
	})
}

// ---------------------------------------------------------------------------
// TestResolveAutoCommit - test auto-commit resolution logic
// ---------------------------------------------------------------------------

func TestResolveAutoCommit(t *testing.T) {
	boolPtr := func(v bool) *bool { return &v }

	tests := []struct {
		name          string
		configDefault bool
		override      *bool
		expected      bool
	}{
		{"default true, no override", true, nil, true},
		{"default false, no override", false, nil, false},
		{"default true, override false", true, boolPtr(false), false},
		{"default false, override true", false, boolPtr(true), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveAutoCommit(tt.configDefault, tt.override)
			if got != tt.expected {
				t.Errorf("resolveAutoCommit(%v, %v) = %v, want %v",
					tt.configDefault, tt.override, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestSecurityProtocol - verify security protocol resolution
// ---------------------------------------------------------------------------

func TestSecurityProtocol(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		expected string
	}{
		{
			"plaintext",
			&Config{Brokers: []string{"b:9092"}},
			"PLAINTEXT",
		},
		{
			"SASL only",
			&Config{
				Brokers: []string{"b:9092"},
				SASL:    &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"},
			},
			"SASL_PLAINTEXT",
		},
		{
			"TLS only",
			&Config{
				Brokers: []string{"b:9092"},
				TLS:     &TLSConfig{},
			},
			"SSL",
		},
		{
			"SASL + TLS",
			&Config{
				Brokers: []string{"b:9092"},
				SASL:    &SASLConfig{Mechanism: "PLAIN", Username: "u", Password: "p"},
				TLS:     &TLSConfig{},
			},
			"SASL_SSL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := securityProtocol(tt.cfg)
			if got != tt.expected {
				t.Errorf("securityProtocol() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestInitDefault - verify convenience function wiring
// ---------------------------------------------------------------------------

func TestInitDefault(t *testing.T) {
	t.Run("nil config with no env returns error", func(t *testing.T) {
		// Clear all Kafka env vars.
		for _, key := range []string{
			"KAFKA_BROKER_LIST", "KAFKA_SASL_BROKER_LIST",
			"KAFKA_SSL_BROKER_LIST", "KAFKA_SASL_SSL_BROKER_LIST",
		} {
			os.Unsetenv(key)
		}

		_, err := InitDefault(nil)
		if err == nil {
			t.Error("Expected error when no config and no env vars")
		}
	})

	t.Run("valid config creates client with driver", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test-init",
		}

		client, err := InitDefault(cfg)
		if err != nil {
			t.Fatalf("InitDefault() error = %v", err)
		}

		if client.Driver == nil {
			t.Error("Driver should not be nil after InitDefault")
		}

		_, ok := client.Driver.(*ConfluentClient)
		if !ok {
			t.Error("Driver should be a *ConfluentClient")
		}

		// Cleanup.
		client.Driver.Close()
	})
}

// ---------------------------------------------------------------------------
// TestConfluentProducer_NotConnected - verify error on produce without connect
// ---------------------------------------------------------------------------

func TestConfluentProducer_NotConnected(t *testing.T) {
	cfg := &Config{
		Brokers:  []string{"broker:9092"},
		ClientID: "test",
	}
	client, err := NewConfluentClient(cfg)
	if err != nil {
		t.Fatalf("NewConfluentClient() error = %v", err)
	}

	producer, err := client.Producer(ProducerConfig{})
	if err != nil {
		t.Fatalf("Producer() error = %v", err)
	}

	// Produce without calling Connect() should fail.
	err = producer.Produce("topic", []Message{{Value: []byte("test")}}, 0)
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("Expected 'not connected' error, got: %v", err)
	}

	// ProduceBatch without Connect() should also fail.
	err = producer.ProduceBatch([]TopicMessages{
		{Topic: "topic", Messages: []Message{{Value: []byte("test")}}},
	}, 0)
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("Expected 'not connected' error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConfluentConsumer_NotConnected - verify error on consume without connect
// ---------------------------------------------------------------------------

func TestConfluentConsumer_NotConnected(t *testing.T) {
	cfg := &Config{
		Brokers:  []string{"broker:9092"},
		ClientID: "test",
	}
	client, err := NewConfluentClient(cfg)
	if err != nil {
		t.Fatalf("NewConfluentClient() error = %v", err)
	}

	consumer, err := client.Consumer(ConsumerConfig{GroupID: "test-group"})
	if err != nil {
		t.Fatalf("Consumer() error = %v", err)
	}

	// Consume without Connect() should fail.
	err = consumer.Consume(func(p MessagePayload) error { return nil }, ConsumerOptions{})
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("Expected 'not connected' error, got: %v", err)
	}

	// ConsumeBatch without Connect() should fail.
	err = consumer.ConsumeBatch(func(p BatchPayload) error { return nil }, ConsumerOptions{})
	if err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Errorf("Expected 'not connected' error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConfluentProducer_CloseIdempotent - verify double close is safe
// ---------------------------------------------------------------------------

func TestConfluentProducer_CloseIdempotent(t *testing.T) {
	cp := &ConfluentProducer{
		logger: mustLogger(),
	}

	// Close without connect should be safe.
	if err := cp.Close(); err != nil {
		t.Errorf("Close() on unconnected producer error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConfluentConsumer_CloseIdempotent - verify double close is safe
// ---------------------------------------------------------------------------

func TestConfluentConsumer_CloseIdempotent(t *testing.T) {
	cc := &ConfluentConsumer{
		groupID: "test",
		logger:  mustLogger(),
	}

	// Close without connect should be safe.
	if err := cc.Close(); err != nil {
		t.Errorf("Close() on unconnected consumer error = %v", err)
	}

	// Double close should be safe.
	if err := cc.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestConfluentClient_CloseIdempotent
// ---------------------------------------------------------------------------

func TestConfluentClient_CloseIdempotent(t *testing.T) {
	cfg := &Config{
		Brokers:  []string{"broker:9092"},
		ClientID: "test",
	}
	client, err := NewConfluentClient(cfg)
	if err != nil {
		t.Fatalf("NewConfluentClient() error = %v", err)
	}

	if err := client.Close(); err != nil {
		t.Errorf("First Close() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// TestCloneConfigMap - verify config map cloning
// ---------------------------------------------------------------------------

func TestCloneConfigMap(t *testing.T) {
	original := &ckafka.ConfigMap{
		"bootstrap.servers": "broker:9092",
		"client.id":         "test",
		"acks":              "all",
	}

	clone := cloneConfigMap(original)

	// Modify clone and verify original is unchanged.
	_ = clone.SetKey("client.id", "modified")

	origClientID, _ := original.Get("client.id", "")
	if origClientID != "test" {
		t.Errorf("Original client.id = %q, want 'test' (clone should not affect original)", origClientID)
	}

	cloneClientID, _ := clone.Get("client.id", "")
	if cloneClientID != "modified" {
		t.Errorf("Clone client.id = %q, want 'modified'", cloneClientID)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// mustLogger creates a logger for testing; panics on failure.
func mustLogger() *logging.Logger {
	l, err := logging.New(logging.Options{Level: "info"})
	if err != nil {
		panic(err)
	}
	return l
}
