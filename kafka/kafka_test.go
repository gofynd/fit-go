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
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

type healthDriver struct{ err error }

func (d *healthDriver) Producer(ProducerConfig) (KafkaProducer, error) { return nil, nil }
func (d *healthDriver) Consumer(ConsumerConfig) (KafkaConsumer, error) { return nil, nil }
func (d *healthDriver) Close() error                                   { return nil }
func (d *healthDriver) Ping(context.Context) error                     { return d.err }

type noHealthDriver struct{}

func (*noHealthDriver) Producer(ProducerConfig) (KafkaProducer, error) { return nil, nil }
func (*noHealthDriver) Consumer(ConsumerConfig) (KafkaConsumer, error) { return nil, nil }
func (*noHealthDriver) Close() error                                   { return nil }

func TestClientPing(t *testing.T) {
	want := errors.New("broker unavailable password=hunter2 for user@example.com")
	client := &Client{Driver: &healthDriver{err: want}}
	err := client.Ping(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Ping error = %v, want %v", err, want)
	}
	for _, secret := range []string{"hunter2", "user@example.com"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("Ping error leaked %q: %v", secret, err)
		}
	}
	if got := safeKafkaHealthMessage("SASL password=hunter2 owner=user@example.com"); strings.Contains(got, "hunter2") || strings.Contains(got, "user@example.com") {
		t.Fatalf("safeKafkaHealthMessage leaked health details: %q", got)
	}
	if err := (&Client{Driver: &noHealthDriver{}}).Ping(context.Background()); err == nil {
		t.Fatal("Ping unsupported driver: expected error")
	}
	if err := (&Client{}).Ping(context.Background()); err == nil {
		t.Fatal("Ping missing driver: expected error")
	}
}

// ---------------------------------------------------------------------------
// LogLevel tests
// ---------------------------------------------------------------------------

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
	}{
		{"ERROR", LogLevelError},
		{"WARN", LogLevelWarn},
		{"INFO", LogLevelInfo},
		{"DEBUG", LogLevelDebug},
		{"error", LogLevelError},
		{" info ", LogLevelInfo},
		{"", LogLevelNothing},
		{"invalid", LogLevelNothing},
		{"TRACE", LogLevelNothing},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseLogLevel(tt.input); got != tt.expected {
				t.Errorf("ParseLogLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestSplitBrokers(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"broker1:9092", []string{"broker1:9092"}},
		{"broker1:9092,broker2:9092", []string{"broker1:9092", "broker2:9092"}},
		{"broker1:9092, broker2:9092 , broker3:9092", []string{"broker1:9092", "broker2:9092", "broker3:9092"}},
		{"", nil},
		{",,,", nil},
		{"broker1:9092,,broker2:9092", []string{"broker1:9092", "broker2:9092"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitBrokers(tt.input)
			if len(got) != len(tt.expected) {
				t.Errorf("splitBrokers(%q) = %v, want %v", tt.input, got, tt.expected)
				return
			}
			for i, v := range got {
				if v != tt.expected[i] {
					t.Errorf("splitBrokers(%q)[%d] = %q, want %q", tt.input, i, v, tt.expected[i])
				}
			}
		})
	}
}

func TestEnvOrDefault(t *testing.T) {
	key := "TEST_ENV_OR_DEFAULT"
	defer os.Unsetenv(key)

	t.Run("env var set", func(t *testing.T) {
		os.Setenv(key, "value")
		if got := envOrDefault(key, "default"); got != "value" {
			t.Errorf("envOrDefault() = %q, want 'value'", got)
		}
	})

	t.Run("env var not set", func(t *testing.T) {
		os.Unsetenv(key)
		if got := envOrDefault(key, "default"); got != "default" {
			t.Errorf("envOrDefault() = %q, want 'default'", got)
		}
	})
}

// ---------------------------------------------------------------------------
// ConfigFromEnv tests
// ---------------------------------------------------------------------------

func TestConfigFromEnv(t *testing.T) {
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
	}

	t.Run("no env vars", func(t *testing.T) {
		clearKafkaEnv()
		_, err := ConfigFromEnv()
		if err == nil {
			t.Error("Expected error when no broker env vars set")
		}
	})

	t.Run("plaintext KAFKA_BROKER_LIST", func(t *testing.T) {
		clearKafkaEnv()
		os.Setenv("KAFKA_BROKER_LIST", "broker1:9092,broker2:9092")
		defer os.Unsetenv("KAFKA_BROKER_LIST")

		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv() error = %v", err)
		}
		if len(cfg.Brokers) != 2 {
			t.Errorf("Brokers count = %d, want 2", len(cfg.Brokers))
		}
		if cfg.SASL != nil {
			t.Error("SASL should be nil for plaintext")
		}
		if cfg.TLS != nil {
			t.Error("TLS should be nil for plaintext")
		}
	})

	t.Run("SASL only", func(t *testing.T) {
		clearKafkaEnv()
		os.Setenv("KAFKA_SASL_BROKER_LIST", "broker:9092")
		os.Setenv("KAFKA_SASL_USR", "user")
		os.Setenv("KAFKA_SASL_PAS", "pass")
		defer func() {
			os.Unsetenv("KAFKA_SASL_BROKER_LIST")
			os.Unsetenv("KAFKA_SASL_USR")
			os.Unsetenv("KAFKA_SASL_PAS")
		}()

		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv() error = %v", err)
		}
		if cfg.SASL == nil {
			t.Fatal("SASL should not be nil")
		}
		if cfg.SASL.Username != "user" {
			t.Errorf("SASL.Username = %q, want 'user'", cfg.SASL.Username)
		}
		if cfg.SASL.Mechanism != "PLAIN" {
			t.Errorf("SASL.Mechanism = %q, want 'PLAIN'", cfg.SASL.Mechanism)
		}
	})

	t.Run("SASL + SSL highest priority", func(t *testing.T) {
		clearKafkaEnv()
		os.Setenv("KAFKA_BROKER_LIST", "plaintext:9092")
		os.Setenv("KAFKA_SASL_SSL_BROKER_LIST", "secure:9093")
		os.Setenv("KAFKA_SASL_SSL_USR", "user")
		os.Setenv("KAFKA_SASL_SSL_PAS", "pass")
		os.Setenv("KAFKA_SASL_SSL_CA", "/ca.pem")
		os.Setenv("KAFKA_SASL_SSL_CERT", "/cert.pem")
		os.Setenv("KAFKA_SASL_SSL_KEY", "/key.pem")
		defer clearKafkaEnv()

		cfg, err := ConfigFromEnv()
		if err != nil {
			t.Fatalf("ConfigFromEnv() error = %v", err)
		}
		if len(cfg.Brokers) != 1 || cfg.Brokers[0] != "secure:9093" {
			t.Errorf("Brokers = %v, want [secure:9093]", cfg.Brokers)
		}
		if cfg.SASL == nil {
			t.Error("SASL should not be nil")
		}
		if cfg.TLS == nil {
			t.Error("TLS should not be nil")
		}
	})
}

// ---------------------------------------------------------------------------
// NewClient tests
// ---------------------------------------------------------------------------

func TestNewClient(t *testing.T) {
	t.Run("with explicit config", func(t *testing.T) {
		cfg := &Config{
			Brokers:  []string{"broker:9092"},
			ClientID: "test-client",
		}
		client, err := NewClient(cfg)
		if err != nil {
			t.Fatalf("NewClient() error = %v", err)
		}
		if client.Config.ClientID != "test-client" {
			t.Errorf("ClientID = %q, want 'test-client'", client.Config.ClientID)
		}
		if client.Config.Compression != CompressionLZ4 {
			t.Errorf("Compression = %d, want %d (LZ4)", client.Config.Compression, CompressionLZ4)
		}
	})

	t.Run("no brokers", func(t *testing.T) {
		_, err := NewClient(&Config{})
		if err == nil || !strings.Contains(err.Error(), "no brokers") {
			t.Errorf("Expected 'no brokers' error, got: %v", err)
		}
	})

	t.Run("default client ID from SERVICE_NAME", func(t *testing.T) {
		os.Setenv("SERVICE_NAME", "my-service")
		defer os.Unsetenv("SERVICE_NAME")

		client, err := NewClient(&Config{Brokers: []string{"broker:9092"}})
		if err != nil {
			t.Fatalf("NewClient() error = %v", err)
		}
		if client.Config.ClientID != "my-service-client" {
			t.Errorf("ClientID = %q, want 'my-service-client'", client.Config.ClientID)
		}
	})
}

// ---------------------------------------------------------------------------
// TLSConfig tests
// ---------------------------------------------------------------------------

func TestTLSConfig_BuildTLSConfig(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		var cfg *TLSConfig
		tlsCfg, err := cfg.BuildTLSConfig()
		if err != nil {
			t.Errorf("error = %v", err)
		}
		if tlsCfg != nil {
			t.Error("should return nil for nil config")
		}
	})

	t.Run("missing CA file", func(t *testing.T) {
		cfg := &TLSConfig{CAFile: "/nonexistent/ca.pem"}
		_, err := cfg.BuildTLSConfig()
		if err == nil {
			t.Error("should error for missing CA file")
		}
	})
}

// ---------------------------------------------------------------------------
// Env detection tests
// ---------------------------------------------------------------------------

func TestHasSASLSSLEnv(t *testing.T) {
	clearAll := func() {
		for _, k := range []string{"KAFKA_SASL_SSL_BROKER_LIST", "KAFKA_SASL_SSL_CA", "KAFKA_SASL_SSL_CERT", "KAFKA_SASL_SSL_KEY", "KAFKA_SASL_SSL_USR", "KAFKA_SASL_SSL_PAS"} {
			os.Unsetenv(k)
		}
	}

	t.Run("all set", func(t *testing.T) {
		clearAll()
		for _, kv := range [][2]string{{"KAFKA_SASL_SSL_BROKER_LIST", "b:9092"}, {"KAFKA_SASL_SSL_CA", "/ca"}, {"KAFKA_SASL_SSL_CERT", "/cert"}, {"KAFKA_SASL_SSL_KEY", "/key"}, {"KAFKA_SASL_SSL_USR", "u"}, {"KAFKA_SASL_SSL_PAS", "p"}} {
			os.Setenv(kv[0], kv[1])
		}
		defer clearAll()
		if !hasSASLSSLEnv() {
			t.Error("want true")
		}
	})

	t.Run("partial", func(t *testing.T) {
		clearAll()
		os.Setenv("KAFKA_SASL_SSL_BROKER_LIST", "b:9092")
		defer clearAll()
		if hasSASLSSLEnv() {
			t.Error("want false")
		}
	})
}

// ---------------------------------------------------------------------------
// Type tests
// ---------------------------------------------------------------------------

func TestDefaultConsumerConfig(t *testing.T) {
	cfg := DefaultConsumerConfig("test-group")
	if cfg.GroupID != "test-group" {
		t.Errorf("GroupID = %q", cfg.GroupID)
	}
	if cfg.SessionTimeout != 30*time.Second {
		t.Errorf("SessionTimeout = %v", cfg.SessionTimeout)
	}
	if !cfg.AutoCommit {
		t.Error("AutoCommit should default to true")
	}
}

func TestCompressionType(t *testing.T) {
	if CompressionNone != 0 || CompressionGZIP != 1 || CompressionSnappy != 2 || CompressionLZ4 != 3 || CompressionZSTD != 4 {
		t.Error("compression type constants mismatch")
	}
}
