// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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

// Package kafka provides Kafka client initialization, producer, and consumer
// abstractions for the fit.go framework. It is the Go implementation modules/kafka/index.ts.
//
// This package defines interfaces (KafkaClient, KafkaProducer, KafkaConsumer)
// that decouple business logic from the concrete Kafka driver. When a real
// driver (sarama, franz-go, confluent-kafka-go) is integrated, it only needs
// to satisfy these interfaces.
//
// Configuration is resolved from environment variables following the same
// precedence as the implementation:
//
// 1. Explicit Config fields (highest priority)
// 2. KAFKA_SASL_SSL_* env vars (SASL + mutual TLS)
// 3. KAFKA_SSL_* env vars (mutual TLS only)
// 4. KAFKA_SASL_* env vars (SASL only)
// 5. KAFKA_BROKER_LIST env var (plaintext)
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/gofynd/fit-go/logging"
)

// ---------------------------------------------------------------------------
// Log-level mapping (mirrors logLevelMapping)
// ---------------------------------------------------------------------------

// LogLevel represents Kafka-client log verbosity.
type LogLevel int

const (
	LogLevelNothing LogLevel = iota
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

// ParseLogLevel converts the LOG_LEVEL env-var value used across Fynd Commerce
// into a Kafka LogLevel. Unknown values map to LogLevelNothing.
func ParseLogLevel(s string) LogLevel {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "ERROR":
		return LogLevelError
	case "WARN":
		return LogLevelWarn
	case "INFO":
		return LogLevelInfo
	case "DEBUG":
		return LogLevelDebug
	default:
		return LogLevelNothing
	}
}

// ---------------------------------------------------------------------------
// Compression codec (interface for LZ4 / Snappy / GZIP / etc.)
// ---------------------------------------------------------------------------

// CompressionType identifies a Kafka message compression algorithm.
type CompressionType int

const (
	CompressionNone   CompressionType = iota
	CompressionGZIP                   // 1
	CompressionSnappy                 // 2
	CompressionLZ4                    // 3
	CompressionZSTD                   // 4
)

// CompressionCodec compresses and decompresses message payloads.
// Register an LZ4 implementation via Config.Compression when the driver is
// wired up. defaults to LZ4 for all produce calls.
type CompressionCodec interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// ---------------------------------------------------------------------------
// SASL configuration
// ---------------------------------------------------------------------------

// SASLConfig holds SASL authentication credentials.
// Mirrors the SASL options used.
type SASLConfig struct {
	// Mechanism is the SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
	// Defaults to "PLAIN" when not set.
	Mechanism string
	Username  string
	Password  string
}

// ---------------------------------------------------------------------------
// TLS configuration
// ---------------------------------------------------------------------------

// TLSConfig holds mutual-TLS file paths for Kafka SSL connections.
// The paths point to PEM-encoded certificate/key files on disk.
type TLSConfig struct {
	CAFile   string // Path to the CA certificate file
	CertFile string // Path to the client certificate file
	KeyFile  string // Path to the client private key file
}

// BuildTLSConfig reads PEM files and returns a *tls.Config ready for use with
// a Kafka driver. It of reading files at init time
// with rejectUnauthorized: false (InsecureSkipVerify in Go).
func (t *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if t == nil {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // matches rejectUnauthorized: false
	}

	// Load CA certificate
	if t.CAFile != "" {
		caPEM, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: failed to read CA file %q: %w", t.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("kafka: CA file %q contains no valid certificates", t.CAFile)
		}
		tlsCfg.RootCAs = pool
	}

	// Load client certificate + key (mutual TLS)
	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: failed to load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// ---------------------------------------------------------------------------
// Client configuration
// ---------------------------------------------------------------------------

// Config holds all parameters needed to create a Kafka client.
// Fields can be set explicitly or resolved from environment variables via
// ConfigFromEnv().
type Config struct {
	// Brokers is the list of Kafka bootstrap broker addresses (host:port).
	Brokers []string

	// ClientID identifies this application to the Kafka cluster.
	// Defaults to "<SERVICE_NAME>-client".
	ClientID string

	// LogLevel controls the verbosity of Kafka-client internal logging.
	// Resolved from the LOG_LEVEL env var by default.
	LogLevel LogLevel

	// SASL holds SASL authentication config. Nil means no SASL.
	SASL *SASLConfig

	// TLS holds mutual-TLS config. Nil means no TLS.
	TLS *TLSConfig

	// Compression selects the default compression for produced messages.
	// defaults to LZ4. The actual codec must be provided by the
	// driver integration layer.
	Compression CompressionType

	// CompressionCodec is an optional codec implementation for the selected
	// compression type. When nil, the driver's built-in codec is used.
	CompressionCodec CompressionCodec

	// Logger is the fit.go logger instance. When nil, a default logger is
	// created during NewClient.
	Logger *logging.Logger
}

// ---------------------------------------------------------------------------
// KafkaClient interface - driver abstraction
// ---------------------------------------------------------------------------

// KafkaClient is the minimal interface a Kafka driver must implement to
// integrate with fit.go. Sarama, franz-go, or confluent-kafka-go can all
// satisfy this contract.
type KafkaClient interface {
	// Producer creates a new producer bound to this client.
	Producer(config ProducerConfig) (KafkaProducer, error)

	// Consumer creates a new consumer bound to this client.
	Consumer(config ConsumerConfig) (KafkaConsumer, error)

	// Close shuts down the client and releases all resources.
	Close() error
}

// KafkaProducer is the interface a driver's producer must implement.
type KafkaProducer interface {
	// Connect establishes the producer connection to the brokers.
	Connect() error

	// Produce sends messages to a single topic.
	Produce(topic string, messages []Message, acks int) error

	// ProduceBatch sends messages to multiple topics in one call.
	ProduceBatch(topicMessages []TopicMessages, acks int) error

	// Close disconnects the producer gracefully.
	Close() error
}

// KafkaConsumer is the interface a driver's consumer must implement.
type KafkaConsumer interface {
	// Connect subscribes to the given topics and starts the consumer.
	Connect(topics []TopicConfig) error

	// Consume processes messages one at a time via the handler.
	Consume(handler MessageHandler, opts ConsumerOptions) error

	// ConsumeBatch processes messages in batches via the handler.
	ConsumeBatch(handler BatchHandler, opts ConsumerOptions) error

	// Close disconnects the consumer gracefully.
	Close() error
}

// ---------------------------------------------------------------------------
// Client (concrete, wraps driver via interface)
// ---------------------------------------------------------------------------

// Client wraps a Kafka driver and holds the resolved configuration. It is the
// Go equivalent of the Kafka instance returned by kafka.init().
type Client struct {
	Config *Config
	Logger *logging.Logger

	// Driver is the underlying KafkaClient implementation. It is nil until a
	// concrete driver is registered. Code that only needs the config (e.g.
	// config validation at startup) can use Client without a driver.
	Driver KafkaClient
}

// NewClient creates a Kafka Client from the given config. If cfg is nil,
// ConfigFromEnv() is called to resolve configuration from environment
// variables. Returns an error when no brokers can be determined.
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		resolved, err := ConfigFromEnv()
		if err != nil {
			return nil, err
		}
		cfg = resolved
	}

	// Default client ID from SERVICE_NAME.
	if cfg.ClientID == "" {
		svc := os.Getenv("SERVICE_NAME")
		if svc == "" {
			svc = "unknown"
		}
		cfg.ClientID = svc + "-client"
	}

	// Default log level from LOG_LEVEL env var.
	if cfg.LogLevel == LogLevelNothing {
		cfg.LogLevel = ParseLogLevel(os.Getenv("LOG_LEVEL"))
	}

	// Default compression to LZ4.
	if cfg.Compression == CompressionNone {
		cfg.Compression = CompressionLZ4
	}

	// Ensure we have at least one broker.
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: no brokers configured; set KAFKA_BROKER_LIST or provide Config.Brokers")
	}

	// Create a logger if none provided.
	logger := cfg.Logger
	if logger == nil {
		l, err := logging.New(logging.Options{Level: "info"})
		if err != nil {
			return nil, fmt.Errorf("kafka: failed to create logger: %w", err)
		}
		logger = l
	}

	logger.Info("kafka: client configured",
		"brokers", strings.Join(cfg.Brokers, ","),
		"clientId", cfg.ClientID,
		"sasl", cfg.SASL != nil,
		"tls", cfg.TLS != nil,
		"compression", cfg.Compression,
	)

	return &Client{
		Config: cfg,
		Logger: logger,
	}, nil
}

// ConfigFromEnv builds a Config entirely from environment variables.
// It follows the same precedence as kafka.init():
//
// 1. KAFKA_SASL_SSL_* (SASL + mutual TLS)
// 2. KAFKA_SSL_* (mutual TLS only)
// 3. KAFKA_SASL_* (SASL only)
// 4. KAFKA_BROKER_LIST (plaintext)
func ConfigFromEnv() (*Config, error) {
	cfg := &Config{}

	// --- Priority 1: SASL + SSL ---
	if hasSASLSSLEnv() {
		cfg.Brokers = splitBrokers(os.Getenv("KAFKA_SASL_SSL_BROKER_LIST"))
		cfg.SASL = &SASLConfig{
			Mechanism: envOrDefault("KAFKA_SASL_SSL_MECHANISM", "PLAIN"),
			Username:  os.Getenv("KAFKA_SASL_SSL_USR"),
			Password:  os.Getenv("KAFKA_SASL_SSL_PAS"),
		}
		cfg.TLS = &TLSConfig{
			CAFile:   os.Getenv("KAFKA_SASL_SSL_CA"),
			CertFile: os.Getenv("KAFKA_SASL_SSL_CERT"),
			KeyFile:  os.Getenv("KAFKA_SASL_SSL_KEY"),
		}
		return cfg, nil
	}

	// --- Priority 2: SSL only ---
	if hasSSLEnv() {
		cfg.Brokers = splitBrokers(os.Getenv("KAFKA_SSL_BROKER_LIST"))
		cfg.TLS = &TLSConfig{
			CAFile:   os.Getenv("KAFKA_SSL_CA"),
			CertFile: os.Getenv("KAFKA_SSL_CERT"),
			KeyFile:  os.Getenv("KAFKA_SSL_KEY"),
		}
		return cfg, nil
	}

	// --- Priority 3: SASL only ---
	if hasSASLEnv() {
		cfg.Brokers = splitBrokers(os.Getenv("KAFKA_SASL_BROKER_LIST"))
		cfg.SASL = &SASLConfig{
			Mechanism: envOrDefault("KAFKA_SASL_MECHANISM", "PLAIN"),
			Username:  os.Getenv("KAFKA_SASL_USR"),
			Password:  os.Getenv("KAFKA_SASL_PAS"),
		}
		return cfg, nil
	}

	// --- Priority 4: Plaintext ---
	if v := os.Getenv("KAFKA_BROKER_LIST"); v != "" {
		cfg.Brokers = splitBrokers(v)
		return cfg, nil
	}

	return nil, fmt.Errorf("kafka: broker environment variables not found; unable to configure Kafka")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func hasSASLSSLEnv() bool {
	return os.Getenv("KAFKA_SASL_SSL_BROKER_LIST") != "" &&
		os.Getenv("KAFKA_SASL_SSL_CA") != "" &&
		os.Getenv("KAFKA_SASL_SSL_CERT") != "" &&
		os.Getenv("KAFKA_SASL_SSL_KEY") != "" &&
		os.Getenv("KAFKA_SASL_SSL_USR") != "" &&
		os.Getenv("KAFKA_SASL_SSL_PAS") != ""
}

func hasSSLEnv() bool {
	return os.Getenv("KAFKA_SSL_BROKER_LIST") != "" &&
		os.Getenv("KAFKA_SSL_CA") != "" &&
		os.Getenv("KAFKA_SSL_CERT") != "" &&
		os.Getenv("KAFKA_SSL_KEY") != ""
}

func hasSASLEnv() bool {
	return os.Getenv("KAFKA_SASL_BROKER_LIST") != "" &&
		os.Getenv("KAFKA_SASL_USR") != "" &&
		os.Getenv("KAFKA_SASL_PAS") != ""
}

func splitBrokers(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			brokers = append(brokers, p)
		}
	}
	return brokers
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
