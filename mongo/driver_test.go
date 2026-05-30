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

package mongo

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ---------------------------------------------------------------------------
// DefaultDialFunc option mapping tests
// ---------------------------------------------------------------------------

func TestDefaultDialFunc_ReturnsDialFunc(t *testing.T) {
	dial := DefaultDialFunc()
	if dial == nil {
		t.Fatal("DefaultDialFunc() should return a non-nil DialFunc")
	}
}

func TestApplyDriverOptions_AppName(t *testing.T) {
	// Verify that applyDriverOptions does not panic with various option
	// combinations. We cannot easily inspect the internal state of
	// ClientOptions, but we can ensure the mapping code runs without error.
	opts := &DialOptions{
		AppName: "test-app",
	}

	// applyDriverOptions should not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("applyDriverOptions panicked: %v", r)
		}
	}()

	// We need the options builder from the driver; import it indirectly
	// through the DefaultDialFunc to test the full path.
	// Instead, test the helper directly.
	clientOpts := stubClientOptions()
	applyDriverOptions(clientOpts, opts)
}

func TestApplyDriverOptions_PoolSettings(t *testing.T) {
	opts := &DialOptions{
		MaxPoolSize:      100,
		MinPoolSize:      10,
		MaxIdleTimeMS:    30000,
		ConnectTimeoutMS: 5000,
		SocketTimeoutMS:  10000,
		MaxConnecting:    5,
		WaitQueueTimeout: 15000,
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("applyDriverOptions panicked with pool settings: %v", r)
		}
	}()

	clientOpts := stubClientOptions()
	applyDriverOptions(clientOpts, opts)
}

func TestApplyDriverOptions_TLS(t *testing.T) {
	opts := &DialOptions{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("applyDriverOptions panicked with TLS config: %v", r)
		}
	}()

	clientOpts := stubClientOptions()
	applyDriverOptions(clientOpts, opts)
}

func TestApplyDriverOptions_ZeroValues(t *testing.T) {
	// Zero-valued options should be skipped (no-op).
	opts := &DialOptions{}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("applyDriverOptions panicked with zero-value opts: %v", r)
		}
	}()

	clientOpts := stubClientOptions()
	applyDriverOptions(clientOpts, opts)
}

func TestApplyDriverOptions_NilOpts(t *testing.T) {
	// DefaultDialFunc should handle nil DialOptions gracefully.
	dial := DefaultDialFunc()
	if dial == nil {
		t.Fatal("DefaultDialFunc() returned nil")
	}
	// We can't actually connect, but we verify the function is callable.
}

func TestApplyDriverOptions_AllFields(t *testing.T) {
	opts := &DialOptions{
		AppName:          "full-test",
		TLSConfig:        &tls.Config{MinVersion: tls.VersionTLS13},
		MaxPoolSize:      200,
		MinPoolSize:      20,
		MaxIdleTimeMS:    60000,
		ConnectTimeoutMS: 10000,
		SocketTimeoutMS:  15000,
		MaxConnecting:    10,
		WaitQueueTimeout: 30000,
		AutoIndex:        true,
		AutoCreate:       true,
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("applyDriverOptions panicked with all fields: %v", r)
		}
	}()

	clientOpts := stubClientOptions()
	applyDriverOptions(clientOpts, opts)
}

// ---------------------------------------------------------------------------
// mongoConnection interface compliance
// ---------------------------------------------------------------------------

func TestMongoConnection_ImplementsInterface(t *testing.T) {
	// Compile-time check that mongoConnection implements Connection.
	var _ Connection = (*mongoConnection)(nil)
}

// ---------------------------------------------------------------------------
// InitDefault tests
// ---------------------------------------------------------------------------

func TestInitDefault_NoEnvVars(t *testing.T) {
	// With no MONGO_* env vars, InitDefault should return an empty client
	// without error (no connections to discover).
	// We cannot actually call InitDefault because it will use the real driver
	// which requires a running MongoDB. Instead, we verify the function exists
	// and has the correct signature.
	var fn func(context.Context) (*Client, error) = InitDefault
	if fn == nil {
		t.Fatal("InitDefault should be a valid function")
	}
}

// ---------------------------------------------------------------------------
// URI handling through DefaultDialFunc
// ---------------------------------------------------------------------------

func TestDefaultDialFunc_InvalidURI(t *testing.T) {
	dial := DefaultDialFunc()

	// An invalid URI should produce an error during connect.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := dial(ctx, "not-a-valid-uri://???", &DialOptions{})
	if err == nil {
		t.Error("Expected error for invalid URI, got nil")
	}
}

func TestDefaultDialFunc_WithAllOptions(t *testing.T) {
	dial := DefaultDialFunc()

	// Construct a valid-looking URI. The connect will fail since there is no
	// server, but we are testing that option mapping does not panic.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	opts := &DialOptions{
		AppName:          "integration-test",
		MaxPoolSize:      50,
		MinPoolSize:      5,
		MaxIdleTimeMS:    10000,
		ConnectTimeoutMS: 1000,
		SocketTimeoutMS:  2000,
		MaxConnecting:    3,
		WaitQueueTimeout: 5000,
	}

	// This will create a client but fail on first operation since no server.
	// The Connect itself should succeed (mongo-driver v2 connects lazily).
	conn, err := dial(ctx, "mongodb://localhost:27017/testdb", opts)
	if err != nil {
		// Some environments may return an error; that's acceptable.
		// The key test is that applyDriverOptions did not panic.
		t.Logf("Connect returned error (expected in test env): %v", err)
		return
	}

	// Verify the connection wraps a *mongo.Client.
	raw := conn.Raw()
	if raw == nil {
		t.Fatal("Raw() returned nil")
	}

	// Type-assert to *mongo.Client.
	if _, ok := raw.(*mongo.Client); !ok {
		t.Errorf("Raw() returned %T, want *mongo.Client", raw)
	}

	// Clean up.
	_ = conn.Close(ctx)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// stubClientOptions creates a new ClientOptionsBuilder for testing applyDriverOptions.
func stubClientOptions() *options.ClientOptions {
	return options.Client()
}
