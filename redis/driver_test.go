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

package redis

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Interface compliance checks
// ---------------------------------------------------------------------------

func TestStandaloneConnection_ImplementsConnection(t *testing.T) {
	var _ Connection = (*standaloneConnection)(nil)
}

func TestClusterConnection_ImplementsConnection(t *testing.T) {
	var _ Connection = (*clusterConnection)(nil)
}

func TestClusterConnection_ImplementsClusterConnection(t *testing.T) {
	var _ ClusterConnection = (*clusterConnection)(nil)
}

func TestSentinelConnection_ImplementsConnection(t *testing.T) {
	var _ Connection = (*sentinelConnection)(nil)
}

// ---------------------------------------------------------------------------
// DefaultDialFunc tests
// ---------------------------------------------------------------------------

func TestDefaultDialFunc_ReturnsDialFunc(t *testing.T) {
	dial := DefaultDialFunc()
	if dial == nil {
		t.Fatal("DefaultDialFunc() should return a non-nil DialFunc")
	}
}

func TestDefaultDialFunc_CreatesStandaloneConnection(t *testing.T) {
	dial := DefaultDialFunc()

	opts := &DialOptions{
		Addr: "localhost:6379",
		Password: "secret",
		Username: "user",
		DB: 2,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultDialFunc() error = %v", err)
	}

	if conn == nil {
		t.Fatal("Connection should not be nil")
	}

	if conn.IsCluster() {
		t.Error("Standalone connection should not be cluster")
	}

	// Verify Raw returns a *goredis.Client.
	raw := conn.Raw()
	client, ok := raw.(*goredis.Client)
	if !ok {
		t.Fatalf("Raw() returned %T, want *goredis.Client", raw)
	}

	// Verify options were applied by inspecting the client's Options.
	redisOpts := client.Options()
	if redisOpts.Addr != "localhost:6379" {
		t.Errorf("Addr = %q, want 'localhost:6379'", redisOpts.Addr)
	}
	if redisOpts.Password != "secret" {
		t.Errorf("Password = %q, want 'secret'", redisOpts.Password)
	}
	if redisOpts.Username != "user" {
		t.Errorf("Username = %q, want 'user'", redisOpts.Username)
	}
	if redisOpts.DB != 2 {
		t.Errorf("DB = %d, want 2", redisOpts.DB)
	}

	_ = conn.Close()
}

func TestDefaultDialFunc_AppliesAllOptions(t *testing.T) {
	dial := DefaultDialFunc()

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
		MinVersion: tls.VersionTLS12,
	}

	opts := &DialOptions{
		Addr: "myhost:6380",
		Password: "pass",
		Username: "admin",
		DB: 3,
		ClientName: "test-client",
		TLSConfig: tlsCfg,
		ConnectTimeout: 5 * time.Second,
		SocketTimeout: 10 * time.Second,
		KeepAlive: 30 * time.Second,
		MaxRetries: 3,
		PoolSize: 50,
		MinIdleConns: 10,
		ReadOnly: false,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultDialFunc() error = %v", err)
	}

	client := conn.Raw().(*goredis.Client)
	redisOpts := client.Options()

	if redisOpts.Addr != "myhost:6380" {
		t.Errorf("Addr = %q, want 'myhost:6380'", redisOpts.Addr)
	}
	if redisOpts.ClientName != "test-client" {
		t.Errorf("ClientName = %q, want 'test-client'", redisOpts.ClientName)
	}
	if redisOpts.TLSConfig == nil {
		t.Error("TLSConfig should be set")
	}
	if redisOpts.DialTimeout != 5*time.Second {
		t.Errorf("DialTimeout = %v, want 5s", redisOpts.DialTimeout)
	}
	if redisOpts.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout = %v, want 10s", redisOpts.ReadTimeout)
	}
	if redisOpts.WriteTimeout != 10*time.Second {
		t.Errorf("WriteTimeout = %v, want 10s", redisOpts.WriteTimeout)
	}
	if redisOpts.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", redisOpts.MaxRetries)
	}
	if redisOpts.PoolSize != 50 {
		t.Errorf("PoolSize = %d, want 50", redisOpts.PoolSize)
	}
	if redisOpts.MinIdleConns != 10 {
		t.Errorf("MinIdleConns = %d, want 10", redisOpts.MinIdleConns)
	}
	if redisOpts.ConnMaxIdleTime != 30*time.Second {
		t.Errorf("ConnMaxIdleTime = %v, want 30s", redisOpts.ConnMaxIdleTime)
	}

	_ = conn.Close()
}

func TestDefaultDialFunc_ZeroValues(t *testing.T) {
	dial := DefaultDialFunc()

	// Zero-valued options (except Addr) should leave go-redis defaults.
	opts := &DialOptions{
		Addr: "localhost:6379",
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultDialFunc() error = %v", err)
	}

	client := conn.Raw().(*goredis.Client)
	redisOpts := client.Options()

	if redisOpts.Password != "" {
		t.Errorf("Password should be empty, got %q", redisOpts.Password)
	}
	if redisOpts.Username != "" {
		t.Errorf("Username should be empty, got %q", redisOpts.Username)
	}
	if redisOpts.DB != 0 {
		t.Errorf("DB should be 0, got %d", redisOpts.DB)
	}

	_ = conn.Close()
}

// ---------------------------------------------------------------------------
// DefaultClusterDialFunc tests
// ---------------------------------------------------------------------------

func TestDefaultClusterDialFunc_ReturnsClusterDialFunc(t *testing.T) {
	dial := DefaultClusterDialFunc()
	if dial == nil {
		t.Fatal("DefaultClusterDialFunc() should return a non-nil ClusterDialFunc")
	}
}

func TestDefaultClusterDialFunc_CreatesClusterConnection(t *testing.T) {
	dial := DefaultClusterDialFunc()

	opts := &ClusterDialOptions{
		Addrs: []string{"node1:6379", "node2:6379", "node3:6379"},
		Password: "cluster-pass",
		Username: "cluster-user",
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultClusterDialFunc() error = %v", err)
	}

	if conn == nil {
		t.Fatal("Connection should not be nil")
	}

	if !conn.IsCluster() {
		t.Error("Cluster connection should report IsCluster=true")
	}

	// Verify Raw returns *goredis.ClusterClient.
	raw := conn.Raw()
	_, ok := raw.(*goredis.ClusterClient)
	if !ok {
		t.Fatalf("Raw() returned %T, want *goredis.ClusterClient", raw)
	}

	_ = conn.Close()
}

func TestDefaultClusterDialFunc_AppliesOptions(t *testing.T) {
	dial := DefaultClusterDialFunc()

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	opts := &ClusterDialOptions{
		Addrs: []string{"h1:6379", "h2:6379"},
		Password: "pass",
		Username: "admin",
		ClientName: "cluster-client",
		TLSConfig: tlsCfg,
		ConnectTimeout: 3 * time.Second,
		SocketTimeout: 5 * time.Second,
		KeepAlive: 15 * time.Second,
		ReadOnly: true,
		PoolSize: 100,
		MinIdleConns: 20,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultClusterDialFunc() error = %v", err)
	}

	// We can verify it's a cluster connection.
	if !conn.IsCluster() {
		t.Error("Expected cluster connection")
	}

	// Verify it implements ClusterConnection.
	cc, ok := conn.(ClusterConnection)
	if !ok {
		t.Fatal("Cluster connection should implement ClusterConnection interface")
	}
	_ = cc // We verified the type assertion.

	_ = conn.Close()
}

// ---------------------------------------------------------------------------
// DefaultSentinelDialFunc tests
// ---------------------------------------------------------------------------

func TestDefaultSentinelDialFunc_ReturnsSentinelDialFunc(t *testing.T) {
	dial := DefaultSentinelDialFunc()
	if dial == nil {
		t.Fatal("DefaultSentinelDialFunc() should return a non-nil SentinelDialFunc")
	}
}

func TestDefaultSentinelDialFunc_CreatesSentinelConnection(t *testing.T) {
	dial := DefaultSentinelDialFunc()

	opts := &SentinelDialOptions{
		MasterName: "mymaster",
		SentinelAddrs: []string{"sentinel1:26379", "sentinel2:26379"},
		Password: "master-pass",
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultSentinelDialFunc() error = %v", err)
	}

	if conn == nil {
		t.Fatal("Connection should not be nil")
	}

	if conn.IsCluster() {
		t.Error("Sentinel connection should not report as cluster")
	}

	// Verify Raw returns *goredis.Client.
	raw := conn.Raw()
	_, ok := raw.(*goredis.Client)
	if !ok {
		t.Fatalf("Raw() returned %T, want *goredis.Client", raw)
	}

	_ = conn.Close()
}

func TestDefaultSentinelDialFunc_AppliesAllOptions(t *testing.T) {
	dial := DefaultSentinelDialFunc()

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	opts := &SentinelDialOptions{
		MasterName: "mymaster",
		SentinelAddrs: []string{"s1:26379", "s2:26379", "s3:26379"},
		Password: "master-pass",
		Username: "master-user",
		SentinelPassword: "sentinel-pass",
		SentinelUsername: "sentinel-user",
		DB: 1,
		ClientName: "sentinel-client",
		TLSConfig: tlsCfg,
		EnableTLSForSentinel: true,
		ConnectTimeout: 3 * time.Second,
		SocketTimeout: 5 * time.Second,
		KeepAlive: 10 * time.Second,
		PoolSize: 25,
		MinIdleConns: 5,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultSentinelDialFunc() error = %v", err)
	}

	if conn == nil {
		t.Fatal("Connection should not be nil")
	}

	// Verify it wraps a go-redis Client (failover client).
	raw := conn.Raw()
	_, ok := raw.(*goredis.Client)
	if !ok {
		t.Fatalf("Raw() returned %T, want *goredis.Client", raw)
	}

	_ = conn.Close()
}

func TestDefaultSentinelDialFunc_ReadOnly(t *testing.T) {
	dial := DefaultSentinelDialFunc()

	opts := &SentinelDialOptions{
		MasterName: "mymaster",
		SentinelAddrs: []string{"s1:26379"},
		ReadOnly: true,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultSentinelDialFunc() error = %v", err)
	}

	if conn == nil {
		t.Fatal("Connection should not be nil")
	}

	_ = conn.Close()
}

// ---------------------------------------------------------------------------
// InitDefault tests
// ---------------------------------------------------------------------------

func TestInitDefault_NoEnvVars(t *testing.T) {
	// Verify the function has the correct signature.
	var fn func(context.Context) (*Client, error) = InitDefault
	if fn == nil {
		t.Fatal("InitDefault should be a valid function")
	}
}

// ---------------------------------------------------------------------------
// Option mapping edge cases
// ---------------------------------------------------------------------------

func TestDefaultDialFunc_DB0NotSkipped(t *testing.T) {
	// DB=0 is the default; verify we don't accidentally set it to something else.
	dial := DefaultDialFunc()

	opts := &DialOptions{
		Addr: "localhost:6379",
		DB: 0,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultDialFunc() error = %v", err)
	}

	client := conn.Raw().(*goredis.Client)
	if client.Options().DB != 0 {
		t.Errorf("DB = %d, want 0", client.Options().DB)
	}

	_ = conn.Close()
}

func TestDefaultClusterDialFunc_ReadOnlyRouting(t *testing.T) {
	dial := DefaultClusterDialFunc()

	opts := &ClusterDialOptions{
		Addrs: []string{"node1:6379"},
		ReadOnly: true,
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultClusterDialFunc() error = %v", err)
	}

	if !conn.IsCluster() {
		t.Error("Should be cluster connection")
	}

	_ = conn.Close()
}

func TestDefaultDialFunc_TLSOnly(t *testing.T) {
	// Test that TLS config alone is applied without other options.
	dial := DefaultDialFunc()

	opts := &DialOptions{
		Addr: "secure-host:6380",
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	conn, err := dial(context.Background(), opts)
	if err != nil {
		t.Fatalf("DefaultDialFunc() error = %v", err)
	}

	client := conn.Raw().(*goredis.Client)
	if client.Options().TLSConfig == nil {
		t.Error("TLSConfig should be set")
	}

	_ = conn.Close()
}

// ---------------------------------------------------------------------------
// Standalone connection Ping/Close (without real server)
// ---------------------------------------------------------------------------

func TestStandaloneConnection_PingWithoutServer(t *testing.T) {
	dial := DefaultDialFunc()

	conn, err := dial(context.Background(), &DialOptions{
		Addr: "localhost:1", // unlikely to have a server here
	})
	if err != nil {
		t.Fatalf("Dial should succeed (lazy connect): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = conn.Ping(ctx)
	if err == nil {
		t.Error("Ping should fail without a running server")
	}

	_ = conn.Close()
}
