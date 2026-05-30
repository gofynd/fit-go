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

package groupcache

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Metrics tests (no groupcache pool needed)
// ---------------------------------------------------------------------------

func TestMetricsSnapshot(t *testing.T) {
	m := NewMetrics()

	// Initially all zeros.
	snap := m.Snapshot()
	if snap.Hits != 0 || snap.Misses != 0 || snap.Loads != 0 || snap.HitRate != 0 {
		t.Fatalf("expected zero snapshot, got %+v", snap)
	}

	// Record some operations.
	m.RecordHit()
	m.RecordHit()
	m.RecordHit()
	m.RecordMiss()
	m.RecordLoad()
	m.RecordPeerLoad()
	m.RecordError()

	snap = m.Snapshot()
	if snap.Hits != 3 {
		t.Errorf("expected 3 hits, got %d", snap.Hits)
	}
	if snap.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", snap.Misses)
	}
	if snap.Loads != 1 {
		t.Errorf("expected 1 load, got %d", snap.Loads)
	}
	if snap.PeerLoads != 1 {
		t.Errorf("expected 1 peer load, got %d", snap.PeerLoads)
	}
	if snap.Errors != 1 {
		t.Errorf("expected 1 error, got %d", snap.Errors)
	}

	// Hit rate should be 3/4 = 0.75.
	expectedRate := 0.75
	if snap.HitRate != expectedRate {
		t.Errorf("expected hit rate %f, got %f", expectedRate, snap.HitRate)
	}
}

func TestMetricsReset(t *testing.T) {
	m := NewMetrics()
	m.RecordHit()
	m.RecordMiss()
	m.RecordLoad()

	m.Reset()
	snap := m.Snapshot()
	if snap.Hits != 0 || snap.Misses != 0 || snap.Loads != 0 {
		t.Fatalf("expected zero snapshot after reset, got %+v", snap)
	}
}

func TestMetricsSnapshotZeroDivision(t *testing.T) {
	m := NewMetrics()
	snap := m.Snapshot()
	if snap.HitRate != 0 {
		t.Errorf("expected 0 hit rate with no gets, got %f", snap.HitRate)
	}
}

// ---------------------------------------------------------------------------
// Environment variable resolution tests
// ---------------------------------------------------------------------------

func TestResolveSelf_Defaults(t *testing.T) {
	// Clear env vars.
	t.Setenv("GROUPCACHE_SELF", "")
	t.Setenv("GROUPCACHE_PORT", "")
	t.Setenv("POD_IP", "")

	self, port := resolveSelf(Config{})
	if port != "8081" {
		t.Errorf("expected default port 8081, got %s", port)
	}
	if self != "http://127.0.0.1:8081" {
		t.Errorf("expected default self http://127.0.0.1:8081, got %s", self)
	}
}

func TestResolveSelf_FromEnv(t *testing.T) {
	t.Setenv("POD_IP", "10.0.0.5")
	t.Setenv("GROUPCACHE_PORT", "9090")
	t.Setenv("GROUPCACHE_SELF", "")

	self, port := resolveSelf(Config{})
	if port != "9090" {
		t.Errorf("expected port 9090, got %s", port)
	}
	if self != "http://10.0.0.5:9090" {
		t.Errorf("expected self http://10.0.0.5:9090, got %s", self)
	}
}

func TestResolveSelf_ExplicitConfig(t *testing.T) {
	self, port := resolveSelf(Config{
		Self: "http://myhost:7070",
		Port: "7070",
	})
	if port != "7070" {
		t.Errorf("expected port 7070, got %s", port)
	}
	if self != "http://myhost:7070" {
		t.Errorf("expected self http://myhost:7070, got %s", self)
	}
}

func TestResolvePeers_SelfAlwaysIncluded(t *testing.T) {
	t.Setenv("GROUPCACHE_PEERS", "")
	peers := resolvePeers(Config{}, "http://127.0.0.1:8081")
	if len(peers) != 1 || peers[0] != "http://127.0.0.1:8081" {
		t.Errorf("expected [http://127.0.0.1:8081], got %v", peers)
	}
}

func TestResolvePeers_FromEnv(t *testing.T) {
	t.Setenv("GROUPCACHE_PEERS", "http://10.0.0.1:8081, http://10.0.0.2:8081")
	peers := resolvePeers(Config{}, "http://10.0.0.3:8081")
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d: %v", len(peers), peers)
	}
	if peers[0] != "http://10.0.0.3:8081" {
		t.Errorf("expected self as first peer, got %s", peers[0])
	}
}

func TestResolvePeers_ExplicitIncludesSelf(t *testing.T) {
	t.Setenv("GROUPCACHE_PEERS", "")
	self := "http://10.0.0.1:8081"
	peers := resolvePeers(Config{
		Peers: []string{self, "http://10.0.0.2:8081"},
	}, self)

	// Self should not be duplicated.
	count := 0
	for _, p := range peers {
		if p == self {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected self to appear once, appeared %d times in %v", count, peers)
	}
}

func TestIsEnabled_EnvVar(t *testing.T) {
	t.Setenv("GROUPCACHE_ENABLED", "true")
	if !isEnabled(Config{}) {
		t.Error("expected enabled with GROUPCACHE_ENABLED=true")
	}

	t.Setenv("GROUPCACHE_ENABLED", "false")
	if isEnabled(Config{}) {
		t.Error("expected disabled with GROUPCACHE_ENABLED=false")
	}
}

func TestIsEnabled_ExplicitConfig(t *testing.T) {
	t.Setenv("GROUPCACHE_ENABLED", "false")
	if !isEnabled(Config{Self: "http://localhost:8081"}) {
		t.Error("expected enabled with explicit Self")
	}
}

// ---------------------------------------------------------------------------
// K8s config resolution tests
// ---------------------------------------------------------------------------

func TestResolveK8sConfig_Defaults(t *testing.T) {
	t.Setenv("GROUPCACHE_K8S_NAMESPACE", "")
	t.Setenv("GROUPCACHE_K8S_SERVICE_NAME", "")
	t.Setenv("GROUPCACHE_K8S_PORT", "")

	cfg := resolveK8sConfig(K8sDiscoveryConfig{})
	if cfg.PortName != "groupcache" {
		t.Errorf("expected default port name 'groupcache', got %q", cfg.PortName)
	}
}

func TestResolveK8sConfig_FromEnv(t *testing.T) {
	t.Setenv("GROUPCACHE_K8S_NAMESPACE", "prod")
	t.Setenv("GROUPCACHE_K8S_SERVICE_NAME", "my-service")
	t.Setenv("GROUPCACHE_K8S_PORT", "http-cache")

	cfg := resolveK8sConfig(K8sDiscoveryConfig{})
	if cfg.Namespace != "prod" {
		t.Errorf("expected namespace 'prod', got %q", cfg.Namespace)
	}
	if cfg.ServiceName != "my-service" {
		t.Errorf("expected service 'my-service', got %q", cfg.ServiceName)
	}
	if cfg.PortName != "http-cache" {
		t.Errorf("expected port 'http-cache', got %q", cfg.PortName)
	}
}

// ---------------------------------------------------------------------------
// extractPeers tests
// ---------------------------------------------------------------------------

func TestExtractPeers(t *testing.T) {
	endpoints := k8sEndpoints{
		Subsets: []k8sSubset{
			{
				Addresses: []k8sAddress{
					{IP: "10.0.0.1"},
					{IP: "10.0.0.2"},
					{IP: "10.0.0.3"},
				},
				Ports: []k8sPort{
					{Name: "groupcache", Port: 8081, Protocol: "TCP"},
					{Name: "http", Port: 8080, Protocol: "TCP"},
				},
			},
		},
	}

	peers := extractPeers(endpoints, "groupcache")
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d: %v", len(peers), peers)
	}
	if peers[0] != "http://10.0.0.1:8081" {
		t.Errorf("expected http://10.0.0.1:8081, got %s", peers[0])
	}
}

func TestExtractPeers_FallbackToFirstPort(t *testing.T) {
	endpoints := k8sEndpoints{
		Subsets: []k8sSubset{
			{
				Addresses: []k8sAddress{{IP: "10.0.0.1"}},
				Ports:     []k8sPort{{Name: "http", Port: 8080}},
			},
		},
	}

	peers := extractPeers(endpoints, "nonexistent")
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0] != "http://10.0.0.1:8080" {
		t.Errorf("expected http://10.0.0.1:8080, got %s", peers[0])
	}
}

func TestExtractPeers_EmptySubsets(t *testing.T) {
	endpoints := k8sEndpoints{Subsets: nil}
	peers := extractPeers(endpoints, "groupcache")
	if len(peers) != 0 {
		t.Errorf("expected 0 peers, got %d", len(peers))
	}
}

func TestExtractPeers_PortByNumber(t *testing.T) {
	endpoints := k8sEndpoints{
		Subsets: []k8sSubset{
			{
				Addresses: []k8sAddress{{IP: "10.0.0.1"}},
				Ports:     []k8sPort{{Name: "cache", Port: 9090}},
			},
		},
	}

	peers := extractPeers(endpoints, "9090")
	if len(peers) != 1 || peers[0] != "http://10.0.0.1:9090" {
		t.Errorf("expected http://10.0.0.1:9090, got %v", peers)
	}
}

// ---------------------------------------------------------------------------
// Health check tests
// ---------------------------------------------------------------------------

func TestShouldSkip(t *testing.T) {
	t.Setenv("SKIP_HEALTH_CHECK_GROUPCACHE", "true")
	if !shouldSkip("SKIP_HEALTH_CHECK_GROUPCACHE") {
		t.Error("expected skip when env var is true")
	}

	t.Setenv("SKIP_HEALTH_CHECK_GROUPCACHE", "false")
	if shouldSkip("SKIP_HEALTH_CHECK_GROUPCACHE") {
		t.Error("expected no skip when env var is false")
	}

	t.Setenv("SKIP_HEALTH_CHECK_GROUPCACHE", "")
	if shouldSkip("SKIP_HEALTH_CHECK_GROUPCACHE") {
		t.Error("expected no skip when env var is empty")
	}
}
