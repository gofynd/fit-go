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

package groupcache

import (
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Cache metrics
// ---------------------------------------------------------------------------

// Metrics tracks cache operation counters. All operations are atomic and
// safe for concurrent use. These metrics supplement the per-group Stats
// maintained by groupcache itself, providing application-level counters
// across all groups.
type Metrics struct {
	hits atomic.Int64
	misses atomic.Int64
	loads atomic.Int64
	peerLoads atomic.Int64
	errors atomic.Int64
}

// MetricsSnapshot is a point-in-time snapshot of cache metrics, suitable for
// JSON serialization and health check reporting.
type MetricsSnapshot struct {
	Hits int64 `json:"hits"`
	Misses int64 `json:"misses"`
	Loads int64 `json:"loads"`
	PeerLoads int64 `json:"peerLoads"`
	Errors int64 `json:"errors"`
	HitRate float64 `json:"hitRate"`
}

// NewMetrics creates a new Metrics instance with all counters at zero.
func NewMetrics() *Metrics {
	return &Metrics{}
}

// RecordHit increments the cache hit counter.
func (m *Metrics) RecordHit() {
	m.hits.Add(1)
}

// RecordMiss increments the cache miss counter.
func (m *Metrics) RecordMiss() {
	m.misses.Add(1)
}

// RecordLoad increments the data load counter (getter invocation).
func (m *Metrics) RecordLoad() {
	m.loads.Add(1)
}

// RecordPeerLoad increments the peer load counter (value fetched from peer).
func (m *Metrics) RecordPeerLoad() {
	m.peerLoads.Add(1)
}

// RecordError increments the error counter.
func (m *Metrics) RecordError() {
	m.errors.Add(1)
}

// Snapshot returns a point-in-time snapshot of all metrics, including the
// calculated hit rate. The hit rate is computed as hits / (hits + misses);
// if there are no gets, it is 0.
func (m *Metrics) Snapshot() MetricsSnapshot {
	hits := m.hits.Load()
	misses := m.misses.Load()

	var hitRate float64
	total := hits + misses
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return MetricsSnapshot{
		Hits: hits,
		Misses: misses,
		Loads: m.loads.Load(),
		PeerLoads: m.peerLoads.Load(),
		Errors: m.errors.Load(),
		HitRate: hitRate,
	}
}

// Reset sets all counters back to zero. Primarily useful for testing.
func (m *Metrics) Reset() {
	m.hits.Store(0)
	m.misses.Store(0)
	m.loads.Store(0)
	m.peerLoads.Store(0)
	m.errors.Store(0)
}
