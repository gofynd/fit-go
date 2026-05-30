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
	"context"
	"fmt"
	"time"

	gc "github.com/mailgun/groupcache/v2"
)

// ---------------------------------------------------------------------------
// Group management
// ---------------------------------------------------------------------------

// GroupConfig defines a cache group. Groups are the fundamental unit of caching
// in groupcache - each group has its own namespace, size limit, and getter
// function that is called on cache miss to load from the data source.
type GroupConfig struct {
	// Name is the unique cache group name. Must be unique across the
	// application. Examples: "users", "products", "config".
	Name string

	// MaxBytes is the maximum cache size in bytes for this group.
	// This limit applies to the sum of the main cache and hot cache.
	// Example: 64 << 20 for 64 MiB.
	MaxBytes int64

	// Getter is the function called on cache miss to load the value from
	// the data source (database, API, etc.). The function receives the key
	// and must populate the dest Sink with the value.
	//
	// The getter is guaranteed to be called at most once per key across all
	// peers (single-flight deduplication). Other callers wait for the result.
	Getter gc.GetterFunc
}

// CreateGroup creates a new named cache group. Groups are created once and
// shared across the application. If a group with the same name already exists,
// the existing group is returned.
//
// The getter function is called on cache miss:
//
//	client.CreateGroup(GroupConfig{
//	 Name: "users",
//	 MaxBytes: 64 << 20, // 64 MiB
//	 Getter: gc.GetterFunc(func(ctx context.Context, key string, dest gc.Sink) error {
//	 user, err := db.GetUser(ctx, key)
//	 if err != nil {
//	 return err
//	 }
//	 data, _ := json.Marshal(user)
//	 return dest.SetBytes(data, time.Now().Add(5*time.Minute))
//	 }),
//	})
func (c *Client) CreateGroup(cfg GroupConfig) *gc.Group {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return existing group if already created.
	if g, ok := c.groups[cfg.Name]; ok {
		c.logger.Warn("groupcache: group already exists, returning existing",
			"group", cfg.Name,
		)
		return g
	}

	// Wrap the getter to track metrics.
	wrappedGetter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Sink) error {
		c.metrics.loads.Add(1)
		err := cfg.Getter.Get(ctx, key, dest)
		if err != nil {
			c.metrics.errors.Add(1)
		}
		return err
	})

	g := gc.NewGroup(cfg.Name, cfg.MaxBytes, wrappedGetter)
	c.groups[cfg.Name] = g

	c.logger.Info("groupcache: group created",
		"group", cfg.Name,
		"maxBytes", cfg.MaxBytes,
	)

	return g
}

// GetGroup returns an existing group by name, or nil if not found.
func (c *Client) GetGroup(name string) *gc.Group {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.groups[name]
}

// RemoveGroup removes a cache group. This deregisters the group from
// groupcache and removes it from the client's tracking map. Primarily useful
// for testing and cleanup.
func (c *Client) RemoveGroup(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.groups[name]; ok {
		gc.DeregisterGroup(name)
		delete(c.groups, name)
		c.logger.Info("groupcache: group removed", "group", name)
	}
}

// GroupNames returns the names of all registered groups.
func (c *Client) GroupNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.groups))
	for name := range c.groups {
		names = append(names, name)
	}
	return names
}

// GroupStats returns the groupcache stats for the named group, or nil if the
// group does not exist. The returned Stats struct contains Gets, CacheHits,
// PeerLoads, LocalLoads, and other counters maintained by groupcache itself.
func (c *Client) GroupStats(name string) *gc.Stats {
	c.mu.RLock()
	g := c.groups[name]
	c.mu.RUnlock()

	if g == nil {
		return nil
	}
	stats := g.Stats
	return &stats
}

// Get retrieves a value from the named group. It is a convenience wrapper
// around groupcache.Group.Get that also updates fit-go metrics.
func (c *Client) Get(ctx context.Context, group, key string) ([]byte, error) {
	g := c.GetGroup(group)
	if g == nil {
		return nil, fmt.Errorf("groupcache: group %q not found", group)
	}

	var dest gc.ByteView
	if err := g.Get(ctx, key, gc.ByteViewSink(&dest)); err != nil {
		c.metrics.misses.Add(1)
		return nil, err
	}
	c.metrics.hits.Add(1)
	return dest.ByteSlice(), nil
}

// Set explicitly sets a value in the named group's cache. The value will
// expire at the given time. If hotCache is true, the value is also placed
// in the hot cache for non-owner peers.
func (c *Client) Set(ctx context.Context, group, key string, value []byte, expire time.Time, hotCache bool) error {
	g := c.GetGroup(group)
	if g == nil {
		return fmt.Errorf("groupcache: group %q not found", group)
	}
	return g.Set(ctx, key, value, expire, hotCache)
}

// Remove invalidates a key from the named group across all peers.
func (c *Client) Remove(ctx context.Context, group, key string) error {
	g := c.GetGroup(group)
	if g == nil {
		return fmt.Errorf("groupcache: group %q not found", group)
	}
	return g.Remove(ctx, key)
}
