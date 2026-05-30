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
	"encoding/json"
	"fmt"
	"time"

	gc "github.com/mailgun/groupcache/v2"
)

// ---------------------------------------------------------------------------
// TypedGroup - generic typed wrapper
// ---------------------------------------------------------------------------

// TypedGroup wraps a groupcache.Group with typed get/set operations. This
// avoids the need for callers to marshal/unmarshal manually and provides a
// type-safe API.
//
// Values are stored as JSON-encoded bytes in the cache.
//
// Usage:
//
//	type User struct {
//	 ID string `json:"id"`
//	 Name string `json:"name"`
//	}
//
//	users := groupcache.NewTypedGroup[User](client, "users", 64<<20,
//	 func(ctx context.Context, key string) (User, error) {
//	 return db.GetUser(ctx, key)
//	 },
//	)
//
//	user, err := users.Get(ctx, "user-123")
type TypedGroup[T any] struct {
	group  *gc.Group
	client *Client
	name   string
}

// NewTypedGroup creates a typed wrapper around a groupcache group. The loader
// function is called on cache miss to fetch the value from the data source.
// Values are automatically marshalled to/from JSON for cache storage.
//
// The maxBytes parameter controls the maximum cache size in bytes for this
// group. Values are cached indefinitely (no TTL) by default; use
// NewTypedGroupWithTTL for time-based expiration.
func NewTypedGroup[T any](client *Client, name string, maxBytes int64, loader func(ctx context.Context, key string) (T, error)) *TypedGroup[T] {
	getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Sink) error {
		val, err := loader(ctx, key)
		if err != nil {
			return err
		}
		data, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("groupcache: failed to marshal value for key %q: %w", key, err)
		}
		return dest.SetBytes(data, time.Time{})
	})

	g := client.CreateGroup(GroupConfig{
		Name:     name,
		MaxBytes: maxBytes,
		Getter:   getter,
	})

	return &TypedGroup[T]{
		group:  g,
		client: client,
		name:   name,
	}
}

// NewTypedGroupWithTTL creates a typed group where cached values expire after
// the given duration. On cache miss the loader is called to refresh the value.
func NewTypedGroupWithTTL[T any](client *Client, name string, maxBytes int64, ttl time.Duration, loader func(ctx context.Context, key string) (T, error)) *TypedGroup[T] {
	getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Sink) error {
		val, err := loader(ctx, key)
		if err != nil {
			return err
		}
		data, err := json.Marshal(val)
		if err != nil {
			return fmt.Errorf("groupcache: failed to marshal value for key %q: %w", key, err)
		}
		return dest.SetBytes(data, time.Now().Add(ttl))
	})

	g := client.CreateGroup(GroupConfig{
		Name:     name,
		MaxBytes: maxBytes,
		Getter:   getter,
	})

	return &TypedGroup[T]{
		group:  g,
		client: client,
		name:   name,
	}
}

// Get retrieves a value by key, loading from the data source on cache miss.
// The returned value is deserialized from the cached JSON bytes.
func (g *TypedGroup[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	var dest gc.ByteView

	if err := g.group.Get(ctx, key, gc.ByteViewSink(&dest)); err != nil {
		g.client.metrics.misses.Add(1)
		return zero, err
	}

	g.client.metrics.hits.Add(1)

	var val T
	if err := json.Unmarshal(dest.ByteSlice(), &val); err != nil {
		return zero, fmt.Errorf("groupcache: failed to unmarshal value for key %q: %w", key, err)
	}
	return val, nil
}

// Remove invalidates a cached entry across all peers.
func (g *TypedGroup[T]) Remove(ctx context.Context, key string) error {
	return g.group.Remove(ctx, key)
}

// Set explicitly sets a cached value with the given expiry time.
func (g *TypedGroup[T]) Set(ctx context.Context, key string, value T, expire time.Time) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("groupcache: failed to marshal value for key %q: %w", key, err)
	}
	return g.group.Set(ctx, key, data, expire, false)
}

// Group returns the underlying groupcache.Group for advanced operations.
func (g *TypedGroup[T]) Group() *gc.Group {
	return g.group
}

// Name returns the group name.
func (g *TypedGroup[T]) Name() string {
	return g.name
}
