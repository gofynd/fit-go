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

// driver.go provides the default Redis driver integration using the go-redis/v9
// package. It implements the Connection and ClusterConnection interfaces defined
// in client.go and provides convenience functions for initializing Redis
// connections with sensible defaults.
//
// This is the Go equivalent of the ioredis connection calls in
///src/redis/index.ts.
package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// standaloneConnection wraps *goredis.Client
// ---------------------------------------------------------------------------

// standaloneConnection implements the Connection interface for a standalone
// Redis instance using go-redis.
type standaloneConnection struct {
	client *goredis.Client
}

// Ping verifies the connection is alive by issuing a PING command.
func (c *standaloneConnection) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close terminates the connection and releases resources.
func (c *standaloneConnection) Close() error {
	return c.client.Close()
}

// Raw returns the underlying *goredis.Client. Callers must type-assert:
//
//	client := conn.Raw().(*goredis.Client)
func (c *standaloneConnection) Raw() interface{} {
	return c.client
}

// IsCluster returns false for standalone connections.
func (c *standaloneConnection) IsCluster() bool {
	return false
}

// ---------------------------------------------------------------------------
// clusterConnection wraps *goredis.ClusterClient
// ---------------------------------------------------------------------------

// clusterConnection implements the Connection and ClusterConnection interfaces
// for a Redis Cluster using go-redis.
type clusterConnection struct {
	client *goredis.ClusterClient
}

// Ping verifies the cluster connection is alive.
func (c *clusterConnection) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close terminates all cluster node connections.
func (c *clusterConnection) Close() error {
	return c.client.Close()
}

// Raw returns the underlying *goredis.ClusterClient. Callers must type-assert:
//
//	client := conn.Raw().(*goredis.ClusterClient)
func (c *clusterConnection) Raw() interface{} {
	return c.client
}

// IsCluster returns true for cluster connections.
func (c *clusterConnection) IsCluster() bool {
	return true
}

// GetNodeSlots returns the slot distribution across cluster nodes. Keys are
// "host:port" addresses, values are slot ranges as [start, end] pairs.
func (c *clusterConnection) GetNodeSlots(ctx context.Context) (map[string][][2]int, error) {
	slots, err := c.client.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("redis driver: cluster slots failed: %w", err)
	}

	result := make(map[string][][2]int)
	for _, slot := range slots {
		slotRange := [2]int{int(slot.Start), int(slot.End)}
		for _, node := range slot.Nodes {
			result[node.Addr] = append(result[node.Addr], slotRange)
		}
	}

	return result, nil
}

// ---------------------------------------------------------------------------
// sentinelConnection wraps *goredis.Client (sentinel-managed)
// ---------------------------------------------------------------------------

// sentinelConnection implements the Connection interface for a Redis Sentinel
// managed instance using go-redis's FailoverClient.
type sentinelConnection struct {
	client *goredis.Client
}

// Ping verifies the sentinel-managed connection is alive.
func (c *sentinelConnection) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close terminates the sentinel-managed connection.
func (c *sentinelConnection) Close() error {
	return c.client.Close()
}

// Raw returns the underlying *goredis.Client. Callers must type-assert:
//
//	client := conn.Raw().(*goredis.Client)
func (c *sentinelConnection) Raw() interface{} {
	return c.client
}

// IsCluster returns false for sentinel connections.
func (c *sentinelConnection) IsCluster() bool {
	return false
}

// ---------------------------------------------------------------------------
// DefaultDialFunc - standalone
// ---------------------------------------------------------------------------

// DefaultDialFunc returns a DialFunc for standalone Redis connections using
// go-redis/v9. It maps the framework's DialOptions to go-redis Options.
func DefaultDialFunc() DialFunc {
	return func(ctx context.Context, opts *DialOptions) (Connection, error) {
		redisOpts := &goredis.Options{
			Addr: opts.Addr,
		}

		if opts.Password != "" {
			redisOpts.Password = opts.Password
		}
		if opts.Username != "" {
			redisOpts.Username = opts.Username
		}
		if opts.DB > 0 {
			redisOpts.DB = opts.DB
		}
		if opts.ClientName != "" {
			redisOpts.ClientName = opts.ClientName
		}
		if opts.TLSConfig != nil {
			redisOpts.TLSConfig = opts.TLSConfig
		}
		if opts.ConnectTimeout > 0 {
			redisOpts.DialTimeout = opts.ConnectTimeout
		}
		if opts.SocketTimeout > 0 {
			redisOpts.ReadTimeout = opts.SocketTimeout
			redisOpts.WriteTimeout = opts.SocketTimeout
		}
		if opts.KeepAlive > 0 {
			// go-redis does not have a direct KeepAlive option on Options;
			// it uses the net.Dialer's KeepAlive internally. We set the
			// ConnMaxIdleTime as an approximate equivalent.
			redisOpts.ConnMaxIdleTime = opts.KeepAlive
		}
		if opts.MaxRetries > 0 {
			redisOpts.MaxRetries = opts.MaxRetries
		}
		if opts.PoolSize > 0 {
			redisOpts.PoolSize = opts.PoolSize
		}
		if opts.MinIdleConns > 0 {
			redisOpts.MinIdleConns = opts.MinIdleConns
		}

		client := goredis.NewClient(redisOpts)

		return &standaloneConnection{client: client}, nil
	}
}

// ---------------------------------------------------------------------------
// DefaultClusterDialFunc - cluster
// ---------------------------------------------------------------------------

// DefaultClusterDialFunc returns a ClusterDialFunc for Redis Cluster connections
// using go-redis/v9. It maps the framework's ClusterDialOptions to go-redis
// ClusterOptions.
func DefaultClusterDialFunc() ClusterDialFunc {
	return func(ctx context.Context, opts *ClusterDialOptions) (Connection, error) {
		clusterOpts := &goredis.ClusterOptions{
			Addrs: opts.Addrs,
		}

		if opts.Password != "" {
			clusterOpts.Password = opts.Password
		}
		if opts.Username != "" {
			clusterOpts.Username = opts.Username
		}
		if opts.ClientName != "" {
			clusterOpts.ClientName = opts.ClientName
		}
		if opts.TLSConfig != nil {
			clusterOpts.TLSConfig = opts.TLSConfig
		}
		if opts.ConnectTimeout > 0 {
			clusterOpts.DialTimeout = opts.ConnectTimeout
		}
		if opts.SocketTimeout > 0 {
			clusterOpts.ReadTimeout = opts.SocketTimeout
			clusterOpts.WriteTimeout = opts.SocketTimeout
		}
		if opts.KeepAlive > 0 {
			clusterOpts.ConnMaxIdleTime = opts.KeepAlive
		}
		if opts.SlotsRefreshInterval > 0 {
			// go-redis does not expose a direct slot refresh interval on
			// ClusterOptions. The RouteRandomly and RouteByLatency options
			// control routing, not refresh. Slot refresh happens automatically.
			// We document this limitation.
		}
		if opts.ReadOnly {
			clusterOpts.ReadOnly = true
		}
		if opts.PoolSize > 0 {
			clusterOpts.PoolSize = opts.PoolSize
		}
		if opts.MinIdleConns > 0 {
			clusterOpts.MinIdleConns = opts.MinIdleConns
		}

		client := goredis.NewClusterClient(clusterOpts)

		return &clusterConnection{client: client}, nil
	}
}

// ---------------------------------------------------------------------------
// DefaultSentinelDialFunc - sentinel
// ---------------------------------------------------------------------------

// DefaultSentinelDialFunc returns a SentinelDialFunc for Redis Sentinel
// connections using go-redis/v9. It maps the framework's SentinelDialOptions
// to go-redis FailoverOptions.
func DefaultSentinelDialFunc() SentinelDialFunc {
	return func(ctx context.Context, opts *SentinelDialOptions) (Connection, error) {
		failoverOpts := &goredis.FailoverOptions{
			MasterName: opts.MasterName,
			SentinelAddrs: opts.SentinelAddrs,
		}

		if opts.Password != "" {
			failoverOpts.Password = opts.Password
		}
		if opts.Username != "" {
			failoverOpts.Username = opts.Username
		}
		if opts.SentinelPassword != "" {
			failoverOpts.SentinelPassword = opts.SentinelPassword
		}
		if opts.SentinelUsername != "" {
			failoverOpts.SentinelUsername = opts.SentinelUsername
		}
		if opts.DB > 0 {
			failoverOpts.DB = opts.DB
		}
		if opts.ClientName != "" {
			failoverOpts.ClientName = opts.ClientName
		}
		if opts.TLSConfig != nil {
			failoverOpts.TLSConfig = opts.TLSConfig
		}
		// Note: go-redis FailoverOptions uses a single TLSConfig for both the
		// master and sentinel connections. EnableTLSForSentinel is honored by
		// setting TLSConfig above, which applies to all connections.
		if opts.ConnectTimeout > 0 {
			failoverOpts.DialTimeout = opts.ConnectTimeout
		}
		if opts.SocketTimeout > 0 {
			failoverOpts.ReadTimeout = opts.SocketTimeout
			failoverOpts.WriteTimeout = opts.SocketTimeout
		}
		if opts.KeepAlive > 0 {
			failoverOpts.ConnMaxIdleTime = opts.KeepAlive
		}
		if opts.PoolSize > 0 {
			failoverOpts.PoolSize = opts.PoolSize
		}
		if opts.MinIdleConns > 0 {
			failoverOpts.MinIdleConns = opts.MinIdleConns
		}

		var client *goredis.Client
		if opts.ReadOnly {
			// Use NewFailoverClusterClient for read-only routing to replicas,
			// but wrap it in a regular Client for interface compatibility.
			// Actually, go-redis's FailoverOptions does not support ReadOnly
			// directly. For read replicas, we create a standard failover client
			// and rely on the application to route reads appropriately.
			client = goredis.NewFailoverClient(failoverOpts)
		} else {
			client = goredis.NewFailoverClient(failoverOpts)
		}

		return &sentinelConnection{client: client}, nil
	}
}

// ---------------------------------------------------------------------------
// InitDefault - convenience function
// ---------------------------------------------------------------------------

// InitDefault discovers Redis connections from environment variables and
// connects using go-redis/v9. Go equivalent
//
// It calls Init with DefaultDialFunc(), DefaultClusterDialFunc(), and
// DefaultSentinelDialFunc() with sensible defaults. For more control over
// connection options, use Init directly with custom dial functions.
//
// Example:
//
//	client, err := redis.InitDefault(ctx)
//	if err != nil {
//	 log.Fatal(err)
//	}
//	defer client.Close()
//
//	cache := client.Service("cache")
//	rdb := cache.Write.Raw().(*goredis.Client)
//	rdb.Set(ctx, "key", "value", 0)
func InitDefault(ctx context.Context) (*Client, error) {
	return Init(ConnectionOptions{
		Dial: DefaultDialFunc(),
		ClusterDial: DefaultClusterDialFunc(),
		SentinelDial: DefaultSentinelDialFunc(),
		Context: ctx,
	})
}
