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

// driver.go provides the default Redis driver integration using the go-redis/v9
// package. It implements the Connection and ClusterConnection interfaces defined
// in client.go and provides convenience functions for initializing Redis
// connections with sensible defaults.
//
// This is the Go equivalent of the ioredis connection calls in
// /src/redis/index.ts.
package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

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
		if opts.MinRetryBackoff > 0 {
			redisOpts.MinRetryBackoff = opts.MinRetryBackoff
		}
		if opts.MaxRetryBackoff > 0 {
			redisOpts.MaxRetryBackoff = opts.MaxRetryBackoff
		}
		if opts.DialerRetries > 0 {
			redisOpts.DialerRetries = opts.DialerRetries
		}
		if opts.DialerRetryTimeout > 0 {
			redisOpts.DialerRetryTimeout = opts.DialerRetryTimeout
		}
		if opts.PoolSize > 0 {
			redisOpts.PoolSize = opts.PoolSize
		}
		if opts.MinIdleConns > 0 {
			redisOpts.MinIdleConns = opts.MinIdleConns
		}

		client := goredis.NewClient(redisOpts)

		// Some managed/proxied Redis deployments reject the CLIENT command, so
		// go-redis's CLIENT SETNAME (from ClientName) / SETINFO (identity) handshake
		// fails the very first command — surfacing as "ERR unknown command 'client'
		// ... 'setname'". Legacy ioredis set no client name and disabled the lib
		// handshake, so it never hit this.
		//
		// We probe whenever go-redis WOULD send a CLIENT command — i.e. a client
		// name is set, or identity (SETINFO) is enabled, which is the default. So
		// this synchronous one-shot PING runs on essentially every dial, not only
		// against proxies: that is the cost of detecting the rejection up-front (the
		// alternative is the handshake failing on the first real command in
		// production). On that specific rejection we transparently rebuild without
		// the client name and with identity disabled. The cluster and sentinel dial
		// funcs below apply the same fallback.
		if redisOpts.ClientName != "" || !redisOpts.DisableIdentity {
			if clientHandshakeRejected(ctx, client) {
				_ = client.Close()
				redisOpts.ClientName = ""
				redisOpts.DisableIdentity = true
				client = goredis.NewClient(redisOpts)
			}
		}

		attachTracingHook(client)
		return &standaloneConnection{client: client}, nil
	}
}

// isClientCommandUnsupported reports whether err is the server rejecting go-redis's
// CLIENT SETNAME/SETINFO handshake because it does not support the CLIENT command
// (e.g. a restricted Redis-compatible proxy). Matched on the redis-server error
// text "ERR unknown command 'client' ...". Returns false for nil and for any
// other error (network, auth, a different unknown command), so the fallback only
// triggers for this specific, recoverable case.
func isClientCommandUnsupported(err error) bool {
	if err == nil {
		return false
	}
	// Anchor on "client" being the REJECTED command (quoted right after "unknown
	// command"), not merely present somewhere — otherwise a different unknown
	// command whose args happen to include "client" would false-positive and
	// wrongly strip the client name. Redis quotes with backticks or single quotes
	// depending on version; the message is lower-cased above.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "unknown command `client`") ||
		strings.Contains(msg, "unknown command 'client'") {
		return true
	}
	// Redis < 7.2 supports CLIENT but not the SETINFO subcommand go-redis sends for
	// lib identity; it rejects with "unknown subcommand ... 'setinfo'" (SETNAME is
	// also subcommand-gated on some proxies). Same recoverable case — rebuild
	// without the identity handshake — so match it too.
	if strings.Contains(msg, "unknown subcommand") &&
		(strings.Contains(msg, "setinfo") || strings.Contains(msg, "setname")) {
		return true
	}
	return false
}

// pinger is the subset of a go-redis client (standalone, cluster, failover) used
// to probe the CLIENT handshake.
type pinger interface {
	Ping(context.Context) *goredis.StatusCmd
}

// clientHandshakeRejected probes c with a short one-shot PING and reports whether
// the server rejected go-redis's CLIENT SETNAME/SETINFO handshake — i.e. the
// caller should rebuild the client without the client name and with identity
// disabled. Call only when a CLIENT command would actually be sent (ClientName
// set or identity enabled). Any non-rejection ping error (network, auth, …) is
// treated as "not rejected" and left for the caller's health check to surface.
func clientHandshakeRejected(ctx context.Context, c pinger) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return isClientCommandUnsupported(c.Ping(probeCtx).Err())
}

// ---------------------------------------------------------------------------
// DefaultClusterDialFunc - cluster
// ---------------------------------------------------------------------------

// DefaultClusterDialFunc returns a ClusterDialFunc for Redis Cluster connections
// using go-redis/v9. It maps the framework's ClusterDialOptions to go-redis
// ClusterOptions. Like the standalone path, it probes for and recovers from a
// proxy that rejects the CLIENT SETNAME/SETINFO handshake.
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
		if opts.MaxRetries > 0 {
			clusterOpts.MaxRetries = opts.MaxRetries
		}
		if opts.MinRetryBackoff > 0 {
			clusterOpts.MinRetryBackoff = opts.MinRetryBackoff
		}
		if opts.MaxRetryBackoff > 0 {
			clusterOpts.MaxRetryBackoff = opts.MaxRetryBackoff
		}
		if opts.DialerRetries > 0 {
			clusterOpts.DialerRetries = opts.DialerRetries
		}
		if opts.DialerRetryTimeout > 0 {
			clusterOpts.DialerRetryTimeout = opts.DialerRetryTimeout
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

		// Recover from a proxy that rejects the CLIENT handshake (see the
		// standalone path for the rationale).
		if clusterOpts.ClientName != "" || !clusterOpts.DisableIdentity {
			if clientHandshakeRejected(ctx, client) {
				_ = client.Close()
				clusterOpts.ClientName = ""
				clusterOpts.DisableIdentity = true
				client = goredis.NewClusterClient(clusterOpts)
			}
		}

		attachTracingHook(client)
		return &clusterConnection{client: client}, nil
	}
}

// ---------------------------------------------------------------------------
// DefaultSentinelDialFunc - sentinel
// ---------------------------------------------------------------------------

// DefaultSentinelDialFunc returns a SentinelDialFunc for Redis Sentinel
// connections using go-redis/v9. It maps the framework's SentinelDialOptions to
// go-redis FailoverOptions. Like the standalone path, it probes for and recovers
// from a proxy that rejects the CLIENT SETNAME/SETINFO handshake.
func DefaultSentinelDialFunc() SentinelDialFunc {
	return func(ctx context.Context, opts *SentinelDialOptions) (Connection, error) {
		failoverOpts := &goredis.FailoverOptions{
			MasterName:    opts.MasterName,
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
		if opts.MaxRetries > 0 {
			failoverOpts.MaxRetries = opts.MaxRetries
		}
		if opts.MinRetryBackoff > 0 {
			failoverOpts.MinRetryBackoff = opts.MinRetryBackoff
		}
		if opts.MaxRetryBackoff > 0 {
			failoverOpts.MaxRetryBackoff = opts.MaxRetryBackoff
		}
		if opts.DialerRetries > 0 {
			failoverOpts.DialerRetries = opts.DialerRetries
		}
		if opts.DialerRetryTimeout > 0 {
			failoverOpts.DialerRetryTimeout = opts.DialerRetryTimeout
		}
		if opts.PoolSize > 0 {
			failoverOpts.PoolSize = opts.PoolSize
		}
		if opts.MinIdleConns > 0 {
			failoverOpts.MinIdleConns = opts.MinIdleConns
		}

		// Note: go-redis FailoverOptions does not support ReadOnly directly; reads
		// to replicas are left to the application to route. Both modes use a
		// standard failover client.
		client := goredis.NewFailoverClient(failoverOpts)

		// Recover from a proxy that rejects the CLIENT handshake (see the
		// standalone path for the rationale).
		if failoverOpts.ClientName != "" || !failoverOpts.DisableIdentity {
			if clientHandshakeRejected(ctx, client) {
				_ = client.Close()
				failoverOpts.ClientName = ""
				failoverOpts.DisableIdentity = true
				client = goredis.NewFailoverClient(failoverOpts)
			}
		}

		attachTracingHook(client)
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
		Dial:         DefaultDialFunc(),
		ClusterDial:  DefaultClusterDialFunc(),
		SentinelDial: DefaultSentinelDialFunc(),
		Context:      ctx,
	})
}
