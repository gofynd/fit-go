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

// Package redis provides Redis connection management for the fit.go framework.
// Go implementation of modules/redis/index.ts and redis/index.ts.
//
// The package manages read/write Redis connections per service, supporting both
// standalone Redis and Redis Cluster modes. Connections are auto-discovered from
// environment variables matching the pattern:
//
//	REDIS_{SERVICE}_READ_WRITE - connection string for read-write operations
//	REDIS_{SERVICE}_READ_ONLY - connection string for read-only operations
//
// Connection strings can be provided directly or as GSM secret references when
// DB_CONNECTION_PROVIDER=GSM is set.
//
// # Connection string formats
//
//	redis://host:port/db - standalone Redis
//	redis://user:pass@host:port/db - standalone with auth
//	redis://host1:port1,host2:port2... - Redis Cluster (multiple hosts)
//	redis-sentinel://host:port?master=mymaster - Redis Sentinel
//
// The ?sharded_db=true query parameter also triggers cluster mode.
//
// # Pool tuning environment variables
//
//	REDIS_{SERVICE}_{TYPE}_CONNECTION_TIMEOUT - connect timeout in ms
//	REDIS_{SERVICE}_{TYPE}_SOCKET_TIMEOUT - socket timeout in ms
//	REDIS_{SERVICE}_{TYPE}_KEEP_ALIVE - keep-alive interval in ms
//	REDIS_{SERVICE}_{TYPE}_COMMAND_MAX_RETRIES - go-redis command retry count
//	REDIS_{SERVICE}_{TYPE}_COMMAND_MIN_RETRY_BACKOFF - minimum command retry backoff in ms
//	REDIS_{SERVICE}_{TYPE}_COMMAND_MAX_RETRY_BACKOFF - maximum command retry backoff in ms
//	REDIS_{SERVICE}_{TYPE}_DIALER_RETRIES - connect attempts within one command attempt
//	REDIS_{SERVICE}_{TYPE}_DIALER_RETRY_TIMEOUT - fixed connect-attempt delay in ms
//
// Command retries are go-redis per-command retries with jittered exponential
// backoff. They are not an ioredis-compatible offline queue: commands are not
// accepted into a shared FIFO while disconnected, retry schedules are owned by
// individual callers, and Close does not drain queued commands.
//
// # SSL/TLS configuration
//
//	REDIS_{SERVICE}_SSL_CA or REDIS_SSL_CA - path to CA certificate
//	REDIS_{SERVICE}_SSL_CERT or REDIS_SSL_CERT - path to client certificate
//	REDIS_{SERVICE}_SSL_KEY or REDIS_SSL_KEY - path to client key
//
// # Other
//
//	SERVICE_NAME - used for client name
//	K8S_POD_NAME, K8S_POD_NAMESPACE - used for client name derivation
package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Driver interface contracts
// ---------------------------------------------------------------------------

// Connection represents a single Redis connection (standalone or cluster).
// This interface abstracts the underlying driver (e.g., go-redis) so that the
// framework can initialize, health-check, and shut down connections without
// importing the driver directly.
type Connection interface {
	// Ping verifies the connection is alive.
	Ping(ctx context.Context) error

	// Close terminates the connection and releases resources.
	Close() error

	// Raw returns the underlying driver client.
	// For standalone: *redis.Client; for cluster: *redis.ClusterClient.
	// Callers must type-assert to the concrete type.
	Raw() interface{}

	// IsCluster reports whether this connection is a cluster connection.
	IsCluster() bool
}

// ClusterConnection extends Connection with cluster-specific operations.
// This mirrors the Cluster class redis/index.ts.
type ClusterConnection interface {
	Connection

	// GetNodeSlots returns the slot distribution across cluster nodes.
	// Keys are "host:port", values are slot ranges.
	GetNodeSlots(ctx context.Context) (map[string][][2]int, error)
}

// DialFunc creates a standalone Redis connection.
type DialFunc func(ctx context.Context, opts *DialOptions) (Connection, error)

// AdvancedDialFunc is retained for fork source compatibility.
// Deprecated: use ConfiguredDialFunc.
type AdvancedDialFunc func(ctx context.Context, opts *AdvancedDialOptions) (Connection, error)

// ClusterDialFunc creates a Redis Cluster connection.
type ClusterDialFunc func(ctx context.Context, opts *ClusterDialOptions) (Connection, error)

// AdvancedClusterDialFunc is retained for fork source compatibility.
// Deprecated: use ConfiguredClusterDialFunc.
type AdvancedClusterDialFunc func(ctx context.Context, opts *AdvancedClusterDialOptions) (Connection, error)

// SentinelDialFunc creates a Redis Sentinel connection.
type SentinelDialFunc func(ctx context.Context, opts *SentinelDialOptions) (Connection, error)

// AdvancedSentinelDialFunc is retained for fork source compatibility.
// Deprecated: use ConfiguredSentinelDialFunc.
type AdvancedSentinelDialFunc func(ctx context.Context, opts *AdvancedSentinelDialOptions) (Connection, error)

// DialOptions carries parameters for standalone Redis connections.
type DialOptions struct {
	// Addr is the "host:port" address.
	Addr string

	// Password for AUTH command.
	Password string

	// Username for ACL-based auth (Redis 6+).
	Username string

	// DB is the database number to select.
	DB int

	// ClientName is the connection name visible in CLIENT LIST.
	ClientName string

	// TLSConfig for encrypted connections.
	TLSConfig *tls.Config

	// ConnectTimeout is the dial timeout.
	ConnectTimeout time.Duration

	// SocketTimeout is the read/write timeout.
	SocketTimeout time.Duration

	// KeepAlive is the TCP keep-alive interval.
	KeepAlive time.Duration

	// MaxRetries is the max number of retries per command.
	MaxRetries int

	// PoolSize is the max number of connections in the pool.
	PoolSize int

	// MinIdleConns is the minimum number of idle connections.
	MinIdleConns int

	// ReadOnly indicates this should be a read-only connection.
	ReadOnly bool
}

// AdvancedDialOptions is retained for fork source compatibility.
// Deprecated: use DialSettings.
type AdvancedDialOptions struct {
	DialOptions
	Protocol           RedisProtocol
	MinRetryBackoff    time.Duration
	MaxRetryBackoff    time.Duration
	DialerRetries      int
	DialerRetryTimeout time.Duration
}

// ClusterDialOptions carries parameters for Redis Cluster connections.
type ClusterDialOptions struct {
	// Addrs are the seed "host:port" addresses.
	Addrs []string

	// Password for AUTH command.
	Password string

	// Username for ACL-based auth.
	Username string

	// ClientName is the connection name.
	ClientName string

	// TLSConfig for encrypted connections.
	TLSConfig *tls.Config

	// ConnectTimeout is the dial timeout per node.
	ConnectTimeout time.Duration

	// SocketTimeout is the read/write timeout.
	SocketTimeout time.Duration

	// KeepAlive is the TCP keep-alive interval.
	KeepAlive time.Duration

	// SlotsRefreshInterval is the interval for refreshing cluster slots.
	// Defaults to 5 seconds.
	SlotsRefreshInterval time.Duration

	// ReadOnly sends reads to replica nodes when true.
	ReadOnly bool

	// PoolSize is the max number of connections per node.
	PoolSize int

	// MinIdleConns is the minimum idle connections per node.
	MinIdleConns int
}

// AdvancedClusterDialOptions is retained for fork source compatibility.
// Deprecated: use ClusterDialSettings.
type AdvancedClusterDialOptions struct {
	ClusterDialOptions
	Protocol           RedisProtocol
	MaxRetries         int
	MinRetryBackoff    time.Duration
	MaxRetryBackoff    time.Duration
	DialerRetries      int
	DialerRetryTimeout time.Duration
}

// SentinelDialOptions carries parameters for Redis Sentinel connections.
type SentinelDialOptions struct {
	// MasterName is the name of the sentinel-monitored master.
	MasterName string

	// SentinelAddrs are the "host:port" addresses of sentinel nodes.
	SentinelAddrs []string

	// Password for the Redis master.
	Password string

	// Username for the Redis master (ACL-based auth).
	Username string

	// SentinelPassword for sentinel nodes.
	SentinelPassword string

	// SentinelUsername for sentinel nodes.
	SentinelUsername string

	// DB is the database number.
	DB int

	// ClientName is the connection name.
	ClientName string

	// TLSConfig for encrypted connections.
	TLSConfig *tls.Config

	// EnableTLSForSentinel enables TLS for sentinel connections.
	EnableTLSForSentinel bool

	// ConnectTimeout is the dial timeout.
	ConnectTimeout time.Duration

	// SocketTimeout is the read/write timeout.
	SocketTimeout time.Duration

	// KeepAlive is the TCP keep-alive interval.
	KeepAlive time.Duration

	// ReadOnly routes reads to replicas via READONLY command.
	ReadOnly bool

	// PoolSize is the max number of connections.
	PoolSize int

	// MinIdleConns is the minimum idle connections.
	MinIdleConns int
}

// AdvancedSentinelDialOptions is retained for fork source compatibility.
// Deprecated: use SentinelDialSettings.
type AdvancedSentinelDialOptions struct {
	SentinelDialOptions
	Protocol           RedisProtocol
	MaxRetries         int
	MinRetryBackoff    time.Duration
	MaxRetryBackoff    time.Duration
	DialerRetries      int
	DialerRetryTimeout time.Duration
}

// ---------------------------------------------------------------------------
// Service connections
// ---------------------------------------------------------------------------

// ServiceConnection holds read and write connections for a single service.
type ServiceConnection struct {
	Read  Connection
	Write Connection
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// ConnectionOptions configures the Redis client initialization.
type ConnectionOptions struct {
	// Dial creates a standalone Redis connection. Required.
	Dial DialFunc

	// ClusterDial creates a Redis Cluster connection.
	// Required if any service uses cluster mode.
	ClusterDial ClusterDialFunc

	// SentinelDial creates a Redis Sentinel connection.
	// Required if any service uses sentinel mode.
	SentinelDial SentinelDialFunc

	// DefaultConnectTimeout applies to all connections unless overridden by env.
	// Defaults to 10 seconds.
	DefaultConnectTimeout time.Duration

	// Context for connection establishment.
	Context context.Context
}

// AdvancedConnectionOptions contains post-main Redis protocol and ioredis
// compatibility controls without changing ConnectionOptions' positional layout.
// Deprecated: use CompatibilityOptions.
type AdvancedConnectionOptions struct {
	Dial                  DialFunc
	ClusterDial           ClusterDialFunc
	SentinelDial          SentinelDialFunc
	DefaultConnectTimeout time.Duration
	Context               context.Context
	AdvancedDial          AdvancedDialFunc
	AdvancedClusterDial   AdvancedClusterDialFunc
	AdvancedSentinelDial  AdvancedSentinelDialFunc

	// ProtocolByService explicitly selects RESP2 or RESP3 for named Redis
	// services. Keys are matched case-insensitively against names discovered
	// from REDIS_{SERVICE}_READ_{WRITE|ONLY}. Services not present retain the
	// existing driver default, making this option safe for incremental rollout.
	ProtocolByService map[string]RedisProtocol

	// IORedisCompatibility selects the exact standalone ioredis connection
	// lifecycle for named services. Keys are case-insensitive service names as
	// discovered from REDIS_{SERVICE}_READ_{WRITE|ONLY}. Services not present in
	// this map continue to use the existing go-redis dialers unchanged.
	//
	// The compatibility transport owns its standalone, Sentinel, or Cluster
	// topology instead of falling back to go-redis. This keeps the accepted
	// command FIFO and reconnect budget connection-scoped like ioredis 4.x.
	IORedisCompatibility map[string]IORedisCompatibilityProfile
}

// IORedisCompatibilityProfile identifies a source-derived ioredis wire and
// reconnect profile. It is a string so unsupported values fail with a useful
// configuration error instead of silently falling back to go-redis.
type IORedisCompatibilityProfile string

// RedisProtocol selects the Redis serialization protocol used by the default
// go-redis transport. The zero value deliberately means "driver default" so
// adding this field cannot change existing callers.
type RedisProtocol int

const (
	RedisProtocolDefault RedisProtocol = 0
	RedisProtocolRESP2   RedisProtocol = 2
	RedisProtocolRESP3   RedisProtocol = 3
)

const (
	// IORedisCompatibilityV4 reproduces the shared standalone behavior of
	// ioredis 4.x used by legacy FIT.js: eager first-ready initialization,
	// connection-owned offline FIFO, min(attempt*50ms, 2s) reconnect delay and
	// maxRetriesPerRequest=20. ioredis 4 does not issue CLIENT SETINFO.
	IORedisCompatibilityV4 IORedisCompatibilityProfile = "ioredis-v4"

	// IORedisCompatibilityV5 reproduces the common ioredis 5.x RESP2
	// connection lifecycle: eager first-ready initialization, a
	// connection-owned offline FIFO, min(attempt*50ms, 2s) reconnect delay,
	// maxRetriesPerRequest=20, and best-effort CLIENT SETINFO metadata. The
	// metadata version follows the current compatibility oracle (5.11.1), while
	// application behavior remains defined by this profile rather than a package
	// patch number.
	IORedisCompatibilityV5 IORedisCompatibilityProfile = "ioredis-v5-resp2"

	// IORedisCompatibilityV582 pins ioredis 5.8.2 startup metadata while
	// retaining the ioredis 5 connection lifecycle. Use this only when the
	// deployed legacy lockfile makes the patch-level wire identity observable.
	IORedisCompatibilityV582 IORedisCompatibilityProfile = "ioredis-v5.8.2"
)

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client manages Redis connections for all discovered services. It is the Go
// equivalent of the RedisConnections map.
type Client struct {
	mu       sync.RWMutex
	services map[string]*ServiceConnection
}

// envPattern matches REDIS_{SERVICE}_READ_{WRITE|ONLY} environment variables.
var envPattern = regexp.MustCompile(`^REDIS_(.+)_READ_(WRITE|ONLY)$`)

// Init discovers Redis connection environment variables, resolves connection
// strings, and establishes connections. It mirrors initRedis() in
// /src/redis/index.ts.
func Init(opts ConnectionOptions) (*Client, error) {
	return initConnections(advancedConnectionOptions(opts), true)
}

func advancedConnectionOptions(opts ConnectionOptions) AdvancedConnectionOptions {
	return AdvancedConnectionOptions{
		Dial:                  opts.Dial,
		ClusterDial:           opts.ClusterDial,
		SentinelDial:          opts.SentinelDial,
		DefaultConnectTimeout: opts.DefaultConnectTimeout,
		Context:               opts.Context,
	}
}

// InitAdvanced initializes Redis with post-main protocol and ioredis controls.
// Deprecated: use InitWithCompatibility.
func InitAdvanced(opts AdvancedConnectionOptions) (*Client, error) {
	return initConnections(opts, false)
}

func initConnections(opts AdvancedConnectionOptions, legacy bool) (*Client, error) {
	if opts.Dial == nil && opts.AdvancedDial == nil {
		return nil, fmt.Errorf("redis: DialFunc must be provided")
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.DefaultConnectTimeout == 0 {
		opts.DefaultConnectTimeout = 10 * time.Second
	}

	c := &Client{
		services: make(map[string]*ServiceConnection),
	}

	clientName := getAppName()

	var jobs []connJobEntry

	for _, env := range os.Environ() {
		idx := strings.IndexByte(env, '=')
		if idx < 0 {
			continue
		}
		key := env[:idx]
		value := env[idx+1:]

		matches := envPattern.FindStringSubmatch(key)
		if matches == nil {
			continue
		}

		serviceNameUpper := matches[1]
		connTypeRaw := matches[2]
		connType := "write"
		if strings.EqualFold(connTypeRaw, "ONLY") {
			connType = "read"
		}

		serviceName := strings.ToLower(serviceNameUpper)
		if serviceName == "" || value == "" {
			continue
		}

		jobs = append(jobs, connJobEntry{
			serviceName:      serviceName,
			serviceNameUpper: serviceNameUpper,
			connType:         connType,
			connStringOrRef:  value,
		})
	}

	if len(jobs) == 0 {
		return c, nil
	}

	type connResult struct {
		serviceName string
		connType    string
		conn        Connection
		err         error
	}

	results := make(chan connResult, len(jobs))
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(j connJobEntry) {
			defer wg.Done()

			connString, err := resolveConnectionString(j.connStringOrRef)
			if err != nil {
				results <- connResult{
					serviceName: j.serviceName,
					connType:    j.connType,
					err:         fmt.Errorf("redis: resolve connection for %s_%s: %w", j.serviceName, j.connType, err),
				}
				return
			}

			// Ensure scheme.
			if !strings.Contains(connString, "://") {
				connString = "redis://" + connString
			}

			tlsCfg, err := loadTLSConfig("REDIS", j.serviceNameUpper)
			if err != nil {
				results <- connResult{
					serviceName: j.serviceName,
					connType:    j.connType,
					err:         fmt.Errorf("redis: TLS configuration for %s_%s: %w", j.serviceName, j.connType, err),
				}
				return
			}
			envOpts := getRedisEnvOptions(j.serviceNameUpper, j.connType)
			if legacy {
				// The original Init/InitDefault contract predates protocol,
				// ioredis-compatibility, and retry controls. New environment
				// variables must not alter or invalidate those entry points.
				envOpts = legacyRedisEnvOptions(envOpts)
			}

			conn, err := dialFromURIAdvanced(ctx, connString, j, opts, clientName, tlsCfg, envOpts)
			if err != nil {
				results <- connResult{
					serviceName: j.serviceName,
					connType:    j.connType,
					err:         err,
				}
				return
			}

			// Verify connectivity.
			pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := conn.Ping(pingCtx); err != nil {
				_ = conn.Close()
				results <- connResult{
					serviceName: j.serviceName,
					connType:    j.connType,
					err:         fmt.Errorf("redis: ping failed for %s_%s: %w", j.serviceName, j.connType, err),
				}
				return
			}

			results <- connResult{
				serviceName: j.serviceName,
				connType:    j.connType,
				conn:        conn,
			}
		}(job)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var errs []error
	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
			continue
		}

		c.mu.Lock()
		if c.services[res.serviceName] == nil {
			c.services[res.serviceName] = &ServiceConnection{}
		}
		sc := c.services[res.serviceName]
		if res.connType == "read" {
			sc.Read = res.conn
		} else {
			sc.Write = res.conn
		}
		c.mu.Unlock()
	}

	if len(errs) > 0 {
		_ = c.Close()
		return nil, fmt.Errorf("redis: init failed: %v", errs)
	}

	return c, nil
}

// Service returns the read/write connections for the given service name.
func (c *Client) Service(name string) *ServiceConnection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.services[strings.ToLower(name)]
}

// Services returns a copy of all service connections.
func (c *Client) Services() map[string]*ServiceConnection {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*ServiceConnection, len(c.services))
	for k, v := range c.services {
		result[k] = v
	}
	return result
}

// Ping checks all connections are alive.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for name, sc := range c.services {
		if sc.Read != nil {
			if err := sc.Read.Ping(ctx); err != nil {
				return fmt.Errorf("redis: ping failed for %s_read: %w", name, err)
			}
		}
		if sc.Write != nil {
			if err := sc.Write.Ping(ctx); err != nil {
				return fmt.Errorf("redis: ping failed for %s_write: %w", name, err)
			}
		}
	}
	return nil
}

// Close terminates all connections gracefully.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	for name, sc := range c.services {
		if sc.Read != nil {
			if err := sc.Read.Close(); err != nil {
				errs = append(errs, fmt.Errorf("redis: close %s_read: %w", name, err))
			}
		}
		if sc.Write != nil {
			if err := sc.Write.Close(); err != nil {
				errs = append(errs, fmt.Errorf("redis: close %s_write: %w", name, err))
			}
		}
	}

	c.services = make(map[string]*ServiceConnection)

	if len(errs) > 0 {
		return fmt.Errorf("redis: close errors: %v", errs)
	}
	return nil
}

// HealthCheck returns a function compatible with health.CheckFunc.
func (c *Client) HealthCheck() func() string {
	return func() string {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Ping(ctx); err != nil {
			return err.Error()
		}
		return ""
	}
}

// ---------------------------------------------------------------------------
// URI parsing and dial routing
// ---------------------------------------------------------------------------

// parsedURI holds the components extracted from a Redis connection URI.
type parsedURI struct {
	Scheme   string
	Username string
	Password string
	Hosts    []hostPort
	DB       int
	Options  map[string]string
}

type hostPort struct {
	Host string
	Port string
}

func (h hostPort) Addr() string {
	if h.Port == "" {
		return h.Host + ":6379"
	}
	return h.Host + ":" + h.Port
}

// parseRedisURI parses a Redis connection string into its components.
func parseRedisURI(rawURI string) (*parsedURI, error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("redis: invalid URI: %w", err)
	}

	p := &parsedURI{
		Scheme:  strings.ToLower(u.Scheme),
		Options: make(map[string]string),
	}

	// Auth.
	if u.User != nil {
		p.Username = u.User.Username()
		p.Password, _ = u.User.Password()
	}

	// Hosts - support comma-separated hosts in the host portion.
	hostStr := u.Host
	if hostStr == "" {
		hostStr = "localhost:6379"
	}
	for _, h := range strings.Split(hostStr, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		host, port := h, ""
		if colonIdx := strings.LastIndex(h, ":"); colonIdx >= 0 {
			host = h[:colonIdx]
			port = h[colonIdx+1:]
		}
		p.Hosts = append(p.Hosts, hostPort{Host: host, Port: port})
	}

	// Database.
	dbPath := strings.TrimPrefix(u.Path, "/")
	if dbPath != "" {
		if db, err := strconv.Atoi(dbPath); err == nil {
			p.DB = db
		}
	}

	// Query parameters.
	for k, v := range u.Query() {
		if len(v) > 0 {
			p.Options[strings.ToLower(k)] = v[0]
		}
	}

	return p, nil
}

type connJobEntry struct {
	serviceName      string
	serviceNameUpper string
	connType         string
	connStringOrRef  string
}

// envPoolOpts holds pool settings read from environment variables.
type envPoolOpts struct {
	ConnectTimeout     time.Duration
	SocketTimeout      time.Duration
	KeepAlive          time.Duration
	MaxRetries         int
	MinRetryBackoff    time.Duration
	MaxRetryBackoff    time.Duration
	DialerRetries      int
	DialerRetryTimeout time.Duration
}

// dialFromURI parses the connection string and routes to the appropriate dial
// function (standalone, cluster, or sentinel).
func dialFromURI(
	ctx context.Context,
	connString string,
	job connJobEntry,
	opts ConnectionOptions,
	clientName string,
	tlsCfg *tls.Config,
	envOpts envPoolOpts,
) (Connection, error) {
	return dialFromURIAdvanced(ctx, connString, job, advancedConnectionOptions(opts), clientName, tlsCfg, envOpts)
}

func dialFromURIAdvanced(
	ctx context.Context,
	connString string,
	job connJobEntry,
	opts AdvancedConnectionOptions,
	clientName string,
	tlsCfg *tls.Config,
	envOpts envPoolOpts,
) (Connection, error) {
	parsed, err := parseRedisURI(connString)
	if err != nil {
		return nil, fmt.Errorf("redis: parse URI for %s_%s: %w", job.serviceName, job.connType, err)
	}
	tlsCfg, err = applyRedisURITLS(parsed, tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("redis: TLS options for %s_%s: %w", job.serviceName, job.connType, err)
	}

	connectTimeout := opts.DefaultConnectTimeout
	if envOpts.ConnectTimeout > 0 {
		connectTimeout = envOpts.ConnectTimeout
	}

	isReadOnly := job.connType == "read"
	compatibilityProfile, compatibilityEnabled := ioredisCompatibilityProfile(opts, job.serviceName)
	protocol, err := redisProtocolForService(opts, job.serviceName)
	if err != nil {
		return nil, fmt.Errorf("redis: protocol for %s_%s: %w", job.serviceName, job.connType, err)
	}
	if compatibilityEnabled && protocol == RedisProtocolRESP3 {
		return nil, fmt.Errorf("redis: %s compatibility for %s_%s requires RESP2", compatibilityProfile, job.serviceName, job.connType)
	}

	// Route: Sentinel
	if parsed.Scheme == "redis-sentinel" {
		if compatibilityEnabled {
			masterName := parsed.Options["master"]
			if masterName == "" {
				return nil, fmt.Errorf("redis: master name missing for sentinel connection %s_%s", job.serviceName, job.connType)
			}
			if isReadOnly {
				return nil, fmt.Errorf("redis: %s compatibility for %s_%s does not support Sentinel read replicas", compatibilityProfile, job.serviceName, job.connType)
			}
			sentinelAddrs := make([]string, 0, len(parsed.Hosts))
			for _, host := range parsed.Hosts {
				sentinelAddrs = append(sentinelAddrs, host.Addr())
			}
			sentinelUsername := parsed.Username
			if value, ok := parsed.Options["sentinelusername"]; ok {
				sentinelUsername = value
			}
			sentinelPassword := parsed.Password
			if value, ok := parsed.Options["sentinelpassword"]; ok {
				sentinelPassword = value
			}
			return dialIORedisCompatibleSentinel(ctx, compatibilityProfile, ioredisSentinelOptions{
				SentinelAddrs:    sentinelAddrs,
				MasterName:       masterName,
				Username:         parsed.Username,
				Password:         parsed.Password,
				SentinelUsername: sentinelUsername,
				SentinelPassword: sentinelPassword,
				DB:               parsed.DB,
				ConnectionName:   clientName,
				TLSConfig:        tlsCfg,
				ConnectTimeout:   connectTimeout,
				SocketTimeout:    envOpts.SocketTimeout,
				KeepAlive:        envOpts.KeepAlive,
			})
		}
		if opts.SentinelDial == nil && opts.AdvancedSentinelDial == nil {
			return nil, fmt.Errorf("redis: sentinel connection required for %s_%s but SentinelDial not provided", job.serviceName, job.connType)
		}
		masterName := parsed.Options["master"]
		if masterName == "" {
			return nil, fmt.Errorf("redis: master name missing for sentinel connection %s_%s", job.serviceName, job.connType)
		}

		sentOpts := &AdvancedSentinelDialOptions{
			SentinelDialOptions: SentinelDialOptions{
				MasterName:     masterName,
				SentinelAddrs:  make([]string, 0, len(parsed.Hosts)),
				Password:       parsed.Password,
				Username:       parsed.Username,
				ClientName:     clientName,
				TLSConfig:      tlsCfg,
				ConnectTimeout: connectTimeout,
				SocketTimeout:  envOpts.SocketTimeout,
				KeepAlive:      envOpts.KeepAlive,
				DB:             parsed.DB,
				ReadOnly:       isReadOnly,
			},
			MaxRetries:         envOpts.MaxRetries,
			MinRetryBackoff:    envOpts.MinRetryBackoff,
			MaxRetryBackoff:    envOpts.MaxRetryBackoff,
			DialerRetries:      envOpts.DialerRetries,
			DialerRetryTimeout: envOpts.DialerRetryTimeout,
			Protocol:           protocol,
		}

		for _, h := range parsed.Hosts {
			sentOpts.SentinelAddrs = append(sentOpts.SentinelAddrs, h.Addr())
		}

		// Sentinel-specific auth overrides from query params.
		if v, ok := parsed.Options["sentinelusername"]; ok {
			sentOpts.SentinelUsername = v
		} else {
			sentOpts.SentinelUsername = parsed.Username
		}
		if v, ok := parsed.Options["sentinelpassword"]; ok {
			sentOpts.SentinelPassword = v
		} else {
			sentOpts.SentinelPassword = parsed.Password
		}

		if tlsCfg != nil {
			sentOpts.EnableTLSForSentinel = true
		}

		if opts.AdvancedSentinelDial != nil {
			return opts.AdvancedSentinelDial(ctx, sentOpts)
		}
		if advancedSentinelDialControlsSet(sentOpts) {
			return nil, fmt.Errorf("redis: advanced sentinel controls require AdvancedSentinelDial")
		}
		return opts.SentinelDial(ctx, &sentOpts.SentinelDialOptions)
	}

	// Route: Cluster (multiple hosts or sharded_db=true).
	isCluster := len(parsed.Hosts) > 1 || parsed.Options["sharded_db"] == "true"
	if isCluster {
		if compatibilityEnabled {
			if isReadOnly {
				return nil, fmt.Errorf("redis: %s compatibility for %s_%s does not support Cluster replica reads", compatibilityProfile, job.serviceName, job.connType)
			}
			clusterAddrs := make([]string, 0, len(parsed.Hosts))
			for _, host := range parsed.Hosts {
				clusterAddrs = append(clusterAddrs, host.Addr())
			}
			return dialIORedisCompatibleCluster(ctx, compatibilityProfile, ioredisClusterOptions{
				SeedAddrs:      clusterAddrs,
				Username:       parsed.Username,
				Password:       parsed.Password,
				DB:             parsed.DB,
				ConnectionName: clientName,
				TLSConfig:      tlsCfg,
				ConnectTimeout: connectTimeout,
				SocketTimeout:  envOpts.SocketTimeout,
				KeepAlive:      envOpts.KeepAlive,
			})
		}
		if opts.ClusterDial == nil && opts.AdvancedClusterDial == nil {
			return nil, fmt.Errorf("redis: cluster connection required for %s_%s but ClusterDial not provided", job.serviceName, job.connType)
		}

		clusterOpts := &AdvancedClusterDialOptions{
			ClusterDialOptions: ClusterDialOptions{
				Addrs:                make([]string, 0, len(parsed.Hosts)),
				Password:             parsed.Password,
				Username:             parsed.Username,
				ClientName:           clientName,
				TLSConfig:            tlsCfg,
				ConnectTimeout:       connectTimeout,
				SocketTimeout:        envOpts.SocketTimeout,
				KeepAlive:            envOpts.KeepAlive,
				SlotsRefreshInterval: 5 * time.Second,
				ReadOnly:             isReadOnly,
			},
			Protocol:           protocol,
			MaxRetries:         envOpts.MaxRetries,
			MinRetryBackoff:    envOpts.MinRetryBackoff,
			MaxRetryBackoff:    envOpts.MaxRetryBackoff,
			DialerRetries:      envOpts.DialerRetries,
			DialerRetryTimeout: envOpts.DialerRetryTimeout,
		}

		for _, h := range parsed.Hosts {
			clusterOpts.Addrs = append(clusterOpts.Addrs, h.Addr())
		}

		if opts.AdvancedClusterDial != nil {
			return opts.AdvancedClusterDial(ctx, clusterOpts)
		}
		if advancedClusterDialControlsSet(clusterOpts) {
			return nil, fmt.Errorf("redis: advanced cluster controls require AdvancedClusterDial")
		}
		return opts.ClusterDial(ctx, &clusterOpts.ClusterDialOptions)
	}

	// Route: Standalone.
	addr := "localhost:6379"
	if len(parsed.Hosts) > 0 {
		addr = parsed.Hosts[0].Addr()
	}
	if compatibilityEnabled {
		return dialIORedisCompatibleStandalone(ctx, compatibilityProfile, IORedisRESPOptions{
			Addr:           addr,
			Username:       parsed.Username,
			Password:       parsed.Password,
			DB:             parsed.DB,
			ConnectionName: clientName,
			TLSConfig:      tlsCfg,
			ConnectTimeout: connectTimeout,
			SocketTimeout:  envOpts.SocketTimeout,
			KeepAlive:      envOpts.KeepAlive,
		})
	}

	dialOpts := &AdvancedDialOptions{
		DialOptions: DialOptions{
			Addr:           addr,
			Password:       parsed.Password,
			Username:       parsed.Username,
			DB:             parsed.DB,
			ClientName:     clientName,
			TLSConfig:      tlsCfg,
			ConnectTimeout: connectTimeout,
			SocketTimeout:  envOpts.SocketTimeout,
			KeepAlive:      envOpts.KeepAlive,
			MaxRetries:     envOpts.MaxRetries,
			ReadOnly:       isReadOnly,
		},
		Protocol:           protocol,
		MinRetryBackoff:    envOpts.MinRetryBackoff,
		MaxRetryBackoff:    envOpts.MaxRetryBackoff,
		DialerRetries:      envOpts.DialerRetries,
		DialerRetryTimeout: envOpts.DialerRetryTimeout,
	}

	if opts.AdvancedDial != nil {
		return opts.AdvancedDial(ctx, dialOpts)
	}
	if advancedDialControlsSet(dialOpts) {
		return nil, fmt.Errorf("redis: advanced standalone controls require AdvancedDial")
	}
	return opts.Dial(ctx, &dialOpts.DialOptions)
}

// applyRedisURITLS ensures a connection string that explicitly requests TLS
// can never fall through to a plaintext dial. Certificate environment
// variables still take precedence; when they are absent, rediss:// and
// tls=true/ssl=true use the system root pool with hostname verification.
func applyRedisURITLS(parsed *parsedURI, configured *tls.Config) (*tls.Config, error) {
	if parsed == nil {
		return configured, nil
	}

	requested := parsed.Scheme == "rediss"
	for _, key := range []string{"tls", "ssl"} {
		value, present := parsed.Options[key]
		if !present {
			continue
		}
		enabled, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid %s query value %q: %w", key, value, err)
		}
		requested = requested || enabled
	}

	if !requested || configured != nil {
		return configured, nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}, nil
}

func ioredisCompatibilityProfile(opts AdvancedConnectionOptions, serviceName string) (IORedisCompatibilityProfile, bool) {
	for configuredService, profile := range opts.IORedisCompatibility {
		if strings.EqualFold(strings.TrimSpace(configuredService), serviceName) {
			return profile, true
		}
	}
	return "", false
}

func redisProtocolForService(opts AdvancedConnectionOptions, serviceName string) (RedisProtocol, error) {
	var selected RedisProtocol
	matched := false
	for configuredService, protocol := range opts.ProtocolByService {
		if !strings.EqualFold(strings.TrimSpace(configuredService), serviceName) {
			continue
		}
		if err := validateRedisProtocol(protocol); err != nil {
			return RedisProtocolDefault, err
		}
		if matched && selected != protocol {
			return RedisProtocolDefault, fmt.Errorf("conflicting Redis protocols %d and %d", selected, protocol)
		}
		selected = protocol
		matched = true
	}
	return selected, nil
}

func advancedDialControlsSet(opts *AdvancedDialOptions) bool {
	return opts.Protocol != RedisProtocolDefault || opts.MinRetryBackoff != 0 ||
		opts.MaxRetryBackoff != 0 || opts.DialerRetries != 0 || opts.DialerRetryTimeout != 0
}

func advancedClusterDialControlsSet(opts *AdvancedClusterDialOptions) bool {
	return opts.Protocol != RedisProtocolDefault || opts.MaxRetries != 0 ||
		opts.MinRetryBackoff != 0 || opts.MaxRetryBackoff != 0 ||
		opts.DialerRetries != 0 || opts.DialerRetryTimeout != 0
}

func advancedSentinelDialControlsSet(opts *AdvancedSentinelDialOptions) bool {
	return opts.Protocol != RedisProtocolDefault || opts.MaxRetries != 0 ||
		opts.MinRetryBackoff != 0 || opts.MaxRetryBackoff != 0 ||
		opts.DialerRetries != 0 || opts.DialerRetryTimeout != 0
}

func validateRedisProtocol(protocol RedisProtocol) error {
	switch protocol {
	case RedisProtocolDefault, RedisProtocolRESP2, RedisProtocolRESP3:
		return nil
	default:
		return fmt.Errorf("unsupported Redis protocol %d", protocol)
	}
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// getRedisEnvOptions reads pool tuning from environment variables.
func getRedisEnvOptions(serviceNameUpper, connType string) envPoolOpts {
	envConnType := "READ_WRITE"
	if connType == "read" {
		envConnType = "READ_ONLY"
	}
	prefix := fmt.Sprintf("REDIS_%s_%s_", serviceNameUpper, envConnType)

	var opts envPoolOpts

	if v := os.Getenv(prefix + "CONNECTION_TIMEOUT"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			opts.ConnectTimeout = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "SOCKET_TIMEOUT"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			opts.SocketTimeout = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "KEEP_ALIVE"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil {
			opts.KeepAlive = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "COMMAND_MAX_RETRIES"); v != "" {
		if retries, err := strconv.Atoi(v); err == nil && retries > 0 {
			opts.MaxRetries = retries
		}
	}
	if v := os.Getenv(prefix + "COMMAND_MIN_RETRY_BACKOFF"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			opts.MinRetryBackoff = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "COMMAND_MAX_RETRY_BACKOFF"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			opts.MaxRetryBackoff = time.Duration(ms) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "DIALER_RETRIES"); v != "" {
		if retries, err := strconv.Atoi(v); err == nil && retries > 0 {
			opts.DialerRetries = retries
		}
	}
	if v := os.Getenv(prefix + "DIALER_RETRY_TIMEOUT"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			opts.DialerRetryTimeout = time.Duration(ms) * time.Millisecond
		}
	}

	return opts
}

func legacyRedisEnvOptions(opts envPoolOpts) envPoolOpts {
	return envPoolOpts{
		ConnectTimeout: opts.ConnectTimeout,
		SocketTimeout:  opts.SocketTimeout,
		KeepAlive:      opts.KeepAlive,
	}
}

// loadTLSConfig builds a *tls.Config from SSL environment variables.
func loadTLSConfig(dbType, serviceNameUpper string) (*tls.Config, error) {
	caPath := envWithFallback(
		fmt.Sprintf("%s_%s_SSL_CA", dbType, serviceNameUpper),
		fmt.Sprintf("%s_SSL_CA", dbType),
	)
	certPath := envWithFallback(
		fmt.Sprintf("%s_%s_SSL_CERT", dbType, serviceNameUpper),
		fmt.Sprintf("%s_SSL_CERT", dbType),
	)
	keyPath := envWithFallback(
		fmt.Sprintf("%s_%s_SSL_KEY", dbType, serviceNameUpper),
		fmt.Sprintf("%s_SSL_KEY", dbType),
	)

	configuredValues := 0
	for _, value := range []string{caPath, certPath, keyPath} {
		if value != "" {
			configuredValues++
		}
	}
	if configuredValues == 0 {
		return nil, nil
	}
	if configuredValues != 3 {
		return nil, fmt.Errorf("CA, certificate, and key must all be configured")
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load client certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		return nil, fmt.Errorf("CA certificate contains no valid PEM certificates")
	}

	return &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		// Keep certificate-chain and hostname verification enabled. Dialers fill
		// ServerName from the selected Redis address when it is not configured.
		InsecureSkipVerify: false,
		MinVersion:         tls.VersionTLS12,
	}, nil
}

// resolveConnectionString resolves a value as either a direct connection string
// or a GSM secret reference.
func resolveConnectionString(value string) (string, error) {
	if strings.EqualFold(os.Getenv("DB_CONNECTION_PROVIDER"), "GSM") {
		gsmMu.RLock()
		resolver := gsmResolver
		gsmMu.RUnlock()

		if resolver == nil {
			return "", fmt.Errorf("redis: GSM resolver not configured; call redis.SetGSMResolver() before Init")
		}

		version := os.Getenv("DB_CONNECTION_SECRET_VERSION")
		if version == "" {
			version = "latest"
		}
		return resolver(value, version)
	}
	return value, nil
}

// GSMResolverFunc fetches a secret by name and version from Google Secret Manager.
type GSMResolverFunc func(secretName, version string) (string, error)

var (
	gsmMu       sync.RWMutex
	gsmResolver GSMResolverFunc
)

// SetGSMResolver configures the function used to fetch secrets from GSM.
// Must be called before Init when DB_CONNECTION_PROVIDER=GSM.
// Typically: redis.SetGSMResolver(config.GetSecretFromGSM)
func SetGSMResolver(fn GSMResolverFunc) {
	gsmMu.Lock()
	defer gsmMu.Unlock()
	gsmResolver = fn
}

// getAppName derives a client name for Redis connections.
func getAppName() string {
	podName := os.Getenv("K8S_POD_NAME")
	namespace := os.Getenv("K8S_POD_NAMESPACE")
	serviceName := os.Getenv("SERVICE_NAME")

	deploymentName := getDeploymentName(podName)
	if deploymentName == "" {
		deploymentName = serviceName
	}

	if namespace != "" && namespace != "default" {
		if deploymentName != "" {
			return namespace + "-" + deploymentName
		}
		return namespace
	}
	return deploymentName
}

func getDeploymentName(podName string) string {
	if podName == "" {
		return ""
	}
	if idx := strings.Index(podName, "dply"); idx >= 0 {
		return podName[:idx+4]
	}
	if idx := strings.Index(podName, "cron"); idx >= 0 {
		return podName[:idx+4]
	}
	return ""
}

func envWithFallback(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}
