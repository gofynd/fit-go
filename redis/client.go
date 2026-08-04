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

// ClusterDialFunc creates a Redis Cluster connection.
type ClusterDialFunc func(ctx context.Context, opts *ClusterDialOptions) (Connection, error)

// SentinelDialFunc creates a Redis Sentinel connection.
type SentinelDialFunc func(ctx context.Context, opts *SentinelDialOptions) (Connection, error)

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

	// MinRetryBackoff is go-redis's minimum jittered exponential command retry
	// backoff. A zero value leaves the go-redis default unchanged.
	MinRetryBackoff time.Duration

	// MaxRetryBackoff is go-redis's maximum jittered exponential command retry
	// backoff. A zero value leaves the go-redis default unchanged.
	MaxRetryBackoff time.Duration

	// DialerRetries is the number of connection attempts made while acquiring a
	// connection for one command attempt. It is distinct from MaxRetries.
	DialerRetries int

	// DialerRetryTimeout is the fixed delay between connection attempts within
	// one command attempt.
	DialerRetryTimeout time.Duration

	// PoolSize is the max number of connections in the pool.
	PoolSize int

	// MinIdleConns is the minimum number of idle connections.
	MinIdleConns int

	// ReadOnly indicates this should be a read-only connection.
	ReadOnly bool
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

	// MaxRetries is the max number of go-redis command/redirect retries.
	MaxRetries int

	// MinRetryBackoff is the minimum jittered exponential retry backoff.
	MinRetryBackoff time.Duration

	// MaxRetryBackoff is the maximum jittered exponential retry backoff.
	MaxRetryBackoff time.Duration

	// DialerRetries is the number of connection attempts within one retry.
	DialerRetries int

	// DialerRetryTimeout is the fixed delay between connection attempts.
	DialerRetryTimeout time.Duration

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

	// MaxRetries is the max number of go-redis command retries.
	MaxRetries int

	// MinRetryBackoff is the minimum jittered exponential command retry backoff.
	MinRetryBackoff time.Duration

	// MaxRetryBackoff is the maximum jittered exponential command retry backoff.
	MaxRetryBackoff time.Duration

	// DialerRetries is the number of connection attempts within one command retry.
	DialerRetries int

	// DialerRetryTimeout is the fixed delay between connection attempts.
	DialerRetryTimeout time.Duration

	// ReadOnly routes reads to replicas via READONLY command.
	ReadOnly bool

	// PoolSize is the max number of connections.
	PoolSize int

	// MinIdleConns is the minimum idle connections.
	MinIdleConns int
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
	if opts.Dial == nil {
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

			tlsCfg := loadTLSConfig("REDIS", j.serviceNameUpper)
			envOpts := getRedisEnvOptions(j.serviceNameUpper, j.connType)

			conn, err := dialFromURI(ctx, connString, j, opts, clientName, tlsCfg, envOpts)
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
	parsed, err := parseRedisURI(connString)
	if err != nil {
		return nil, fmt.Errorf("redis: parse URI for %s_%s: %w", job.serviceName, job.connType, err)
	}

	connectTimeout := opts.DefaultConnectTimeout
	if envOpts.ConnectTimeout > 0 {
		connectTimeout = envOpts.ConnectTimeout
	}

	isReadOnly := job.connType == "read"

	// Route: Sentinel
	if parsed.Scheme == "redis-sentinel" {
		if opts.SentinelDial == nil {
			return nil, fmt.Errorf("redis: sentinel connection required for %s_%s but SentinelDial not provided", job.serviceName, job.connType)
		}
		masterName := parsed.Options["master"]
		if masterName == "" {
			return nil, fmt.Errorf("redis: master name missing for sentinel connection %s_%s", job.serviceName, job.connType)
		}

		sentOpts := &SentinelDialOptions{
			MasterName:         masterName,
			SentinelAddrs:      make([]string, 0, len(parsed.Hosts)),
			Password:           parsed.Password,
			Username:           parsed.Username,
			ClientName:         clientName,
			TLSConfig:          tlsCfg,
			ConnectTimeout:     connectTimeout,
			SocketTimeout:      envOpts.SocketTimeout,
			KeepAlive:          envOpts.KeepAlive,
			MaxRetries:         envOpts.MaxRetries,
			MinRetryBackoff:    envOpts.MinRetryBackoff,
			MaxRetryBackoff:    envOpts.MaxRetryBackoff,
			DialerRetries:      envOpts.DialerRetries,
			DialerRetryTimeout: envOpts.DialerRetryTimeout,
			DB:                 parsed.DB,
			ReadOnly:           isReadOnly,
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

		return opts.SentinelDial(ctx, sentOpts)
	}

	// Route: Cluster (multiple hosts or sharded_db=true).
	isCluster := len(parsed.Hosts) > 1 || parsed.Options["sharded_db"] == "true"
	if isCluster {
		if opts.ClusterDial == nil {
			return nil, fmt.Errorf("redis: cluster connection required for %s_%s but ClusterDial not provided", job.serviceName, job.connType)
		}

		clusterOpts := &ClusterDialOptions{
			Addrs:                make([]string, 0, len(parsed.Hosts)),
			Password:             parsed.Password,
			Username:             parsed.Username,
			ClientName:           clientName,
			TLSConfig:            tlsCfg,
			ConnectTimeout:       connectTimeout,
			SocketTimeout:        envOpts.SocketTimeout,
			KeepAlive:            envOpts.KeepAlive,
			MaxRetries:           envOpts.MaxRetries,
			MinRetryBackoff:      envOpts.MinRetryBackoff,
			MaxRetryBackoff:      envOpts.MaxRetryBackoff,
			DialerRetries:        envOpts.DialerRetries,
			DialerRetryTimeout:   envOpts.DialerRetryTimeout,
			SlotsRefreshInterval: 5 * time.Second,
			ReadOnly:             isReadOnly,
		}

		for _, h := range parsed.Hosts {
			clusterOpts.Addrs = append(clusterOpts.Addrs, h.Addr())
		}

		return opts.ClusterDial(ctx, clusterOpts)
	}

	// Route: Standalone.
	addr := "localhost:6379"
	if len(parsed.Hosts) > 0 {
		addr = parsed.Hosts[0].Addr()
	}

	dialOpts := &DialOptions{
		Addr:               addr,
		Password:           parsed.Password,
		Username:           parsed.Username,
		DB:                 parsed.DB,
		ClientName:         clientName,
		TLSConfig:          tlsCfg,
		ConnectTimeout:     connectTimeout,
		SocketTimeout:      envOpts.SocketTimeout,
		KeepAlive:          envOpts.KeepAlive,
		MaxRetries:         envOpts.MaxRetries,
		MinRetryBackoff:    envOpts.MinRetryBackoff,
		MaxRetryBackoff:    envOpts.MaxRetryBackoff,
		DialerRetries:      envOpts.DialerRetries,
		DialerRetryTimeout: envOpts.DialerRetryTimeout,
		ReadOnly:           isReadOnly,
	}

	return opts.Dial(ctx, dialOpts)
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

// loadTLSConfig builds a *tls.Config from SSL environment variables.
func loadTLSConfig(dbType, serviceNameUpper string) *tls.Config {
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

	if caPath == "" || certPath == "" || keyPath == "" {
		return nil
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		// Match: rejectUnauthorized: false
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	}
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
