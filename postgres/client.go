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

// Package postgres provides PostgreSQL connection management for the fit.go framework
// using pgx/v5 native connection pools.
//
// The package manages read/write PostgreSQL connections per service using pgxpool.Pool.
// Connections are auto-discovered from environment variables matching the pattern:
//
//	POSTGRES_{SERVICE}_READ_WRITE - connection string for read-write operations
//	POSTGRES_{SERVICE}_READ_ONLY - connection string for read-only operations
//
// Connection strings can be provided directly or as GSM secret references when
// DB_CONNECTION_PROVIDER=GSM is set.
//
// # Connection string format
//
//	postgres://user:password@host:port/dbname?sslmode=require
//	postgresql://user:password@host:port/dbname
//
// # Pool tuning environment variables
//
//	POSTGRES_{SERVICE}_{TYPE}_MAX_POOL_SIZE - max connections in the pool
//	POSTGRES_{SERVICE}_{TYPE}_MIN_POOL_SIZE - min idle connections maintained
//	POSTGRES_{SERVICE}_{TYPE}_MAX_IDLE_TIME - max idle time in ms
//	POSTGRES_{SERVICE}_{TYPE}_CONNECTION_TIMEOUT - max connection lifetime in ms
//	POSTGRES_{SERVICE}_{TYPE}_HEALTH_CHECK_PERIOD - health check interval in ms
//
// Where {TYPE} is READ_WRITE or READ_ONLY.
//
// # SSL/TLS configuration
//
//	POSTGRES_{SERVICE}_SSL_CA or POSTGRES_SSL_CA - path to CA certificate
//	POSTGRES_{SERVICE}_SSL_CERT or POSTGRES_SSL_CERT - path to client certificate
//	POSTGRES_{SERVICE}_SSL_KEY or POSTGRES_SSL_KEY - path to client key
//	POSTGRES_{SERVICE}_SSL_SERVER_NAME - TLS ServerName (required for SSL)
//
// # Other
//
//	SERVICE_NAME - used for application_name in connections
//	K8S_POD_NAME, K8S_POD_NAMESPACE - used for app name derivation
package postgres

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

	"github.com/jackc/pgx/v5/pgxpool"
)

// ServiceConnection holds read and write *pgxpool.Pool connections for a single service.
type ServiceConnection struct {
	Read  *pgxpool.Pool
	Write *pgxpool.Pool
}

// ConnectionOptions configures the PostgreSQL client initialization.
type ConnectionOptions struct {
	// MaxConns is the maximum number of connections per pool. Defaults to 20.
	MaxConns int32
	// MinConns is the minimum number of idle connections per pool. Defaults to 5.
	MinConns int32
	// MaxConnLifetime is the maximum lifetime of a connection. Defaults to 1 hour.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime is the maximum idle time of a connection. Defaults to 30 minutes.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod is the interval between health checks. Defaults to 1 minute.
	HealthCheckPeriod time.Duration
	// Context for connection establishment and initial pings.
	Context context.Context
	// PerService allows per-service pool overrides.
	PerService map[string]ServicePoolOverrides
}

// ServicePoolOverrides allows programmatic pool configuration per service.
type ServicePoolOverrides struct {
	Read  *PoolOverrides
	Write *PoolOverrides
}

// PoolOverrides holds optional pool tuning values. Zero values are ignored.
type PoolOverrides struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// Client manages PostgreSQL connections for all discovered services.
type Client struct {
	mu       sync.RWMutex
	services map[string]*ServiceConnection
}

var envRegex = regexp.MustCompile(`^POSTGRES_(.+)_READ_(WRITE|ONLY)$`)

// InitDefault discovers PostgreSQL connections from environment variables and
// establishes pgxpool.Pool connections with sensible defaults.
func InitDefault() (*Client, error) {
	return InitWithContext(context.Background(), ConnectionOptions{})
}

// InitWithContext discovers connections and creates pools with the given context and options.
func InitWithContext(ctx context.Context, opts ConnectionOptions) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	opts = applyOptionDefaults(opts)

	c := &Client{
		services: make(map[string]*ServiceConnection),
	}

	type connEntry struct {
		readRef  string
		writeRef string
	}
	connMap := make(map[string]*connEntry)
	upperNames := make(map[string]string)

	for _, env := range os.Environ() {
		idx := strings.IndexByte(env, '=')
		if idx < 0 {
			continue
		}
		key := env[:idx]
		value := env[idx+1:]

		matches := envRegex.FindStringSubmatch(key)
		if matches == nil {
			continue
		}

		serviceNameUpper := matches[1]
		connTypeRaw := matches[2]
		serviceName := strings.ToLower(serviceNameUpper)

		if serviceName == "" || value == "" {
			continue
		}

		if connMap[serviceName] == nil {
			connMap[serviceName] = &connEntry{}
		}
		upperNames[serviceName] = serviceNameUpper

		if strings.EqualFold(connTypeRaw, "ONLY") {
			connMap[serviceName].readRef = value
		} else {
			connMap[serviceName].writeRef = value
		}
	}

	if len(connMap) == 0 {
		return c, nil
	}

	appName := getAppName()

	for serviceName, entry := range connMap {
		serviceNameUpper := upperNames[serviceName]
		tlsCfg := loadPostgresTLSConfig(serviceNameUpper)

		sc := &ServiceConnection{}

		if entry.writeRef != "" {
			writeConnStr, err := resolveConnectionString(entry.writeRef)
			if err != nil {
				return nil, fmt.Errorf("postgres: resolve write connection for %s: %w", serviceName, err)
			}
			writePool, err := createPool(ctx, writeConnStr, appName, tlsCfg, poolSettingsFromEnv(serviceNameUpper, "WRITE", opts, serviceName, "write"))
			if err != nil {
				return nil, fmt.Errorf("postgres: create write pool for %s: %w", serviceName, err)
			}
			sc.Write = writePool
		}

		if entry.readRef != "" {
			readConnStr, err := resolveConnectionString(entry.readRef)
			if err != nil {
				if sc.Write != nil {
					sc.Write.Close()
				}
				return nil, fmt.Errorf("postgres: resolve read connection for %s: %w", serviceName, err)
			}
			readPool, err := createPool(ctx, readConnStr, appName, tlsCfg, poolSettingsFromEnv(serviceNameUpper, "ONLY", opts, serviceName, "read"))
			if err != nil {
				if sc.Write != nil {
					sc.Write.Close()
				}
				return nil, fmt.Errorf("postgres: create read pool for %s: %w", serviceName, err)
			}
			sc.Read = readPool
		}

		// If only one connection is provided, use it for both.
		if sc.Write == nil && sc.Read != nil {
			sc.Write = sc.Read
		} else if sc.Read == nil && sc.Write != nil {
			sc.Read = sc.Write
		}

		c.services[serviceName] = sc
	}

	// Verify all connections.
	for name, sc := range c.services {
		if sc.Read != nil {
			if err := sc.Read.Ping(ctx); err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("postgres: ping failed for %s_read: %w", name, err)
			}
		}
		if sc.Write != nil && sc.Write != sc.Read {
			if err := sc.Write.Ping(ctx); err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("postgres: ping failed for %s_write: %w", name, err)
			}
		}
	}

	return c, nil
}

// Service returns the connections for the given service name (case-insensitive).
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
				return fmt.Errorf("postgres: ping failed for %s_read: %w", name, err)
			}
		}
		if sc.Write != nil && sc.Write != sc.Read {
			if err := sc.Write.Ping(ctx); err != nil {
				return fmt.Errorf("postgres: ping failed for %s_write: %w", name, err)
			}
		}
	}
	return nil
}

// Close terminates all connection pools gracefully.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sc := range c.services {
		if sc.Read != nil {
			sc.Read.Close()
		}
		if sc.Write != nil && sc.Write != sc.Read {
			sc.Write.Close()
		}
	}
	c.services = make(map[string]*ServiceConnection)
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
// Pool creation
// ---------------------------------------------------------------------------

type poolSettings struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

func createPool(ctx context.Context, connStr, appName string, tlsCfg *tls.Config, settings poolSettings) (*pgxpool.Pool, error) {
	// Append application_name to connection string if not present.
	if appName != "" && !strings.Contains(connStr, "application_name") {
		sep := "?"
		if strings.Contains(connStr, "?") {
			sep = "&"
		}
		connStr += sep + "application_name=" + url.QueryEscape(appName)
	}

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}

	poolCfg.MaxConns = settings.MaxConns
	poolCfg.MinConns = settings.MinConns
	poolCfg.MaxConnLifetime = settings.MaxConnLifetime
	poolCfg.MaxConnIdleTime = settings.MaxConnIdleTime
	poolCfg.HealthCheckPeriod = settings.HealthCheckPeriod

	if tlsCfg != nil {
		poolCfg.ConnConfig.TLSConfig = tlsCfg
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	return pool, nil
}

func poolSettingsFromEnv(serviceNameUpper, connTypeEnv string, opts ConnectionOptions, serviceName, connType string) poolSettings {
	ps := poolSettings{
		MaxConns:          opts.MaxConns,
		MinConns:          opts.MinConns,
		MaxConnLifetime:   opts.MaxConnLifetime,
		MaxConnIdleTime:   opts.MaxConnIdleTime,
		HealthCheckPeriod: opts.HealthCheckPeriod,
	}

	prefix := fmt.Sprintf("POSTGRES_%s_READ_%s_", serviceNameUpper, connTypeEnv)

	if v := os.Getenv(prefix + "MAX_POOL_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ps.MaxConns = int32(n)
		}
	}
	if v := os.Getenv(prefix + "MIN_POOL_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ps.MinConns = int32(n)
		}
	}
	if v := os.Getenv(prefix + "MAX_IDLE_TIME"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ps.MaxConnIdleTime = time.Duration(n) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "CONNECTION_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ps.MaxConnLifetime = time.Duration(n) * time.Millisecond
		}
	}
	if v := os.Getenv(prefix + "HEALTH_CHECK_PERIOD"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			ps.HealthCheckPeriod = time.Duration(n) * time.Millisecond
		}
	}

	// Apply caller overrides.
	if opts.PerService != nil {
		if svcOverrides, ok := opts.PerService[serviceName]; ok {
			var po *PoolOverrides
			if connType == "read" {
				po = svcOverrides.Read
			} else {
				po = svcOverrides.Write
			}
			if po != nil {
				if po.MaxConns > 0 {
					ps.MaxConns = po.MaxConns
				}
				if po.MinConns > 0 {
					ps.MinConns = po.MinConns
				}
				if po.MaxConnLifetime > 0 {
					ps.MaxConnLifetime = po.MaxConnLifetime
				}
				if po.MaxConnIdleTime > 0 {
					ps.MaxConnIdleTime = po.MaxConnIdleTime
				}
				if po.HealthCheckPeriod > 0 {
					ps.HealthCheckPeriod = po.HealthCheckPeriod
				}
			}
		}
	}

	return ps
}

// ---------------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------------

func loadPostgresTLSConfig(serviceNameUpper string) *tls.Config {
	serverName := os.Getenv(fmt.Sprintf("POSTGRES_%s_SSL_SERVER_NAME", serviceNameUpper))
	caPath := envWithFallback(
		fmt.Sprintf("POSTGRES_%s_SSL_CA", serviceNameUpper),
		"POSTGRES_SSL_CA",
	)
	certPath := envWithFallback(
		fmt.Sprintf("POSTGRES_%s_SSL_CERT", serviceNameUpper),
		"POSTGRES_SSL_CERT",
	)
	keyPath := envWithFallback(
		fmt.Sprintf("POSTGRES_%s_SSL_KEY", serviceNameUpper),
		"POSTGRES_SSL_KEY",
	)

	if caPath == "" || certPath == "" || keyPath == "" || serverName == "" {
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
		ServerName:   serverName,
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
}

// ---------------------------------------------------------------------------
// Connection string resolution
// ---------------------------------------------------------------------------

func resolveConnectionString(value string) (string, error) {
	if strings.EqualFold(os.Getenv("DB_CONNECTION_PROVIDER"), "GSM") {
		gsmMu.RLock()
		resolver := gsmResolver
		gsmMu.RUnlock()

		if resolver == nil {
			return "", fmt.Errorf("postgres: GSM resolver not configured; call SetGSMResolver() before Init")
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
func SetGSMResolver(fn GSMResolverFunc) {
	gsmMu.Lock()
	defer gsmMu.Unlock()
	gsmResolver = fn
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

func applyOptionDefaults(opts ConnectionOptions) ConnectionOptions {
	if opts.MaxConns == 0 {
		opts.MaxConns = 20
	}
	if opts.MinConns == 0 {
		opts.MinConns = 5
	}
	if opts.MaxConnLifetime == 0 {
		opts.MaxConnLifetime = 1 * time.Hour
	}
	if opts.MaxConnIdleTime == 0 {
		opts.MaxConnIdleTime = 30 * time.Minute
	}
	if opts.HealthCheckPeriod == 0 {
		opts.HealthCheckPeriod = 1 * time.Minute
	}
	return opts
}

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
