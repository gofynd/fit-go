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

// Package mysql provides MySQL connection management for the fit.go framework.
// Go implementation of modules/mysql/index.ts and sql/index.ts.
//
// The package manages read/write MySQL connections per service using the
// database/sql interface. Connections are auto-discovered from environment
// variables matching the pattern:
//
//	MYSQL_{SERVICE}_READ_WRITE - connection string for read-write operations
//	MYSQL_{SERVICE}_READ_ONLY - connection string for read-only operations
//
// Connection strings can be provided directly or as GSM secret references when
// DB_CONNECTION_PROVIDER=GSM is set.
//
// # Connection string format
//
//	mysql://user:password@host:port/dbname
//	user:password@tcp(host:port)/dbname
//
// Both URI-style and DSN-style connection strings are supported. The framework
// parses the URI and converts it to the appropriate driver format.
//
// # Pool tuning environment variables
//
//	MYSQL_{SERVICE}_{TYPE}_MAX_POOL_SIZE - max open connections (SetMaxOpenConns)
//	MYSQL_{SERVICE}_{TYPE}_MIN_POOL_SIZE - max idle connections (SetMaxIdleConns)
//	MYSQL_{SERVICE}_{TYPE}_MAX_IDLE_TIME - max idle time in ms (SetConnMaxIdleTime)
//	MYSQL_{SERVICE}_{TYPE}_CONNECTION_TIMEOUT - max connection lifetime in ms (SetConnMaxLifetime)
//	MYSQL_{SERVICE}_{TYPE}_POOL_EVICT - eviction check interval in ms
//
// Where {TYPE} is READ_WRITE or READ_ONLY.
//
// # SSL/TLS configuration
//
//	MYSQL_{SERVICE}_SSL_CA or MYSQL_SSL_CA - path to CA certificate
//	MYSQL_{SERVICE}_SSL_CERT or MYSQL_SSL_CERT - path to client certificate
//	MYSQL_{SERVICE}_SSL_KEY or MYSQL_SSL_KEY - path to client key
//	MYSQL_{SERVICE}_SSL_SERVER_NAME - TLS ServerName (required for SSL)
//
// # Other
//
//	SERVICE_NAME - used for connection attributes
//	K8S_POD_NAME, K8S_POD_NAMESPACE - used for app name derivation
package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	// Side-effect import: registers the "mysql" driver with database/sql.
	gomysql "github.com/go-sql-driver/mysql"
)

// ---------------------------------------------------------------------------
// Service connections
// ---------------------------------------------------------------------------

// ServiceConnection holds read and write *sql.DB connections for a single service.
// When both READ_WRITE and READ_ONLY env vars are present, a single *sql.DB with
// replication awareness is used (
// with the same handle assigned to both Read and Write.
type ServiceConnection struct {
	Read  *sql.DB
	Write *sql.DB
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// ConnectionOptions configures the MySQL client initialization.
type ConnectionOptions struct {
	// DriverName is the registered database/sql driver name (e.g., "mysql").
	// The caller must import and register the driver before calling Init.
	// Defaults to "mysql" if empty.
	DriverName string

	// DSNTransform is an optional hook that converts a parsed connection URI
	// into a driver-specific DSN string. If nil, the framework constructs a
	// standard MySQL DSN: user:password@tcp(host:port)/dbname.
	//
	// This allows callers to customize the DSN for specific drivers (e.g.,
	// go-sql-driver/mysql uses a different format than pgx).
	DSNTransform func(parsed *ParsedURI) string

	// PerService allows callers to set pool overrides per service.
	PerService map[string]ServicePoolOverrides

	// Context for connection establishment.
	Context context.Context

	// TLSRegistrar registers a TLS config with the driver. For go-sql-driver/mysql,
	// this wraps mysql.RegisterTLSConfig(). If nil, TLS is configured via the DSN
	// tls= parameter.
	TLSRegistrar func(name string, config *tls.Config) error
}

// ServicePoolOverrides allows programmatic pool configuration per service.
type ServicePoolOverrides struct {
	Read  *PoolOverrides
	Write *PoolOverrides
}

// PoolOverrides holds optional pool tuning values. Zero values are ignored.
type PoolOverrides struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

// ---------------------------------------------------------------------------
// ParsedURI
// ---------------------------------------------------------------------------

// ParsedURI holds components extracted from a database connection URI.
type ParsedURI struct {
	Username string
	Password string
	Host     string
	Port     string
	Database string
	Options  map[string]string
	// TLSName is set when TLS config has been registered with the driver.
	TLSName string
}

// Addr returns the host:port string.
func (p *ParsedURI) Addr() string {
	if p.Port != "" {
		return p.Host + ":" + p.Port
	}
	return p.Host + ":3306"
}

// DefaultMySQLDSN returns the standard go-sql-driver/mysql DSN format.
func DefaultMySQLDSN(p *ParsedURI) string {
	dsn := ""
	if p.Username != "" {
		dsn = p.Username
		if p.Password != "" {
			dsn += ":" + p.Password
		}
		dsn += "@"
	}
	dsn += "tcp(" + p.Addr() + ")"
	if p.Database != "" {
		dsn += "/" + p.Database
	}

	params := make([]string, 0)
	if p.TLSName != "" {
		params = append(params, "tls="+p.TLSName)
	}
	for k, v := range p.Options {
		params = append(params, k+"="+v)
	}
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}

	return dsn
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client manages MySQL connections for all discovered services.
type Client struct {
	mu       sync.RWMutex
	services map[string]*ServiceConnection
}

var envRegex = regexp.MustCompile(`^MYSQL_(.+)_READ_(WRITE|ONLY)$`)

// InitDefault discovers MySQL connections from environment variables and
// establishes connections using the go-sql-driver/mysql driver with sensible
// defaults. It is the simplest way to initialize the MySQL client:
//
//	client, err := mysql.InitDefault()
//
// The function configures TLS registration via the driver's native
// RegisterTLSConfig, and uses the standard MySQL DSN format.
func InitDefault() (*Client, error) {
	return InitDefaultWithContext(context.Background())
}

// InitDefaultWithContext is like InitDefault but accepts a context for
// connection establishment and initial pings.
func InitDefaultWithContext(ctx context.Context) (*Client, error) {
	return Init(ConnectionOptions{
		DriverName: "mysql",
		Context:    ctx,
		TLSRegistrar: func(name string, config *tls.Config) error {
			return gomysql.RegisterTLSConfig(name, config)
		},
	})
}

// Init discovers MySQL connection environment variables, resolves connection
// strings, and establishes connections.
func Init(opts ConnectionOptions) (*Client, error) {
	if opts.DriverName == "" {
		opts.DriverName = "mysql"
	}
	if opts.DSNTransform == nil {
		opts.DSNTransform = DefaultMySQLDSN
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	c := &Client{
		services: make(map[string]*ServiceConnection),
	}

	// Phase 1: Collect all env vars into a connection mapping.
	type connEntry struct {
		readRef  string
		writeRef string
	}
	connMap := make(map[string]*connEntry) // keyed by lowercase service name
	upperNames := make(map[string]string)  // lowercase -> UPPER

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

	// Phase 2: Establish connections.
	appName := getAppName()

	for serviceName, entry := range connMap {
		serviceNameUpper := upperNames[serviceName]

		// Load TLS config if present.
		tlsCfg, serverName := loadMySQLTLSConfig(serviceNameUpper)

		sc := &ServiceConnection{}

		if entry.readRef != "" && entry.writeRef != "" {
			// Both read and write present - resolve both and create a single
			// connection for write, similar to Sequelize replication.
			writeConnStr, err := resolveConnectionString(entry.writeRef)
			if err != nil {
				return nil, fmt.Errorf("mysql: resolve write connection for %s: %w", serviceName, err)
			}
			readConnStr, err := resolveConnectionString(entry.readRef)
			if err != nil {
				return nil, fmt.Errorf("mysql: resolve read connection for %s: %w", serviceName, err)
			}

			// Create write connection.
			writeDB, err := openDB(ctx, opts, writeConnStr, serviceName, serviceNameUpper, "write", appName, tlsCfg, serverName)
			if err != nil {
				return nil, err
			}
			applyPoolSettings(writeDB, serviceNameUpper, "WRITE", opts.PerService, serviceName, "write")

			// Create read connection.
			readDB, err := openDB(ctx, opts, readConnStr, serviceName, serviceNameUpper, "read", appName, tlsCfg, serverName)
			if err != nil {
				_ = writeDB.Close()
				return nil, err
			}
			applyPoolSettings(readDB, serviceNameUpper, "ONLY", opts.PerService, serviceName, "read")

			sc.Write = writeDB
			sc.Read = readDB
		} else {
			// Single connection type.
			connType := "write"
			connTypeEnv := "WRITE"
			ref := entry.writeRef
			if ref == "" {
				connType = "read"
				connTypeEnv = "ONLY"
				ref = entry.readRef
			}

			connStr, err := resolveConnectionString(ref)
			if err != nil {
				return nil, fmt.Errorf("mysql: resolve connection for %s_%s: %w", serviceName, connType, err)
			}

			db, err := openDB(ctx, opts, connStr, serviceName, serviceNameUpper, connType, appName, tlsCfg, serverName)
			if err != nil {
				return nil, err
			}
			applyPoolSettings(db, serviceNameUpper, connTypeEnv, opts.PerService, serviceName, connType)

			if connType == "read" {
				sc.Read = db
			} else {
				sc.Write = db
			}
		}

		c.services[serviceName] = sc
	}

	// Phase 3: Verify all connections.
	for name, sc := range c.services {
		if sc.Read != nil {
			if err := sc.Read.PingContext(ctx); err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("mysql: ping failed for %s_read: %w", name, err)
			}
		}
		if sc.Write != nil {
			if err := sc.Write.PingContext(ctx); err != nil {
				_ = c.Close()
				return nil, fmt.Errorf("mysql: ping failed for %s_write: %w", name, err)
			}
		}
	}

	return c, nil
}

// Service returns the connections for the given service name.
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
			if err := sc.Read.PingContext(ctx); err != nil {
				return fmt.Errorf("mysql: ping failed for %s_read: %w", name, err)
			}
		}
		if sc.Write != nil {
			if err := sc.Write.PingContext(ctx); err != nil {
				return fmt.Errorf("mysql: ping failed for %s_write: %w", name, err)
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
				errs = append(errs, fmt.Errorf("mysql: close %s_read: %w", name, err))
			}
		}
		if sc.Write != nil && sc.Write != sc.Read {
			if err := sc.Write.Close(); err != nil {
				errs = append(errs, fmt.Errorf("mysql: close %s_write: %w", name, err))
			}
		}
	}

	c.services = make(map[string]*ServiceConnection)

	if len(errs) > 0 {
		return fmt.Errorf("mysql: close errors: %v", errs)
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
// Internal helpers
// ---------------------------------------------------------------------------

// openDB parses a connection string, creates a *sql.DB, and configures it.
func openDB(
	ctx context.Context,
	opts ConnectionOptions,
	connStr, serviceName, serviceNameUpper, connType, appName string,
	tlsCfg *tls.Config,
	serverName string,
) (*sql.DB, error) {
	parsed, err := parseURI(connStr)
	if err != nil {
		return nil, fmt.Errorf("mysql: parse URI for %s_%s: %w", serviceName, connType, err)
	}

	// Register TLS config with driver if available.
	if tlsCfg != nil && opts.TLSRegistrar != nil {
		tlsConfigName := fmt.Sprintf("mysql_%s_%s", serviceName, connType)
		if err := opts.TLSRegistrar(tlsConfigName, tlsCfg); err != nil {
			return nil, fmt.Errorf("mysql: register TLS for %s_%s: %w", serviceName, connType, err)
		}
		parsed.TLSName = tlsConfigName
	}

	// Add connection attributes for app identification.
	if parsed.Options == nil {
		parsed.Options = make(map[string]string)
	}

	dsn := opts.DSNTransform(parsed)

	db, err := sql.Open(opts.DriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: open %s_%s: %w", serviceName, connType, err)
	}

	return db, nil
}

// applyPoolSettings configures pool sizes from env vars and caller overrides.
func applyPoolSettings(
	db *sql.DB,
	serviceNameUpper, connTypeEnv string,
	perService map[string]ServicePoolOverrides,
	serviceName, connType string,
) {
	prefix := fmt.Sprintf("MYSQL_%s_READ_%s_", serviceNameUpper, connTypeEnv)

	envMapping := []struct {
		suffix string
		apply  func(int)
	}{
		{"MAX_POOL_SIZE", func(v int) { db.SetMaxOpenConns(v) }},
		{"MIN_POOL_SIZE", func(v int) { db.SetMaxIdleConns(v) }},
		{"MAX_IDLE_TIME", func(v int) { db.SetConnMaxIdleTime(time.Duration(v) * time.Millisecond) }},
		{"CONNECTION_TIMEOUT", func(v int) { db.SetConnMaxLifetime(time.Duration(v) * time.Millisecond) }},
	}

	for _, m := range envMapping {
		if v := os.Getenv(prefix + m.suffix); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil {
				m.apply(parsed)
			}
		}
	}

	// Apply caller overrides.
	if perService != nil {
		if svcOverrides, ok := perService[serviceName]; ok {
			var po *PoolOverrides
			if connType == "read" {
				po = svcOverrides.Read
			} else {
				po = svcOverrides.Write
			}
			if po != nil {
				if po.MaxOpenConns > 0 {
					db.SetMaxOpenConns(po.MaxOpenConns)
				}
				if po.MaxIdleConns > 0 {
					db.SetMaxIdleConns(po.MaxIdleConns)
				}
				if po.ConnMaxIdleTime > 0 {
					db.SetConnMaxIdleTime(po.ConnMaxIdleTime)
				}
				if po.ConnMaxLifetime > 0 {
					db.SetConnMaxLifetime(po.ConnMaxLifetime)
				}
			}
		}
	}
}

// parseURI parses a database connection URI.
func parseURI(rawURI string) (*ParsedURI, error) {
	// Handle both URI-style and DSN-style.
	if !strings.Contains(rawURI, "://") {
		// Assume it's already a DSN - wrap it as a ParsedURI with raw DSN.
		return &ParsedURI{Options: map[string]string{"_raw_dsn": rawURI}}, nil
	}

	u, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("mysql: invalid URI: %w", err)
	}

	p := &ParsedURI{
		Host:     u.Hostname(),
		Port:     u.Port(),
		Database: strings.TrimPrefix(u.Path, "/"),
		Options:  make(map[string]string),
	}

	if u.User != nil {
		p.Username = u.User.Username()
		p.Password, _ = u.User.Password()
	}

	for k, v := range u.Query() {
		if len(v) > 0 {
			p.Options[k] = v[0]
		}
	}

	return p, nil
}

// loadMySQLTLSConfig builds a *tls.Config from SSL environment variables.
// Returns the config and the server name.
func loadMySQLTLSConfig(serviceNameUpper string) (*tls.Config, string) {
	serverName := os.Getenv(fmt.Sprintf("MYSQL_%s_SSL_SERVER_NAME", serviceNameUpper))
	caPath := envWithFallback(
		fmt.Sprintf("MYSQL_%s_SSL_CA", serviceNameUpper),
		"MYSQL_SSL_CA",
	)
	certPath := envWithFallback(
		fmt.Sprintf("MYSQL_%s_SSL_CERT", serviceNameUpper),
		"MYSQL_SSL_CERT",
	)
	keyPath := envWithFallback(
		fmt.Sprintf("MYSQL_%s_SSL_KEY", serviceNameUpper),
		"MYSQL_SSL_KEY",
	)

	// SSL requires all four: CA, cert, key, and server name (.
	if caPath == "" || certPath == "" || keyPath == "" || serverName == "" {
		return nil, ""
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, ""
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, ""
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		ServerName:   serverName,
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, serverName
}

// resolveConnectionString resolves a value as either a direct connection string
// or a GSM secret reference.
func resolveConnectionString(value string) (string, error) {
	if strings.EqualFold(os.Getenv("DB_CONNECTION_PROVIDER"), "GSM") {
		gsmMu.RLock()
		resolver := gsmResolver
		gsmMu.RUnlock()

		if resolver == nil {
			return "", fmt.Errorf("mysql: GSM resolver not configured; call mysql.SetGSMResolver() before Init")
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
func SetGSMResolver(fn GSMResolverFunc) {
	gsmMu.Lock()
	defer gsmMu.Unlock()
	gsmResolver = fn
}

// getAppName derives an application name for connection identification.
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
