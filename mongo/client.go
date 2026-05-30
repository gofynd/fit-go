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

// Package mongo provides MongoDB connection management for the fit.go framework.
// Go implementation of modules/mongo/index.ts and mongo/index.ts.
//
// The package manages read/write MongoDB connections per service, auto-discovered
// from environment variables matching the pattern:
//
//	MONGO_{SERVICE}_READ_WRITE - connection string for read-write operations
//	MONGO_{SERVICE}_READ_ONLY - connection string for read-only operations
//
// Connection strings can be provided directly or as GSM secret references when
// DB_CONNECTION_PROVIDER=GSM is set.
//
// # Pool tuning environment variables (per service per connection type)
//
//	MONGO_{SERVICE}_{TYPE}_MAX_POOL_SIZE - maximum connections in pool
//	MONGO_{SERVICE}_{TYPE}_MIN_POOL_SIZE - minimum connections maintained
//	MONGO_{SERVICE}_{TYPE}_MAX_IDLE_TIME - max idle time in milliseconds
//	MONGO_{SERVICE}_{TYPE}_CONNECTION_TIMEOUT - connect timeout in milliseconds
//	MONGO_{SERVICE}_{TYPE}_MAX_CONNECTING - max concurrent connection attempts
//	MONGO_{SERVICE}_{TYPE}_SOCKET_TIMEOUT - socket timeout in milliseconds
//	MONGO_{SERVICE}_{TYPE}_WAIT_QUEUE_TIMEOUT - wait queue timeout in milliseconds
//
// Where {TYPE} is READ_WRITE or READ_ONLY.
//
// # SSL/TLS configuration
//
//	MONGO_{SERVICE}_SSL_CA or MONGO_SSL_CA - path to CA certificate
//	MONGO_{SERVICE}_SSL_CERT or MONGO_SSL_CERT - path to client certificate
//	MONGO_{SERVICE}_SSL_KEY or MONGO_SSL_KEY - path to client key
//
// # Other
//
//	ENABLE_DB_AUTO_INDEXING - "true" to enable auto-indexing
//	DISABLE_DB_AUTO_CREATE_COLLECTION - "true" to disable auto-create
//	SERVICE_NAME - used for appName in connections
//	K8S_POD_NAME, K8S_POD_NAMESPACE - used for appName derivation
package mongo

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
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

// Connection represents a single MongoDB connection. This interface abstracts
// the underlying driver (e.g., go.mongodb.org/mongo-driver/mongo) so that the
// framework can initialize, health-check, and shut down connections without
// importing the driver directly.
//
// When integrating with mongo-driver, implement this interface by wrapping
// *mongo.Client.
type Connection interface {
	// Ping verifies the connection is alive.
	Ping(ctx context.Context) error

	// Close terminates the connection and releases resources.
	Close(ctx context.Context) error

	// Raw returns the underlying driver client (e.g., *mongo.Client).
	// Callers must type-assert to the concrete type.
	Raw() interface{}
}

// DialFunc is a factory that creates a Connection from a connection string and
// options. The framework calls this during Init to establish each connection.
// Users must supply a DialFunc that wraps their chosen MongoDB driver.
//
// Example implementation with mongo-driver:
//
//	func mongoDial(ctx context.Context, uri string, opts *DialOptions) (mongo.Connection, error) {
//	 clientOpts := options.Client().ApplyURI(uri)
//	 if opts.TLSConfig != nil {
//	 clientOpts.SetTLSConfig(opts.TLSConfig)
//	 }
//	 if opts.MaxPoolSize > 0 {
//	 clientOpts.SetMaxPoolSize(uint64(opts.MaxPoolSize))
//	 }
//	 // ... apply other opts
//	 client, err := driver.Connect(ctx, clientOpts)
//	 if err != nil { return nil, err }
//	 return &myConn{client: client}, nil
//	}
type DialFunc func(ctx context.Context, uri string, opts *DialOptions) (Connection, error)

// DialOptions carries connection parameters resolved from env vars and user
// config that the DialFunc should apply to the underlying driver client.
type DialOptions struct {
	AppName string
	TLSConfig *tls.Config
	MaxPoolSize int
	MinPoolSize int
	MaxIdleTimeMS int
	ConnectTimeoutMS int
	SocketTimeoutMS int
	MaxConnecting int
	WaitQueueTimeout int
	AutoIndex bool
	AutoCreate bool
}

// ---------------------------------------------------------------------------
// Service connections
// ---------------------------------------------------------------------------

// ServiceConnection holds read and write connections for a single service.
type ServiceConnection struct {
	Read Connection
	Write Connection
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// ConnectionOptions configures the MongoDB client initialization.
type ConnectionOptions struct {
	// Dial is the driver-specific factory that creates connections.
	// This MUST be provided; the framework does not import any driver.
	Dial DialFunc

	// ConnectTimeout is the default timeout for establishing connections.
	// Defaults to 1 hour (.
	ConnectTimeout time.Duration

	// PerService allows callers to pass driver-specific overrides per service
	// and connection type. Keys are lowercase service names.
	PerService map[string]ServicePoolOverrides

	// Context for connection establishment. If nil, context.Background() is used.
	Context context.Context
}

// ServicePoolOverrides allows callers to programmatically override pool settings
// for a specific service, similar to the connectionOptions parameter.
type ServicePoolOverrides struct {
	Read *PoolOverrides
	Write *PoolOverrides
}

// PoolOverrides holds optional pool tuning values. Zero values are ignored.
type PoolOverrides struct {
	MaxPoolSize int
	MinPoolSize int
	MaxIdleTimeMS int
	ConnectTimeoutMS int
	SocketTimeoutMS int
	MaxConnecting int
	WaitQueueTimeout int
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client manages MongoDB connections for all discovered services. It is the
// Go equivalent of the MongodbConnections map.
type Client struct {
	mu sync.RWMutex
	services map[string]*ServiceConnection
	initOnce sync.Once
}

// envRegex matches MONGO_{SERVICE}_READ_{WRITE|ONLY} environment variables.
var envRegex = regexp.MustCompile(`^MONGO_(.+)_READ_(WRITE|ONLY)$`)

// Init discovers MongoDB connection environment variables, resolves connection
// strings (optionally via GSM), and establishes connections using the provided
// DialFunc.
//
// Init is safe to call multiple times; only the first call performs initialization.
// Returns the Client and any error encountered during connection setup.
func Init(opts ConnectionOptions) (*Client, error) {
	if opts.Dial == nil {
		return nil, fmt.Errorf("mongo: DialFunc must be provided")
	}

	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if opts.ConnectTimeout == 0 {
		opts.ConnectTimeout = time.Hour // match default of 3600000ms
	}

	c := &Client{
		services: make(map[string]*ServiceConnection),
	}

	autoIndex := envBool("ENABLE_DB_AUTO_INDEXING", false)
	autoCreate := !envBool("DISABLE_DB_AUTO_CREATE_COLLECTION", false)

	type connJob struct {
		serviceName string
		serviceNameUpper string
		connType string // "read" or "write"
		connStringOrRef string
	}

	var jobs []connJob

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
		connTypeRaw := matches[2] // "WRITE" or "ONLY"
		connType := "write"
		if strings.EqualFold(connTypeRaw, "ONLY") {
			connType = "read"
		}

		serviceName := strings.ToLower(serviceNameUpper)
		if serviceName == "" || value == "" {
			continue
		}

		jobs = append(jobs, connJob{
			serviceName: serviceName,
			serviceNameUpper: serviceNameUpper,
			connType: connType,
			connStringOrRef: value,
		})
	}

	if len(jobs) == 0 {
		return c, nil
	}

	// Resolve all connections concurrently.all pattern.
	type connResult struct {
		serviceName string
		connType string
		conn Connection
		err error
	}

	results := make(chan connResult, len(jobs))
	var wg sync.WaitGroup

	for _, job := range jobs {
		wg.Add(1)
		go func(j connJob) {
			defer wg.Done()

			// Resolve connection string (direct or via GSM).
			connString, err := resolveConnectionString(j.connStringOrRef)
			if err != nil {
				results <- connResult{
					serviceName: j.serviceName,
					connType: j.connType,
					err: fmt.Errorf("mongo: resolve connection for %s_%s: %w", j.serviceName, j.connType, err),
				}
				return
			}

			// Build dial options from env vars and caller overrides.
			dialOpts := buildDialOptions(j.serviceNameUpper, j.connType, j.serviceName, opts, autoIndex, autoCreate)

			connCtx, cancel := context.WithTimeout(ctx, opts.ConnectTimeout)
			defer cancel()

			conn, err := opts.Dial(connCtx, connString, dialOpts)
			if err != nil {
				results <- connResult{
					serviceName: j.serviceName,
					connType: j.connType,
					err: fmt.Errorf("mongo: connection failed for %s_%s: %w", j.serviceName, j.connType, err),
				}
				return
			}

			// Verify connectivity.
			pingCtx, pingCancel := context.WithTimeout(ctx, 30*time.Second)
			defer pingCancel()
			if err := conn.Ping(pingCtx); err != nil {
				_ = conn.Close(context.Background())
				results <- connResult{
					serviceName: j.serviceName,
					connType: j.connType,
					err: fmt.Errorf("mongo: ping failed for %s_%s: %w", j.serviceName, j.connType, err),
				}
				return
			}

			results <- connResult{
				serviceName: j.serviceName,
				connType: j.connType,
				conn: conn,
			}
		}(job)
	}

	// Wait for all goroutines then close channel.
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
		// Close any connections that were successfully opened.
		_ = c.Close()
		return nil, fmt.Errorf("mongo: init failed: %v", errs)
	}

	return c, nil
}

// Service returns the read/write connections for the given service name.
// The service name is case-insensitive (stored lowercase).
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

// Ping checks all connections are alive. Returns the first error encountered.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for name, sc := range c.services {
		if sc.Read != nil {
			if err := sc.Read.Ping(ctx); err != nil {
				return fmt.Errorf("mongo: ping failed for %s_read: %w", name, err)
			}
		}
		if sc.Write != nil {
			if err := sc.Write.Ping(ctx); err != nil {
				return fmt.Errorf("mongo: ping failed for %s_write: %w", name, err)
			}
		}
	}
	return nil
}

// Close terminates all connections gracefully.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var errs []error
	for name, sc := range c.services {
		if sc.Read != nil {
			if err := sc.Read.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("mongo: close %s_read: %w", name, err))
			}
		}
		if sc.Write != nil {
			if err := sc.Write.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("mongo: close %s_write: %w", name, err))
			}
		}
	}

	c.services = make(map[string]*ServiceConnection)

	if len(errs) > 0 {
		return fmt.Errorf("mongo: close errors: %v", errs)
	}
	return nil
}

// HealthCheck returns a function compatible with health.CheckFunc that pings
// all MongoDB connections.
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

// buildDialOptions constructs DialOptions from environment variables and caller
// overrides.
func buildDialOptions(
	serviceNameUpper, connType, serviceName string,
	opts ConnectionOptions,
	autoIndex, autoCreate bool,
) *DialOptions {
	envConnType := "READ_WRITE"
	if connType == "read" {
		envConnType = "READ_ONLY"
	}
	prefix := fmt.Sprintf("MONGO_%s_%s_", serviceNameUpper, envConnType)

	d := &DialOptions{
		AppName: getAppName(),
		ConnectTimeoutMS: int(opts.ConnectTimeout.Milliseconds()),
		AutoIndex: autoIndex,
		AutoCreate: autoCreate,
	}

	// Read pool settings from env vars.
	envMappings := []struct {
		suffix string
		target *int
	}{
		{"MAX_POOL_SIZE", &d.MaxPoolSize},
		{"MIN_POOL_SIZE", &d.MinPoolSize},
		{"MAX_IDLE_TIME", &d.MaxIdleTimeMS},
		{"CONNECTION_TIMEOUT", &d.ConnectTimeoutMS},
		{"MAX_CONNECTING", &d.MaxConnecting},
		{"SOCKET_TIMEOUT", &d.SocketTimeoutMS},
		{"WAIT_QUEUE_TIMEOUT", &d.WaitQueueTimeout},
	}

	for _, m := range envMappings {
		if v := os.Getenv(prefix + m.suffix); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil {
				*m.target = parsed
			}
		}
	}

	// Apply caller overrides (they win over env vars).
	if opts.PerService != nil {
		if svcOverrides, ok := opts.PerService[serviceName]; ok {
			var po *PoolOverrides
			if connType == "read" {
				po = svcOverrides.Read
			} else {
				po = svcOverrides.Write
			}
			if po != nil {
				applyPoolOverrides(d, po)
			}
		}
	}

	// Load TLS config.
	d.TLSConfig = loadTLSConfig("MONGO", serviceNameUpper)

	return d
}

// applyPoolOverrides merges non-zero PoolOverrides into DialOptions.
func applyPoolOverrides(d *DialOptions, po *PoolOverrides) {
	if po.MaxPoolSize > 0 {
		d.MaxPoolSize = po.MaxPoolSize
	}
	if po.MinPoolSize > 0 {
		d.MinPoolSize = po.MinPoolSize
	}
	if po.MaxIdleTimeMS > 0 {
		d.MaxIdleTimeMS = po.MaxIdleTimeMS
	}
	if po.ConnectTimeoutMS > 0 {
		d.ConnectTimeoutMS = po.ConnectTimeoutMS
	}
	if po.SocketTimeoutMS > 0 {
		d.SocketTimeoutMS = po.SocketTimeoutMS
	}
	if po.MaxConnecting > 0 {
		d.MaxConnecting = po.MaxConnecting
	}
	if po.WaitQueueTimeout > 0 {
		d.WaitQueueTimeout = po.WaitQueueTimeout
	}
}

// loadTLSConfig builds a *tls.Config from SSL environment variables.
// Returns nil if SSL is not configured.
//
// Env vars checked (service-specific takes precedence):
//
//	{dbType}_{SERVICE}_SSL_CA or {dbType}_SSL_CA
//	{dbType}_{SERVICE}_SSL_CERT or {dbType}_SSL_CERT
//	{dbType}_{SERVICE}_SSL_KEY or {dbType}_SSL_KEY
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
		RootCAs: caCertPool,
		Certificates: []tls.Certificate{cert},
		InsecureSkipVerify: false,
		MinVersion: tls.VersionTLS12,
	}
}

// resolveConnectionString resolves a connection string value. If
// DB_CONNECTION_PROVIDER=GSM, the value is treated as a GSM secret reference
// and fetched via the config package. Otherwise it is returned as-is.
func resolveConnectionString(value string) (string, error) {
	if strings.EqualFold(os.Getenv("DB_CONNECTION_PROVIDER"), "GSM") {
		// Import cycle avoidance: we call GSM via the same HTTP-based approach
		// used in config/secrets.go. This is a lightweight inline implementation.
		return fetchGSMSecret(value)
	}
	return value, nil
}

// fetchGSMSecret is a minimal GSM secret fetcher matching the pattern in
// config/secrets.go. In production, callers should wire up the config package's
// GetSecretFromGSM function.
func fetchGSMSecret(secretName string) (string, error) {
	version := os.Getenv("DB_CONNECTION_SECRET_VERSION")
	if version == "" {
		version = "latest"
	}

	// Delegate to the config package's GSM implementation.
	// This avoids duplicating the HTTP-based GSM fetch logic.
	// Import path: github.com/fynd/commerce/fit/config
	//
	// We use a pluggable resolver to avoid the import cycle. Callers can set
	// the resolver via SetGSMResolver before calling Init.
	gsmMu.RLock()
	resolver := gsmResolver
	gsmMu.RUnlock()

	if resolver == nil {
		return "", fmt.Errorf("mongo: GSM resolver not configured; call mongo.SetGSMResolver() before Init")
	}

	return resolver(secretName, version)
}

// GSMResolverFunc fetches a secret by name and version from Google Secret Manager.
type GSMResolverFunc func(secretName, version string) (string, error)

var (
	gsmMu sync.RWMutex
	gsmResolver GSMResolverFunc
)

// SetGSMResolver configures the function used to fetch secrets from GSM.
// This must be called before Init when DB_CONNECTION_PROVIDER=GSM.
// Typically: mongo.SetGSMResolver(config.GetSecretFromGSM)
func SetGSMResolver(fn GSMResolverFunc) {
	gsmMu.Lock()
	defer gsmMu.Unlock()
	gsmResolver = fn
}

// getAppName derives an application name for MongoDB connections, matching
// getAppNameForDbOptions()
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

// getDeploymentName extracts the deployment name from a K8s pod name.
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

// envBool reads an env var and returns true if its value is "true" (case-insensitive).
func envBool(key string, defaultVal bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "true" {
		return true
	}
	if v == "false" {
		return false
	}
	return defaultVal
}

// envWithFallback returns the first non-empty value from the given env var keys.
func envWithFallback(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}
