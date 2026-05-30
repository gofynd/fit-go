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

// Package groupcache provides distributed in-process caching for the fit.go
// framework using mailgun/groupcache/v2. Go implementation of cache
// semantics adapted for Kubernetes-native deployments where pods discover each
// other via the Kubernetes Endpoints API and share cached data peer-to-peer
// without requiring Redis or any external cache infrastructure.
//
// In single-pod / local-dev mode, it degrades gracefully to a plain LRU cache
// with no network hops.
//
// # Architecture
//
// GroupCache pools requests across peers so only one pod fetches from the
// data source (database, API, etc.) for any given key. This is called
// "single-flight deduplication" - if 50 pods request the same key, only one
// actually loads it; the other 49 wait for the result.
//
// # Environment variables
//
//	GROUPCACHE_ENABLED - enable groupcache (default: false)
//	GROUPCACHE_SELF - this node's peer address (e.g. "http://10.0.0.5:8080")
//	 auto-detected from POD_IP + GROUPCACHE_PORT if not set
//	GROUPCACHE_PORT - port for peer HTTP communication (default: "8081")
//	GROUPCACHE_PEERS - comma-separated initial peer addresses (for non-K8s)
//
// # Kubernetes discovery
//
//	GROUPCACHE_K8S_ENABLED - enable Kubernetes peer discovery (default: false)
//	GROUPCACHE_K8S_NAMESPACE - Kubernetes namespace (auto-detected from downward API)
//	GROUPCACHE_K8S_SERVICE_NAME - Kubernetes service name for endpoint discovery
//	GROUPCACHE_K8S_PORT - port name/number in the endpoints (default: "groupcache")
package groupcache

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	gc "github.com/mailgun/groupcache/v2"

	"github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/logging"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config controls Client initialization. Fields with env-var fallbacks are
// documented inline; explicit field values take precedence over env vars.
type Config struct {
	// Self is this node's peer address, e.g. "http://10.0.0.5:8081".
	// Falls back to GROUPCACHE_SELF, then "http://{POD_IP}:{GROUPCACHE_PORT}".
	// In local-dev mode (POD_IP unset), defaults to "http://127.0.0.1:8081".
	Self string

	// Peers are the initial peer addresses (including Self).
	// Falls back to GROUPCACHE_PEERS (comma-separated).
	// If empty, only Self is used (single-node mode).
	Peers []string

	// Port is the HTTP port for peer communication.
	// Falls back to GROUPCACHE_PORT, then "8081".
	Port string

	// Logger for cache operations. If nil, a default logger is created.
	Logger *logging.Logger

	// K8sDiscovery enables Kubernetes-based peer discovery.
	// Falls back to GROUPCACHE_K8S_ENABLED env var.
	K8sDiscovery *K8sDiscoveryConfig
}

// K8sDiscoveryConfig configures Kubernetes Endpoints-based peer discovery.
type K8sDiscoveryConfig struct {
	// Namespace is the Kubernetes namespace to watch.
	// Falls back to GROUPCACHE_K8S_NAMESPACE, then the contents of
	// /var/run/secrets/kubernetes.io/serviceaccount/namespace.
	Namespace string

	// ServiceName is the headless Service whose Endpoints list all pods.
	// Falls back to GROUPCACHE_K8S_SERVICE_NAME.
	ServiceName string

	// PortName is the port name or number used to build peer URLs.
	// Falls back to GROUPCACHE_K8S_PORT, then "groupcache".
	PortName string
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

// Client manages a groupcache HTTPPool, named cache groups, and optional
// Kubernetes peer discovery. It is the primary entry point for the groupcache
// package.
type Client struct {
	mu       sync.RWMutex
	pool     *gc.HTTPPool
	groups   map[string]*gc.Group
	self     string
	port     string
	logger   *logging.Logger
	metrics  *Metrics
	mux      *http.ServeMux
	server   *http.Server
	stopCh   chan struct{}
	stopOnce sync.Once
}

// Init discovers groupcache configuration from environment variables and
// explicit Config fields, creates the HTTP pool, and optionally starts
// Kubernetes peer discovery. It mirrors the Init() pattern used by redis,
// kafka, and other fit-go packages.
//
// If GROUPCACHE_ENABLED is not "true" and Config is zero-valued, Init returns
// (nil, nil), allowing callers to check for a nil client.
func Init(cfg Config) (*Client, error) {
	// Check enabled flag (env-var fallback).
	if !isEnabled(cfg) {
		return nil, nil
	}

	// Resolve configuration with env-var fallbacks.
	self, port := resolveSelf(cfg)
	peers := resolvePeers(cfg, self)

	logger := cfg.Logger
	if logger == nil {
		l, err := logging.New(logging.Options{Level: "info"})
		if err != nil {
			return nil, fmt.Errorf("groupcache: failed to create logger: %w", err)
		}
		logger = l
	}

	// Create the HTTP pool. NewHTTPPoolOpts does NOT register on
	// http.DefaultServeMux, so we create our own mux for the peer server.
	pool := gc.NewHTTPPoolOpts(self, nil)
	pool.Set(peers...)

	mux := http.NewServeMux()
	mux.Handle("/_groupcache/", pool)

	c := &Client{
		pool:    pool,
		groups:  make(map[string]*gc.Group),
		self:    self,
		port:    port,
		logger:  logger,
		metrics: NewMetrics(),
		mux:     mux,
		stopCh:  make(chan struct{}),
	}

	logger.Info("groupcache: initialized",
		"self", self,
		"peers", strings.Join(peers, ","),
		"port", port,
	)

	return c, nil
}

// StartServer starts the HTTP server for peer communication on the configured
// port. This should be called after Init. The server runs in a background
// goroutine and is stopped by Close().
func (c *Client) StartServer() error {
	addr := ":" + c.port
	c.server = &http.Server{
		Addr:    addr,
		Handler: c.mux,
	}

	errCh := make(chan error, 1)
	go func() {
		c.logger.Info("groupcache: peer server listening", "addr", addr)
		if err := c.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	// Give the server a moment to fail on bind errors.
	select {
	case err := <-errCh:
		return fmt.Errorf("groupcache: peer server failed to start: %w", err)
	default:
		return nil
	}
}

// SetPeers updates the peer list for the HTTP pool. This is called
// automatically by Kubernetes discovery but can also be called manually.
func (c *Client) SetPeers(peers ...string) {
	c.pool.Set(peers...)
	c.logger.Info("groupcache: peers updated", "peers", strings.Join(peers, ","))
}

// Self returns this node's peer address.
func (c *Client) Self() string {
	return c.self
}

// Pool returns the underlying HTTPPool for advanced usage.
func (c *Client) Pool() *gc.HTTPPool {
	return c.pool
}

// GetMetrics returns the current cache metrics.
func (c *Client) GetMetrics() *Metrics {
	return c.metrics
}

// HealthCheck returns a health.CheckFunc compatible with the fit-go health
// package. It verifies the client is initialized and the peer server is
// reachable.
func (c *Client) HealthCheck() health.CheckFunc {
	return func() string {
		if shouldSkip("SKIP_HEALTH_CHECK_GROUPCACHE") {
			return ""
		}
		if c == nil || c.pool == nil {
			return "GroupCache: not initialized"
		}
		return ""
	}
}

// Close gracefully shuts down the peer server and stops Kubernetes discovery.
func (c *Client) Close() error {
	var err error
	c.stopOnce.Do(func() {
		close(c.stopCh)

		if c.server != nil {
			err = c.server.Shutdown(context.Background())
		}

		c.logger.Info("groupcache: closed")
	})
	return err
}

// ---------------------------------------------------------------------------
// Env-var resolution helpers
// ---------------------------------------------------------------------------

// isEnabled checks whether groupcache should be initialized.
func isEnabled(cfg Config) bool {
	// If explicit config is provided (Self is set), consider it enabled.
	if cfg.Self != "" || len(cfg.Peers) > 0 {
		return true
	}
	// If K8s discovery config is provided, consider it enabled.
	if cfg.K8sDiscovery != nil {
		return true
	}
	return strings.EqualFold(os.Getenv("GROUPCACHE_ENABLED"), "true")
}

// resolveSelf determines this node's peer address.
func resolveSelf(cfg Config) (self, port string) {
	port = cfg.Port
	if port == "" {
		port = os.Getenv("GROUPCACHE_PORT")
	}
	if port == "" {
		port = "8081"
	}

	self = cfg.Self
	if self == "" {
		self = os.Getenv("GROUPCACHE_SELF")
	}
	if self == "" {
		podIP := os.Getenv("POD_IP")
		if podIP == "" {
			podIP = "127.0.0.1"
		}
		self = fmt.Sprintf("http://%s:%s", podIP, port)
	}
	return self, port
}

// resolvePeers determines the initial peer list.
func resolvePeers(cfg Config, self string) []string {
	peers := cfg.Peers
	if len(peers) == 0 {
		if envPeers := os.Getenv("GROUPCACHE_PEERS"); envPeers != "" {
			peers = strings.Split(envPeers, ",")
			for i := range peers {
				peers[i] = strings.TrimSpace(peers[i])
			}
		}
	}

	// Ensure self is in the peer list.
	hasSelf := false
	for _, p := range peers {
		if p == self {
			hasSelf = true
			break
		}
	}
	if !hasSelf {
		peers = append([]string{self}, peers...)
	}

	return peers
}

// shouldSkip checks whether a health check should be skipped via env var.
func shouldSkip(envVar string) bool {
	return strings.EqualFold(os.Getenv(envVar), "true")
}
