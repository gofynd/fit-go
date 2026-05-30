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

package groupcache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Kubernetes peer discovery
// ---------------------------------------------------------------------------

// k8sEndpoints mirrors the subset of Kubernetes Endpoints API response we need.
type k8sEndpoints struct {
	Subsets []k8sSubset `json:"subsets"`
}

type k8sSubset struct {
	Addresses []k8sAddress `json:"addresses"`
	Ports     []k8sPort    `json:"ports"`
}

type k8sAddress struct {
	IP string `json:"ip"`
}

type k8sPort struct {
	Name     string `json:"name"`
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
}

// StartK8sDiscovery begins polling Kubernetes Endpoints for peer changes.
// When pods are added or removed, the peer list is automatically updated
// via pool.Set(). Uses in-cluster service account credentials.
//
// The discovery runs in a background goroutine and is stopped when the
// client is closed via Close().
func (c *Client) StartK8sDiscovery(ctx context.Context, cfg K8sDiscoveryConfig) error {
	cfg = resolveK8sConfig(cfg)

	if cfg.ServiceName == "" {
		return fmt.Errorf("groupcache: K8s discovery requires ServiceName (set GROUPCACHE_K8S_SERVICE_NAME)")
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("groupcache: K8s discovery requires Namespace (set GROUPCACHE_K8S_NAMESPACE)")
	}

	c.logger.Info("groupcache: starting K8s peer discovery",
		"namespace", cfg.Namespace,
		"service", cfg.ServiceName,
		"portName", cfg.PortName,
	)

	go c.k8sDiscoveryLoop(ctx, cfg)
	return nil
}

// k8sDiscoveryLoop polls the Kubernetes Endpoints API at a fixed interval
// and updates the peer list when changes are detected.
func (c *Client) k8sDiscoveryLoop(ctx context.Context, cfg K8sDiscoveryConfig) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Perform initial discovery.
	c.discoverPeers(ctx, cfg)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("groupcache: K8s discovery stopped (context cancelled)")
			return
		case <-c.stopCh:
			c.logger.Info("groupcache: K8s discovery stopped (client closed)")
			return
		case <-ticker.C:
			c.discoverPeers(ctx, cfg)
		}
	}
}

// discoverPeers fetches the Kubernetes Endpoints for the configured service
// and updates the peer list.
func (c *Client) discoverPeers(ctx context.Context, cfg K8sDiscoveryConfig) {
	peers, err := fetchK8sEndpoints(ctx, cfg)
	if err != nil {
		c.logger.Error("groupcache: K8s endpoint discovery failed", "error", err)
		return
	}

	if len(peers) == 0 {
		c.logger.Warn("groupcache: K8s discovery returned no peers, keeping current list")
		return
	}

	c.SetPeers(peers...)
}

// fetchK8sEndpoints calls the Kubernetes API to get Endpoints for the service
// and extracts peer URLs from pod IPs + port.
func fetchK8sEndpoints(ctx context.Context, cfg K8sDiscoveryConfig) ([]string, error) {
	// Read the service account token for in-cluster auth.
	tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read service account token: %w", err)
	}

	// Build the API URL. KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT
	// are automatically injected into all pods.
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/PORT not set, not running in cluster")
	}

	apiURL := fmt.Sprintf("https://%s:%s/api/v1/namespaces/%s/endpoints/%s",
		host, port, cfg.Namespace, cfg.ServiceName,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))

	// Use a client that trusts the cluster CA.
	// In-cluster, the CA cert is at a well-known path.
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("K8s API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("K8s API returned %d: %s", resp.StatusCode, string(body))
	}

	var endpoints k8sEndpoints
	if err := json.NewDecoder(resp.Body).Decode(&endpoints); err != nil {
		return nil, fmt.Errorf("failed to decode endpoints: %w", err)
	}

	return extractPeers(endpoints, cfg.PortName), nil
}

// extractPeers builds peer URLs from Kubernetes Endpoints data.
func extractPeers(endpoints k8sEndpoints, portName string) []string {
	var peers []string

	for _, subset := range endpoints.Subsets {
		// Find the port matching our port name.
		peerPort := 0
		for _, p := range subset.Ports {
			if p.Name == portName || fmt.Sprintf("%d", p.Port) == portName {
				peerPort = p.Port
				break
			}
		}
		if peerPort == 0 {
			// If no matching port name, use the first port.
			if len(subset.Ports) > 0 {
				peerPort = subset.Ports[0].Port
			} else {
				continue
			}
		}

		for _, addr := range subset.Addresses {
			peers = append(peers, fmt.Sprintf("http://%s:%d", addr.IP, peerPort))
		}
	}

	return peers
}

// resolveK8sConfig fills in K8sDiscoveryConfig fields from environment
// variables where not explicitly set.
func resolveK8sConfig(cfg K8sDiscoveryConfig) K8sDiscoveryConfig {
	if cfg.Namespace == "" {
		cfg.Namespace = os.Getenv("GROUPCACHE_K8S_NAMESPACE")
	}
	if cfg.Namespace == "" {
		// Try the downward API file.
		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			cfg.Namespace = strings.TrimSpace(string(data))
		}
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = os.Getenv("GROUPCACHE_K8S_SERVICE_NAME")
	}

	if cfg.PortName == "" {
		cfg.PortName = os.Getenv("GROUPCACHE_K8S_PORT")
	}
	if cfg.PortName == "" {
		cfg.PortName = "groupcache"
	}

	return cfg
}
