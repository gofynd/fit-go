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

// Package feature provides feature flag integration for the fit.go framework.
// Go implementation of feature-flag modules (feature-flag/index.ts and
// modules/feature-flag/index.ts), adapted to use a generic HTTP-based feature
// flag backend (e.g. FeatureHub, LaunchDarkly, or any compatible server).
//
// Usage:
//
//	client, err := feature.Init()
//	if err != nil { ... }
//	if client.IsEnabled("my-feature") { ... }
//	val := client.GetValue("some-config-key")
package feature

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Client is a feature flag client that communicates with a feature flag server
// (e.g. FeatureHub). It maintains a local cache of feature flags that is
// refreshed periodically.
//
type Client struct {
	mu sync.RWMutex
	url string
	apiKey string
	flags map[string]interface{}
	userKey string
	session string
	client *http.Client
	stopCh chan struct{}
	stopped bool
}

// Init checks the required environment variables and creates a new feature
// flag Client. Returns nil without error if feature flags are disabled.
//
// Required environment variables (when enabled):
// - FEATURE_FLAG_ENABLED: must be "true" (case-insensitive) to enable
// - FEATURE_FLAG_URL: the feature flag server URL
// - FEATURE_FLAG_API_KEY: the API key for authentication
//
// Optional:
// - FEATURE_FLAG_POLL_INTERVAL: polling interval in seconds (default: 30)
// - ENABLE_FEATURE_FLAG_DEBUG_LOGS: set to "true" for verbose logging
func Init() (*Client, error) {
	enabled := strings.ToLower(strings.TrimSpace(os.Getenv("FEATURE_FLAG_ENABLED")))
	if enabled != "true" {
		return nil, nil
	}

	url := os.Getenv("FEATURE_FLAG_URL")
	apiKey := os.Getenv("FEATURE_FLAG_API_KEY")

	if url == "" || apiKey == "" {
		return nil, fmt.Errorf("feature: FEATURE_FLAG_URL and FEATURE_FLAG_API_KEY are required when feature flags are enabled")
	}

	c := &Client{
		url: strings.TrimRight(url, "/"),
		apiKey: apiKey,
		flags: make(map[string]interface{}),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		stopCh: make(chan struct{}),
	}

	// Perform initial fetch.
	if err := c.refresh(); err != nil {
		return nil, fmt.Errorf("feature: initial flag fetch failed: %w", err)
	}

	// Start background polling.
	go c.poll()

	return c, nil
}

// IsEnabled returns true if the feature flag with the given key is enabled
// (i.e. its value is boolean true or the string "true"). Returns false if
// the key does not exist or the value is not truthy.
func (c *Client) IsEnabled(key string) bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.flags[key]
	if !ok {
		return false
	}

	switch v := val.(type) {
	case bool:
		return v
	case string:
		return strings.ToLower(strings.TrimSpace(v)) == "true"
	case float64:
		return v != 0
	default:
		return false
	}
}

// GetValue returns the raw value of a feature flag. Returns nil if the key
// does not exist.
func (c *Client) GetValue(key string) interface{} {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.flags[key]
}

// SetUserKey sets the user identifier for contextual flag evaluation. Some
// feature flag backends use this to return user-specific flag values.
func (c *Client) SetUserKey(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.userKey = key
	c.mu.Unlock()

	// Re-fetch flags with the new context.
	_ = c.refresh()
}

// SetSessionKey sets the session identifier for contextual flag evaluation.
func (c *Client) SetSessionKey(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.session = key
	c.mu.Unlock()

	// Re-fetch flags with the new context.
	_ = c.refresh()
}

// ResetContext clears the user and session keys and refreshes the flags.
func (c *Client) ResetContext() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.userKey = ""
	c.session = ""
	c.mu.Unlock()

	_ = c.refresh()
}

// Stop terminates the background polling goroutine. It is safe to call
// multiple times.
func (c *Client) Stop() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		close(c.stopCh)
		c.stopped = true
	}
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

// poll runs a background loop that refreshes feature flags at regular intervals.
func (c *Client) poll() {
	intervalStr := os.Getenv("FEATURE_FLAG_POLL_INTERVAL")
	interval := 30 * time.Second
	if intervalStr != "" {
		if secs, err := time.ParseDuration(intervalStr + "s"); err == nil && secs > 0 {
			interval = secs
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			_ = c.refresh()
		}
	}
}

// refresh fetches the current feature flags from the server and updates the
// local cache. The request includes context headers for user/session if set.
func (c *Client) refresh() error {
	c.mu.RLock()
	url := c.url
	apiKey := c.apiKey
	userKey := c.userKey
	session := c.session
	c.mu.RUnlock()

	req, err := http.NewRequest(http.MethodGet, url+"/features", nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Api-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	if userKey != "" {
		req.Header.Set("X-User-Key", userKey)
	}
	if session != "" {
		req.Header.Set("X-Session-Key", session)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("feature flag fetch failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("feature flag read body failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("feature flag server returned %d: %s", resp.StatusCode, string(body))
	}

	var flags map[string]interface{}
	if err := json.Unmarshal(body, &flags); err != nil {
		// Try array-of-features format (FeatureHub style).
		var features []map[string]interface{}
		if err2 := json.Unmarshal(body, &features); err2 != nil {
			return fmt.Errorf("feature flag parse failed: %w", err)
		}
		flags = make(map[string]interface{}, len(features))
		for _, f := range features {
			if key, ok := f["key"].(string); ok {
				flags[key] = f["value"]
			}
		}
	}

	c.mu.Lock()
	c.flags = flags
	c.mu.Unlock()

	return nil
}
