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

// Package feature provides FeatureHub-compatible feature flags for fit-go.
package feature

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	mathrand "math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultInitTimeout       = 5 * time.Second
	defaultReconnectInterval = time.Second
	maxReconnectInterval     = 30 * time.Second
	maxRetainedSnapshotAge   = 30 * time.Second
	maxSSEEventSize          = 16 << 20
)

type edgeStaleError struct {
	delay time.Duration
}

func (e *edgeStaleError) Error() string {
	return fmt.Sprintf("FeatureHub edge marked the stream stale for %s", e.delay)
}

type featureHubHTTPError struct {
	status     int
	retryAfter time.Duration
}

func (e *featureHubHTTPError) Error() string {
	return fmt.Sprintf("FeatureHub returned HTTP %d", e.status)
}

// Client maintains a FeatureHub cache populated from the edge server's SSE
// endpoint. API keys containing an asterisk receive the complete feature set
// and evaluate rollout strategies locally. Other keys reconnect with an
// x-featurehub context and consume values evaluated by the edge server.
type Client struct {
	legacy          *legacyClient
	mu              sync.RWMutex
	url             string
	apiKey          string
	clientEvaluated bool
	features        map[string]*featureState
	attributes      map[string][]string
	defaults        map[string][]string
	httpClient      *http.Client
	reconnectDelay  time.Duration
	snapshotTimeout time.Duration
	refresh         chan struct{}
	// retryJitter and staleRetention are injectable for deterministic tests.
	// Nil/zero select equalRetryJitter and maxRetainedSnapshotAge.
	retryJitter    func(time.Duration) time.Duration
	staleRetention time.Duration

	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	readySignalMu    sync.Mutex
	readySignal      chan struct{}
	terminalFailure  chan struct{}
	terminalFailOnce sync.Once
	stopOnce         sync.Once
	receivedInitial  atomic.Bool
	ready            atomic.Bool
	// contextRevision is mutated while mu is held so context changes are
	// serialized with feature application. Atomic loads keep WaitReady and the
	// stream loop lock-free.
	contextRevision  atomic.Uint64
	readyRevision    atomic.Uint64
	lastErrorMu      sync.RWMutex
	lastStreamingErr error
	featureEvents    atomic.Uint64
	staleTimerMu     sync.Mutex
	staleTimer       *time.Timer
	staleGeneration  uint64
}

// legacyClient retains the released polling transport and synchronous context
// update semantics used by Init. The SSE implementation is selected explicitly
// through InitAdvanced/InitWithOptions.
type legacyClient struct {
	mu      sync.RWMutex
	url     string
	apiKey  string
	flags   map[string]interface{}
	userKey string
	session string
	client  *http.Client
	// snapshotTimeout bounds per-request EvaluationContext snapshots; it does
	// not affect the released polling refresh path.
	snapshotTimeout time.Duration
	stopCh          chan struct{}
	stopped         bool
}

// Options configures a FeatureHub client. Zero durations use the corresponding
// environment variable and then the package default.
type Options struct {
	Enabled             bool
	URL                 string
	APIKey              string
	RequireInitialState bool
	InitTimeout         time.Duration
	ReconnectInterval   time.Duration
	// DefaultAttributes are included in every evaluation context. Values are
	// copied during initialization and cannot be mutated through this map later.
	DefaultAttributes map[string][]string
}

// Init preserves fit-go's released synchronous polling contract. When enabled,
// missing configuration and the initial fetch are startup errors.
func Init() (*Client, error) {
	enabled := strings.EqualFold(strings.TrimSpace(os.Getenv("FEATURE_FLAG_ENABLED")), "true")
	if !enabled {
		return nil, nil
	}
	serverURL := os.Getenv("FEATURE_FLAG_URL")
	apiKey := os.Getenv("FEATURE_FLAG_API_KEY")
	if serverURL == "" || apiKey == "" {
		return nil, fmt.Errorf("feature: FEATURE_FLAG_URL and FEATURE_FLAG_API_KEY are required when feature flags are enabled")
	}
	legacy := &legacyClient{
		url:    strings.TrimRight(serverURL, "/"),
		apiKey: apiKey,
		flags:  make(map[string]interface{}),
		client: &http.Client{Timeout: 10 * time.Second},
		stopCh: make(chan struct{}),

		snapshotTimeout: durationFromEnv("FEATURE_FLAG_INIT_TIMEOUT", defaultInitTimeout),
	}
	if err := legacy.refresh(); err != nil {
		return nil, fmt.Errorf("feature: initial flag fetch failed: %w", err)
	}
	go legacy.poll()
	return &Client{legacy: legacy}, nil
}

// InitAdvanced creates the FeatureHub SSE client from process environment.
// It is explicit so upgrading cannot silently change Init's transport or
// startup-readiness contract.
// Deprecated: use InitStreaming.
func InitAdvanced() (*Client, error) {
	return InitWithOptions(Options{
		Enabled:             strings.EqualFold(strings.TrimSpace(os.Getenv("FEATURE_FLAG_ENABLED")), "true"),
		URL:                 os.Getenv("FEATURE_FLAG_URL"),
		APIKey:              os.Getenv("FEATURE_FLAG_API_KEY"),
		RequireInitialState: boolFromEnv("FEATURE_FLAG_REQUIRE_INITIAL_STATE", false),
	})
}

// InitWithOptions creates a client from explicit values. This is used by
// fit.Init so merged file/environment configuration has the same behavior as
// direct environment-based initialization.
func InitWithOptions(options Options) (*Client, error) {
	if !options.Enabled {
		return nil, nil
	}

	serverURL := strings.TrimSpace(options.URL)
	apiKey := strings.TrimSpace(options.APIKey)
	if serverURL == "" || apiKey == "" {
		if options.RequireInitialState {
			return nil, fmt.Errorf("feature: FEATURE_FLAG_URL and FEATURE_FLAG_API_KEY are required when initial state is required")
		}
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defaults := defaultAttributes(options.DefaultAttributes)
	snapshotTimeout := options.InitTimeout
	if snapshotTimeout <= 0 {
		snapshotTimeout = durationFromEnv("FEATURE_FLAG_INIT_TIMEOUT", defaultInitTimeout)
	}
	client := &Client{
		url:             normalizeFeatureHubURL(serverURL),
		apiKey:          strings.TrimLeft(apiKey, "/"),
		clientEvaluated: strings.Contains(apiKey, "*"),
		features:        make(map[string]*featureState),
		attributes:      cloneAttributes(defaults),
		defaults:        defaults,
		httpClient:      &http.Client{},
		reconnectDelay:  options.ReconnectInterval,
		snapshotTimeout: snapshotTimeout,
		refresh:         make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		done:            make(chan struct{}),
		readySignal:     make(chan struct{}),
		terminalFailure: make(chan struct{}),
	}
	client.contextRevision.Store(1)
	if client.reconnectDelay <= 0 {
		client.reconnectDelay = durationFromEnv("FEATURE_FLAG_RECONNECT_INTERVAL", durationFromEnv("FEATURE_FLAG_POLL_INTERVAL", defaultReconnectInterval))
	}

	go client.run()

	if !options.RequireInitialState {
		return client, nil
	}

	waitContext, waitCancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer waitCancel()
	if err := client.WaitReady(waitContext); err != nil {
		client.Stop()
		if errors.Is(err, context.DeadlineExceeded) {
			if lastErr := client.lastError(); lastErr != nil {
				return nil, fmt.Errorf("feature: FeatureHub did not become ready before timeout: %w", lastErr)
			}
			return nil, fmt.Errorf("feature: FeatureHub did not become ready before timeout")
		}
		return nil, fmt.Errorf("feature: initial FeatureHub stream failed: %w", err)
	}
	return client, nil
}

// normalizeFeatureHubURL mirrors the JavaScript SDK constructor, which accepts
// either the edge host or a URL ending in /features.
func normalizeFeatureHubURL(serverURL string) string {
	normalized := strings.TrimRight(strings.TrimSpace(serverURL), "/")
	normalized = strings.TrimSuffix(normalized, "/features")
	return strings.TrimRight(normalized, "/")
}

// Ready reports whether the latest stream state is ready. Cached feature
// values remain available during reconnects.
func (c *Client) Ready() bool {
	if c != nil && c.legacy != nil {
		return true
	}
	return c != nil && c.ready.Load()
}

// ClientEvaluated reports whether the FeatureHub API key requests the complete
// feature set for local strategy evaluation. FeatureHub denotes these keys with
// an asterisk.
func (c *Client) ClientEvaluated() bool {
	return c != nil && c.clientEvaluated
}

// WaitReady waits until the latest server-evaluated context has received a
// full feature set. Client-evaluated context changes are immediately visible
// once the shared feature repository is ready.
func (c *Client) WaitReady(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.legacy != nil {
		return nil
	}
	targetRevision := c.contextRevision.Load()
	for {
		if c.ready.Load() && (c.clientEvaluated || c.readyRevision.Load() >= targetRevision) {
			return nil
		}
		readySignal := c.currentReadySignal()
		if c.ready.Load() && (c.clientEvaluated || c.readyRevision.Load() >= targetRevision) {
			return nil
		}
		select {
		case <-readySignal:
		case <-c.terminalFailure:
			if err := c.lastError(); err != nil {
				return err
			}
			return errors.New("FeatureHub stream terminated before initial state")
		case <-ctx.Done():
			if lastErr := c.lastError(); lastErr != nil {
				return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
}

// IsEnabled reports whether a BOOLEAN feature evaluates to true.
func (c *Client) IsEnabled(key string) bool {
	if c != nil && c.legacy != nil {
		return legacyBoolean(c.legacy.getValue(key))
	}
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeBoolean {
		return false
	}
	boolean, ok := castBoolean(value)
	return ok && boolean
}

// GetValue evaluates a feature and returns its typed value. JSON feature values
// are decoded to Go values, matching FeatureHub's generic value accessor.
func (c *Client) GetValue(key string) interface{} {
	if c != nil && c.legacy != nil {
		return c.legacy.getValue(key)
	}
	value, valueType, ok := c.evaluatedValue(key)
	if !ok {
		return nil
	}
	return castFeatureValue(valueType, value, true)
}

// GetString returns a STRING feature value.
func (c *Client) GetString(key string) (string, bool) {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeString || value == nil {
		return "", false
	}
	return fmt.Sprint(value), true
}

// GetNumber returns a NUMBER feature value.
func (c *Client) GetNumber(key string) (float64, bool) {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeNumber {
		return 0, false
	}
	number, ok := castNumber(value)
	return number, ok
}

// GetRawJSON returns a JSON feature value without decoding it.
func (c *Client) GetRawJSON(key string) (string, bool) {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeJSON || value == nil {
		return "", false
	}
	if raw, ok := value.(string); ok {
		return raw, true
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

// SetUserKey sets the user-key rollout context.
func (c *Client) SetUserKey(key string) {
	if c != nil && c.legacy != nil {
		c.legacy.setUserKey(key)
		return
	}
	c.setAttributeValues("userkey", []string{key})
}

// SetSessionKey sets the session-key rollout context. FeatureHub gives the
// session key precedence over the user key for default percentage rollouts.
func (c *Client) SetSessionKey(key string) {
	if c != nil && c.legacy != nil {
		c.legacy.setSessionKey(key)
		return
	}
	c.setAttributeValues("session", []string{key})
}

// SetCountry sets the standard FeatureHub country context.
func (c *Client) SetCountry(country string) {
	c.setAttributeValues("country", []string{country})
}

// SetDevice sets the standard FeatureHub device context.
func (c *Client) SetDevice(device string) {
	c.setAttributeValues("device", []string{device})
}

// SetPlatform sets the standard FeatureHub platform context.
func (c *Client) SetPlatform(platform string) {
	c.setAttributeValues("platform", []string{platform})
}

// SetVersion sets the standard FeatureHub semantic-version context.
func (c *Client) SetVersion(version string) {
	c.setAttributeValues("version", []string{version})
}

// SetAttribute sets one custom rollout context value.
func (c *Client) SetAttribute(key string, value interface{}) {
	c.setAttributeValues(key, []string{fmt.Sprint(value)})
}

// SetAttributeValues sets multiple custom context values. A strategy attribute
// matches when any supplied value matches, as in the JavaScript and Python SDKs.
func (c *Client) SetAttributeValues(key string, values []string) {
	c.setAttributeValues(key, values)
}

// ResetContext clears runtime context changes and restores the service/version
// attributes populated during initialization.
func (c *Client) ResetContext() {
	if c == nil {
		return
	}
	if c.legacy != nil {
		c.legacy.resetContext()
		return
	}
	c.replaceAttributes(c.defaults)
}

// Stop terminates the active SSE request and reconnect loop. It is idempotent.
func (c *Client) Stop() {
	if c == nil {
		return
	}
	if c.legacy != nil {
		c.legacy.stop()
		return
	}
	c.stopOnce.Do(func() {
		c.cancel()
		c.clearStaleRetention()
		<-c.done
	})
}

func (c *legacyClient) getValue(key string) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.flags[key]
}

func (c *legacyClient) setUserKey(key string) {
	c.mu.Lock()
	c.userKey = key
	c.mu.Unlock()
	_ = c.refresh()
}

func (c *legacyClient) setSessionKey(key string) {
	c.mu.Lock()
	c.session = key
	c.mu.Unlock()
	_ = c.refresh()
}

func (c *legacyClient) resetContext() {
	c.mu.Lock()
	c.userKey = ""
	c.session = ""
	c.mu.Unlock()
	_ = c.refresh()
}

func (c *legacyClient) snapshotFor(ctx context.Context, attributes map[string][]string) (map[string]interface{}, error) {
	if c == nil {
		return map[string]interface{}{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := c.snapshotTimeout
	if timeout <= 0 {
		timeout = defaultInitTimeout
	}
	requestContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	first := func(key string) string {
		if values := attributes[key]; len(values) > 0 {
			return values[0]
		}
		return ""
	}
	return c.fetchContext(requestContext, first("userkey"), first("session"))
}

func (c *legacyClient) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped {
		close(c.stopCh)
		c.stopped = true
	}
}

func (c *legacyClient) poll() {
	interval := 30 * time.Second
	if raw := os.Getenv("FEATURE_FLAG_POLL_INTERVAL"); raw != "" {
		if parsed, err := time.ParseDuration(raw + "s"); err == nil && parsed > 0 {
			interval = parsed
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

func (c *legacyClient) refresh() error {
	c.mu.RLock()
	userKey, session := c.userKey, c.session
	c.mu.RUnlock()
	flags, err := c.fetch(userKey, session)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.flags = flags
	c.mu.Unlock()
	return nil
}

func (c *legacyClient) fetch(userKey, session string) (map[string]interface{}, error) {
	return c.fetchContext(context.Background(), userKey, session)
}

func (c *legacyClient) fetchContext(ctx context.Context, userKey, session string) (map[string]interface{}, error) {
	c.mu.RLock()
	serverURL, apiKey, client := c.url, c.apiKey, c.client
	c.mu.RUnlock()
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL+"/features", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Api-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")
	if userKey != "" {
		req.Header.Set("X-User-Key", userKey)
	}
	if session != "" {
		req.Header.Set("X-Session-Key", session)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("feature flag fetch failed: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("feature flag read body failed: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("feature flag server returned %d: %s", response.StatusCode, string(body))
	}
	var flags map[string]interface{}
	if err := json.Unmarshal(body, &flags); err != nil {
		var features []map[string]interface{}
		if arrayErr := json.Unmarshal(body, &features); arrayErr != nil {
			return nil, fmt.Errorf("feature flag parse failed: %w", err)
		}
		flags = make(map[string]interface{}, len(features))
		for _, feature := range features {
			if key, ok := feature["key"].(string); ok {
				flags[key] = feature["value"]
			}
		}
	}
	return flags, nil
}

func (c *Client) setAttributeValues(key string, values []string) {
	// Custom FeatureHub attributes are an advanced-client capability. Clients
	// returned by the compatibility Init path delegate only the original
	// user/session context contract and intentionally ignore additive context
	// setters instead of dereferencing their uninitialised advanced state.
	if c == nil || c.legacy != nil || strings.TrimSpace(key) == "" {
		return
	}
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		cleaned = append(cleaned, value)
	}
	c.mu.Lock()
	previous := c.attributes[key]
	if len(cleaned) == 0 {
		delete(c.attributes, key)
	} else {
		c.attributes[key] = cleaned
	}
	changed := !stringSlicesEqual(previous, cleaned)
	if changed && !c.clientEvaluated {
		// Keep the attribute update, revision change, and readiness invalidation
		// in one critical section with feature-event application. Otherwise a
		// buffered event from the previous stream can be applied between the
		// attribute write and revision increment and publish stale values for the
		// new server-evaluated context.
		c.contextRevision.Add(1)
		c.ready.Store(false)
	}
	c.mu.Unlock()
	if changed {
		c.notifyContextChanged()
	}
}

func (c *Client) evaluatedValue(key string) (interface{}, string, bool) {
	if c == nil {
		return nil, "", false
	}
	if c.legacy != nil {
		value := c.legacy.getValue(key)
		return value, featureValueType(value), value != nil
	}
	c.mu.RLock()
	feature, ok := c.features[key]
	attributes := cloneAttributes(c.attributes)
	c.mu.RUnlock()
	if !ok || feature == nil {
		return nil, "", false
	}
	value := feature.Value
	if c.clientEvaluated {
		if strategyValue, matched := applyStrategies(feature, attributes, time.Now().UTC()); matched {
			value = strategyValue
		}
	}
	return value, feature.Type, value != nil
}

func (c *Client) evaluatedValueFor(key string, attributes map[string][]string) (interface{}, string, bool) {
	if c == nil {
		return nil, "", false
	}
	if c.legacy != nil {
		value := c.legacy.getValue(key)
		return value, featureValueType(value), value != nil
	}
	c.mu.RLock()
	feature, ok := c.features[key]
	c.mu.RUnlock()
	if !ok || feature == nil {
		return nil, "", false
	}
	value := feature.Value
	if c.clientEvaluated {
		if strategyValue, matched := applyStrategies(feature, attributes, time.Now().UTC()); matched {
			value = strategyValue
		}
	}
	return value, feature.Type, value != nil
}

func featureValueType(value interface{}) string {
	switch value.(type) {
	case bool:
		return featureTypeBoolean
	case string:
		return featureTypeString
	case float32, float64, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, json.Number:
		return featureTypeNumber
	case nil:
		return ""
	default:
		return featureTypeJSON
	}
}

func (c *Client) replaceAttributes(attributes map[string][]string) {
	if c == nil || c.legacy != nil {
		return
	}
	c.mu.Lock()
	changed := !attributeMapsEqual(c.attributes, attributes)
	if changed {
		c.attributes = cloneAttributes(attributes)
		if !c.clientEvaluated {
			c.contextRevision.Add(1)
			c.ready.Store(false)
		}
	}
	c.mu.Unlock()
	if changed {
		c.notifyContextChanged()
	}
}

// notifyContextChanged wakes the stream loop after the context state and its
// revision have been updated atomically under c.mu.
func (c *Client) notifyContextChanged() {
	if c == nil || c.clientEvaluated {
		return
	}
	select {
	case c.refresh <- struct{}{}:
	default:
	}
	if debugFeatureLogs() {
		slog.Debug("fit/feature: server-evaluated context changed; reconnecting")
	}
}

func (c *Client) run() {
	defer close(c.done)
	retryAttempt := 0
	// A server-directed stale window allows one immediate context-change
	// interrupt. Later changes are coalesced until the window ends; they are
	// already recorded in c.attributes, so the next request sends the latest
	// context without turning rapid SetUserKey calls into a reconnect storm.
	// staleRegimeDelay is the latest edge.stale delay while the edge keeps
	// replying stale; zero means no stale regime is active.
	var staleWindowEnd time.Time
	var staleRegimeDelay time.Duration
	staleInterrupted := false
	for {
		revision := c.contextRevision.Load()
		featureEventsBefore := c.featureEvents.Load()
		streamContext, streamCancel := context.WithCancel(c.ctx)
		type streamResult struct {
			permanent bool
			err       error
		}
		resultChannel := make(chan streamResult, 1)
		go func() {
			permanent, err := c.consumeStream(streamContext, revision)
			resultChannel <- streamResult{permanent: permanent, err: err}
		}()

		var result streamResult
		refreshed := false
		select {
		case result = <-resultChannel:
		case <-c.refresh:
			refreshed = true
			streamCancel()
			result = <-resultChannel
			draining := true
			for draining {
				select {
				case <-c.refresh:
				default:
					draining = false
				}
			}
		case <-c.ctx.Done():
			streamCancel()
			<-resultChannel
			return
		}
		streamCancel()
		if c.ctx.Err() != nil {
			return
		}

		featureEventReceived := c.featureEvents.Load() != featureEventsBefore
		if featureEventReceived {
			// Only feature-state events prove useful stream progress; they reset
			// outage backoff and end any server-directed stale window.
			retryAttempt = 0
			staleWindowEnd = time.Time{}
			staleRegimeDelay = 0
			staleInterrupted = false
		}
		if refreshed && retryAttempt == 0 {
			if staleRegimeDelay <= 0 {
				continue
			}
			// The edge is still in a stale regime. A context change that cancels
			// an in-flight request is that window's single interrupt; once it has
			// been used, later changes wait for the window to end below.
			if now := time.Now(); !now.Before(staleWindowEnd) {
				staleWindowEnd = now.Add(staleRegimeDelay)
				staleInterrupted = true
				continue
			}
			if !staleInterrupted {
				staleInterrupted = true
				continue
			}
		}

		// A refresh-cancelled stream reports only its own cancellation; it is
		// not a stream failure and must not alter readiness or backoff.
		permanent, err := result.permanent, result.err
		var stale *edgeStaleError
		isStale := !refreshed && errors.As(err, &stale)
		if isStale {
			staleRegimeDelay = stale.delay
			now := time.Now()
			if staleWindowEnd.IsZero() || !now.Before(staleWindowEnd) {
				staleWindowEnd = now.Add(stale.delay)
				staleInterrupted = false
			}
		}
		if !refreshed && !isStale {
			staleWindowEnd = time.Time{}
			staleRegimeDelay = 0
			staleInterrupted = false
		}
		if !refreshed && err != nil {
			c.setLastError(err)
			if isStale && c.ready.Load() {
				c.retainReadyDuringStaleWindow()
			} else {
				c.ready.Store(false)
			}
			if permanent {
				// A permanent response is terminal regardless of whether the
				// stream was previously ready. Wake every current and future
				// waiter with lastStreamingErr instead of leaving a post-ready
				// failure indistinguishable from a reconnect in progress.
				c.failTerminal()
				slog.Error("fit/feature: FeatureHub stream stopped after a permanent response", "error", err.Error())
				return
			}
			if debugFeatureLogs() {
				slog.Warn("fit/feature: FeatureHub stream disconnected; reconnecting")
			}
			if !isStale && !featureEventReceived {
				retryAttempt++
			}
		}

		delay := c.retryDelayForAttempt(err, retryAttempt)
		if staleInterrupted && time.Now().Before(staleWindowEnd) {
			delay = time.Until(staleWindowEnd)
		}
		timer := time.NewTimer(delay)
		waiting := true
		for waiting {
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-c.refresh:
				// Context changes during an outage update the next request but do
				// not bypass the shared failure backoff. A stale wait may be
				// interrupted once per stale window.
				if isStale && !staleInterrupted {
					staleInterrupted = true
					timer.Stop()
					waiting = false
				}
			case <-timer.C:
				// An ended stale window is replaced by the next stale reply or by
				// the next context-change interrupt.
				waiting = false
			}
		}
	}
}

func (c *Client) consumeStream(ctx context.Context, revision uint64) (bool, error) {
	req, err := c.newStreamRequest(ctx, nil)
	if err != nil {
		return true, featureHubRequestError{}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, featureHubRequestError{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return isPermanentFeatureHTTPStatus(resp.StatusCode), &featureHubHTTPError{
			status: resp.StatusCode, retryAfter: featureRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		}
	}

	scanner := newFeatureSSEScanner(resp.Body)
	var eventName string
	var data []string
	dispatch := func() error {
		if eventName == "" && len(data) == 0 {
			return nil
		}
		err := c.handleEvent(eventName, strings.Join(data, "\n"), revision)
		eventName = ""
		data = data[:0]
		return err
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return false, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			field, value = line, ""
		} else {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "event":
			eventName = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	// EOF does not terminate an SSE event. Only a blank line dispatches the
	// accumulated fields; discard an unfinished event when the stream closes.
	return false, io.EOF
}

type featureHubRequestError struct{}

func (e featureHubRequestError) Error() string { return "FeatureHub stream request failed" }

func isPermanentFeatureHTTPStatus(status int) bool {
	if status < http.StatusBadRequest || status >= http.StatusInternalServerError {
		return false
	}
	switch status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, 425: // Too Early
		return false
	default:
		return true
	}
}

func (c *Client) newStreamRequest(ctx context.Context, attributes map[string][]string) (*http.Request, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/features/"+c.apiKey, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if !c.clientEvaluated {
		if attributes == nil {
			c.mu.RLock()
			attributes = cloneAttributes(c.attributes)
			c.mu.RUnlock()
		}
		header := featureHubContextHeader(attributes)
		if header != "" {
			req.Header.Set("x-featurehub", header)
			query := req.URL.Query()
			query.Set("xfeaturehub", header)
			req.URL.RawQuery = query.Encode()
		}
	}
	return req, nil
}

func (c *Client) evaluatedFeaturesForContext(ctx context.Context, attributes map[string][]string) (map[string]*featureState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := c.snapshotTimeout
	if timeout <= 0 {
		timeout = defaultInitTimeout
	}
	var requestContext context.Context
	var cancel context.CancelFunc
	if _, bounded := ctx.Deadline(); bounded {
		requestContext, cancel = context.WithCancel(ctx)
	} else {
		requestContext, cancel = context.WithTimeout(ctx, timeout)
	}
	stopClientCancel := func() bool { return false }
	if c.ctx != nil {
		stopClientCancel = context.AfterFunc(c.ctx, cancel)
	}
	defer func() {
		stopClientCancel()
		cancel()
	}()
	features, _, err := c.consumeEvaluationSnapshot(requestContext, attributes)
	if err != nil && requestContext.Err() != nil {
		return nil, requestContext.Err()
	}
	return features, err
}

func (c *Client) consumeEvaluationSnapshot(ctx context.Context, attributes map[string][]string) (map[string]*featureState, bool, error) {
	req, err := c.newStreamRequest(ctx, attributes)
	if err != nil {
		return nil, true, featureHubRequestError{}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, featureHubRequestError{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, isPermanentFeatureHTTPStatus(resp.StatusCode), &featureHubHTTPError{
			status: resp.StatusCode, retryAfter: featureRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		}
	}

	scanner := newFeatureSSEScanner(resp.Body)
	var eventName string
	var data []string
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			if strings.HasPrefix(line, ":") {
				continue
			}
			field, value, found := strings.Cut(line, ":")
			if found {
				value = strings.TrimPrefix(value, " ")
			}
			switch field {
			case "event":
				eventName = value
			case "data":
				data = append(data, value)
			}
			continue
		}

		payload := strings.Join(data, "\n")
		switch eventName {
		case "features":
			var states []*featureState
			if err := json.Unmarshal([]byte(payload), &states); err != nil {
				return nil, false, fmt.Errorf("decode FeatureHub features event: %w", err)
			}
			features := make(map[string]*featureState, len(states))
			for _, state := range states {
				if state != nil && state.Key != "" {
					features[state.Key] = state
				}
			}
			return features, false, nil
		case "failure", "error":
			return nil, false, fmt.Errorf("FeatureHub emitted %s", eventName)
		case "config":
			var config map[string]json.RawMessage
			if err := json.Unmarshal([]byte(payload), &config); err == nil {
				if raw, ok := config["edge.stale"]; ok {
					seconds, stale, parseErr := edgeStaleSeconds(raw)
					if parseErr == nil && stale {
						return nil, false, &edgeStaleError{delay: c.staleDelay(seconds)}
					}
				}
			}
		case "bye":
			return nil, false, io.EOF
		}
		eventName = ""
		data = data[:0]
	}
	if err := scanner.Err(); err != nil {
		return nil, false, err
	}
	return nil, false, io.EOF
}

func (c *Client) handleEvent(name, data string, revision uint64) error {
	switch name {
	case "", "ack":
		return nil
	case "bye":
		return io.EOF
	case "failure", "error":
		return fmt.Errorf("FeatureHub emitted %s", name)
	case "config":
		var config map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &config); err != nil {
			return fmt.Errorf("decode FeatureHub config event: %w", err)
		}
		if raw, ok := config["edge.stale"]; ok {
			seconds, stale, err := edgeStaleSeconds(raw)
			if err != nil {
				return fmt.Errorf("decode FeatureHub edge.stale: %w", err)
			}
			if stale {
				return &edgeStaleError{delay: c.staleDelay(seconds)}
			}
			return nil
		}
	case "features":
		var features []*featureState
		if err := json.Unmarshal([]byte(data), &features); err != nil {
			return fmt.Errorf("decode FeatureHub features event: %w", err)
		}
		c.applyFullFeatureSet(features, revision)
	case "feature":
		var feature featureState
		if err := json.Unmarshal([]byte(data), &feature); err != nil {
			return fmt.Errorf("decode FeatureHub feature event: %w", err)
		}
		c.applyFeature(&feature, revision)
	case "delete_feature":
		var feature featureState
		if err := json.Unmarshal([]byte(data), &feature); err != nil {
			return fmt.Errorf("decode FeatureHub delete event: %w", err)
		}
		c.deleteFeature(&feature, revision)
	}
	return nil
}

func (c *Client) staleDelay(seconds float64) time.Duration {
	minimum := c.retryDelay(nil)
	if minimum > maxReconnectInterval {
		minimum = maxReconnectInterval
	}
	if math.IsNaN(seconds) || seconds <= 0 {
		return minimum
	}
	if math.IsInf(seconds, 1) || seconds >= maxReconnectInterval.Seconds() {
		return maxReconnectInterval
	}
	delay := time.Duration(seconds * float64(time.Second))
	if delay < minimum {
		return minimum
	}
	return delay
}

// edgeStaleSeconds mirrors the useful FeatureHub JavaScript SDK coercions
// without inheriting JavaScript's zero-delay loop for malformed/truthy values.
// Numeric zero, negative values, false, null, and empty strings do not mark a
// healthy stream stale. A true value is equivalent to one second, as it is in
// JavaScript multiplication, and positive numeric strings are accepted.
func edgeStaleSeconds(raw json.RawMessage) (float64, bool, error) {
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false, err
	}
	switch typed := value.(type) {
	case nil:
		return 0, false, nil
	case bool:
		if typed {
			return 1, true, nil
		}
		return 0, false, nil
	case float64:
		if typed <= 0 {
			return 0, false, nil
		}
		return typed, true, nil
	case string:
		if strings.TrimSpace(typed) == "" {
			return 0, false, nil
		}
		seconds, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false, err
		}
		// ParseFloat accepts "NaN" and "Inf"; neither is a usable delay.
		if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
			return 0, false, errors.New("non-finite numeric value")
		}
		if seconds <= 0 {
			return 0, false, nil
		}
		return seconds, true, nil
	default:
		return 0, false, fmt.Errorf("unsupported %T value", value)
	}
}

func (c *Client) retryDelay(err error) time.Duration {
	delay := c.reconnectDelay
	var stale *edgeStaleError
	if errors.As(err, &stale) && stale.delay > 0 {
		delay = stale.delay
	}
	if delay <= 0 {
		delay = defaultReconnectInterval
	}
	return delay
}

func (c *Client) retryDelayForAttempt(err error, attempt int) time.Duration {
	delay := c.retryDelay(err)
	var stale *edgeStaleError
	if errors.As(err, &stale) {
		// edge.stale is server-directed pacing already capped by staleDelay; it
		// is not mixed with outage backoff or jitter.
		return delay
	}
	maximum := maxReconnectInterval
	if delay > maximum {
		// A configured base interval is a lower bound even when it exceeds the
		// package's normal backoff ceiling.
		maximum = delay
	}
	for index := 1; index < attempt && delay < maximum; index++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	jitter := c.retryJitter
	if jitter == nil {
		jitter = equalRetryJitter
	}
	if jittered := jitter(delay); jittered > 0 {
		delay = jittered
	}
	var httpErr *featureHubHTTPError
	if errors.As(err, &httpErr) && httpErr.retryAfter > delay {
		// featureRetryAfter already caps the server hint at 30s.
		delay = httpErr.retryAfter
		if delay > maximum {
			delay = maximum
		}
	}
	return delay
}

// equalRetryJitter keeps retry delays in the upper half of the exponential
// window so many clients recovering from one edge outage do not reconnect in
// lockstep.
func equalRetryJitter(delay time.Duration) time.Duration {
	if delay <= time.Nanosecond {
		return delay
	}
	half := delay / 2
	span := delay - half
	if span <= time.Nanosecond {
		return delay
	}
	return half + time.Duration(mathrand.Int64N(int64(span)))
}

func featureRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseUint(value, 10, 64); err == nil {
		if seconds >= uint64(maxReconnectInterval/time.Second) {
			return maxReconnectInterval
		}
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		delay := retryAt.Sub(now)
		if delay <= 0 {
			return 0
		}
		if delay > maxReconnectInterval {
			return maxReconnectInterval
		}
		return delay
	}
	// A syntactically numeric value larger than uint64 represents a very long
	// server delay; cap it instead of accidentally treating it as no guidance.
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0
		}
	}
	return maxReconnectInterval
}

// retainReadyDuringStaleWindow keeps a ready snapshot available while the
// edge reports edge.stale. The expiry deadline is fixed by the first stale
// notice of a window: later stale replies never extend it, so a continuously
// stale edge cannot keep a snapshot ready for longer than the retention bound.
// Only a features/feature/delete_feature event (clearStaleRetention) ends the
// window.
func (c *Client) retainReadyDuringStaleWindow() {
	window := c.staleRetention
	if window <= 0 || window > maxRetainedSnapshotAge {
		window = maxRetainedSnapshotAge
	}
	c.staleTimerMu.Lock()
	defer c.staleTimerMu.Unlock()
	if c.ctx == nil || c.ctx.Err() != nil {
		return
	}
	if c.staleTimer != nil {
		return
	}
	c.staleGeneration++
	generation := c.staleGeneration
	c.staleTimer = time.AfterFunc(window, func() {
		c.staleTimerMu.Lock()
		defer c.staleTimerMu.Unlock()
		if generation != c.staleGeneration || c.ctx == nil || c.ctx.Err() != nil {
			return
		}
		c.staleTimer = nil
		c.setLastError(errors.New("FeatureHub cached snapshot expired while the edge remained stale"))
		c.ready.Store(false)
		c.signalReadyChange()
	})
}

func (c *Client) clearStaleRetention() {
	c.staleTimerMu.Lock()
	c.staleGeneration++
	if c.staleTimer != nil {
		c.staleTimer.Stop()
		c.staleTimer = nil
	}
	c.staleTimerMu.Unlock()
}

func (c *Client) applyFullFeatureSet(features []*featureState, revision uint64) bool {
	incoming := make(map[string]struct{}, len(features))
	c.mu.Lock()
	if c.contextRevision.Load() != revision {
		c.mu.Unlock()
		return false
	}
	for _, feature := range features {
		if feature == nil || feature.Key == "" {
			continue
		}
		incoming[feature.Key] = struct{}{}
		if current := c.features[feature.Key]; current == nil || featureVersion(feature) >= featureVersion(current) {
			c.features[feature.Key] = feature
		}
	}
	for key := range c.features {
		if _, exists := incoming[key]; !exists {
			delete(c.features, key)
		}
	}
	// Clear a stale-expiry timer before publishing readiness. Holding c.mu keeps
	// this operation ordered with a concurrent context revision change; the
	// timer's generation check prevents a callback already in flight from
	// invalidating the newly accepted snapshot.
	c.clearStaleRetention()
	c.featureEvents.Add(1)
	c.readyRevision.Store(revision)
	c.ready.Store(true)
	c.receivedInitial.Store(true)
	c.setLastError(nil)
	c.mu.Unlock()
	c.signalReadyChange()
	return true
}

func (c *Client) applyFeature(feature *featureState, revision uint64) bool {
	if feature == nil || feature.Key == "" {
		return false
	}
	c.mu.Lock()
	if c.contextRevision.Load() != revision {
		c.mu.Unlock()
		return false
	}
	if current := c.features[feature.Key]; current == nil || featureVersion(feature) >= featureVersion(current) {
		c.features[feature.Key] = feature
	}
	c.clearStaleRetention()
	c.featureEvents.Add(1)
	c.mu.Unlock()
	return true
}

func (c *Client) deleteFeature(feature *featureState, revision uint64) bool {
	if feature == nil || feature.Key == "" {
		return false
	}
	c.mu.Lock()
	if c.contextRevision.Load() != revision {
		c.mu.Unlock()
		return false
	}
	current := c.features[feature.Key]
	if current == nil || feature.Version == nil || *feature.Version == 0 || *feature.Version >= featureVersion(current) {
		delete(c.features, feature.Key)
	}
	c.clearStaleRetention()
	c.featureEvents.Add(1)
	c.mu.Unlock()
	return true
}

func (c *Client) failTerminal() {
	c.terminalFailOnce.Do(func() { close(c.terminalFailure) })
}

func (c *Client) signalReadyChange() {
	c.readySignalMu.Lock()
	close(c.readySignal)
	c.readySignal = make(chan struct{})
	c.readySignalMu.Unlock()
}

func (c *Client) currentReadySignal() <-chan struct{} {
	c.readySignalMu.Lock()
	defer c.readySignalMu.Unlock()
	return c.readySignal
}

func (c *Client) setLastError(err error) {
	c.lastErrorMu.Lock()
	c.lastStreamingErr = err
	c.lastErrorMu.Unlock()
}

func (c *Client) lastError() error {
	c.lastErrorMu.RLock()
	defer c.lastErrorMu.RUnlock()
	return c.lastStreamingErr
}

func defaultAttributes(configured map[string][]string) map[string][]string {
	attributes := cloneAttributes(configured)
	serviceName := strings.TrimSpace(os.Getenv("SERVICE_NAME"))
	if serviceName == "" {
		serviceName = legacyDefaultServiceName()
	}
	if serviceName != "" && len(attributes["service_name"]) == 0 {
		attributes["service_name"] = []string{serviceName}
	}
	if platformVersion := strings.TrimSpace(os.Getenv("PLATFORM_VERSION")); platformVersion != "" {
		if len(attributes["platform_version"]) == 0 {
			attributes["platform_version"] = []string{strings.Split(platformVersion, "-")[0]}
		}
		if len(attributes["release_candidate_version"]) == 0 {
			attributes["release_candidate_version"] = []string{platformVersion}
		}
	}
	return attributes
}

func cloneAttributes(source map[string][]string) map[string][]string {
	if source == nil {
		return make(map[string][]string)
	}
	cloned := make(map[string][]string, len(source))
	for key, values := range source {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func attributeMapsEqual(left, right map[string][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, values := range left {
		if !stringSlicesEqual(values, right[key]) {
			return false
		}
	}
	return true
}

func durationFromEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	if duration, err := time.ParseDuration(value); err == nil && duration > 0 {
		return duration
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second))
	}
	return fallback
}

func debugFeatureLogs() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("ENABLE_FEATURE_FLAG_DEBUG_LOGS")), "true")
}

func boolFromEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
