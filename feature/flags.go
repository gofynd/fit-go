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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	maxSSEEventSize          = 16 << 20
)

type edgeStaleError struct {
	delay time.Duration
}

func (e *edgeStaleError) Error() string {
	return fmt.Sprintf("FeatureHub edge marked the stream stale for %s", e.delay)
}

// Client maintains a FeatureHub cache populated from the edge server's SSE
// endpoint. API keys containing an asterisk receive the complete feature set
// and evaluate rollout strategies locally. Other keys reconnect with an
// x-featurehub context and consume values evaluated by the edge server.
type Client struct {
	mu              sync.RWMutex
	url             string
	apiKey          string
	clientEvaluated bool
	features        map[string]*featureState
	attributes      map[string][]string
	defaults        map[string][]string
	httpClient      *http.Client
	reconnectDelay  time.Duration
	refresh         chan struct{}

	ctx              context.Context
	cancel           context.CancelFunc
	done             chan struct{}
	readySignalMu    sync.Mutex
	readySignal      chan struct{}
	terminalFailure  chan struct{}
	initialFailOnce  sync.Once
	stopOnce         sync.Once
	receivedInitial  atomic.Bool
	ready            atomic.Bool
	contextRevision  atomic.Uint64
	readyRevision    atomic.Uint64
	lastErrorMu      sync.RWMutex
	lastStreamingErr error
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

// Init creates and starts a FeatureHub client. It returns nil when feature
// flags are disabled or optional configuration is incomplete. Set
// FEATURE_FLAG_REQUIRE_INITIAL_STATE=true to make FeatureHub a startup
// dependency and wait for the initial full "features" SSE event.
func Init() (*Client, error) {
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
	client := &Client{
		url:             normalizeFeatureHubURL(serverURL),
		apiKey:          strings.TrimLeft(apiKey, "/"),
		clientEvaluated: strings.Contains(apiKey, "*"),
		features:        make(map[string]*featureState),
		attributes:      cloneAttributes(defaults),
		defaults:        defaults,
		httpClient:      &http.Client{},
		reconnectDelay:  options.ReconnectInterval,
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

	initTimeout := options.InitTimeout
	if initTimeout <= 0 {
		initTimeout = durationFromEnv("FEATURE_FLAG_INIT_TIMEOUT", defaultInitTimeout)
	}
	waitContext, waitCancel := context.WithTimeout(context.Background(), initTimeout)
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
			return ctx.Err()
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
}

// IsEnabled reports whether a BOOLEAN feature evaluates to true.
func (c *Client) IsEnabled(key string) bool {
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
	c.setAttributeValues("userkey", []string{key})
}

// SetSessionKey sets the session-key rollout context. FeatureHub gives the
// session key precedence over the user key for default percentage rollouts.
func (c *Client) SetSessionKey(key string) {
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
	c.replaceAttributes(c.defaults)
}

// Stop terminates the active SSE request and reconnect loop. It is idempotent.
func (c *Client) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		c.cancel()
		<-c.done
	})
}

func (c *Client) setAttributeValues(key string, values []string) {
	if c == nil || strings.TrimSpace(key) == "" {
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
	c.mu.Unlock()
	if changed {
		c.contextChanged()
	}
}

func (c *Client) evaluatedValue(key string) (interface{}, string, bool) {
	if c == nil {
		return nil, "", false
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

func (c *Client) replaceAttributes(attributes map[string][]string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	changed := !attributeMapsEqual(c.attributes, attributes)
	if changed {
		c.attributes = cloneAttributes(attributes)
	}
	c.mu.Unlock()
	if changed {
		c.contextChanged()
	}
}

func (c *Client) contextChanged() {
	if c == nil || c.clientEvaluated {
		return
	}
	c.contextRevision.Add(1)
	c.ready.Store(false)
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
	for {
		revision := c.contextRevision.Load()
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
		if refreshed {
			continue
		}

		permanent, err := result.permanent, result.err
		if err != nil {
			c.setLastError(err)
			c.ready.Store(false)
			if permanent && !c.receivedInitial.Load() {
				c.failInitial()
				return
			}
			if debugFeatureLogs() {
				slog.Warn("fit/feature: FeatureHub stream disconnected; reconnecting")
			}
		}

		delay := c.reconnectDelay
		var stale *edgeStaleError
		if errors.As(err, &stale) {
			delay = stale.delay
		}
		timer := time.NewTimer(delay)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-c.refresh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (c *Client) consumeStream(ctx context.Context, revision uint64) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/features/"+c.apiKey, nil)
	if err != nil {
		return true, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if !c.clientEvaluated {
		c.mu.RLock()
		header := featureHubContextHeader(c.attributes)
		c.mu.RUnlock()
		if header != "" {
			req.Header.Set("x-featurehub", header)
			query := req.URL.Query()
			query.Set("xfeaturehub", header)
			req.URL.RawQuery = query.Encode()
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return resp.StatusCode >= 400 && resp.StatusCode < 500,
			fmt.Errorf("FeatureHub returned HTTP %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), maxSSEEventSize)
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
				return isPermanentStreamError(err), err
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
	if err := dispatch(); err != nil {
		return isPermanentStreamError(err), err
	}
	return false, io.EOF
}

func isPermanentStreamError(err error) bool {
	var stale *edgeStaleError
	return !errors.As(err, &stale)
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
			var seconds float64
			if err := json.Unmarshal(raw, &seconds); err != nil {
				var stale bool
				if boolErr := json.Unmarshal(raw, &stale); boolErr != nil {
					return fmt.Errorf("decode FeatureHub edge.stale: %w", err)
				}
				if stale {
					seconds = c.reconnectDelay.Seconds()
				}
			}
			if seconds > 0 {
				return &edgeStaleError{delay: time.Duration(seconds * float64(time.Second))}
			}
		}
	case "features":
		var features []*featureState
		if err := json.Unmarshal([]byte(data), &features); err != nil {
			return fmt.Errorf("decode FeatureHub features event: %w", err)
		}
		c.applyFullFeatureSet(features)
		c.markReady(revision)
	case "feature":
		var feature featureState
		if err := json.Unmarshal([]byte(data), &feature); err != nil {
			return fmt.Errorf("decode FeatureHub feature event: %w", err)
		}
		c.applyFeature(&feature)
	case "delete_feature":
		var feature featureState
		if err := json.Unmarshal([]byte(data), &feature); err != nil {
			return fmt.Errorf("decode FeatureHub delete event: %w", err)
		}
		c.deleteFeature(&feature)
	}
	return nil
}

func (c *Client) applyFullFeatureSet(features []*featureState) {
	incoming := make(map[string]struct{}, len(features))
	c.mu.Lock()
	defer c.mu.Unlock()
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
}

func (c *Client) applyFeature(feature *featureState) {
	if feature == nil || feature.Key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if current := c.features[feature.Key]; current == nil || featureVersion(feature) >= featureVersion(current) {
		c.features[feature.Key] = feature
	}
}

func (c *Client) deleteFeature(feature *featureState) {
	if feature == nil || feature.Key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.features[feature.Key]
	if current == nil || feature.Version == nil || *feature.Version == 0 || *feature.Version >= featureVersion(current) {
		delete(c.features, feature.Key)
	}
}

func (c *Client) failInitial() {
	c.initialFailOnce.Do(func() { close(c.terminalFailure) })
}

func (c *Client) markReady(revision uint64) {
	c.readyRevision.Store(revision)
	c.ready.Store(true)
	c.receivedInitial.Store(true)
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
