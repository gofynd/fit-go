// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sseEvent struct {
	name string
	data interface{}
}

type featureTestServer struct {
	server     *httptest.Server
	events     chan sseEvent
	request    chan *http.Request
	disconnect chan struct{}
	once       sync.Once
}

func newFeatureTestServer(t *testing.T, initial ...sseEvent) *featureTestServer {
	t.Helper()
	stream := &featureTestServer{
		events:     make(chan sseEvent, 16),
		request:    make(chan *http.Request, 4),
		disconnect: make(chan struct{}),
	}
	for _, event := range initial {
		stream.events <- event
	}
	stream.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case stream.request <- r.Clone(r.Context()):
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server does not support flushing")
			return
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case <-stream.disconnect:
				return
			case event := <-stream.events:
				encoded, err := json.Marshal(event.data)
				if err != nil {
					t.Errorf("encode event: %v", err)
					return
				}
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.name, encoded)
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(func() {
		stream.once.Do(func() { close(stream.disconnect) })
		stream.server.Close()
	})
	return stream
}

func startFeatureClient(t *testing.T, stream *featureTestServer) *Client {
	t.Helper()
	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("FEATURE_FLAG_URL", stream.server.URL+"/")
	t.Setenv("FEATURE_FLAG_API_KEY", "default/environment/sdk-key*client")
	t.Setenv("FEATURE_FLAG_REQUIRE_INITIAL_STATE", "true")
	t.Setenv("FEATURE_FLAG_INIT_TIMEOUT", "1s")
	client, err := Init()
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(client.Stop)
	return client
}

func TestInitDisabledAndRequiredConfiguration(t *testing.T) {
	t.Setenv("FEATURE_FLAG_ENABLED", "false")
	client, err := Init()
	if err != nil || client != nil {
		t.Fatalf("disabled Init = %#v, %v; want nil, nil", client, err)
	}

	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("FEATURE_FLAG_URL", "")
	t.Setenv("FEATURE_FLAG_API_KEY", "key")
	client, err = Init()
	if err != nil || client != nil {
		t.Fatalf("optional incomplete Init = %#v, %v; want nil, nil", client, err)
	}
	t.Setenv("FEATURE_FLAG_REQUIRE_INITIAL_STATE", "true")
	if _, err = Init(); err == nil {
		t.Fatal("required initial state should require URL and API key")
	}
}

func TestInitWithOptionsDoesNotRequireProcessEnvironment(t *testing.T) {
	version := int64(1)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{{
		ID: "id", Key: "flag", Version: &version, Type: featureTypeBoolean, Value: true,
	}}})
	t.Setenv("FEATURE_FLAG_ENABLED", "false")
	client, err := InitWithOptions(Options{
		Enabled: true, URL: stream.server.URL, APIKey: "key", RequireInitialState: true, InitTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	defer client.Stop()
	if !client.IsEnabled("flag") {
		t.Fatal("explicitly configured feature client did not load the initial state")
	}
}

func TestFeatureHubURLAcceptsFeaturesSuffix(t *testing.T) {
	version := int64(1)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{{
		ID: "id", Key: "flag", Version: &version, Type: featureTypeBoolean, Value: true,
	}}})
	client, err := InitWithOptions(Options{
		Enabled:             true,
		URL:                 stream.server.URL + "/features/",
		APIKey:              "default/environment/sdk-key*client",
		RequireInitialState: true,
		InitTimeout:         time.Second,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	defer client.Stop()

	select {
	case request := <-stream.request:
		if request.URL.Path != "/features/default/environment/sdk-key*client" {
			t.Fatalf("FeatureHub path = %q", request.URL.Path)
		}
	case <-time.After(time.Second):
		t.Fatal("FeatureHub request not observed")
	}
}

func TestFeatureHubSSEEndpointAndTypedValues(t *testing.T) {
	version := int64(1)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{
		{ID: "boolean-id", Key: "boolean", Version: &version, Type: featureTypeBoolean, Value: true},
		{ID: "string-id", Key: "string", Version: &version, Type: featureTypeString, Value: "value"},
		{ID: "number-id", Key: "number", Version: &version, Type: featureTypeNumber, Value: 42.5},
		{ID: "json-id", Key: "json", Version: &version, Type: featureTypeJSON, Value: `{"enabled":true}`},
	}})
	client := startFeatureClient(t, stream)

	select {
	case request := <-stream.request:
		if request.URL.Path != "/features/default/environment/sdk-key*client" {
			t.Fatalf("FeatureHub path = %q", request.URL.Path)
		}
		if request.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("Accept = %q", request.Header.Get("Accept"))
		}
	case <-time.After(time.Second):
		t.Fatal("FeatureHub request not observed")
	}

	if !client.Ready() || !client.IsEnabled("boolean") {
		t.Fatal("boolean feature should be ready and enabled")
	}
	if value, ok := client.GetString("string"); !ok || value != "value" {
		t.Fatalf("string = %q, %v", value, ok)
	}
	if value, ok := client.GetNumber("number"); !ok || value != 42.5 {
		t.Fatalf("number = %v, %v", value, ok)
	}
	decoded, ok := client.GetValue("json").(map[string]interface{})
	if !ok || decoded["enabled"] != true {
		t.Fatalf("json = %#v", client.GetValue("json"))
	}
}

func TestContextStrategiesMatchLegacyFeatureHubBehavior(t *testing.T) {
	t.Setenv("METROPLEX_MODULE", "communications")
	t.Setenv("SERVICE_NAME", "communication")
	t.Setenv("PLATFORM_VERSION", "v2.4.0-RC12")
	version := int64(1)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{
		{
			ID: "service-feature-id", Key: "service-feature", Version: &version, Type: featureTypeBoolean, Value: false,
			Strategies: []rolloutStrategy{{Value: true, Attributes: []strategyAttribute{{
				FieldName: "service_name", Type: "STRING", Conditional: "EQUALS", Values: []interface{}{"communication"},
			}}}},
		},
		{
			ID: "language-feature-id", Key: "language-feature", Version: &version, Type: featureTypeBoolean, Value: false,
			Strategies: []rolloutStrategy{{Value: true, Attributes: []strategyAttribute{{
				FieldName: "languages", Type: "STRING", Conditional: "EQUALS", Values: []interface{}{"english"},
			}}}},
		},
	}})
	client := startFeatureClient(t, stream)

	if !client.IsEnabled("service-feature") {
		t.Fatal("SERVICE_NAME should populate the service_name context")
	}
	client.SetAttributeValues("languages", []string{"italian", "english", "german"})
	if !client.IsEnabled("language-feature") {
		t.Fatal("any matching custom attribute value should satisfy a strategy")
	}
	client.ResetContext()
	if client.IsEnabled("language-feature") || !client.IsEnabled("service-feature") {
		t.Fatal("ResetContext should remove runtime attributes and preserve service defaults")
	}
}

func TestFeatureUpdatesVersioningAndDelete(t *testing.T) {
	version1 := int64(1)
	version2 := int64(2)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{{
		ID: "id", Key: "flag", Version: &version1, Type: featureTypeBoolean, Value: false,
	}}})
	client := startFeatureClient(t, stream)

	stream.events <- sseEvent{name: "feature", data: &featureState{
		ID: "id", Key: "flag", Version: &version2, Type: featureTypeBoolean, Value: true,
	}}
	eventually(t, func() bool { return client.IsEnabled("flag") })

	stream.events <- sseEvent{name: "feature", data: &featureState{
		ID: "id", Key: "flag", Version: &version1, Type: featureTypeBoolean, Value: false,
	}}
	time.Sleep(20 * time.Millisecond)
	if !client.IsEnabled("flag") {
		t.Fatal("an older feature event replaced a newer cached value")
	}

	stream.events <- sseEvent{name: "delete_feature", data: &featureState{Key: "flag", Version: &version1}}
	time.Sleep(20 * time.Millisecond)
	if !client.IsEnabled("flag") {
		t.Fatal("an older delete event removed a newer feature")
	}
	stream.events <- sseEvent{name: "delete_feature", data: &featureState{Key: "flag", Version: &version2}}
	eventually(t, func() bool { return client.GetValue("flag") == nil })
}

func TestFeatureHubPercentageCalculationMatchesJavaScriptSDK(t *testing.T) {
	if got := determineClientPercentage("user-1", "feature-id"); got != 200835 {
		t.Fatalf("percentage = %v; want JavaScript SDK result 200835", got)
	}
	if got := determineClientPercentage("abc", "123"); got != 55367 {
		t.Fatalf("percentage = %v; want JavaScript SDK result 55367", got)
	}
}

func TestPercentageBucketsFollowCurrentFeatureHubSDK(t *testing.T) {
	feature := &featureState{
		ID: "bucket-feature",
		Strategies: []rolloutStrategy{
			{Percentage: 100_000, Value: "first"},
			{Percentage: 100_000, Value: "second"},
		},
	}
	var userKey string
	for index := 0; index < 10_000; index++ {
		candidate := fmt.Sprintf("bucket-user-%d", index)
		percentage := determineClientPercentage(candidate, feature.ID)
		if percentage > 100_000 && percentage <= 200_000 {
			userKey = candidate
			break
		}
	}
	if userKey == "" {
		t.Fatal("failed to find deterministic percentage fixture")
	}
	value, matched := applyStrategies(feature, map[string][]string{"userkey": {userKey}}, time.Now())
	if !matched || value != "second" {
		t.Fatalf("current cumulative rollout = %#v, %v; want second, true", value, matched)
	}
}

func TestInitUsesBoundedReadinessTimeout(t *testing.T) {
	stream := newFeatureTestServer(t)
	t.Setenv("FEATURE_FLAG_ENABLED", "true")
	t.Setenv("FEATURE_FLAG_URL", stream.server.URL)
	t.Setenv("FEATURE_FLAG_API_KEY", "key")
	t.Setenv("FEATURE_FLAG_REQUIRE_INITIAL_STATE", "true")
	t.Setenv("FEATURE_FLAG_INIT_TIMEOUT", "30ms")
	started := time.Now()
	if _, err := Init(); err == nil {
		t.Fatal("Init should time out without an initial features event")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Init timeout was not bounded: %s", elapsed)
	}
}

func TestOptionalInitDoesNotBlockOnFeatureHubReadiness(t *testing.T) {
	stream := newFeatureTestServer(t)
	started := time.Now()
	client, err := InitWithOptions(Options{
		Enabled: true, URL: stream.server.URL, APIKey: "key", InitTimeout: time.Hour,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	defer client.Stop()
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("optional FeatureHub initialization blocked for %s", elapsed)
	}
	if client.Ready() {
		t.Fatal("client should not be ready before a full features event")
	}
}

func TestServerEvaluatedContextReconnectsWithFeatureHubHeader(t *testing.T) {
	t.Setenv("SERVICE_NAME", "communication")
	t.Setenv("METROPLEX_MODULE", "communications")
	t.Setenv("PLATFORM_VERSION", "")
	requests := make(chan string, 4)
	var requestCount atomic.Int32
	version := int64(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("x-featurehub")
		if queryContext := r.URL.Query().Get("xfeaturehub"); queryContext != header {
			t.Errorf("xfeaturehub query = %q; want %q", queryContext, header)
		}
		requests <- header
		value := strings.Contains(header, "userkey=user%20one%2Ctwo!")
		requestCount.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "event: features\ndata: %s\n\n", mustJSON(t, []*featureState{{
			ID: "server-id", Key: "server-flag", Version: &version, Type: featureTypeBoolean, Value: value,
			Strategies: []rolloutStrategy{{Value: true, Attributes: []strategyAttribute{{
				FieldName: "service_name", Type: "STRING", Conditional: "EQUALS", Values: []interface{}{"communication"},
			}}}},
		}}))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := InitWithOptions(Options{
		Enabled: true, URL: server.URL, APIKey: "server-key", RequireInitialState: true, InitTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	defer client.Stop()
	if client.ClientEvaluated() {
		t.Fatal("API key without an asterisk should use server evaluation")
	}
	if initial := <-requests; initial != "service_name=communication" {
		t.Fatalf("initial x-featurehub = %q", initial)
	}
	if client.IsEnabled("server-flag") {
		t.Fatal("initial server-evaluated value should be false")
	}

	client.SetUserKey("user one,two!")
	readyContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.WaitReady(readyContext); err != nil {
		t.Fatalf("WaitReady after context change: %v", err)
	}
	if refreshed := <-requests; refreshed != "service_name=communication,userkey=user%20one%2Ctwo!" {
		t.Fatalf("refreshed x-featurehub = %q", refreshed)
	}
	if requestCount.Load() < 2 || !client.IsEnabled("server-flag") {
		t.Fatal("server-evaluated context did not refresh the feature value")
	}
}

func TestClientEvaluatedContextsAreIndependent(t *testing.T) {
	version := int64(1)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{{
		ID: "context-id", Key: "context-flag", Version: &version, Type: featureTypeBoolean, Value: false,
		Strategies: []rolloutStrategy{{Value: true, Attributes: []strategyAttribute{{
			FieldName: "segment", Type: "STRING", Conditional: "EQUALS", Values: []interface{}{"blue"},
		}}}},
	}}})
	client := startFeatureClient(t, stream)
	blue := client.NewContext().Attribute("segment", "blue")
	red := client.NewContext().Attribute("segment", "red")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := blue.Build(ctx); err != nil {
		t.Fatalf("blue context Build: %v", err)
	}
	if err := red.Build(ctx); err != nil {
		t.Fatalf("red context Build: %v", err)
	}
	if !blue.IsEnabled("context-flag") || red.IsEnabled("context-flag") || client.IsEnabled("context-flag") {
		t.Fatal("client-evaluated request contexts leaked attributes")
	}
}

func TestFeatureHubEdgeStaleReconnectsAfterAdvertisedDelay(t *testing.T) {
	requestTimes := make(chan time.Time, 4)
	var requestCount atomic.Int32
	version := int64(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := requestCount.Add(1)
		requestTimes <- time.Now()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "event: features\ndata: %s\n\n", mustJSON(t, []*featureState{{
			ID: "id", Key: "flag", Version: &version, Type: featureTypeBoolean, Value: true,
		}}))
		if current == 1 {
			_, _ = fmt.Fprint(w, "event: config\ndata: {\"edge.stale\":0.04}\n\n")
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := InitWithOptions(Options{
		Enabled: true, URL: server.URL, APIKey: "key*client", RequireInitialState: true,
		InitTimeout: time.Second, ReconnectInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	defer client.Stop()
	first := <-requestTimes
	select {
	case second := <-requestTimes:
		if delay := second.Sub(first); delay < 30*time.Millisecond {
			t.Fatalf("edge.stale reconnect delay = %s; want advertised delay", delay)
		}
	case <-time.After(time.Second):
		t.Fatal("FeatureHub did not reconnect after edge.stale")
	}
}

func TestRequiredInitialStateSurvivesEdgeStaleBeforeFeatures(t *testing.T) {
	var requestCount atomic.Int32
	version := int64(1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := requestCount.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if current == 1 {
			_, _ = fmt.Fprint(w, "event: config\ndata: {\"edge.stale\":0.01}\n\n")
		} else {
			_, _ = fmt.Fprintf(w, "event: features\ndata: %s\n\n", mustJSON(t, []*featureState{{
				ID: "id", Key: "flag", Version: &version, Type: featureTypeBoolean, Value: true,
			}}))
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := InitWithOptions(Options{
		Enabled: true, URL: server.URL, APIKey: "key*client", RequireInitialState: true,
		InitTimeout: time.Second, ReconnectInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("InitWithOptions: %v", err)
	}
	defer client.Stop()
	if requestCount.Load() < 2 || !client.IsEnabled("flag") {
		t.Fatal("required initialization did not recover from edge.stale")
	}
}

func TestLegacyNumberMatcherParseMode(t *testing.T) {
	attribute := strategyAttribute{Conditional: "EQUALS", Values: []interface{}{"1.9"}}
	if !matchNumber("1", attribute) {
		t.Fatal("integer supplied value should use JavaScript parseInt for candidates")
	}
	if matchNumber("1.0", attribute) {
		t.Fatal("decimal supplied value should use JavaScript parseFloat for candidates")
	}
	attribute.Values = []interface{}{"10.8suffix"}
	if !matchNumber("10suffix", attribute) {
		t.Fatal("JavaScript numeric parsers should accept numeric prefixes")
	}
}

func TestFeatureHubContextHeaderMatchesJavaScriptEncoding(t *testing.T) {
	header := featureHubContextHeader(map[string][]string{
		"userkey": {"user one", "two!", "✓"},
		"country": {"IN"},
	})
	if header != "country=IN,userkey=user%20one%2Ctwo!%2C%E2%9C%93" {
		t.Fatalf("x-featurehub = %q", header)
	}
}

func TestClientConcurrentContextAndFeatureReads(t *testing.T) {
	version := int64(1)
	stream := newFeatureTestServer(t, sseEvent{name: "features", data: []*featureState{{
		ID: "id", Key: "flag", Version: &version, Type: featureTypeBoolean, Value: true,
	}}})
	client := startFeatureClient(t, stream)

	var wait sync.WaitGroup
	for index := 0; index < 50; index++ {
		wait.Add(2)
		go func(index int) {
			defer wait.Done()
			client.SetUserKey(fmt.Sprintf("user-%d", index))
		}(index)
		go func() {
			defer wait.Done()
			_ = client.IsEnabled("flag")
		}()
	}
	wait.Wait()
	client.Stop()
	client.Stop()
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func mustJSON(t *testing.T, value interface{}) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(encoded)
}
