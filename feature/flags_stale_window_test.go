// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newStaleWindowTestClient(t *testing.T, transport featureRoundTripFunc, retention time.Duration) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		url:             "http://feature.test",
		apiKey:          "server-key",
		features:        make(map[string]*featureState),
		attributes:      make(map[string][]string),
		httpClient:      &http.Client{Transport: transport},
		reconnectDelay:  time.Millisecond,
		snapshotTimeout: time.Second,
		retryJitter:     func(delay time.Duration) time.Duration { return delay },
		staleRetention:  retention,
		refresh:         make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		done:            make(chan struct{}),
		readySignal:     make(chan struct{}),
		terminalFailure: make(chan struct{}),
	}
	client.contextRevision.Store(1)
	return client
}

func sseResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

// Repeated stale replies must not extend readiness: the deadline is fixed by
// the first stale notice of the window. Rapid context changes during stale
// windows interrupt at most once per window, and the latest context is sent.
func TestFeatureHubStaleOnlyRepliesExpireRetentionAndCoalesceContextChanges(t *testing.T) {
	const retention = 80 * time.Millisecond
	version := int64(1)
	featuresJSON, err := json.Marshal([]*featureState{{ID: "id", Key: "flag", Version: &version, Type: featureTypeBoolean, Value: true}})
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	var headersMu sync.Mutex
	var headers []string
	firstStale := make(chan time.Time, 1)
	client := newStaleWindowTestClient(t, func(request *http.Request) (*http.Response, error) {
		count := requests.Add(1)
		headersMu.Lock()
		headers = append(headers, request.Header.Get("x-featurehub"))
		headersMu.Unlock()
		if count == 1 {
			select {
			case firstStale <- time.Now():
			default:
			}
			return sseResponse(fmt.Sprintf("event: features\ndata: %s\n\nevent: config\ndata: {\"edge.stale\":0.01}\n\n", featuresJSON)), nil
		}
		return sseResponse("event: config\ndata: {\"edge.stale\":0.01}\n\n"), nil
	}, retention)
	go client.run()
	defer client.Stop()

	var start time.Time
	select {
	case start = <-firstStale:
	case <-time.After(time.Second):
		t.Fatal("initial FeatureHub request did not start")
	}
	readyContext, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := client.WaitReady(readyContext); err != nil {
		t.Fatalf("initial features did not make the client ready: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for client.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if client.Ready() {
		t.Fatal("stale-only replies kept the snapshot ready")
	}
	if elapsed := time.Since(start); elapsed > retention+60*time.Millisecond {
		t.Fatalf("stale snapshot stayed ready for %s; retention deadline must not be extended past %s", elapsed, retention)
	}
	if got := requests.Load(); got < 4 {
		t.Fatalf("only %d stale requests before expiry; test did not exercise repeated stale notices", got)
	}
	if err := client.lastError(); err == nil || !strings.Contains(err.Error(), "snapshot expired") {
		t.Fatalf("expiry error = %v", err)
	}
	if client.IsEnabled("flag") != true {
		t.Fatal("cached values must stay available after readiness expires")
	}

	// Context churn: one SetUserKey per millisecond for 200ms with 10ms stale
	// windows. Without coalescing this would produce ~200 requests.
	before := requests.Load()
	churnStart := time.Now()
	index := 0
	for time.Since(churnStart) < 200*time.Millisecond {
		client.SetUserKey(fmt.Sprintf("user-%d", index))
		index++
		time.Sleep(time.Millisecond)
	}
	latest := fmt.Sprintf("userkey=user-%d", index-1)
	churnRequests := requests.Load() - before
	windows := int32(time.Since(churnStart)/(10*time.Millisecond)) + 1
	t.Logf("%d context changes -> %d requests over ~%d stale windows", index, churnRequests, windows)
	if churnRequests > 2*windows+4 {
		t.Fatalf("%d context changes produced %d requests over ~%d stale windows; want at most one interrupt per window", index, churnRequests, windows)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		headersMu.Lock()
		last := headers[len(headers)-1]
		headersMu.Unlock()
		if strings.Contains(last, latest) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("latest context %q was never applied", latest)
}

func TestFeatureHubStaleRetentionDeadlineIsNotExtended(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{ctx: ctx, staleRetention: 40 * time.Millisecond, readySignal: make(chan struct{})}
	client.ready.Store(true)
	start := time.Now()
	client.retainReadyDuringStaleWindow()
	for client.Ready() && time.Since(start) < time.Second {
		client.retainReadyDuringStaleWindow()
		time.Sleep(time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond+50*time.Millisecond {
		t.Fatalf("stale notices every 1ms extended readiness to %s", elapsed)
	}

	// A fresh feature event ends the window; the next stale notice starts a new one.
	client.ready.Store(true)
	client.clearStaleRetention()
	client.retainReadyDuringStaleWindow()
	client.clearStaleRetention()
	time.Sleep(60 * time.Millisecond)
	if !client.Ready() {
		t.Fatal("cleared stale window still expired readiness")
	}
}

func TestFeatureHubStaleExpiryGenerationRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{ctx: ctx, staleRetention: 50 * time.Microsecond, readySignal: make(chan struct{})}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for index := 0; index < 500; index++ {
				switch (worker + index) % 3 {
				case 0:
					client.ready.Store(true)
					client.retainReadyDuringStaleWindow()
				case 1:
					client.clearStaleRetention()
				default:
					_ = client.Ready()
				}
			}
		}(worker)
	}
	wg.Wait()
	client.clearStaleRetention()
	client.ready.Store(true)
	time.Sleep(5 * time.Millisecond)
	if !client.Ready() {
		t.Fatal("an expiry callback from a cleared generation marked the snapshot unavailable")
	}
	client.staleTimerMu.Lock()
	defer client.staleTimerMu.Unlock()
	if client.staleTimer != nil {
		t.Fatal("cleared stale retention left a timer armed")
	}
}

func TestFeatureHubRetryDelayJitterAndConfiguredBase(t *testing.T) {
	client := &Client{reconnectDelay: 100 * time.Millisecond}
	for range 200 {
		got := client.retryDelayForAttempt(errors.New("transient"), 1)
		if got < 50*time.Millisecond || got > 100*time.Millisecond {
			t.Fatalf("equal-jitter delay = %s; want within [50ms, 100ms]", got)
		}
	}
	client.retryJitter = func(delay time.Duration) time.Duration { return delay }
	if got := client.retryDelayForAttempt(errors.New("transient"), 20); got != maxReconnectInterval {
		t.Fatalf("capped backoff = %s; want %s", got, maxReconnectInterval)
	}
	client.reconnectDelay = time.Minute
	if got := client.retryDelayForAttempt(errors.New("transient"), 20); got != time.Minute {
		t.Fatalf("configured base above 30s = %s; want honoured 1m", got)
	}
	if got := client.staleDelay(math.Inf(1)); got != maxReconnectInterval {
		t.Fatalf("stale delay with large base = %s; want cap %s", got, maxReconnectInterval)
	}
	client.reconnectDelay = 10 * time.Millisecond
	retryAfter := &featureHubHTTPError{status: http.StatusServiceUnavailable, retryAfter: featureRetryAfter("120", time.Now())}
	if got := client.retryDelayForAttempt(retryAfter, 1); got != maxReconnectInterval {
		t.Fatalf("Retry-After delay = %s; want 30s cap", got)
	}
	if got := client.staleDelay(math.NaN()); got != 10*time.Millisecond {
		t.Fatalf("NaN stale delay = %s; want reconnect floor", got)
	}
}

func TestEdgeStaleRejectsNonFiniteStrings(t *testing.T) {
	for _, raw := range []string{`"NaN"`, `"nan"`, `"Inf"`, `"+Inf"`, `"-Inf"`, `"Infinity"`} {
		if _, stale, err := edgeStaleSeconds(json.RawMessage(raw)); err == nil || stale {
			t.Fatalf("edgeStaleSeconds(%s) = (stale=%v, err=%v); want rejection", raw, stale, err)
		}
	}
}

func TestLegacySnapshotForIsBoundedBySnapshotTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	legacy := &legacyClient{
		url: "http://feature.test", apiKey: "key", snapshotTimeout: 20 * time.Millisecond,
		client: &http.Client{Transport: featureRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			select {
			case <-request.Context().Done():
				return nil, request.Context().Err()
			case <-release:
				return nil, errors.New("released")
			}
		})},
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := legacy.snapshotFor(ctx, map[string][]string{"userkey": {"u"}}); err == nil {
		t.Fatal("blocked snapshot fetch succeeded")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("snapshot fetch took %s; want bounded by snapshotTimeout", elapsed)
	}
}
