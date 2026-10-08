// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFeatureRetryAfterIsBounded(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{
		"12":                   12 * time.Second,
		"99999999999999999999": maxReconnectInterval,
		now.Add(time.Minute).Format(http.TimeFormat): maxReconnectInterval,
		"not-a-delay": 0,
	} {
		if got := featureRetryAfter(value, now); got != want {
			t.Errorf("featureRetryAfter(%q) = %s; want %s", value, got, want)
		}
	}
}

func TestFeatureStaleDelayHasFloorAndCap(t *testing.T) {
	t.Parallel()

	client := &Client{reconnectDelay: 20 * time.Millisecond}
	if got := client.staleDelay(0.000001); got != 20*time.Millisecond {
		t.Fatalf("tiny stale delay = %s; want reconnect floor", got)
	}
	if got := client.staleDelay(300); got != maxReconnectInterval {
		t.Fatalf("large stale delay = %s; want cap", got)
	}
}

func TestFeatureStaleRetentionExpiresAndCannotArmAfterStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{ctx: ctx, cancel: cancel, readySignal: make(chan struct{}), staleRetention: 5 * time.Millisecond}
	client.ready.Store(true)
	client.retainReadyDuringStaleWindow()
	time.Sleep(20 * time.Millisecond)
	if client.Ready() {
		t.Fatal("expired stale snapshot still reports ready")
	}
	if err := client.lastError(); err == nil || !strings.Contains(err.Error(), "snapshot expired") {
		t.Fatalf("stale expiry error = %v", err)
	}

	cancel()
	client.staleRetention = time.Second
	client.ready.Store(true)
	client.retainReadyDuringStaleWindow()
	client.staleTimerMu.Lock()
	timer := client.staleTimer
	client.staleTimerMu.Unlock()
	if timer != nil {
		t.Fatal("stopped client armed a stale-expiry timer")
	}
}

func TestFeatureBackoffResetsOnlyAfterFeatureEvents(t *testing.T) {
	t.Parallel()

	client := &Client{reconnectDelay: 10 * time.Millisecond, retryJitter: func(delay time.Duration) time.Duration { return delay }}
	failure := errors.New("stream failed")
	if got := client.retryDelayForAttempt(failure, 4); got != 80*time.Millisecond {
		t.Fatalf("fourth retry delay = %s; want exponential backoff", got)
	}
	if got := client.retryDelayForAttempt(&edgeStaleError{delay: time.Millisecond}, 9); got != time.Millisecond {
		t.Fatalf("stale retry delay = %s; stale hint must not receive outage exponent", got)
	}
}
