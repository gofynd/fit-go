// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"encoding/json"
	"testing"
)

func TestBufferedOldStreamEventsCannotMutateNewContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	versionOne := int64(1)
	client := &Client{
		features: map[string]*featureState{
			"flag": {ID: "flag-id", Key: "flag", Version: &versionOne, Type: featureTypeBoolean, Value: false},
		},
		attributes:      make(map[string][]string),
		refresh:         make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		readySignal:     make(chan struct{}),
		terminalFailure: make(chan struct{}),
	}
	client.contextRevision.Store(1)
	client.readyRevision.Store(1)
	client.ready.Store(true)

	oldRevision := client.contextRevision.Load()
	client.SetUserKey("new-user")
	newRevision := client.contextRevision.Load()
	if newRevision != oldRevision+1 {
		t.Fatalf("context revision = %d; want %d", newRevision, oldRevision+1)
	}
	if client.Ready() {
		t.Fatal("context change did not invalidate readiness")
	}

	versionTwo := int64(2)
	oldFull := marshalFeatureEvent(t, []*featureState{
		{ID: "flag-id", Key: "flag", Version: &versionTwo, Type: featureTypeBoolean, Value: true},
		{ID: "old-only-id", Key: "old-only", Version: &versionTwo, Type: featureTypeBoolean, Value: true},
	})
	if err := client.handleEvent("features", oldFull, oldRevision); err != nil {
		t.Fatalf("old full event: %v", err)
	}
	assertFeatureBoolean(t, client, "flag", false)
	assertFeatureMissing(t, client, "old-only")
	if client.Ready() {
		t.Fatal("old full event marked the new context ready")
	}
	if got := client.featureEvents.Load(); got != 0 {
		t.Fatalf("old full event count = %d; want 0", got)
	}

	oldIncremental := marshalFeatureEvent(t, &featureState{
		ID: "flag-id", Key: "flag", Version: &versionTwo, Type: featureTypeBoolean, Value: true,
	})
	if err := client.handleEvent("feature", oldIncremental, oldRevision); err != nil {
		t.Fatalf("old incremental event: %v", err)
	}
	if err := client.handleEvent("delete_feature", oldIncremental, oldRevision); err != nil {
		t.Fatalf("old delete event: %v", err)
	}
	assertFeatureBoolean(t, client, "flag", false)
	if got := client.featureEvents.Load(); got != 0 {
		t.Fatalf("old incremental/delete event count = %d; want 0", got)
	}

	currentFull := marshalFeatureEvent(t, []*featureState{
		{ID: "flag-id", Key: "flag", Version: &versionTwo, Type: featureTypeBoolean, Value: true},
	})
	if err := client.handleEvent("features", currentFull, newRevision); err != nil {
		t.Fatalf("current full event: %v", err)
	}
	assertFeatureBoolean(t, client, "flag", true)
	if !client.Ready() || client.readyRevision.Load() != newRevision {
		t.Fatalf("current full event readiness = %v at revision %d; want ready revision %d", client.Ready(), client.readyRevision.Load(), newRevision)
	}

	// A buffered event can still arrive after the replacement stream is ready.
	// It must neither overwrite/delete the new value nor disturb readiness.
	if err := client.handleEvent("feature", marshalFeatureEvent(t, &featureState{
		ID: "flag-id", Key: "flag", Version: pointerInt64(99), Type: featureTypeBoolean, Value: false,
	}), oldRevision); err != nil {
		t.Fatalf("late old incremental event: %v", err)
	}
	if err := client.handleEvent("delete_feature", marshalFeatureEvent(t, &featureState{
		Key: "flag", Version: pointerInt64(99),
	}), oldRevision); err != nil {
		t.Fatalf("late old delete event: %v", err)
	}
	assertFeatureBoolean(t, client, "flag", true)
	if !client.Ready() || client.readyRevision.Load() != newRevision {
		t.Fatal("late old events disturbed current-context readiness")
	}
	if got := client.featureEvents.Load(); got != 1 {
		t.Fatalf("accepted feature event count = %d; want 1", got)
	}

	versionThree := int64(3)
	currentIncremental := marshalFeatureEvent(t, &featureState{
		ID: "flag-id", Key: "flag", Version: &versionThree, Type: featureTypeBoolean, Value: false,
	})
	if err := client.handleEvent("feature", currentIncremental, newRevision); err != nil {
		t.Fatalf("current incremental event: %v", err)
	}
	assertFeatureBoolean(t, client, "flag", false)
	if err := client.handleEvent("delete_feature", currentIncremental, newRevision); err != nil {
		t.Fatalf("current delete event: %v", err)
	}
	assertFeatureMissing(t, client, "flag")
	if got := client.featureEvents.Load(); got != 3 {
		t.Fatalf("accepted feature event count = %d; want 3", got)
	}
}

func marshalFeatureEvent(t *testing.T, value interface{}) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func assertFeatureBoolean(t *testing.T, client *Client, key string, want bool) {
	t.Helper()
	client.mu.RLock()
	feature := client.features[key]
	client.mu.RUnlock()
	if feature == nil || feature.Value != want {
		t.Fatalf("feature %q = %#v; want boolean %v", key, feature, want)
	}
}

func assertFeatureMissing(t *testing.T, client *Client, key string) {
	t.Helper()
	client.mu.RLock()
	_, exists := client.features[key]
	client.mu.RUnlock()
	if exists {
		t.Fatalf("feature %q unexpectedly exists", key)
	}
}

func pointerInt64(value int64) *int64 { return &value }
