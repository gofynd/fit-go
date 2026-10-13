// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func namespaceTestClient(clientEvaluated bool) *Client {
	client := &Client{
		clientEvaluated: clientEvaluated,
		features:        make(map[string]*featureState),
		attributes:      make(map[string][]string),
		refresh:         make(chan struct{}, 1),
		readySignal:     make(chan struct{}),
	}
	client.contextRevision.Store(1)
	return client
}

func namespaceTestFlag(id string, version int64, value bool) *featureState {
	return &featureState{ID: id, Key: "flag", Type: featureTypeBoolean, Version: &version, Value: value}
}

func TestStreamingCurrentContextFullSnapshotReplacesOtherUserVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		version, value := 2, true
		if strings.Contains(request.Header.Get("x-featurehub"), "userkey=second-user") {
			version, value = 1, false
		}
		_, _ = fmt.Fprintf(w, "event: features\ndata: [{\"id\":\"id\",\"key\":\"flag\",\"type\":\"BOOLEAN\",\"version\":%d,\"value\":%t}]\n\n", version, value)
		w.(http.Flusher).Flush()
		<-request.Context().Done()
	}))
	defer server.Close()
	client, err := InitWithOptions(Options{
		Enabled: true, URL: server.URL, APIKey: "server-key", RequireInitialState: true,
		InitTimeout: time.Second, DefaultAttributes: map[string][]string{"userkey": {"first-user"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	if !client.IsEnabled("flag") {
		t.Fatal("first-user snapshot was not applied")
	}
	client.SetUserKey("second-user")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.WaitReady(ctx); err != nil {
		t.Fatalf("second-user snapshot: %v", err)
	}
	if client.IsEnabled("flag") {
		t.Fatal("ready second-user stream retained the first user's true value")
	}
}

func TestStreamingRecreatedFeatureHasIndependentVersion(t *testing.T) {
	for _, clientEvaluated := range []bool{false, true} {
		for _, incremental := range []bool{false, true} {
			t.Run(fmt.Sprintf("client_evaluated=%v/incremental=%v", clientEvaluated, incremental), func(t *testing.T) {
				client := namespaceTestClient(clientEvaluated)
				client.applyFullFeatureSet([]*featureState{namespaceTestFlag("old-id", 100, true)}, 1)
				replacement := namespaceTestFlag("new-id", 1, false)
				if incremental {
					client.applyFeature(replacement, 1)
				} else {
					client.applyFullFeatureSet([]*featureState{replacement}, 1)
				}
				if client.IsEnabled("flag") || client.features["flag"].ID != "new-id" {
					t.Fatal("recreated flag retained the deleted UUID's value/version")
				}
				// A delayed deletion belongs to the old UUID, not its replacement.
				client.deleteFeature(namespaceTestFlag("old-id", 101, true), 1)
				if client.features["flag"] == nil || client.features["flag"].ID != "new-id" {
					t.Fatal("deletion of the old UUID removed the recreated flag")
				}
				client.deleteFeature(namespaceTestFlag("new-id", 2, false), 1)
				if client.features["flag"] != nil {
					t.Fatal("current UUID deletion did not remove the flag")
				}
			})
		}
	}
}

func TestStreamingVersionOrderingWithinSameNamespace(t *testing.T) {
	for _, clientEvaluated := range []bool{false, true} {
		t.Run(fmt.Sprintf("client_evaluated=%v", clientEvaluated), func(t *testing.T) {
			client := namespaceTestClient(clientEvaluated)
			client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 10, true)}, 1)
			client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 9, false)}, 1)
			client.applyFeature(namespaceTestFlag("id", 8, false), 1)
			client.deleteFeature(namespaceTestFlag("id", 7, false), 1)
			if !client.IsEnabled("flag") || featureVersion(client.features["flag"]) != 10 {
				t.Fatal("same UUID/context accepted an out-of-order rollback or deletion")
			}
			client.applyFeature(namespaceTestFlag("id", 10, false), 1)
			if client.IsEnabled("flag") {
				t.Fatal("equal version update no longer applies")
			}
		})
	}
}

func TestStreamingMissingUUIDRetainsVersionOrdering(t *testing.T) {
	for _, identities := range []struct{ current, incoming string }{
		{current: "known-id"},
		{incoming: "known-id"},
		{},
	} {
		for _, incremental := range []bool{false, true} {
			t.Run(fmt.Sprintf("current=%q/incoming=%q/incremental=%v", identities.current, identities.incoming, incremental), func(t *testing.T) {
				client := namespaceTestClient(false)
				client.applyFullFeatureSet([]*featureState{namespaceTestFlag(identities.current, 10, true)}, 1)
				apply := func(version int64) {
					incoming := namespaceTestFlag(identities.incoming, version, false)
					if incremental {
						client.applyFeature(incoming, 1)
					} else {
						client.applyFullFeatureSet([]*featureState{incoming}, 1)
					}
				}
				apply(9)
				if !client.IsEnabled("flag") || featureVersion(client.features["flag"]) != 10 {
					t.Fatal("missing UUID bypassed the existing version ordering")
				}
				apply(11)
				if client.IsEnabled("flag") || client.features["flag"].ID != identities.incoming {
					t.Fatal("newer key-only update or UUID introduction did not apply")
				}
			})
		}
	}
}

func TestStreamingDeleteIdentityPrecedesVersionShortcuts(t *testing.T) {
	for _, version := range []*int64{nil, pointerInt64(0), pointerInt64(100)} {
		client := namespaceTestClient(false)
		client.applyFullFeatureSet([]*featureState{namespaceTestFlag("new-id", 1, true)}, 1)
		client.deleteFeature(&featureState{ID: "old-id", Key: "flag", Version: version}, 1)
		if !client.IsEnabled("flag") {
			t.Fatal("old UUID deletion bypassed identity through a missing/zero/newer version")
		}
		// Key-only deletes remain compatible, including unversioned deletes.
		client.deleteFeature(&featureState{Key: "flag", Version: version}, 1)
		if client.features["flag"] != nil {
			t.Fatal("key-only deletion compatibility changed")
		}
	}
}

func TestStreamingNewContextIncrementalAndDeleteDoNotMixOldValues(t *testing.T) {
	for _, deleteFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_first=%v", deleteFirst), func(t *testing.T) {
			client := namespaceTestClient(false)
			client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 10, true),
				{ID: "other-id", Key: "other-user-only", Type: featureTypeBoolean, Value: true}}, 1)
			client.SetUserKey("new-user")
			revision := client.contextRevision.Load()
			if deleteFirst {
				client.deleteFeature(namespaceTestFlag("id", 1, false), revision)
			} else {
				client.applyFeature(namespaceTestFlag("id", 1, false), revision)
			}
			if client.features["other-user-only"] != nil || client.IsEnabled("flag") {
				t.Fatal("new-context event retained values from the previous user")
			}
			if client.Ready() {
				t.Fatal("incremental/delete event marked an incomplete new context ready")
			}
			client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 2, false)}, revision)
			if !client.Ready() || client.IsEnabled("flag") {
				t.Fatal("current-context full snapshot was not published")
			}
			// The rejected old stream must not clear or corrupt the new namespace.
			client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 100, true)}, 1)
			client.applyFeature(namespaceTestFlag("id", 100, true), 1)
			client.deleteFeature(namespaceTestFlag("id", 100, true), 1)
			if !client.Ready() || client.features["flag"] == nil || client.IsEnabled("flag") {
				t.Fatal("old-stream event disturbed the current namespace")
			}
		})
	}
}

func TestStreamingClientEvaluatedContextKeepsSharedVersionNamespace(t *testing.T) {
	client := namespaceTestClient(true)
	client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 10, true)}, 1)
	client.SetUserKey("another-user")
	client.applyFullFeatureSet([]*featureState{namespaceTestFlag("id", 9, false)}, 1)
	if client.contextRevision.Load() != 1 || !client.Ready() || !client.IsEnabled("flag") {
		t.Fatal("client-evaluated attribute change reset the shared feature namespace")
	}
}
