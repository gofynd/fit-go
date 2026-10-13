// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package cloudstore

import (
	"context"
	"strings"
	"testing"

	"github.com/gofynd/fit-go/migration"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

func TestStoreRoundTripAndMissingState(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	store, err := New(bucket, "service/.migrate")
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Migrations) != 0 {
		t.Fatalf("missing object state = %+v, want empty", state)
	}
	timestamp := int64(123)
	state.Migrations["v1.0.0-1-one"] = &timestamp
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Migrations["v1.0.0-1-one"] == nil || *loaded.Migrations["v1.0.0-1-one"] != timestamp {
		t.Fatalf("loaded state = %+v", loaded)
	}
}

func TestStoreRejectsUnsafeKeys(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	for _, key := range []string{"", "../state", "path/../../state"} {
		if _, err := New(bucket, key); err == nil {
			t.Fatalf("New accepted key %q", key)
		}
	}
}

func TestRunnerRequiresAndUsesCloudFence(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	unfenced, err := New(bucket, "service/.migrate")
	if err != nil {
		t.Fatal(err)
	}
	ordinaryLocker := migration.NewMemoryStore()
	_, err = migration.NewRunner([]migration.Migration{{
		ID: "v1.0.0-1-one", Up: func(context.Context) error { return nil },
	}}, unfenced, ordinaryLocker)
	if err == nil || !strings.Contains(err.Error(), "fencing") {
		t.Fatalf("NewRunner with unfenced cloud store error = %v", err)
	}

	var observedToken string
	fenced, err := NewFenced(bucket, "service/.migrate", func(
		ctx context.Context,
		bucket *blob.Bucket,
		key string,
		data []byte,
		token string,
	) error {
		observedToken = token
		return bucket.WriteAll(ctx, key, data, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	locker := migration.FuncLeaseLocker(func(ctx context.Context) (migration.Lease, error) {
		return migration.Lease{
			Context:    ctx,
			FenceToken: "lease-9",
			Unlock:     func() error { return nil },
		}, nil
	})
	runner, err := migration.NewRunner([]migration.Migration{{
		ID: "v1.0.0-1-one", Up: func(context.Context) error { return nil },
	}}, fenced, locker)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), migration.RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	if observedToken != "lease-9" {
		t.Fatalf("fenced writer token = %q", observedToken)
	}
}

var _ migration.Store = (*Store)(nil)
