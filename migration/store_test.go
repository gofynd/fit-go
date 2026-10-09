// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreRoundTripAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", ".migrate")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	state := NewState()
	timestamp := int64(42)
	state.Migrations["v1_0_0-1-one"] = &timestamp
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
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestFileLockerHonorsContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", ".migrate.lock")
	first := NewFileLocker(path)
	second := NewFileLocker(path)
	unlock, err := first.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = second.Lock(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Lock() error = %v, want deadline exceeded", err)
	}
}

func TestFileLockerSerializesConcurrentCallsOnSameInstance(t *testing.T) {
	locker := NewFileLocker(filepath.Join(t.TempDir(), ".migrate.lock"))
	unlock, err := locker.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, err = locker.Lock(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reentrant Lock() error = %v, want deadline exceeded", err)
	}
}

func TestMemoryStoreReturnsDeepCopies(t *testing.T) {
	store := NewMemoryStore()
	state := NewState()
	timestamp := int64(1)
	state.Migrations["v1.0.0-1-one"] = &timestamp
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	state.Migrations["v1.0.0-1-one"] = nil
	loaded, _ := store.Load(context.Background())
	if loaded.Migrations["v1.0.0-1-one"] == nil {
		t.Fatal("store retained caller-owned map")
	}
}
