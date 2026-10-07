// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package migration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// FileStore persists pyfit-compatible JSON state using an atomic rename.
type FileStore struct {
	path string
	mode fs.FileMode
	lock *FileLocker
}

// NewFileStore creates a local state store and a sibling advisory lock file.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, errors.New("migration: empty state path")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("migration: resolve state path: %w", err)
	}
	return &FileStore{
		path: absolute,
		mode: 0o600,
		lock: NewFileLocker(absolute + ".lock"),
	}, nil
}

// Locker returns the process-safe locker associated with this store.
func (store *FileStore) Locker() Locker { return store.lock }

// Load reads state. A missing file is equivalent to empty state.
func (store *FileStore) Load(_ context.Context) (State, error) {
	file, err := os.Open(store.path)
	if errors.Is(err, os.ErrNotExist) {
		return NewState(), nil
	}
	if err != nil {
		return State{}, fmt.Errorf("migration: open state: %w", err)
	}
	defer file.Close()
	state, err := DecodeState(file)
	if err != nil {
		return State{}, fmt.Errorf("migration: load %s: %w", store.path, err)
	}
	return state, nil
}

// Save writes state to a temporary file, fsyncs it, and atomically replaces the
// old state file. The containing directory is synced after rename.
func (store *FileStore) Save(ctx context.Context, state State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var encoded bytes.Buffer
	if err := EncodeState(&encoded, state); err != nil {
		return err
	}
	data := encoded.Bytes()

	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("migration: create state directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".migrate-*")
	if err != nil {
		return fmt.Errorf("migration: create temporary state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err := temporary.Chmod(store.mode); err != nil {
		temporary.Close()
		return fmt.Errorf("migration: set state permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("migration: write temporary state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("migration: sync temporary state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("migration: close temporary state: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("migration: replace state: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("migration: open state directory for sync: %w", err)
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("migration: sync state directory: %w", errors.Join(syncErr, closeErr))
	}
	return nil
}

// FileLocker uses an OS advisory lock and supports context cancellation while
// waiting. It serializes processes sharing the same filesystem.
type FileLocker struct {
	path  string
	flock *flock.Flock
	gate  chan struct{}
}

// NewFileLocker creates a locker at path.
func NewFileLocker(path string) *FileLocker {
	return &FileLocker{path: path, flock: flock.New(path), gate: make(chan struct{}, 1)}
}

// Lock acquires the file lock until the returned function is called.
func (locker *FileLocker) Lock(ctx context.Context) (UnlockFunc, error) {
	if locker == nil || locker.flock == nil || locker.path == "" || locker.gate == nil {
		return nil, errors.New("migration: invalid file locker")
	}
	select {
	case locker.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	releaseGate := func() { <-locker.gate }
	if err := os.MkdirAll(filepath.Dir(locker.path), 0o750); err != nil {
		releaseGate()
		return nil, fmt.Errorf("migration: create lock directory: %w", err)
	}
	locked, err := locker.flock.TryLockContext(ctx, 50*time.Millisecond)
	if err != nil {
		releaseGate()
		return nil, err
	}
	if !locked {
		releaseGate()
		return nil, errors.New("migration: lock was not acquired")
	}
	var once sync.Once
	var unlockErr error
	return func() error {
		once.Do(func() {
			unlockErr = locker.flock.Unlock()
			releaseGate()
		})
		return unlockErr
	}, nil
}

// FuncLocker adapts an application-owned distributed lock function. This is
// useful for Kubernetes Lease, Redis SET NX, or database advisory locks.
type FuncLocker func(context.Context) (UnlockFunc, error)

// Lock implements Locker.
func (lock FuncLocker) Lock(ctx context.Context) (UnlockFunc, error) {
	if lock == nil {
		return nil, errors.New("migration: nil lock function")
	}
	return lock(ctx)
}

// FuncLeaseLocker adapts an application-owned renewable lease acquisition.
// Lock exists only to satisfy LeaseLocker; Runner uses LockLease and therefore
// preserves lease-loss cancellation and fencing.
type FuncLeaseLocker func(context.Context) (Lease, error)

// LockLease acquires the renewable lease.
func (lock FuncLeaseLocker) LockLease(ctx context.Context) (Lease, error) {
	if lock == nil {
		return Lease{}, errors.New("migration: nil lease lock function")
	}
	return lock(ctx)
}

// Lock acquires the lease and returns only its release function. Prefer using
// this adapter through Runner so its lease context and fence token are retained.
func (lock FuncLeaseLocker) Lock(ctx context.Context) (UnlockFunc, error) {
	lease, err := lock.LockLease(ctx)
	if err != nil {
		return nil, err
	}
	return lease.Unlock, nil
}

// MemoryStore is a concurrency-safe state store and locker for tests and
// ephemeral tools. It intentionally does not provide cross-process locking.
type MemoryStore struct {
	mu    sync.Mutex
	state State
	lock  chan struct{}
}

// NewMemoryStore creates an initialized memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: NewState(), lock: make(chan struct{}, 1)}
}

// Load returns a deep copy of the current state.
func (store *MemoryStore) Load(context.Context) (State, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return cloneState(store.state)
}

// Save replaces state with a deep copy.
func (store *MemoryStore) Save(_ context.Context, state State) error {
	copy, err := cloneState(state)
	if err != nil {
		return err
	}
	store.mu.Lock()
	store.state = copy
	store.mu.Unlock()
	return nil
}

// Locker returns this store as an in-process locker.
func (store *MemoryStore) Locker() Locker { return store }

// Lock implements Locker.
func (store *MemoryStore) Lock(ctx context.Context) (UnlockFunc, error) {
	select {
	case store.lock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() error {
		once.Do(func() { <-store.lock })
		return nil
	}, nil
}

func cloneState(state State) (State, error) {
	var data bytes.Buffer
	if err := EncodeState(&data, state); err != nil {
		return State{}, err
	}
	return DecodeState(&data)
}
