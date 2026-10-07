// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Package cloudstore provides GCS and S3 migration state storage through Go
// Cloud. Locking remains application-owned because object replacement alone is
// not a distributed execution lock.
package cloudstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gofynd/fit-go/migration"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob" // Register gs:// bucket URLs.
	_ "gocloud.dev/blob/s3blob"  // Register s3:// bucket URLs.
	"gocloud.dev/gcerrors"
)

// Store persists one migration state object in a bucket.
type Store struct {
	bucket      *blob.Bucket
	key         string
	owned       bool
	fencedWrite FencedWriteFunc
}

// FencedWriteFunc must atomically verify fenceToken against the current lock
// owner and persist data. A read-check followed by an unconditional object
// write is not sufficient because a newer owner can intervene between them.
type FencedWriteFunc func(
	ctx context.Context,
	bucket *blob.Bucket,
	key string,
	data []byte,
	fenceToken string,
) error

// New wraps an existing bucket. The caller retains ownership of the bucket.
func New(bucket *blob.Bucket, key string) (*Store, error) {
	return newStore(bucket, key, false, nil)
}

// NewFenced wraps an existing bucket and requires fenced writes when used by a
// migration Runner. The caller retains ownership of the bucket.
func NewFenced(bucket *blob.Bucket, key string, write FencedWriteFunc) (*Store, error) {
	return newStore(bucket, key, false, write)
}

// Open opens a gs:// or s3:// bucket URL using standard provider credential
// discovery. Close must be called when the store is no longer used.
func Open(ctx context.Context, bucketURL, key string) (*Store, error) {
	if strings.TrimSpace(bucketURL) == "" {
		return nil, errors.New("migration cloudstore: empty bucket URL")
	}
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("migration cloudstore: open bucket: %w", err)
	}
	store, err := newStore(bucket, key, true, nil)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	return store, nil
}

// OpenFenced opens a cloud bucket and configures an application-owned fenced
// write. Close must be called when the store is no longer used.
func OpenFenced(ctx context.Context, bucketURL, key string, write FencedWriteFunc) (*Store, error) {
	if strings.TrimSpace(bucketURL) == "" {
		return nil, errors.New("migration cloudstore: empty bucket URL")
	}
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("migration cloudstore: open bucket: %w", err)
	}
	store, err := newStore(bucket, key, true, write)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	return store, nil
}

func newStore(bucket *blob.Bucket, key string, owned bool, write FencedWriteFunc) (*Store, error) {
	if bucket == nil {
		return nil, errors.New("migration cloudstore: nil bucket")
	}
	key = strings.TrimSpace(strings.TrimPrefix(key, "/"))
	if key == "" {
		return nil, errors.New("migration cloudstore: empty object key")
	}
	if strings.Contains(key, "..") {
		return nil, errors.New("migration cloudstore: object key must not contain '..'")
	}
	return &Store{bucket: bucket, key: key, owned: owned, fencedWrite: write}, nil
}

// Load reads migration state. A missing object is equivalent to empty state.
func (store *Store) Load(ctx context.Context) (migration.State, error) {
	reader, err := store.bucket.NewReader(ctx, store.key, nil)
	if gcerrors.Code(err) == gcerrors.NotFound {
		return migration.NewState(), nil
	}
	if err != nil {
		return migration.State{}, fmt.Errorf("migration cloudstore: read state: %w", err)
	}
	state, decodeErr := migration.DecodeState(reader)
	closeErr := reader.Close()
	if decodeErr != nil {
		return migration.State{}, fmt.Errorf("migration cloudstore: decode state: %w", decodeErr)
	}
	if closeErr != nil {
		return migration.State{}, fmt.Errorf("migration cloudstore: close state reader: %w", closeErr)
	}
	return state, nil
}

// Save replaces the state object. Runner locking must cover the read-modify-
// write sequence; this method intentionally does not claim to provide locking.
func (store *Store) Save(ctx context.Context, state migration.State) error {
	return store.write(ctx, state)
}

// SaveFenced persists state through the application-owned conditional writer.
func (store *Store) SaveFenced(ctx context.Context, state migration.State, fenceToken string) error {
	fenceToken = strings.TrimSpace(fenceToken)
	if fenceToken == "" {
		return errors.New("migration cloudstore: empty fence token")
	}
	if store.fencedWrite == nil {
		return errors.New("migration cloudstore: no fenced writer configured")
	}
	var encoded bytes.Buffer
	if err := migration.EncodeState(&encoded, state); err != nil {
		return err
	}
	if err := store.fencedWrite(ctx, store.bucket, store.key, encoded.Bytes(), fenceToken); err != nil {
		return fmt.Errorf("migration cloudstore: fenced write state: %w", err)
	}
	return nil
}

func (store *Store) write(ctx context.Context, state migration.State) error {
	var encoded bytes.Buffer
	if err := migration.EncodeState(&encoded, state); err != nil {
		return err
	}
	if err := store.bucket.WriteAll(ctx, store.key, encoded.Bytes(), &blob.WriterOptions{
		ContentType:  "application/json",
		CacheControl: "no-store",
	}); err != nil {
		return fmt.Errorf("migration cloudstore: write state: %w", err)
	}
	return nil
}

// MigrationFencingRequired prevents Runner from using a renewable distributed
// lock without lease-loss notification and a conditional state write.
func (*Store) MigrationFencingRequired() bool { return true }

// ValidateMigrationFencing verifies that the conditional writer is configured.
func (store *Store) ValidateMigrationFencing() error {
	if store == nil || store.fencedWrite == nil {
		return errors.New("migration cloudstore: use NewFenced or OpenFenced with an atomic fenced writer")
	}
	return nil
}

// Close closes a bucket opened by Open. It is a no-op for buckets passed to New.
func (store *Store) Close() error {
	if store == nil || !store.owned || store.bucket == nil {
		return nil
	}
	store.owned = false
	return store.bucket.Close()
}

var (
	_ migration.Store              = (*Store)(nil)
	_ migration.FencedStore        = (*Store)(nil)
	_ migration.FencingRequirement = (*Store)(nil)
)
