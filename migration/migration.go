// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Package migration provides a deterministic, compiled database migration
// runner. It replaces the runtime module loading used by fitmigrate and pyfit
// with Go functions registered by the application.
package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrNoMigrations indicates that no migrations were registered.
	ErrNoMigrations = errors.New("migration: no migrations registered")
	// ErrIrreversible indicates that a migration has no Down function.
	ErrIrreversible = errors.New("migration: migration is irreversible")
	// ErrStateDrift indicates that persisted state does not match compiled code.
	ErrStateDrift = errors.New("migration: persisted state does not match registry")
)

const maxStateBytes = 4 << 20

var (
	idPattern      = regexp.MustCompile(`^v(\d+)[._](\d+)[._](\d+)-(\d+)-([A-Za-z0-9][A-Za-z0-9_-]*)$`)
	versionPattern = regexp.MustCompile(`^v(\d+)[._](\d+)[._](\d+)$`)
	nodeIDPattern  = regexp.MustCompile(`^(\d{3})(\d{3})(\d{3})(\d{3})-([A-Za-z0-9][A-Za-z0-9_-]*)$`)
)

// Func is an application-owned migration operation.
type Func func(context.Context) error

// Migration is one compiled migration. Checksum should be a stable digest of
// the migration's behavior or source. When present, changing it after the
// migration has run is treated as state drift.
type Migration struct {
	ID          string
	Description string
	Checksum    string
	Up          Func
	Down        Func
}

// ID is the parsed, canonical identity of a migration.
type ID struct {
	Major    uint64
	Minor    uint64
	Patch    uint64
	Sequence uint64
	Name     string
}

func (id ID) String() string {
	return fmt.Sprintf("v%d.%d.%d-%d-%s", id.Major, id.Minor, id.Patch, id.Sequence, id.Name)
}

// Version returns the semver portion of the migration ID.
func (id ID) Version() string {
	return fmt.Sprintf("v%d.%d.%d", id.Major, id.Minor, id.Patch)
}

// ParseID parses dotted Node migration IDs and underscored pyfit IDs.
func ParseID(value string) (ID, error) {
	value = strings.TrimSpace(value)
	for _, suffix := range []string{".go", ".js", ".py"} {
		value = strings.TrimSuffix(value, suffix)
	}
	match := idPattern.FindStringSubmatch(value)
	if match == nil {
		match = nodeIDPattern.FindStringSubmatch(value)
	}
	if match == nil {
		return ID{}, fmt.Errorf("migration: invalid ID %q; want vX.Y.Z-N-name", value)
	}
	parts := make([]uint64, 4)
	for i := range parts {
		parsed, err := strconv.ParseUint(match[i+1], 10, 64)
		if err != nil {
			return ID{}, fmt.Errorf("migration: invalid numeric component in %q: %w", value, err)
		}
		parts[i] = parsed
	}
	return ID{Major: parts[0], Minor: parts[1], Patch: parts[2], Sequence: parts[3], Name: match[5]}, nil
}

// CanonicalID returns a dotted, extension-free migration ID.
func CanonicalID(value string) (string, error) {
	id, err := ParseID(value)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func compareID(left, right ID) int {
	leftParts := [...]uint64{left.Major, left.Minor, left.Patch, left.Sequence}
	rightParts := [...]uint64{right.Major, right.Minor, right.Patch, right.Sequence}
	for i := range leftParts {
		if leftParts[i] < rightParts[i] {
			return -1
		}
		if leftParts[i] > rightParts[i] {
			return 1
		}
	}
	return strings.Compare(left.Name, right.Name)
}

// State is the durable migration state. Its core JSON fields are compatible
// with pyfit's MigrationHistory; Checksums is a forward-compatible extension.
type State struct {
	LastRunMigration  string            `json:"last_run_migration"`
	LastRunTimestamp  int64             `json:"last_run_timestamp"`
	Migrations        map[string]*int64 `json:"migrations"`
	SkippedMigrations map[string]int64  `json:"skipped_migrations"`
	Checksums         map[string]string `json:"checksums,omitempty"`
}

// NewState returns initialized, empty migration state.
func NewState() State {
	return State{
		Migrations:        make(map[string]*int64),
		SkippedMigrations: make(map[string]int64),
		Checksums:         make(map[string]string),
	}
}

func (state *State) normalize() error {
	if state.Migrations == nil {
		state.Migrations = make(map[string]*int64)
	}
	if state.SkippedMigrations == nil {
		state.SkippedMigrations = make(map[string]int64)
	}
	if state.Checksums == nil {
		state.Checksums = make(map[string]string)
	}

	canonicalMigrations := make(map[string]*int64, len(state.Migrations))
	for key, timestamp := range state.Migrations {
		canonical, err := CanonicalID(key)
		if err != nil {
			return fmt.Errorf("migration: invalid persisted migration %q: %w", key, err)
		}
		if _, duplicate := canonicalMigrations[canonical]; duplicate {
			return fmt.Errorf("migration: duplicate persisted migration %q", canonical)
		}
		canonicalMigrations[canonical] = timestamp
	}
	state.Migrations = canonicalMigrations

	canonicalSkipped := make(map[string]int64, len(state.SkippedMigrations))
	for key, timestamp := range state.SkippedMigrations {
		canonical, err := CanonicalID(key)
		if err != nil {
			return fmt.Errorf("migration: invalid skipped migration %q: %w", key, err)
		}
		if _, duplicate := canonicalSkipped[canonical]; duplicate {
			return fmt.Errorf("migration: duplicate skipped migration %q", canonical)
		}
		canonicalSkipped[canonical] = timestamp
	}
	state.SkippedMigrations = canonicalSkipped

	canonicalChecksums := make(map[string]string, len(state.Checksums))
	for key, checksum := range state.Checksums {
		canonical, err := CanonicalID(key)
		if err != nil {
			return fmt.Errorf("migration: invalid checksum migration %q: %w", key, err)
		}
		if _, duplicate := canonicalChecksums[canonical]; duplicate {
			return fmt.Errorf("migration: duplicate checksum migration %q", canonical)
		}
		canonicalChecksums[canonical] = checksum
	}
	state.Checksums = canonicalChecksums

	if state.LastRunMigration != "" {
		canonical, err := CanonicalID(state.LastRunMigration)
		if err != nil {
			return fmt.Errorf("migration: invalid last migration: %w", err)
		}
		state.LastRunMigration = canonical
	}
	return nil
}

// DecodeState reads fit-go/pyfit map state and legacy node-migrate array state.
func DecodeState(reader io.Reader) (State, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxStateBytes+1))
	if err != nil {
		return State{}, fmt.Errorf("migration: read state: %w", err)
	}
	if len(data) > maxStateBytes {
		return State{}, fmt.Errorf("migration: state exceeds %d bytes", maxStateBytes)
	}
	if strings.TrimSpace(string(data)) == "" {
		return NewState(), nil
	}

	var probe struct {
		Migrations json.RawMessage `json:"migrations"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return State{}, fmt.Errorf("migration: decode state: %w", err)
	}
	if len(probe.Migrations) > 0 && probe.Migrations[0] == '[' {
		return decodeNodeState(data)
	}

	var raw struct {
		LastRunMigration  string            `json:"last_run_migration"`
		LastRunTimestamp  json.RawMessage   `json:"last_run_timestamp"`
		Migrations        map[string]*int64 `json:"migrations"`
		SkippedMigrations map[string]int64  `json:"skipped_migrations"`
		Checksums         map[string]string `json:"checksums"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return State{}, fmt.Errorf("migration: decode state: %w", err)
	}
	state := State{
		LastRunMigration:  raw.LastRunMigration,
		Migrations:        raw.Migrations,
		SkippedMigrations: raw.SkippedMigrations,
		Checksums:         raw.Checksums,
	}
	state.LastRunTimestamp, err = decodeTimestamp(raw.LastRunTimestamp)
	if err != nil {
		return State{}, err
	}
	if err := state.normalize(); err != nil {
		return State{}, err
	}
	return state, nil
}

// EncodeState writes normalized, indented state JSON.
func EncodeState(writer io.Writer, state State) error {
	if writer == nil {
		return errors.New("migration: nil state writer")
	}
	if err := state.normalize(); err != nil {
		return err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("migration: encode state: %w", err)
	}
	if encoded.Len() > maxStateBytes {
		return fmt.Errorf("migration: state exceeds %d bytes", maxStateBytes)
	}
	if _, err := io.Copy(writer, &encoded); err != nil {
		return fmt.Errorf("migration: write state: %w", err)
	}
	return nil
}

func decodeTimestamp(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" || string(raw) == `""` {
		return 0, nil
	}
	var number int64
	if err := json.Unmarshal(raw, &number); err == nil {
		return number, nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr == nil {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("migration: invalid timestamp %s", string(raw))
}

func decodeNodeState(data []byte) (State, error) {
	var raw struct {
		LastRun    string `json:"lastRun"`
		Migrations []struct {
			Title     string `json:"title"`
			Timestamp *int64 `json:"timestamp"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return State{}, fmt.Errorf("migration: decode node state: %w", err)
	}
	state := NewState()
	for _, item := range raw.Migrations {
		canonical, err := CanonicalID(item.Title)
		if err != nil {
			return State{}, fmt.Errorf("migration: decode node migration %q: %w", item.Title, err)
		}
		if _, duplicate := state.Migrations[canonical]; duplicate {
			return State{}, fmt.Errorf("migration: duplicate node migration %q", canonical)
		}
		timestamp := normalizeNodeTimestamp(item.Timestamp)
		state.Migrations[canonical] = timestamp
		if timestamp != nil && *timestamp > state.LastRunTimestamp {
			state.LastRunTimestamp = *timestamp
		}
	}
	if raw.LastRun != "" {
		canonical, err := CanonicalID(raw.LastRun)
		if err != nil {
			return State{}, fmt.Errorf("migration: decode node lastRun: %w", err)
		}
		state.LastRunMigration = canonical
	}
	return state, nil
}

func normalizeNodeTimestamp(timestamp *int64) *int64 {
	if timestamp == nil {
		return nil
	}
	value := *timestamp
	// node-migrate persists Date.now() milliseconds. Values already in the
	// nanosecond range are left untouched for forward compatibility.
	const maxConvertibleMilliseconds = int64(^uint64(0)>>1) / int64(time.Millisecond)
	if value > 0 && value <= maxConvertibleMilliseconds {
		value *= int64(time.Millisecond)
	}
	return &value
}

// Store persists migration state. Implementations must not retain or mutate the
// State passed to Save after Save returns.
type Store interface {
	Load(context.Context) (State, error)
	Save(context.Context, State) error
}

// FencedStore persists state only when fenceToken still owns the distributed
// migration lease. Implementations must validate the token atomically with the
// state write; checking it in a separate request does not prevent stale owners.
type FencedStore interface {
	Store
	SaveFenced(context.Context, State, string) error
}

// FencingRequirement lets a store require lease-loss detection and fenced
// writes. Cloud state stores should implement this interface and return true.
type FencingRequirement interface {
	MigrationFencingRequired() bool
	ValidateMigrationFencing() error
}

// UnlockFunc releases an acquired migration lock.
type UnlockFunc func() error

// Locker serializes migration runs. Cloud state stores should use a
// distributed implementation such as a Kubernetes Lease.
type Locker interface {
	Lock(context.Context) (UnlockFunc, error)
}

// Lease is a renewable distributed lock acquisition. Context must be canceled
// as soon as ownership is lost. FenceToken must become invalid for every later
// owner so stale processes cannot checkpoint migration state.
type Lease struct {
	Context    context.Context
	FenceToken string
	Unlock     UnlockFunc
}

// LeaseLocker adds lease-loss notification and fencing to Locker. Runner calls
// LockLease instead of Lock whenever a locker implements this interface.
type LeaseLocker interface {
	Locker
	LockLease(context.Context) (Lease, error)
}

type fenceTokenContextKey struct{}

// FenceTokenFromContext returns the current migration fence token. Migration
// functions that mutate a datastore should include this token in the same
// transaction or conditional write as their mutation.
func FenceTokenFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	token, ok := ctx.Value(fenceTokenContextKey{}).(string)
	return token, ok && token != ""
}

// Status describes one registered migration and its durable state.
type Status struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	Applied     bool   `json:"applied"`
	Skipped     bool   `json:"skipped"`
	Timestamp   int64  `json:"timestamp,omitempty"`
}

// RunOptions select an inclusive migration range. To accepts vX.Y.Z or a full
// migration ID. From is optional; pending migrations before From are durably
// marked as skipped, matching pyfit's explicit from-version behavior.
type RunOptions struct {
	From string
	To   string
}

// RevertOptions reverts applied migrations in reverse order, inclusively from
// the latest migration down through To. To accepts a version or full ID.
type RevertOptions struct {
	To string
}

// Runner executes a validated migration registry against durable state.
type Runner struct {
	migrations []Migration
	parsed     []ID
	store      Store
	locker     Locker
	now        func() time.Time
}

// NewRunner validates migrations and requires an explicit locker. Requiring a
// locker prevents cloud-backed migration commands from accidentally running
// concurrently in multiple pods.
func NewRunner(migrations []Migration, store Store, locker Locker) (*Runner, error) {
	if len(migrations) == 0 {
		return nil, ErrNoMigrations
	}
	if store == nil {
		return nil, errors.New("migration: nil store")
	}
	if locker == nil {
		return nil, errors.New("migration: nil locker")
	}
	if _, leaseBased := locker.(LeaseLocker); leaseBased {
		if _, fenced := store.(FencedStore); !fenced {
			return nil, errors.New("migration: LeaseLocker requires a FencedStore")
		}
	}
	if requirement, ok := store.(FencingRequirement); ok && requirement.MigrationFencingRequired() {
		if err := requirement.ValidateMigrationFencing(); err != nil {
			return nil, fmt.Errorf("migration: invalid store fencing: %w", err)
		}
		if _, ok := store.(FencedStore); !ok {
			return nil, errors.New("migration: fencing-required store does not implement FencedStore")
		}
		if _, ok := locker.(LeaseLocker); !ok {
			return nil, errors.New("migration: fencing-required store requires a LeaseLocker")
		}
	}

	type entry struct {
		migration Migration
		id        ID
	}
	entries := make([]entry, 0, len(migrations))
	seen := make(map[string]struct{}, len(migrations))
	for _, item := range migrations {
		id, err := ParseID(item.ID)
		if err != nil {
			return nil, err
		}
		if item.Up == nil {
			return nil, fmt.Errorf("migration: %s has no Up function", id.String())
		}
		canonical := id.String()
		if _, duplicate := seen[canonical]; duplicate {
			return nil, fmt.Errorf("migration: duplicate ID %s", canonical)
		}
		seen[canonical] = struct{}{}
		item.ID = canonical
		entries = append(entries, entry{migration: item, id: id})
	}
	sort.Slice(entries, func(i, j int) bool { return compareID(entries[i].id, entries[j].id) < 0 })

	runner := &Runner{store: store, locker: locker, now: time.Now}
	for _, item := range entries {
		runner.migrations = append(runner.migrations, item.migration)
		runner.parsed = append(runner.parsed, item.id)
	}
	return runner, nil
}

// List returns all registered migrations with their current durable state.
func (runner *Runner) List(ctx context.Context) ([]Status, error) {
	state, err := runner.loadState(ctx)
	if err != nil {
		return nil, err
	}
	if err := runner.validateState(state); err != nil {
		return nil, err
	}
	return runner.statuses(state), nil
}

// Run applies pending migrations through the requested target.
func (runner *Runner) Run(ctx context.Context, options RunOptions) ([]Status, error) {
	return runner.withLock(ctx, func(runCtx context.Context) ([]Status, error) {
		state, err := runner.loadState(runCtx)
		if err != nil {
			return nil, err
		}
		if err := runner.validateState(state); err != nil {
			return nil, err
		}
		from, to, err := runner.resolveRunRange(options)
		if err != nil {
			return nil, err
		}

		if err := runner.backfillChecksums(runCtx, &state); err != nil {
			return nil, err
		}

		for index := from; index <= to; index++ {
			migration := runner.migrations[index]
			if timestamp := state.Migrations[migration.ID]; timestamp != nil {
				continue
			}

			if err := checkExecutionContext(runCtx); err != nil {
				return nil, err
			}
			if err := migration.Up(runCtx); err != nil {
				return nil, fmt.Errorf("migration: apply %s: %w", migration.ID, err)
			}
			if err := checkExecutionContext(runCtx); err != nil {
				return nil, fmt.Errorf("migration: applied %s after lease loss; state was not checkpointed: %w", migration.ID, err)
			}
			now := runner.timestamp()
			state.Migrations[migration.ID] = &now
			delete(state.SkippedMigrations, migration.ID)
			state.LastRunMigration = migration.ID
			state.LastRunTimestamp = now
			if migration.Checksum != "" {
				state.Checksums[migration.ID] = migration.Checksum
			}
			if err := runner.saveState(runCtx, state); err != nil {
				return nil, fmt.Errorf("migration: applied %s but state checkpoint failed: %w", migration.ID, err)
			}
		}
		// Match pyfit's from-version semantics: only suppress earlier migrations
		// after the requested range has completed successfully.
		for index := 0; index < from; index++ {
			migration := runner.migrations[index]
			if state.Migrations[migration.ID] != nil {
				continue
			}
			now := runner.timestamp()
			state.Migrations[migration.ID] = &now
			state.SkippedMigrations[migration.ID] = now
			if migration.Checksum != "" {
				state.Checksums[migration.ID] = migration.Checksum
			}
			if err := runner.saveState(runCtx, state); err != nil {
				return nil, fmt.Errorf("migration: save skipped %s: %w", migration.ID, err)
			}
		}
		return runner.statuses(state), nil
	})
}

// Revert reverts every applied migration from the latest down through To.
func (runner *Runner) Revert(ctx context.Context, options RevertOptions) ([]Status, error) {
	if strings.TrimSpace(options.To) == "" {
		return nil, errors.New("migration: revert target is required; use RevertOne for the latest migration")
	}
	return runner.withLock(ctx, func(runCtx context.Context) ([]Status, error) {
		state, err := runner.loadState(runCtx)
		if err != nil {
			return nil, err
		}
		if err := runner.validateState(state); err != nil {
			return nil, err
		}
		to, err := runner.resolveTarget(options.To, true)
		if err != nil {
			return nil, err
		}
		return runner.revertState(runCtx, state, to)
	})
}

// RevertOne reverts only the latest applied migration.
func (runner *Runner) RevertOne(ctx context.Context) ([]Status, error) {
	return runner.withLock(ctx, func(runCtx context.Context) ([]Status, error) {
		state, err := runner.loadState(runCtx)
		if err != nil {
			return nil, err
		}
		if err := runner.validateState(state); err != nil {
			return nil, err
		}
		for index := len(runner.migrations) - 1; index >= 0; index-- {
			if state.Migrations[runner.migrations[index].ID] != nil {
				return runner.revertState(runCtx, state, index)
			}
		}
		return runner.statuses(state), nil
	})
}

func (runner *Runner) revertState(ctx context.Context, state State, to int) ([]Status, error) {
	for index := len(runner.migrations) - 1; index >= to; index-- {
		migration := runner.migrations[index]
		if state.Migrations[migration.ID] == nil {
			continue
		}
		if _, skipped := state.SkippedMigrations[migration.ID]; !skipped && migration.Down == nil {
			return nil, fmt.Errorf("%w: %s", ErrIrreversible, migration.ID)
		}
	}
	for index := len(runner.migrations) - 1; index >= to; index-- {
		migration := runner.migrations[index]
		if state.Migrations[migration.ID] == nil {
			continue
		}
		if _, skipped := state.SkippedMigrations[migration.ID]; !skipped {
			if err := checkExecutionContext(ctx); err != nil {
				return nil, err
			}
			if err := migration.Down(ctx); err != nil {
				return nil, fmt.Errorf("migration: revert %s: %w", migration.ID, err)
			}
			if err := checkExecutionContext(ctx); err != nil {
				return nil, fmt.Errorf("migration: reverted %s after lease loss; state was not checkpointed: %w", migration.ID, err)
			}
		}
		state.Migrations[migration.ID] = nil
		delete(state.SkippedMigrations, migration.ID)
		delete(state.Checksums, migration.ID)
		state.LastRunMigration, state.LastRunTimestamp = runner.latestApplied(state)
		if err := runner.saveState(ctx, state); err != nil {
			return nil, fmt.Errorf("migration: reverted %s but state checkpoint failed: %w", migration.ID, err)
		}
	}
	return runner.statuses(state), nil
}

func (runner *Runner) latestApplied(state State) (string, int64) {
	for index := len(runner.migrations) - 1; index >= 0; index-- {
		migration := runner.migrations[index]
		if timestamp := state.Migrations[migration.ID]; timestamp != nil {
			return migration.ID, *timestamp
		}
	}
	return "", 0
}

func (runner *Runner) withLock(ctx context.Context, operation func(context.Context) ([]Status, error)) ([]Status, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx := ctx
	var unlock UnlockFunc
	cancelRun := func() {}
	defer cancelRun()
	if leaseLocker, ok := runner.locker.(LeaseLocker); ok {
		lease, err := leaseLocker.LockLease(ctx)
		if err != nil {
			return nil, fmt.Errorf("migration: acquire lease: %w", err)
		}
		if lease.Unlock == nil {
			return nil, errors.New("migration: lease locker returned a nil unlock function")
		}
		if lease.Context == nil {
			return nil, errors.Join(
				errors.New("migration: lease locker returned a nil context"),
				wrapUnlockError(lease.Unlock()),
			)
		}
		token := strings.TrimSpace(lease.FenceToken)
		if token == "" {
			return nil, errors.Join(
				errors.New("migration: lease locker returned an empty fence token"),
				wrapUnlockError(lease.Unlock()),
			)
		}
		if _, ok := runner.store.(FencedStore); !ok {
			return nil, errors.Join(
				errors.New("migration: LeaseLocker requires a FencedStore"),
				wrapUnlockError(lease.Unlock()),
			)
		}
		leaseCtx, cancel := context.WithCancel(lease.Context)
		stopParentCancellation := context.AfterFunc(ctx, cancel)
		cancelRun = func() {
			stopParentCancellation()
			cancel()
		}
		runCtx = context.WithValue(leaseCtx, fenceTokenContextKey{}, token)
		unlock = lease.Unlock
	} else {
		var err error
		unlock, err = runner.locker.Lock(ctx)
		if err != nil {
			return nil, fmt.Errorf("migration: acquire lock: %w", err)
		}
		if unlock == nil {
			return nil, errors.New("migration: locker returned a nil unlock function")
		}
	}

	var (
		result       []Status
		operationErr error
		unlockErr    error
	)
	func() {
		defer func() { unlockErr = unlock() }()
		result, operationErr = operation(runCtx)
	}()
	if operationErr != nil {
		if unlockErr != nil {
			return nil, errors.Join(operationErr, fmt.Errorf("migration: release lock: %w", unlockErr))
		}
		return nil, operationErr
	}
	if unlockErr != nil {
		return nil, fmt.Errorf("migration: release lock: %w", unlockErr)
	}
	return result, nil
}

func wrapUnlockError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("migration: release invalid lease: %w", err)
}

func checkExecutionContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("migration: execution context ended: %w", err)
	}
	return nil
}

func (runner *Runner) loadState(ctx context.Context) (State, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := checkExecutionContext(ctx); err != nil {
		return State{}, err
	}
	state, err := runner.store.Load(ctx)
	if err != nil {
		return State{}, err
	}
	if err := state.normalize(); err != nil {
		return State{}, fmt.Errorf("migration: normalize state: %w", err)
	}
	return state, nil
}

func (runner *Runner) saveState(ctx context.Context, state State) error {
	if err := checkExecutionContext(ctx); err != nil {
		return err
	}
	if token, ok := FenceTokenFromContext(ctx); ok {
		store, supported := runner.store.(FencedStore)
		if !supported {
			return errors.New("migration: fenced execution requires a FencedStore")
		}
		return store.SaveFenced(ctx, state, token)
	}
	if requirement, ok := runner.store.(FencingRequirement); ok && requirement.MigrationFencingRequired() {
		return errors.New("migration: fencing-required store cannot be saved without a fence token")
	}
	return runner.store.Save(ctx, state)
}

func (runner *Runner) backfillChecksums(ctx context.Context, state *State) error {
	changed := false
	for _, migration := range runner.migrations {
		if state.Migrations[migration.ID] == nil || migration.Checksum == "" || state.Checksums[migration.ID] != "" {
			continue
		}
		state.Checksums[migration.ID] = migration.Checksum
		changed = true
	}
	if !changed {
		return nil
	}
	if err := runner.saveState(ctx, *state); err != nil {
		return fmt.Errorf("migration: save checksum baseline: %w", err)
	}
	return nil
}

func (runner *Runner) timestamp() int64 {
	return runner.now().UTC().UnixNano()
}

func (runner *Runner) validateState(state State) error {
	registered := make(map[string]Migration, len(runner.migrations))
	for _, migration := range runner.migrations {
		registered[migration.ID] = migration
	}
	for id, timestamp := range state.Migrations {
		if timestamp == nil {
			continue
		}
		migration, exists := registered[id]
		if !exists {
			return fmt.Errorf("%w: applied migration %s is not compiled into this binary", ErrStateDrift, id)
		}
		if persisted := state.Checksums[id]; persisted != "" && persisted != migration.Checksum {
			return fmt.Errorf("%w: checksum changed for %s", ErrStateDrift, id)
		}
	}
	for id := range state.SkippedMigrations {
		if state.Migrations[id] == nil {
			return fmt.Errorf("%w: skipped migration %s is not marked applied", ErrStateDrift, id)
		}
	}
	for id := range state.Checksums {
		if _, exists := registered[id]; !exists {
			return fmt.Errorf("%w: checksum migration %s is not compiled into this binary", ErrStateDrift, id)
		}
	}
	return nil
}

func (runner *Runner) statuses(state State) []Status {
	result := make([]Status, 0, len(runner.migrations))
	for _, migration := range runner.migrations {
		timestamp := state.Migrations[migration.ID]
		_, skipped := state.SkippedMigrations[migration.ID]
		status := Status{ID: migration.ID, Description: migration.Description, Applied: timestamp != nil, Skipped: skipped}
		if timestamp != nil {
			status.Timestamp = *timestamp
		}
		result = append(result, status)
	}
	return result
}

func (runner *Runner) resolveRunRange(options RunOptions) (int, int, error) {
	to, err := runner.resolveTarget(options.To, false)
	if err != nil {
		return 0, 0, err
	}
	from := 0
	if strings.TrimSpace(options.From) != "" {
		from, err = runner.resolveTarget(options.From, true)
		if err != nil {
			return 0, 0, err
		}
	}
	if from > to {
		return 0, 0, fmt.Errorf("migration: from target %q is after to target %q", options.From, options.To)
	}
	return from, to, nil
}

func (runner *Runner) resolveTarget(target string, firstInVersion bool) (int, error) {
	target = strings.TrimSpace(target)
	if target == "" || strings.EqualFold(target, "latest") {
		if firstInVersion && target == "" {
			return len(runner.migrations) - 1, nil
		}
		return len(runner.migrations) - 1, nil
	}
	if version := versionPattern.FindStringSubmatch(target); version != nil {
		major, _ := strconv.ParseUint(version[1], 10, 64)
		minor, _ := strconv.ParseUint(version[2], 10, 64)
		patch, _ := strconv.ParseUint(version[3], 10, 64)
		canonical := fmt.Sprintf("v%d.%d.%d", major, minor, patch)
		match := -1
		for index, id := range runner.parsed {
			if id.Version() != canonical {
				continue
			}
			if firstInVersion {
				return index, nil
			}
			match = index
		}
		if match >= 0 {
			return match, nil
		}
		return 0, fmt.Errorf("migration: target version %q is not registered", target)
	}
	canonical, err := CanonicalID(target)
	if err != nil {
		return 0, err
	}
	for index, migration := range runner.migrations {
		if migration.ID == canonical {
			return index, nil
		}
	}
	return 0, fmt.Errorf("migration: target %q is not registered", target)
}
