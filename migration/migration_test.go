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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseIDSupportsLegacyFormats(t *testing.T) {
	t.Parallel()
	for input, expected := range map[string]string{
		"v2.10.0-12-add_company.js":    "v2.10.0-12-add_company",
		"v2_10_0-12-add_company.py":    "v2.10.0-12-add_company",
		"002010000012-add_company.js":  "v2.10.0-12-add_company",
		"v0002.0010.0000-0012-name.go": "v2.10.0-12-name",
	} {
		input, expected := input, expected
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			actual, err := CanonicalID(input)
			if err != nil {
				t.Fatal(err)
			}
			if actual != expected {
				t.Fatalf("CanonicalID() = %q, want %q", actual, expected)
			}
		})
	}
}

func TestRunnerSortsAndCheckpointsEachMigration(t *testing.T) {
	store := newCountingStore()
	var applied []string
	runner := mustRunner(t, []Migration{
		recordingMigration("v2.10.0-10-ten", &applied),
		recordingMigration("v2.9.9-1-old", &applied),
		recordingMigration("v2.10.0-2-two", &applied),
	}, store, store)
	runner.now = incrementingClock()

	statuses, err := runner.Run(context.Background(), RunOptions{To: "v2.10.0"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"up:v2.9.9-1-old", "up:v2.10.0-2-two", "up:v2.10.0-10-ten"}
	if !reflect.DeepEqual(applied, want) {
		t.Fatalf("applied = %v, want %v", applied, want)
	}
	if store.saves != 3 {
		t.Fatalf("state saves = %d, want 3", store.saves)
	}
	for _, status := range statuses {
		if !status.Applied || status.Skipped {
			t.Fatalf("unexpected status: %+v", status)
		}
	}
}

func TestRunnerFailurePreservesLastSuccessfulCheckpoint(t *testing.T) {
	store := newCountingStore()
	boom := errors.New("boom")
	runner := mustRunner(t, []Migration{
		{ID: "v1.0.0-1-first", Up: func(context.Context) error { return nil }},
		{ID: "v1.0.0-2-second", Up: func(context.Context) error { return boom }},
	}, store, store)
	runner.now = incrementingClock()

	_, err := runner.Run(context.Background(), RunOptions{To: "latest"})
	if !errors.Is(err, boom) {
		t.Fatalf("Run() error = %v, want boom", err)
	}
	state, _ := store.Load(context.Background())
	if state.Migrations["v1.0.0-1-first"] == nil {
		t.Fatal("first migration was not checkpointed")
	}
	if state.Migrations["v1.0.0-2-second"] != nil {
		t.Fatal("failed migration was marked applied")
	}
	if store.saves != 1 {
		t.Fatalf("state saves = %d, want 1", store.saves)
	}
}

func TestRunnerFromMarksEarlierMigrationsSkipped(t *testing.T) {
	store := NewMemoryStore()
	var calls []string
	runner := mustRunner(t, []Migration{
		recordingMigration("v1.0.0-1-first", &calls),
		recordingMigration("v1.1.0-1-second", &calls),
		recordingMigration("v1.2.0-1-third", &calls),
	}, store, store)
	runner.now = incrementingClock()

	statuses, err := runner.Run(context.Background(), RunOptions{From: "v1.1.0", To: "v1.2.0"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"up:v1.1.0-1-second", "up:v1.2.0-1-third"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	if !statuses[0].Applied || !statuses[0].Skipped {
		t.Fatalf("first status = %+v, want applied skipped", statuses[0])
	}
}

func TestRunnerFromDoesNotPersistSkipsWhenSelectedMigrationFails(t *testing.T) {
	store := NewMemoryStore()
	boom := errors.New("boom")
	runner := mustRunner(t, []Migration{
		{ID: "v1.0.0-1-first", Up: func(context.Context) error { return nil }},
		{ID: "v1.1.0-1-selected", Up: func(context.Context) error { return boom }},
	}, store, store)
	_, err := runner.Run(context.Background(), RunOptions{From: "v1.1.0", To: "latest"})
	if !errors.Is(err, boom) {
		t.Fatalf("Run() error = %v", err)
	}
	state, _ := store.Load(context.Background())
	if state.Migrations["v1.0.0-1-first"] != nil || len(state.SkippedMigrations) != 0 {
		t.Fatalf("failed run persisted skips: %+v", state)
	}
}

func TestRunnerRevertUsesReverseOrderAndDoesNotRunDownForSkipped(t *testing.T) {
	store := NewMemoryStore()
	var calls []string
	runner := mustRunner(t, []Migration{
		recordingMigration("v1.0.0-1-first", &calls),
		recordingMigration("v1.1.0-1-second", &calls),
		recordingMigration("v1.2.0-1-third", &calls),
	}, store, store)
	runner.now = incrementingClock()
	if _, err := runner.Run(context.Background(), RunOptions{From: "v1.1.0", To: "latest"}); err != nil {
		t.Fatal(err)
	}
	calls = nil

	statuses, err := runner.Revert(context.Background(), RevertOptions{To: "v1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"down:v1.2.0-1-third", "down:v1.1.0-1-second"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("revert calls = %v, want %v", calls, want)
	}
	for _, status := range statuses {
		if status.Applied || status.Skipped {
			t.Fatalf("unexpected status after revert: %+v", status)
		}
	}
}

func TestRunnerRejectsUnknownAppliedAndChangedChecksum(t *testing.T) {
	for name, state := range map[string]State{
		"unknown": func() State {
			state := NewState()
			timestamp := int64(1)
			state.Migrations["v1.0.0-9-unknown"] = &timestamp
			return state
		}(),
		"checksum": func() State {
			state := NewState()
			timestamp := int64(1)
			state.Migrations["v1.0.0-1-known"] = &timestamp
			state.Checksums["v1.0.0-1-known"] = "old"
			return state
		}(),
		"unknown checksum entry": func() State {
			state := NewState()
			state.Checksums["v1.0.0-9-unknown"] = "checksum"
			return state
		}(),
	} {
		name, state := name, state
		t.Run(name, func(t *testing.T) {
			store := NewMemoryStore()
			if err := store.Save(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			runner := mustRunner(t, []Migration{{
				ID: "v1.0.0-1-known", Checksum: "new", Up: func(context.Context) error { return nil },
			}}, store, store)
			_, err := runner.List(context.Background())
			if !errors.Is(err, ErrStateDrift) {
				t.Fatalf("List() error = %v, want ErrStateDrift", err)
			}
		})
	}
}

func TestRunnerTreatsLegacyLastRunAsAdvisory(t *testing.T) {
	store := NewMemoryStore()
	state := NewState()
	state.LastRunMigration = "v1.0.0-9-reverted"
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known", Up: func(context.Context) error { return nil },
	}}, store, store)
	if _, err := runner.List(context.Background()); err != nil {
		t.Fatalf("List rejected advisory legacy lastRun: %v", err)
	}
}

func TestRunnerLeaseUsesFenceForMigrationAndCheckpoint(t *testing.T) {
	store := newFencedMemoryStore()
	locker := FuncLeaseLocker(func(ctx context.Context) (Lease, error) {
		return Lease{Context: ctx, FenceToken: "fence-42", Unlock: func() error { return nil }}, nil
	})
	var observed string
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known",
		Up: func(ctx context.Context) error {
			observed, _ = FenceTokenFromContext(ctx)
			return nil
		},
	}}, store, locker)
	if _, err := runner.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	if observed != "fence-42" || !reflect.DeepEqual(store.tokens, []string{"fence-42"}) {
		t.Fatalf("migration token = %q, checkpoint tokens = %v", observed, store.tokens)
	}
}

func TestRunnerLeaseLossPreventsCheckpoint(t *testing.T) {
	store := newFencedMemoryStore()
	leaseCtx, cancelLease := context.WithCancel(context.Background())
	locker := FuncLeaseLocker(func(context.Context) (Lease, error) {
		return Lease{Context: leaseCtx, FenceToken: "fence-1", Unlock: func() error { return nil }}, nil
	})
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known",
		Up: func(context.Context) error {
			cancelLease()
			return nil
		},
	}}, store, locker)
	_, err := runner.Run(context.Background(), RunOptions{To: "latest"})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "state was not checkpointed") {
		t.Fatalf("Run() error = %v", err)
	}
	state, loadErr := store.Load(context.Background())
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if state.Migrations["v1.0.0-1-known"] != nil || len(store.tokens) != 0 {
		t.Fatalf("lease-lost run checkpointed state: %+v, tokens=%v", state, store.tokens)
	}
}

func TestRunnerRejectsLeaseWithoutFencedStoreDuringConstruction(t *testing.T) {
	store := NewMemoryStore()
	locker := FuncLeaseLocker(func(ctx context.Context) (Lease, error) {
		return Lease{Context: ctx, FenceToken: "fence-1", Unlock: func() error { return nil }}, nil
	})
	called := false
	_, err := NewRunner([]Migration{{
		ID: "v1.0.0-1-known", Up: func(context.Context) error { called = true; return nil },
	}}, store, locker)
	if err == nil || !strings.Contains(err.Error(), "FencedStore") || called {
		t.Fatalf("NewRunner() error = %v, migration called = %t", err, called)
	}
}

func TestRunnerRejectsFencingRequiredStoreWithoutLeaseLocker(t *testing.T) {
	store := newFencedMemoryStore()
	store.required = true
	_, err := NewRunner([]Migration{{
		ID: "v1.0.0-1-known", Up: func(context.Context) error { return nil },
	}}, store, NewMemoryStore())
	if err == nil || !strings.Contains(err.Error(), "LeaseLocker") {
		t.Fatalf("NewRunner() error = %v", err)
	}
}

func TestRunnerNormalizesStateFromCustomStore(t *testing.T) {
	store := &zeroStateStore{}
	locker := NewMemoryStore()
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known",
		Up: func(context.Context) error { return nil },
	}}, store, locker)
	statuses, err := runner.Run(context.Background(), RunOptions{To: "latest"})
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 1 || !statuses[0].Applied {
		t.Fatalf("statuses = %+v", statuses)
	}
}

func TestRunnerRejectsNilUnlockFunction(t *testing.T) {
	store := NewMemoryStore()
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known",
		Up: func(context.Context) error { return nil },
	}}, store, nilUnlockLocker{})
	_, err := runner.Run(context.Background(), RunOptions{To: "latest"})
	if err == nil || !strings.Contains(err.Error(), "nil unlock") {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunnerReleasesLockWhenMigrationPanics(t *testing.T) {
	store := NewMemoryStore()
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-panic",
		Up: func(context.Context) error { panic("migration panic") },
	}}, store, store)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Run did not propagate migration panic")
			}
		}()
		_, _ = runner.Run(context.Background(), RunOptions{To: "latest"})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	unlock, err := store.Lock(ctx)
	if err != nil {
		t.Fatalf("lock remained held after panic: %v", err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerIrreversibleMigrationFailsBeforeStateChange(t *testing.T) {
	store := NewMemoryStore()
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-forward_only", Up: func(context.Context) error { return nil },
	}}, store, store)
	if _, err := runner.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	_, err := runner.Revert(context.Background(), RevertOptions{To: "v1.0.0"})
	if !errors.Is(err, ErrIrreversible) {
		t.Fatalf("Revert() error = %v, want ErrIrreversible", err)
	}
	state, _ := store.Load(context.Background())
	if state.Migrations["v1.0.0-1-forward_only"] == nil {
		t.Fatal("irreversible migration state was removed")
	}
}

func TestRunnerPreflightsIrreversibleRangeBeforeRevertingNewerMigrations(t *testing.T) {
	store := NewMemoryStore()
	var calls []string
	runner := mustRunner(t, []Migration{
		{ID: "v1.0.0-1-forward_only", Up: func(context.Context) error { return nil }},
		recordingMigration("v1.1.0-1-reversible", &calls),
	}, store, store)
	if _, err := runner.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	calls = nil
	_, err := runner.Revert(context.Background(), RevertOptions{To: "v1.0.0"})
	if !errors.Is(err, ErrIrreversible) {
		t.Fatalf("Revert() error = %v", err)
	}
	if len(calls) != 0 {
		t.Fatalf("revert ran before irreversible preflight: %v", calls)
	}
	state, _ := store.Load(context.Background())
	if state.Migrations["v1.0.0-1-forward_only"] == nil || state.Migrations["v1.1.0-1-reversible"] == nil {
		t.Fatalf("state changed during failed preflight: %+v", state)
	}
}

func TestRevertStateCanBeReadByPreviousRegistry(t *testing.T) {
	store := NewMemoryStore()
	migrations := []Migration{
		recordingMigration("v1.0.0-1-first", &[]string{}),
		recordingMigration("v1.1.0-1-second", &[]string{}),
	}
	current := mustRunner(t, migrations, store, store)
	if _, err := current.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	if _, err := current.Revert(context.Background(), RevertOptions{To: "v1.1.0"}); err != nil {
		t.Fatal(err)
	}
	previous := mustRunner(t, migrations[:1], store, store)
	statuses, err := previous.List(context.Background())
	if err != nil {
		t.Fatalf("previous registry rejected reverted state: %v", err)
	}
	if len(statuses) != 1 || !statuses[0].Applied {
		t.Fatalf("previous statuses = %+v", statuses)
	}
}

func TestRunnerBackfillsMissingChecksumBaseline(t *testing.T) {
	store := NewMemoryStore()
	timestamp := int64(1)
	state := NewState()
	state.Migrations["v1.0.0-1-known"] = &timestamp
	if err := store.Save(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known", Checksum: "baseline", Up: func(context.Context) error { return nil },
	}}, store, store)
	if _, err := runner.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	state, _ = store.Load(context.Background())
	if state.Checksums["v1.0.0-1-known"] != "baseline" {
		t.Fatalf("checksum baseline was not persisted: %+v", state.Checksums)
	}
	changed := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known", Checksum: "changed", Up: func(context.Context) error { return nil },
	}}, store, store)
	if _, err := changed.List(context.Background()); !errors.Is(err, ErrStateDrift) {
		t.Fatalf("changed checksum error = %v", err)
	}
}

func TestRevertRequiresExplicitTarget(t *testing.T) {
	store := NewMemoryStore()
	runner := mustRunner(t, []Migration{{
		ID: "v1.0.0-1-known", Up: func(context.Context) error { return nil }, Down: func(context.Context) error { return nil },
	}}, store, store)
	if _, err := runner.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Revert(context.Background(), RevertOptions{}); err == nil {
		t.Fatal("Revert accepted an empty target")
	}
	state, _ := store.Load(context.Background())
	if state.Migrations["v1.0.0-1-known"] == nil {
		t.Fatal("empty-target revert changed state")
	}
}

func TestDecodeStateSupportsPyfitAndNodeMigrate(t *testing.T) {
	t.Parallel()
	pyfit := `{
		"last_run_migration":"v1_2_0-1-add_field",
		"last_run_timestamp":"123",
		"migrations":{"v1_2_0-1-add_field":123,"v1_1_0-1-old":null},
		"skipped_migrations":{}
	}`
	node := `{
		"lastRun":"001002000001-add_field.js",
		"migrations":[{"title":"001002000001-add_field.js","timestamp":1700000000123}]
	}`
	for name, fixture := range map[string]struct {
		input     string
		timestamp int64
	}{
		"pyfit": {input: pyfit, timestamp: 123},
		"node":  {input: node, timestamp: 1700000000123 * int64(time.Millisecond)},
	} {
		name, fixture := name, fixture
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			state, err := DecodeState(strings.NewReader(fixture.input))
			if err != nil {
				t.Fatal(err)
			}
			if state.LastRunMigration != "v1.2.0-1-add_field" || state.LastRunTimestamp != fixture.timestamp {
				t.Fatalf("decoded state = %+v", state)
			}
			if state.Migrations["v1.2.0-1-add_field"] == nil {
				t.Fatal("applied migration missing")
			}
		})
	}
}

func TestDecodeStateRejectsOversizedInput(t *testing.T) {
	_, err := DecodeState(strings.NewReader(strings.Repeat(" ", maxStateBytes+1)))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("DecodeState() error = %v", err)
	}
}

func TestDecodeNodeStateRejectsDuplicateCanonicalAliases(t *testing.T) {
	input := `{"lastRun":"001000000001-one.js","migrations":[` +
		`{"title":"001000000001-one.js","timestamp":1700000000000},` +
		`{"title":"v1.0.0-1-one.js","timestamp":1700000000001}]}`
	if _, err := DecodeState(strings.NewReader(input)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("DecodeState() error = %v", err)
	}
}

func TestEncodeStateUsesPyfitCompatibleShape(t *testing.T) {
	t.Parallel()
	state := NewState()
	timestamp := int64(123)
	state.LastRunMigration = "v1.0.0-1-one"
	state.LastRunTimestamp = timestamp
	state.Migrations["v1.0.0-1-one"] = &timestamp
	var output bytes.Buffer
	if err := EncodeState(&output, state); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"last_run_migration"`, `"last_run_timestamp"`, `"migrations"`, `"skipped_migrations"`} {
		if !strings.Contains(output.String(), key) {
			t.Fatalf("encoded state missing %s: %s", key, output.String())
		}
	}
}

func TestStateRejectsDuplicateAliasesAndOversizedEncoding(t *testing.T) {
	for name, input := range map[string]string{
		"checksums": `{
			"migrations":{"v1.0.0-1-one":1},
			"checksums":{"v1.0.0-1-one":"one","v1_0_0-1-one":"two"}
		}`,
		"skipped": `{
			"migrations":{"v1.0.0-1-one":1},
			"skipped_migrations":{"v1.0.0-1-one":1,"v1_0_0-1-one":2}
		}`,
	} {
		if _, err := DecodeState(strings.NewReader(input)); err == nil {
			t.Fatalf("DecodeState accepted duplicate %s aliases", name)
		}
	}
	state := NewState()
	state.Checksums["v1.0.0-1-one"] = strings.Repeat("x", maxStateBytes)
	if err := EncodeState(&bytes.Buffer{}, state); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("EncodeState error = %v", err)
	}
}

func recordingMigration(id string, calls *[]string) Migration {
	return Migration{
		ID: id,
		Up: func(context.Context) error {
			*calls = append(*calls, "up:"+id)
			return nil
		},
		Down: func(context.Context) error {
			*calls = append(*calls, "down:"+id)
			return nil
		},
	}
}

func mustRunner(t *testing.T, migrations []Migration, store Store, locker Locker) *Runner {
	t.Helper()
	runner, err := NewRunner(migrations, store, locker)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func incrementingClock() func() time.Time {
	now := time.Unix(0, 0)
	return func() time.Time {
		now = now.Add(time.Nanosecond)
		return now
	}
}

type countingStore struct {
	*MemoryStore
	saves int
}

type zeroStateStore struct {
	state State
}

func (store *zeroStateStore) Load(context.Context) (State, error) { return store.state, nil }
func (store *zeroStateStore) Save(_ context.Context, state State) error {
	store.state = state
	return nil
}

type nilUnlockLocker struct{}

func (nilUnlockLocker) Lock(context.Context) (UnlockFunc, error) { return nil, nil }

type fencedMemoryStore struct {
	*MemoryStore
	mu       sync.Mutex
	tokens   []string
	required bool
}

func newFencedMemoryStore() *fencedMemoryStore {
	return &fencedMemoryStore{MemoryStore: NewMemoryStore()}
}

func (store *fencedMemoryStore) SaveFenced(ctx context.Context, state State, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	store.tokens = append(store.tokens, token)
	store.mu.Unlock()
	return store.MemoryStore.Save(ctx, state)
}

func (store *fencedMemoryStore) MigrationFencingRequired() bool { return store.required }

func (*fencedMemoryStore) ValidateMigrationFencing() error { return nil }

func newCountingStore() *countingStore {
	return &countingStore{MemoryStore: NewMemoryStore()}
}

func (store *countingStore) Save(ctx context.Context, state State) error {
	store.saves++
	return store.MemoryStore.Save(ctx, state)
}
