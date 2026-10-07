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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

func TestCreateFileUsesNextSequenceAndNeverOverwrites(t *testing.T) {
	directory := t.TempDir()
	first, err := CreateFile(directory, "migrations", "v2.10.0", "add_company")
	if err != nil {
		t.Fatal(err)
	}
	second, err := CreateFile(directory, "migrations", "v2.10.0", "add_index")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "v2.10.0-1-add_company" || second.ID != "v2.10.0-2-add_index" {
		t.Fatalf("created IDs = %q, %q", first.ID, second.ID)
	}
	data, err := os.ReadFile(first.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `ID:          "v2.10.0-1-add_company"`) {
		t.Fatalf("generated source did not contain migration ID:\n%s", data)
	}
	if filepath.Base(first.Path) != "v2_10_0_1_add_company.go" {
		t.Fatalf("generated filename = %s", filepath.Base(first.Path))
	}
}

func TestCreateFileSerializesConcurrentSequenceAllocation(t *testing.T) {
	directory := t.TempDir()
	const count = 24
	created := make(chan CreatedFile, count)
	errors := make(chan error, count)
	var group sync.WaitGroup
	for index := 0; index < count; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := CreateFileContext(context.Background(), directory, "migrations", "v3.0.0", fmt.Sprintf("change_%d", index))
			if err != nil {
				errors <- err
				return
			}
			created <- result
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	close(created)
	var ids []string
	for result := range created {
		ids = append(ids, result.ID)
		data, err := os.ReadFile(result.Path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), `ID:          "`+result.ID+`"`) ||
			!strings.Contains(string(data), "Up: func(") ||
			!strings.Contains(string(data), "Down: func(") {
			t.Fatalf("published partial skeleton %s:\n%s", result.Path, data)
		}
	}
	if len(ids) != count {
		t.Fatalf("created %d migrations, want %d", len(ids), count)
	}
	sort.Strings(ids)
	seenSequence := make(map[uint64]bool, count)
	for _, id := range ids {
		parsed, err := ParseID(id)
		if err != nil {
			t.Fatal(err)
		}
		seenSequence[parsed.Sequence] = true
	}
	for sequence := uint64(1); sequence <= count; sequence++ {
		if !seenSequence[sequence] {
			t.Fatalf("missing sequence %d in %v", sequence, ids)
		}
	}
	temporary, err := filepath.Glob(filepath.Join(directory, ".fit-migrate-create-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("temporary files remain: %v", temporary)
	}
}

func TestCreateFileHonorsCanceledContextBeforePublishing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	directory := t.TempDir()
	if _, err := CreateFileContext(ctx, directory, "migrations", "v1.0.0", "canceled"); err == nil {
		t.Fatal("CreateFileContext accepted canceled context")
	}
	files, err := filepath.Glob(filepath.Join(directory, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("canceled create published files: %v", files)
	}
}

func TestCreateFileRejectsUnsafeNames(t *testing.T) {
	for _, name := range []string{"../escape", "with-dash", ""} {
		if _, err := CreateFile(t.TempDir(), "migrations", "v1.0.0", name); err == nil {
			t.Fatalf("CreateFile accepted unsafe name %q", name)
		}
	}
}
