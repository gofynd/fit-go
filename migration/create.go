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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	packagePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	namePattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_]*$`)
)

// CreatedFile describes a generated Go migration skeleton.
type CreatedFile struct {
	ID   string
	Path string
}

// CreateFile generates the next migration skeleton using a background context.
func CreateFile(directory, packageName, version, name string) (CreatedFile, error) {
	return CreateFileContext(context.Background(), directory, packageName, version, name)
}

// CreateFileContext generates the next migration skeleton for a version. It
// serializes sequence allocation across processes and atomically publishes a
// fully written file without overwriting an existing migration.
func CreateFileContext(ctx context.Context, directory, packageName, version, name string) (created CreatedFile, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !packagePattern.MatchString(packageName) {
		return CreatedFile{}, fmt.Errorf("migration: invalid Go package %q", packageName)
	}
	if !namePattern.MatchString(name) {
		return CreatedFile{}, fmt.Errorf("migration: invalid migration name %q; use letters, numbers, and underscores", name)
	}
	versionParts := versionPattern.FindStringSubmatch(strings.TrimSpace(version))
	if versionParts == nil {
		return CreatedFile{}, fmt.Errorf("migration: invalid version %q; want vX.Y.Z", version)
	}
	major, _ := strconv.ParseUint(versionParts[1], 10, 64)
	minor, _ := strconv.ParseUint(versionParts[2], 10, 64)
	patch, _ := strconv.ParseUint(versionParts[3], 10, 64)
	canonicalVersion := fmt.Sprintf("v%d.%d.%d", major, minor, patch)

	if directory == "" {
		return CreatedFile{}, errors.New("migration: empty migration directory")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return CreatedFile{}, fmt.Errorf("migration: create directory: %w", err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: resolve directory: %w", err)
	}
	directory, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: resolve directory links: %w", err)
	}
	locker := NewFileLocker(filepath.Join(directory, ".fit-migrate-create.lock"))
	unlock, err := locker.Lock(ctx)
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: lock migration directory: %w", err)
	}
	defer func() {
		if unlockErr := unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("migration: unlock migration directory: %w", unlockErr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return CreatedFile{}, err
	}
	stale, err := filepath.Glob(filepath.Join(directory, ".fit-migrate-create-*.tmp"))
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: find stale temporary files: %w", err)
	}
	for _, path := range stale {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return CreatedFile{}, fmt.Errorf("migration: remove stale temporary file: %w", err)
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: list directory: %w", err)
	}
	prefix := fmt.Sprintf("v%d_%d_%d_", major, minor, patch)
	sequence := uint64(1)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		remainder := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), prefix), ".go")
		separator := strings.IndexByte(remainder, '_')
		if separator <= 0 {
			continue
		}
		number, parseErr := strconv.ParseUint(remainder[:separator], 10, 64)
		if parseErr == nil && number >= sequence {
			sequence = number + 1
		}
	}

	id := fmt.Sprintf("%s-%d-%s", canonicalVersion, sequence, name)
	fileName := fmt.Sprintf("%s%d_%s.go", prefix, sequence, strings.ToLower(name))
	path := filepath.Join(directory, fileName)
	variable := exportedIdentifier(fmt.Sprintf("V%d_%d_%d_%d_%s", major, minor, patch, sequence, name))
	template := fmt.Sprintf(`package %s

import (
	"context"

	"github.com/gofynd/fit-go/migration"
)

// %s is migration %s.
var %s = migration.Migration{
	ID:          %q,
	Description: %q,
	Up: func(ctx context.Context) error {
		return nil
	},
	Down: func(ctx context.Context) error {
		return nil
	},
}
`, packageName, variable, id, variable, id, strings.ReplaceAll(name, "_", " "))

	temporary, err := os.CreateTemp(directory, ".fit-migrate-create-*.tmp")
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: create temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return CreatedFile{}, fmt.Errorf("migration: set temporary file permissions: %w", err)
	}
	if _, err := temporary.WriteString(template); err != nil {
		temporary.Close()
		return CreatedFile{}, fmt.Errorf("migration: write temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return CreatedFile{}, fmt.Errorf("migration: sync temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return CreatedFile{}, fmt.Errorf("migration: close temporary file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return CreatedFile{}, err
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return CreatedFile{}, fmt.Errorf("migration: publish file: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return CreatedFile{}, fmt.Errorf("migration: remove published temporary file: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return CreatedFile{}, fmt.Errorf("migration: open directory for sync: %w", err)
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if syncErr != nil || closeErr != nil {
		return CreatedFile{}, fmt.Errorf("migration: sync directory: %w", errors.Join(syncErr, closeErr))
	}
	return CreatedFile{ID: id, Path: path}, nil
}

func exportedIdentifier(value string) string {
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9')
	})
	var result strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		result.WriteString(strings.ToUpper(part[:1]))
		result.WriteString(part[1:])
	}
	if result.Len() == 0 {
		return "Migration"
	}
	return result.String()
}
