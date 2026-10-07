// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package protofetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

// Mode selects one legacy fitproto operation.
type Mode string

const (
	// Get fetches one named proto and companion type JSON, grouped by server type.
	Get Mode = "get"
	// GetAll fetches proto/type files for configured contract folders.
	GetAll Mode = "getall"
)

// Generator generates language bindings inside the staged output directory.
// It runs before the output directory is transactionally replaced.
type Generator interface {
	Generate(context.Context, string) error
}

// GeneratorFunc adapts a function to Generator.
type GeneratorFunc func(context.Context, string) error

// Generate implements Generator.
func (generate GeneratorFunc) Generate(ctx context.Context, directory string) error {
	if generate == nil {
		return errors.New("protofetch: nil generator")
	}
	return generate(ctx, directory)
}

// CommandGenerator invokes buf, protoc, or another explicitly configured
// executable without a shell. Arguments are application-owned, not read from
// the fetched repository. The command runs inside the staged output directory.
type CommandGenerator struct {
	Executable string
	Arguments  []string
}

// Generate implements Generator.
func (generator CommandGenerator) Generate(ctx context.Context, directory string) error {
	executable := strings.TrimSpace(generator.Executable)
	if executable == "" {
		return errors.New("protofetch: empty generator executable")
	}
	command := exec.CommandContext(ctx, executable, generator.Arguments...)
	command.Dir = directory
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return fmt.Errorf("protofetch: generator %s failed: %w", filepath.Base(executable), err)
	}
	return nil
}

// ProtocGenerator invokes protoc for every staged proto file with Go and gRPC
// outputs using source-relative paths.
type ProtocGenerator struct {
	Executable     string
	ExtraArguments []string
}

// Generate implements Generator.
func (generator ProtocGenerator) Generate(ctx context.Context, directory string) error {
	files, err := listFiles(directory)
	if err != nil {
		return fmt.Errorf("protofetch: list staged proto files: %w", err)
	}
	var protoFiles []string
	for _, file := range files {
		if strings.HasSuffix(file, ".proto") {
			protoFiles = append(protoFiles, filepath.FromSlash(file))
		}
	}
	if len(protoFiles) == 0 {
		return errors.New("protofetch: protoc has no proto inputs")
	}
	executable := strings.TrimSpace(generator.Executable)
	if executable == "" {
		executable = "protoc"
	}
	arguments := []string{
		"--proto_path=.", "--go_out=.", "--go_opt=paths=source_relative",
		"--go-grpc_out=.", "--go-grpc_opt=paths=source_relative",
	}
	arguments = append(arguments, generator.ExtraArguments...)
	arguments = append(arguments, protoFiles...)
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Dir = directory
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return fmt.Errorf("protofetch: protoc failed: %w", err)
	}
	return nil
}

// Options configure one fetch operation.
type Options struct {
	Mode            Mode
	Specification   Specification
	OutputDirectory string
	// OutputRoot is the trust boundary for replacement. OutputDirectory must be
	// a strict descendant. The CLI sets this to its working directory.
	OutputRoot      string
	SourceDirectory string
	ContractsPath   string
	GitExecutable   string
	Generator       Generator
}

// Result describes files installed into the output directory.
type Result struct {
	OutputDirectory string   `json:"output_directory"`
	Files           []string `json:"files"`
}

// Fetch clones or reads contracts, stages selected files, optionally generates
// bindings, then transactionally replaces the output directory using sibling
// renames and crash recovery.
func Fetch(ctx context.Context, options Options) (result Result, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := options.Specification.validate(options.Mode, options.SourceDirectory); err != nil {
		return Result{}, err
	}
	output, err := cleanOutputPath(options.OutputDirectory, options.OutputRoot)
	if err != nil {
		return Result{}, err
	}
	parent := filepath.Dir(output)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return Result{}, fmt.Errorf("protofetch: create output parent: %w", err)
	}
	lockPath := output + ".lock"
	if err := validateLockPath(lockPath); err != nil {
		return Result{}, err
	}
	outputLock := flock.New(lockPath)
	locked, err := outputLock.TryLockContext(ctx, 50*time.Millisecond)
	if err != nil {
		return Result{}, fmt.Errorf("protofetch: acquire output lock: %w", err)
	}
	if !locked {
		return Result{}, errors.New("protofetch: output lock was not acquired")
	}
	defer func() {
		if unlockErr := outputLock.Unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("protofetch: release output lock: %w", unlockErr))
		}
	}()
	if err := recoverOutput(output); err != nil {
		return Result{}, err
	}
	outputMode, err := inspectOutput(output)
	if err != nil {
		return Result{}, err
	}

	stage, err := os.MkdirTemp(parent, ".proto-stage-*")
	if err != nil {
		return Result{}, fmt.Errorf("protofetch: create stage: %w", err)
	}
	defer os.RemoveAll(stage)
	if err := os.Chmod(stage, outputMode); err != nil {
		return Result{}, fmt.Errorf("protofetch: set staged output permissions: %w", err)
	}

	source, cleanup, err := acquireSource(ctx, options)
	if err != nil {
		return Result{}, err
	}
	defer cleanup()
	if err := rejectOutputSourceOverlap(output, source); err != nil {
		return Result{}, err
	}
	contractsPath := options.ContractsPath
	if contractsPath == "" {
		contractsPath = "contracts"
	}
	if filepath.IsAbs(contractsPath) || escapesRoot(contractsPath) {
		return Result{}, errors.New("protofetch: contracts path must stay within the source repository")
	}
	contracts, err := containedDirectory(source, contractsPath)
	if err != nil {
		return Result{}, err
	}
	if info, statErr := os.Stat(contracts); statErr != nil || !info.IsDir() {
		if statErr == nil {
			statErr = errors.New("not a directory")
		}
		return Result{}, fmt.Errorf("protofetch: contracts directory: %w", statErr)
	}

	var files []string
	switch options.Mode {
	case Get:
		files, err = copyNamedProto(contracts, stage, options.Specification.FileName)
	case GetAll:
		files, err = copyFolders(contracts, stage, options.Specification.FolderNames)
	}
	if err != nil {
		return Result{}, err
	}
	if options.Generator != nil {
		if err := options.Generator.Generate(ctx, stage); err != nil {
			return Result{}, err
		}
		files, err = listFiles(stage)
		if err != nil {
			return Result{}, err
		}
	}
	if err := syncTree(stage); err != nil {
		return Result{}, err
	}
	if err := replaceDirectory(output, stage); err != nil {
		return Result{}, err
	}
	sort.Strings(files)
	return Result{OutputDirectory: output, Files: files}, nil
}

func acquireSource(ctx context.Context, options Options) (string, func(), error) {
	if options.SourceDirectory != "" {
		absolute, err := filepath.Abs(options.SourceDirectory)
		if err != nil {
			return "", func() {}, fmt.Errorf("protofetch: resolve source: %w", err)
		}
		info, err := os.Stat(absolute)
		if err != nil || !info.IsDir() {
			if err == nil {
				err = errors.New("not a directory")
			}
			return "", func() {}, fmt.Errorf("protofetch: source directory: %w", err)
		}
		return absolute, func() {}, nil
	}

	temporary, err := os.MkdirTemp("", "fitproto-source-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("protofetch: create clone directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(temporary) }
	repository := filepath.Join(temporary, "repository")
	git := options.GitExecutable
	if git == "" {
		git = "git"
	}
	branch := strings.TrimSpace(options.Specification.Branch)
	if branch == "" {
		branch = "master"
	}
	command := exec.CommandContext(ctx, git, "clone", "--depth", "1", "--branch", branch, "--", options.Specification.Repository(), repository)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("protofetch: git clone failed: %w", err)
	}
	return repository, cleanup, nil
}

func copyNamedProto(contracts, stage, fileName string) ([]string, error) {
	type candidate struct {
		proto      string
		typeJSON   string
		serverType string
	}
	var candidates []candidate
	err := filepath.WalkDir(contracts, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("protofetch: symlink is not allowed in contracts: %s", path)
		}
		if entry.IsDir() || entry.Name() != fileName+".proto" {
			return nil
		}
		serverType, err := inferServerType(contracts, path)
		if err != nil {
			return err
		}
		typeJSON := strings.TrimSuffix(path, ".proto") + ".type.json"
		if info, err := os.Lstat(typeJSON); err != nil || !info.Mode().IsRegular() {
			if err == nil {
				err = errors.New("not a regular file")
			}
			return fmt.Errorf("protofetch: companion type file for %s: %w", path, err)
		}
		candidates = append(candidates, candidate{proto: path, typeJSON: typeJSON, serverType: serverType})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("protofetch: %s.proto not found", fileName)
	}
	seen := make(map[string]struct{}, len(candidates))
	var copied []string
	for _, item := range candidates {
		destination := filepath.Join(item.serverType, fileName+".proto")
		if _, duplicate := seen[destination]; duplicate {
			return nil, fmt.Errorf("protofetch: duplicate %s for server type %s", fileName, item.serverType)
		}
		seen[destination] = struct{}{}
		for _, pair := range [][2]string{
			{item.proto, filepath.Join(stage, item.serverType, fileName+".proto")},
			{item.typeJSON, filepath.Join(stage, item.serverType, fileName+".type.json")},
		} {
			if err := copyRegularFile(pair[0], pair[1]); err != nil {
				return nil, err
			}
			relative, _ := filepath.Rel(stage, pair[1])
			copied = append(copied, filepath.ToSlash(relative))
		}
	}
	return copied, nil
}

func copyFolders(contracts, stage string, folderNames []string) ([]string, error) {
	type selectedFolder struct {
		name   string
		source string
	}
	wanted := make(map[string]struct{}, len(folderNames))
	for _, folder := range folderNames {
		wanted[strings.ToLower(strings.TrimSpace(folder))] = struct{}{}
	}
	found := make(map[string]selectedFolder, len(wanted))
	err := filepath.WalkDir(contracts, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("protofetch: symlink is not allowed in contracts: %s", path)
		}
		if !entry.IsDir() {
			return nil
		}
		name := strings.ToLower(entry.Name())
		if _, selected := wanted[name]; !selected {
			return nil
		}
		protoDirectory := filepath.Join(path, "proto")
		if info, err := os.Stat(protoDirectory); err != nil || !info.IsDir() {
			return nil
		}
		if previous, duplicate := found[name]; duplicate {
			return fmt.Errorf("protofetch: folder %q occurs more than once (%s and %s)", name, previous.source, path)
		}
		found[name] = selectedFolder{name: entry.Name(), source: protoDirectory}
		return filepath.SkipDir
	})
	if err != nil {
		return nil, err
	}
	for name := range wanted {
		if _, exists := found[name]; !exists {
			return nil, fmt.Errorf("protofetch: contract folder %q with proto directory not found", name)
		}
	}

	var copied []string
	for _, folder := range found {
		source := folder.source
		err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("protofetch: symlink is not allowed in proto folder: %s", path)
			}
			if entry.IsDir() {
				return nil
			}
			if !strings.HasSuffix(entry.Name(), ".proto") && !strings.HasSuffix(entry.Name(), ".type.json") {
				return nil
			}
			relative, err := filepath.Rel(source, path)
			if err != nil || escapesRoot(relative) {
				return fmt.Errorf("protofetch: unsafe relative path %s", path)
			}
			destination := filepath.Join(stage, folder.name, relative)
			if err := copyRegularFile(path, destination); err != nil {
				return err
			}
			installed, _ := filepath.Rel(stage, destination)
			copied = append(copied, filepath.ToSlash(installed))
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if len(copied) == 0 {
		return nil, errors.New("protofetch: selected folders contain no proto or type files")
	}
	return copied, nil
}

func inferServerType(contracts, protoPath string) (string, error) {
	relative, err := filepath.Rel(contracts, protoPath)
	if err != nil || escapesRoot(relative) {
		return "", fmt.Errorf("protofetch: proto path escapes contracts root: %s", protoPath)
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	for index := len(parts) - 2; index >= 1; index-- {
		if strings.EqualFold(parts[index], "proto") {
			serverType := strings.ToLower(parts[index-1])
			if !safeName(serverType) {
				return "", fmt.Errorf("protofetch: invalid server type %q", serverType)
			}
			return serverType, nil
		}
	}
	return "", fmt.Errorf("protofetch: cannot infer server type for %s", relative)
}

func copyRegularFile(source, destination string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return fmt.Errorf("protofetch: inspect %s: %w", source, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("protofetch: source is not a regular file: %s", source)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return fmt.Errorf("protofetch: create destination directory: %w", err)
	}
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("protofetch: open source: %w", err)
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("protofetch: create destination: %w", err)
	}
	if _, err := io.Copy(output, input); err != nil {
		output.Close()
		return fmt.Errorf("protofetch: copy file: %w", err)
	}
	if err := output.Sync(); err != nil {
		output.Close()
		return fmt.Errorf("protofetch: sync destination: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("protofetch: close destination: %w", err)
	}
	return nil
}

func replaceDirectory(output, stage string) error {
	backup := output + ".backup"
	if _, err := os.Lstat(backup); err == nil {
		return fmt.Errorf("protofetch: stale backup exists: %s", backup)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("protofetch: inspect backup: %w", err)
	}
	hadOutput := false
	if _, err := os.Lstat(output); err == nil {
		if err := os.Rename(output, backup); err != nil {
			return fmt.Errorf("protofetch: stage old output: %w", err)
		}
		if err := syncDirectory(filepath.Dir(output)); err != nil {
			rollbackErr := os.Rename(backup, output)
			if rollbackErr == nil {
				rollbackErr = syncDirectory(filepath.Dir(output))
			}
			return errors.Join(
				fmt.Errorf("protofetch: sync staged old output: %w", err),
				wrapReplaceError("restore previous output", rollbackErr),
			)
		}
		hadOutput = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("protofetch: inspect old output: %w", err)
	}
	if err := os.Rename(stage, output); err != nil {
		if hadOutput {
			if rollbackErr := os.Rename(backup, output); rollbackErr != nil {
				return errors.Join(
					fmt.Errorf("protofetch: install staged output: %w", err),
					fmt.Errorf("protofetch: restore previous output: %w", rollbackErr),
				)
			}
			if syncErr := syncDirectory(filepath.Dir(output)); syncErr != nil {
				return errors.Join(
					fmt.Errorf("protofetch: install staged output: %w", err),
					fmt.Errorf("protofetch: sync restored previous output: %w", syncErr),
				)
			}
		}
		return fmt.Errorf("protofetch: install staged output: %w", err)
	}
	if err := syncDirectory(filepath.Dir(output)); err != nil {
		return errors.Join(
			fmt.Errorf("protofetch: sync installed output: %w", err),
			rollbackInstalledDirectory(output, stage, backup, hadOutput),
		)
	}
	if hadOutput {
		if err := os.RemoveAll(backup); err != nil {
			return fmt.Errorf("protofetch: remove old output backup: %w", err)
		}
		if err := syncDirectory(filepath.Dir(output)); err != nil {
			return fmt.Errorf("protofetch: sync removed output backup: %w", err)
		}
	}
	return nil
}

func rollbackInstalledDirectory(output, stage, backup string, hadOutput bool) error {
	if err := os.Rename(output, stage); err != nil {
		return fmt.Errorf("protofetch: rollback unsynced output: %w", err)
	}
	if hadOutput {
		if err := os.Rename(backup, output); err != nil {
			reinstallErr := os.Rename(stage, output)
			return errors.Join(
				fmt.Errorf("protofetch: restore previous output after sync failure: %w", err),
				wrapReplaceError("restore installed output after failed rollback", reinstallErr),
			)
		}
	}
	if err := syncDirectory(filepath.Dir(output)); err != nil {
		return fmt.Errorf("protofetch: sync output rollback: %w", err)
	}
	return nil
}

func wrapReplaceError(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("protofetch: %s: %w", action, err)
}

func recoverOutput(output string) error {
	backup := output + ".backup"
	backupInfo, backupErr := os.Lstat(backup)
	if errors.Is(backupErr, os.ErrNotExist) {
		return nil
	}
	if backupErr != nil {
		return fmt.Errorf("protofetch: inspect recovery backup: %w", backupErr)
	}
	if backupInfo.Mode()&os.ModeSymlink != 0 || !backupInfo.IsDir() {
		return fmt.Errorf("protofetch: recovery backup is not a regular directory: %s", backup)
	}
	outputInfo, outputErr := os.Lstat(output)
	if errors.Is(outputErr, os.ErrNotExist) {
		if err := os.Rename(backup, output); err != nil {
			return fmt.Errorf("protofetch: restore interrupted output: %w", err)
		}
		if err := syncDirectory(filepath.Dir(output)); err != nil {
			return fmt.Errorf("protofetch: sync restored output: %w", err)
		}
		return nil
	}
	if outputErr != nil {
		return fmt.Errorf("protofetch: inspect interrupted output: %w", outputErr)
	}
	if outputInfo.Mode()&os.ModeSymlink != 0 || !outputInfo.IsDir() {
		return fmt.Errorf("protofetch: interrupted output is not a regular directory: %s", output)
	}
	// Both paths can only exist after the staged output was installed. The
	// staged tree was fsynced before the rename, so finish that interrupted
	// commit by removing the old backup.
	if err := os.RemoveAll(backup); err != nil {
		return fmt.Errorf("protofetch: finish interrupted output replacement: %w", err)
	}
	if err := syncDirectory(filepath.Dir(output)); err != nil {
		return fmt.Errorf("protofetch: sync recovered output replacement: %w", err)
	}
	return nil
}

func inspectOutput(output string) (os.FileMode, error) {
	info, err := os.Lstat(output)
	if errors.Is(err, os.ErrNotExist) {
		return 0o750, nil
	}
	if err != nil {
		return 0, fmt.Errorf("protofetch: inspect output: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return 0, errors.New("protofetch: output directory must not be a symlink")
	}
	if !info.IsDir() {
		return 0, errors.New("protofetch: output path must be a directory")
	}
	return info.Mode().Perm(), nil
}

func rejectOutputSourceOverlap(output, source string) error {
	resolvedSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return fmt.Errorf("protofetch: resolve source for output safety: %w", err)
	}
	resolvedSource, err = filepath.Abs(resolvedSource)
	if err != nil {
		return fmt.Errorf("protofetch: resolve source for output safety: %w", err)
	}
	relative, err := filepath.Rel(output, resolvedSource)
	if err == nil && (relative == "." || !escapesRoot(relative)) {
		return fmt.Errorf("protofetch: output directory must not contain the source repository: %s", output)
	}
	relative, err = filepath.Rel(resolvedSource, output)
	if err == nil && (relative == "." || !escapesRoot(relative)) {
		return fmt.Errorf("protofetch: source repository must not contain the output directory: %s", output)
	}
	return nil
}

func validateLockPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("protofetch: inspect output lock: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("protofetch: output lock must be a regular non-symlink file: %s", path)
	}
	return nil
}

func cleanOutputPath(outputPath, rootPath string) (string, error) {
	if strings.TrimSpace(rootPath) == "" {
		var err error
		rootPath, err = os.Getwd()
		if err != nil {
			return "", fmt.Errorf("protofetch: resolve output root: %w", err)
		}
	}
	declaredRoot, err := filepath.Abs(rootPath)
	if err != nil {
		return "", fmt.Errorf("protofetch: resolve output root: %w", err)
	}
	root, err := filepath.EvalSymlinks(declaredRoot)
	if err != nil {
		return "", fmt.Errorf("protofetch: resolve output root: %w", err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil || !rootInfo.IsDir() {
		if err == nil {
			err = errors.New("not a directory")
		}
		return "", fmt.Errorf("protofetch: output root: %w", err)
	}

	if strings.TrimSpace(outputPath) == "" {
		outputPath = "proto"
	}
	var output string
	if filepath.IsAbs(outputPath) {
		absoluteOutput := filepath.Clean(outputPath)
		if relative, relErr := filepath.Rel(declaredRoot, absoluteOutput); relErr == nil && relative != "." && !escapesRoot(relative) {
			// Preserve the caller's lexical boundary while using the resolved root.
			// This handles OS aliases such as macOS /var and /private/var without
			// weakening descendant or symlink checks.
			output = filepath.Join(root, relative)
		} else {
			output = absoluteOutput
		}
	} else {
		output = filepath.Join(root, filepath.Clean(outputPath))
	}
	relative, err := filepath.Rel(root, output)
	if err != nil || relative == "." || escapesRoot(relative) {
		return "", fmt.Errorf("protofetch: output directory %q must be a strict descendant of %s", outputPath, root)
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			break
		}
		if statErr != nil {
			return "", fmt.Errorf("protofetch: inspect output path: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("protofetch: symlink is not allowed in output path: %s", current)
		}
	}
	return output, nil
}

func escapesRoot(path string) bool {
	clean := filepath.Clean(path)
	return clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func containedDirectory(root, relative string) (string, error) {
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("protofetch: resolve source directory: %w", err)
	}
	resolvedRoot, err = filepath.Abs(resolvedRoot)
	if err != nil {
		return "", fmt.Errorf("protofetch: resolve source directory: %w", err)
	}
	clean := filepath.Clean(relative)
	current := resolvedRoot
	for _, component := range strings.Split(clean, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return "", fmt.Errorf("protofetch: inspect contracts path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("protofetch: symlink is not allowed in contracts path: %s", current)
		}
	}
	return current, nil
}

func listFiles(root string) ([]string, error) {
	var result []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("protofetch: symlink is not allowed in staged output: %s", path)
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("protofetch: staged output is not a regular file: %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result = append(result, filepath.ToSlash(relative))
		return nil
	})
	return result, err
}

func syncTree(root string) error {
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("protofetch: symlink is not allowed in staged output: %s", path)
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("protofetch: staged output is not a regular file: %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("protofetch: open staged file for sync: %w", err)
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil || closeErr != nil {
			return fmt.Errorf("protofetch: sync staged file: %w", errors.Join(syncErr, closeErr))
		}
		return nil
	})
	if err != nil {
		return err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		if err := syncDirectory(directories[index]); err != nil {
			return fmt.Errorf("protofetch: sync staged directory: %w", err)
		}
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
