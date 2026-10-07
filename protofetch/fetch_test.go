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
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestFetchNamedProtoGroupsByServerAndAtomicallyReplacesOutput(t *testing.T) {
	source := fixtureRepository(t)
	output := filepath.Join(t.TempDir(), "proto")
	writeFile(t, filepath.Join(output, "stale.txt"), "stale")
	if err := os.Chmod(output, 0o755); err != nil {
		t.Fatal(err)
	}

	result, err := Fetch(context.Background(), Options{
		Mode:            Get,
		Specification:   Specification{FileName: "user"},
		SourceDirectory: source,
		OutputDirectory: output,
		OutputRoot:      filepath.Dir(output),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"internal/user.proto", "internal/user.type.json"}
	if !reflect.DeepEqual(result.Files, want) {
		t.Fatalf("files = %v, want %v", result.Files, want)
	}
	if _, err := os.Stat(filepath.Join(output, "stale.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale output still exists: %v", err)
	}
	if info, err := os.Stat(output); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("output mode = %v, %v; want 755", info, err)
	}
	data, err := os.ReadFile(filepath.Join(output, "internal", "user.proto"))
	if err != nil || string(data) != "syntax = \"proto3\";\n" {
		t.Fatalf("installed proto = %q, %v", data, err)
	}
}

func TestFetchGeneratorFailurePreservesExistingOutput(t *testing.T) {
	source := fixtureRepository(t)
	output := filepath.Join(t.TempDir(), "proto")
	writeFile(t, filepath.Join(output, "marker.txt"), "keep")
	boom := errors.New("generation failed")

	_, err := Fetch(context.Background(), Options{
		Mode:            Get,
		Specification:   Specification{FileName: "user"},
		SourceDirectory: source,
		OutputDirectory: output,
		OutputRoot:      filepath.Dir(output),
		Generator:       GeneratorFunc(func(context.Context, string) error { return boom }),
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Fetch() error = %v, want generation failure", err)
	}
	data, readErr := os.ReadFile(filepath.Join(output, "marker.txt"))
	if readErr != nil || string(data) != "keep" {
		t.Fatalf("existing output was not preserved: %q, %v", data, readErr)
	}
}

func TestFetchRejectsSymlinksCreatedByGenerator(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture is Unix-specific")
	}
	source := fixtureRepository(t)
	output := filepath.Join(t.TempDir(), "proto")
	writeFile(t, filepath.Join(output, "marker.txt"), "keep")
	target := filepath.Join(t.TempDir(), "outside.proto")
	writeFile(t, target, "secret")

	_, err := Fetch(context.Background(), Options{
		Mode:            Get,
		Specification:   Specification{FileName: "user"},
		SourceDirectory: source,
		OutputDirectory: output,
		OutputRoot:      filepath.Dir(output),
		Generator: GeneratorFunc(func(_ context.Context, stage string) error {
			return os.Symlink(target, filepath.Join(stage, "generated.proto"))
		}),
	})
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Fetch() error = %v, want staged symlink rejection", err)
	}
	data, readErr := os.ReadFile(filepath.Join(output, "marker.txt"))
	if readErr != nil || string(data) != "keep" {
		t.Fatalf("existing output was not preserved: %q, %v", data, readErr)
	}
}

func TestCommandGeneratorDoesNotExposeProcessOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture is Unix-specific")
	}
	script := filepath.Join(t.TempDir(), "failing-generator")
	writeFile(t, script, "#!/bin/sh\nprintf 'unlabelled-private-value' >&2\nexit 1\n")
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	err := (CommandGenerator{Executable: script}).Generate(context.Background(), t.TempDir())
	if err == nil || strings.Contains(err.Error(), "unlabelled-private-value") {
		t.Fatalf("Generate() error leaked process output: %v", err)
	}
}

func TestFetchAllCopiesOnlyContractFilesAndPreservesSubdirectories(t *testing.T) {
	source := fixtureRepository(t)
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "nested", "extra.proto"), "syntax = \"proto3\";\n")
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "README.md"), "ignore")
	if err := os.Rename(
		filepath.Join(source, "contracts", "services", "internal"),
		filepath.Join(source, "contracts", "services", "Internal"),
	); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "proto")

	result, err := Fetch(context.Background(), Options{
		Mode:            GetAll,
		Specification:   Specification{FolderNames: []string{"INTERNAL", "admin"}},
		SourceDirectory: source,
		OutputDirectory: output,
		OutputRoot:      filepath.Dir(output),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"Internal/nested/extra.proto", "Internal/user.proto", "Internal/user.type.json",
		"admin/content.proto", "admin/content.type.json",
	}
	if !reflect.DeepEqual(result.Files, want) {
		t.Fatalf("files = %v, want %v", result.Files, want)
	}
	if _, err := os.Stat(filepath.Join(output, "Internal", "README.md")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("non-contract file was copied: %v", err)
	}
}

func TestOutputSafetyBoundaryAndInterruptedRecovery(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	for _, output := range []string{".", "../outside", outside, root} {
		if _, err := cleanOutputPath(output, root); err == nil {
			t.Fatalf("cleanOutputPath accepted %q", output)
		}
	}

	source := fixtureRepository(t)
	output := filepath.Join(root, "proto")
	writeFile(t, output, "not a directory")
	_, err := Fetch(context.Background(), Options{
		Mode: Get, Specification: Specification{FileName: "user"},
		SourceDirectory: source, OutputDirectory: output, OutputRoot: root,
	})
	if err == nil || !strings.Contains(err.Error(), "must be a directory") {
		t.Fatalf("Fetch existing file error = %v", err)
	}
	if err := os.Remove(output); err != nil {
		t.Fatal(err)
	}

	backup := output + ".backup"
	writeFile(t, filepath.Join(backup, "marker.txt"), "restore")
	_, err = Fetch(context.Background(), Options{
		Mode: Get, Specification: Specification{FileName: "missing"},
		SourceDirectory: source, OutputDirectory: output, OutputRoot: root,
	})
	if err == nil {
		t.Fatal("Fetch unexpectedly succeeded")
	}
	if data, readErr := os.ReadFile(filepath.Join(output, "marker.txt")); readErr != nil || string(data) != "restore" {
		t.Fatalf("interrupted output was not restored: %q, %v", data, readErr)
	}
}

func TestInterruptedReplacementWithOutputAndBackupFinishesCommit(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "proto")
	backup := output + ".backup"
	writeFile(t, filepath.Join(output, "marker.txt"), "installed")
	writeFile(t, filepath.Join(backup, "marker.txt"), "previous")

	_, err := Fetch(context.Background(), Options{
		Mode: Get, Specification: Specification{FileName: "missing"},
		SourceDirectory: fixtureRepository(t), OutputDirectory: output, OutputRoot: root,
	})
	if err == nil {
		t.Fatal("Fetch unexpectedly succeeded")
	}
	if data, readErr := os.ReadFile(filepath.Join(output, "marker.txt")); readErr != nil || string(data) != "installed" {
		t.Fatalf("installed output was not retained: %q, %v", data, readErr)
	}
	if _, statErr := os.Stat(backup); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("completed replacement retained backup: %v", statErr)
	}
}

func TestFetchRejectsOutputContainingSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "user.proto"), "")
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "user.type.json"), "{}")
	_, err := Fetch(context.Background(), Options{
		Mode: Get, Specification: Specification{FileName: "user"},
		SourceDirectory: source, OutputDirectory: source, OutputRoot: root,
	})
	if err == nil || !strings.Contains(err.Error(), "must not contain the source") {
		t.Fatalf("Fetch overlap error = %v", err)
	}
}

func TestFetchRejectsSourceContainingOutput(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "user.proto"), "")
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "user.type.json"), "{}")
	output := filepath.Join(source, "generated", "proto")
	_, err := Fetch(context.Background(), Options{
		Mode: Get, Specification: Specification{FileName: "user"},
		SourceDirectory: source, OutputDirectory: output, OutputRoot: root,
	})
	if err == nil || !strings.Contains(err.Error(), "source repository must not contain") {
		t.Fatalf("Fetch overlap error = %v", err)
	}
}

func TestFetchRejectsSymlinkOutputLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture is Unix-specific")
	}
	root := t.TempDir()
	output := filepath.Join(root, "proto")
	target := filepath.Join(root, "outside.lock")
	writeFile(t, target, "")
	if err := os.Symlink(target, output+".lock"); err != nil {
		t.Fatal(err)
	}
	_, err := Fetch(context.Background(), Options{
		Mode: Get, Specification: Specification{FileName: "user"},
		SourceDirectory: fixtureRepository(t), OutputDirectory: output, OutputRoot: root,
	})
	if err == nil || !strings.Contains(err.Error(), "output lock") {
		t.Fatalf("Fetch lock error = %v", err)
	}
}

func TestFetchNamedProtoRequiresCompanionTypeFile(t *testing.T) {
	source := t.TempDir()
	writeFile(t, filepath.Join(source, "contracts", "services", "internal", "proto", "user.proto"), "syntax = \"proto3\";\n")
	outputRoot := t.TempDir()
	_, err := Fetch(context.Background(), Options{
		Mode:            Get,
		Specification:   Specification{FileName: "user"},
		SourceDirectory: source,
		OutputDirectory: filepath.Join(outputRoot, "proto"),
		OutputRoot:      outputRoot,
	})
	if err == nil || !strings.Contains(err.Error(), "companion type file") {
		t.Fatalf("Fetch() error = %v", err)
	}
}

func TestFetchRejectsUnsafeInputsAndSymlinks(t *testing.T) {
	source := fixtureRepository(t)
	for _, options := range []Options{
		{Mode: Get, Specification: Specification{FileName: "../user"}, SourceDirectory: source},
		{Mode: Get, Specification: Specification{FileName: "user", Branch: "--upload-pack=bad"}, SourceDirectory: source},
		{Mode: Get, Specification: Specification{FileName: "user"}, SourceDirectory: source, ContractsPath: "../contracts"},
	} {
		outputRoot := t.TempDir()
		options.OutputRoot = outputRoot
		options.OutputDirectory = filepath.Join(outputRoot, "proto")
		if _, err := Fetch(context.Background(), options); err == nil {
			t.Fatalf("Fetch accepted unsafe options: %+v", options)
		}
	}
	if runtime.GOOS != "windows" {
		link := filepath.Join(source, "contracts", "services", "internal", "proto", "linked.proto")
		if err := os.Symlink(filepath.Join(source, "outside"), link); err != nil {
			t.Fatal(err)
		}
		outputRoot := t.TempDir()
		_, err := Fetch(context.Background(), Options{
			Mode:            GetAll,
			Specification:   Specification{FolderNames: []string{"internal"}},
			SourceDirectory: source,
			OutputDirectory: filepath.Join(outputRoot, "proto"),
			OutputRoot:      outputRoot,
		})
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("Fetch symlink error = %v", err)
		}

		external := fixtureRepository(t)
		root := t.TempDir()
		if err := os.Symlink(external, filepath.Join(root, "linked")); err != nil {
			t.Fatal(err)
		}
		outputRoot = t.TempDir()
		_, err = Fetch(context.Background(), Options{
			Mode:            Get,
			Specification:   Specification{FileName: "user"},
			SourceDirectory: root,
			ContractsPath:   filepath.Join("linked", "contracts"),
			OutputDirectory: filepath.Join(outputRoot, "proto"),
			OutputRoot:      outputRoot,
		})
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("Fetch intermediate symlink error = %v", err)
		}
	}
}

func TestProtocGeneratorPassesEveryProtoWithoutShellExpansion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture is Unix-specific")
	}
	directory := t.TempDir()
	writeFile(t, filepath.Join(directory, "internal", "one.proto"), "")
	writeFile(t, filepath.Join(directory, "admin", "two.proto"), "")
	logPath := filepath.Join(t.TempDir(), "args.log")
	script := filepath.Join(t.TempDir(), "fake-protoc")
	writeFile(t, script, "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$PROTO_ARGS_LOG\"\n")
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROTO_ARGS_LOG", logPath)
	if err := (ProtocGenerator{Executable: script}).Generate(context.Background(), directory); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	arguments := string(data)
	for _, expected := range []string{"admin/two.proto", "internal/one.proto", "--go_out=."} {
		if !strings.Contains(arguments, expected) {
			t.Fatalf("protoc arguments missing %q:\n%s", expected, arguments)
		}
	}
}

func TestLoadConfigSupportsGitAliases(t *testing.T) {
	for field := range map[string]struct{}{"gitURI": {}, "gitlabURI": {}} {
		path := filepath.Join(t.TempDir(), "fit.config.json")
		writeFile(t, path, `{"apiSpecifications":{"`+field+`":"repo","fileName":"user"}}`)
		config, err := LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if config.APISpecifications.Repository() != "repo" {
			t.Fatalf("Repository() = %q for %s", config.APISpecifications.Repository(), field)
		}
	}
}

func TestFetchRejectsConflictingGitAliases(t *testing.T) {
	root := t.TempDir()
	_, err := Fetch(context.Background(), Options{
		Mode: Get,
		Specification: Specification{
			GitURI: "one", GitLabURI: "two", FileName: "user",
		},
		SourceDirectory: fixtureRepository(t),
		OutputDirectory: filepath.Join(root, "proto"),
		OutputRoot:      root,
	})
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("Fetch() error = %v", err)
	}
}

func TestLoadConfigRejectsTrailingJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fit.config.json")
	writeFile(t, path, `{"apiSpecifications":{"gitURI":"repo","fileName":"user"}} {}`)
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig accepted trailing JSON")
	}
}

func fixtureRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "contracts", "services", "internal", "proto", "user.proto"), "syntax = \"proto3\";\n")
	writeFile(t, filepath.Join(root, "contracts", "services", "internal", "proto", "user.type.json"), "{}\n")
	writeFile(t, filepath.Join(root, "contracts", "services", "admin", "proto", "content.proto"), "syntax = \"proto3\";\n")
	writeFile(t, filepath.Join(root, "contracts", "services", "admin", "proto", "content.type.json"), "{}\n")
	return root
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
