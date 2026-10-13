// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package protocli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecuteFetchesFromLocalSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	proto := filepath.Join(source, "contracts", "services", "internal", "proto")
	if err := os.MkdirAll(proto, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proto, "user.proto"), []byte("syntax = \"proto3\";\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proto, "user.type.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "fit.config.json")
	if err := os.WriteFile(config, []byte(`{"apiSpecifications":{"fileName":"user"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "output")
	var stdout bytes.Buffer
	err := Execute(context.Background(), []string{
		"get", "--config", config, "--source", source, "--output", output, "--output-root", root,
	}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "internal/user.proto") {
		t.Fatalf("stdout = %s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(output, "internal", "user.proto")); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteHelpSucceeds(t *testing.T) {
	var stdout bytes.Buffer
	if err := Execute(context.Background(), []string{"--help"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "usage:") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestExecuteBufUsesExplicitTemplate(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("shell fixture is Unix-specific")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	proto := filepath.Join(source, "contracts", "services", "internal", "proto")
	if err := os.MkdirAll(proto, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proto, "user.proto"), []byte("syntax = \"proto3\";\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proto, "user.type.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "fit.config.json")
	if err := os.WriteFile(config, []byte(`{"apiSpecifications":{"fileName":"user"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	template := filepath.Join(root, "buf.gen.yaml")
	if err := os.WriteFile(template, []byte("version: v2\nplugins: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	arguments := filepath.Join(root, "args.txt")
	binary := filepath.Join(root, "fake-buf")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$BUF_ARGS\"\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BUF_ARGS", arguments)
	err := Execute(context.Background(), []string{
		"get", "--config", config, "--source", source,
		"--output", filepath.Join(root, "output"), "--output-root", root,
		"--generate", "buf", "--generator-binary", binary, "--buf-template", template,
	}, &bytes.Buffer{}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(arguments)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"generate", "--template", template, "."} {
		if !strings.Contains(string(data), expected) {
			t.Fatalf("buf arguments missing %q: %s", expected, data)
		}
	}
}
