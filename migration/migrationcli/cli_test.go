// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package migrationcli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/gofynd/fit-go/migration"
)

func TestExecuteRunAndList(t *testing.T) {
	store := migration.NewMemoryStore()
	runner, err := migration.NewRunner([]migration.Migration{{
		ID: "v1.0.0-1-one", Up: func(context.Context) error { return nil },
	}}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	cli := CLI{Runner: runner, Stdout: &output}
	if err := cli.Execute(context.Background(), []string{"run", "v1.0.0"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"applied": true`) {
		t.Fatalf("run output = %s", output.String())
	}
	output.Reset()
	if err := cli.Execute(context.Background(), []string{"list"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"id": "v1.0.0-1-one"`) {
		t.Fatalf("list output = %s", output.String())
	}
}

func TestRunArgumentsAreOrderIndependent(t *testing.T) {
	for name, args := range map[string][]string{
		"target before from": {"run", "v1.1.0", "--from", "v1.1.0"},
		"from before target": {"run", "--from=v1.1.0", "v1.1.0"},
	} {
		t.Run(name, func(t *testing.T) {
			store := migration.NewMemoryStore()
			var firstCalled bool
			runner, err := migration.NewRunner([]migration.Migration{
				{ID: "v1.0.0-1-first", Up: func(context.Context) error { firstCalled = true; return nil }},
				{ID: "v1.1.0-1-second", Up: func(context.Context) error { return nil }},
			}, store, store)
			if err != nil {
				t.Fatal(err)
			}
			if err := (CLI{Runner: runner, Stdout: &bytes.Buffer{}}).Execute(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if firstCalled {
				t.Fatal("migration before --from was executed")
			}
		})
	}
}

func TestRunHelpSucceedsWithoutRunnerExecution(t *testing.T) {
	var output bytes.Buffer
	err := (CLI{Stdout: &output}).Execute(context.Background(), []string{"run", "--help"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "usage:") {
		t.Fatalf("help output = %q", output.String())
	}
}
