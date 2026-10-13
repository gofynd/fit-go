// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Package migrationcli provides fitmigrate-style commands for an
// application-linked Go migration binary.
package migrationcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gofynd/fit-go/migration"
)

// CLI is an application-linked migration command. Applications supply their
// compiled Runner, then pass os.Args[1:] to Execute.
type CLI struct {
	Runner       *migration.Runner
	MigrationDir string
	PackageName  string
	Stdout       io.Writer
	Stderr       io.Writer
}

// Execute runs list, run, revert, revert-one, or create.
func (cli CLI) Execute(ctx context.Context, args []string) error {
	stdout := cli.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	if len(args) == 0 {
		return errors.New("migration: command required: list, run, revert, revert-one, or create")
	}

	switch args[0] {
	case "list":
		if cli.Runner == nil {
			return errors.New("migration: list requires a runner")
		}
		statuses, err := cli.Runner.List(ctx)
		if err != nil {
			return err
		}
		return writeJSON(stdout, statuses)
	case "run":
		options, help, err := parseRunArgs(args[1:])
		if err != nil {
			return err
		}
		if help {
			_, err := fmt.Fprintln(stdout, "usage: migration run [--from <version-or-id>] [target]")
			return err
		}
		if cli.Runner == nil {
			return errors.New("migration: run requires a runner")
		}
		statuses, err := cli.Runner.Run(ctx, options)
		if err != nil {
			return err
		}
		return writeJSON(stdout, statuses)
	case "revert":
		if cli.Runner == nil {
			return errors.New("migration: revert requires a runner")
		}
		if len(args) != 2 {
			return errors.New("migration: revert requires a target version or migration ID")
		}
		statuses, err := cli.Runner.Revert(ctx, migration.RevertOptions{To: args[1]})
		if err != nil {
			return err
		}
		return writeJSON(stdout, statuses)
	case "revert-one":
		if cli.Runner == nil {
			return errors.New("migration: revert-one requires a runner")
		}
		if len(args) != 1 {
			return errors.New("migration: revert-one does not accept arguments")
		}
		statuses, err := cli.Runner.RevertOne(ctx)
		if err != nil {
			return err
		}
		return writeJSON(stdout, statuses)
	case "create":
		if len(args) != 3 {
			return errors.New("migration: create requires <version> <name>")
		}
		if strings.TrimSpace(cli.MigrationDir) == "" || strings.TrimSpace(cli.PackageName) == "" {
			return errors.New("migration: create requires MigrationDir and PackageName")
		}
		created, err := migration.CreateFileContext(ctx, cli.MigrationDir, cli.PackageName, args[1], args[2])
		if err != nil {
			return err
		}
		return writeJSON(stdout, created)
	default:
		return fmt.Errorf("migration: unknown command %q", args[0])
	}
}

func parseRunArgs(args []string) (migration.RunOptions, bool, error) {
	options := migration.RunOptions{To: "latest"}
	targetSet := false
	for index := 0; index < len(args); index++ {
		argument := args[index]
		switch {
		case argument == "-h" || argument == "--help":
			return migration.RunOptions{}, true, nil
		case argument == "--from" || argument == "-from":
			index++
			if index >= len(args) || strings.TrimSpace(args[index]) == "" {
				return migration.RunOptions{}, false, errors.New("migration: --from requires a value")
			}
			options.From = args[index]
		case strings.HasPrefix(argument, "--from=") || strings.HasPrefix(argument, "-from="):
			_, value, _ := strings.Cut(argument, "=")
			if strings.TrimSpace(value) == "" {
				return migration.RunOptions{}, false, errors.New("migration: --from requires a value")
			}
			options.From = value
		case strings.HasPrefix(argument, "-"):
			return migration.RunOptions{}, false, fmt.Errorf("migration: unknown run option %q", argument)
		default:
			if targetSet {
				return migration.RunOptions{}, false, errors.New("migration: run accepts at most one target")
			}
			options.To = argument
			targetSet = true
		}
	}
	return options, false, nil
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
