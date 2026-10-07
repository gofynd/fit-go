// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Package protocli implements the fitproto command-line interface.
package protocli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofynd/fit-go/protofetch"
)

// Execute runs a fitproto-compatible get or getall command.
func Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	if len(args) == 0 {
		return errors.New("fitproto: command required: get or getall")
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		_, err := fmt.Fprintln(stdout, "usage: fitproto <get|getall> [options]")
		return err
	}
	mode := protofetch.Mode(strings.ToLower(args[0]))
	if mode != protofetch.Get && mode != protofetch.GetAll {
		return fmt.Errorf("fitproto: unknown command %q", args[0])
	}

	flags := flag.NewFlagSet("fitproto "+string(mode), flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "fit.config.json", "legacy FIT config path")
	output := flags.String("output", "proto", "output directory")
	outputRoot := flags.String("output-root", ".", "trusted root containing the output directory")
	source := flags.String("source", "", "local repository directory instead of git clone")
	contracts := flags.String("contracts", "contracts", "contracts path inside repository")
	generate := flags.String("generate", "none", "none, buf, or protoc")
	generatorBinary := flags.String("generator-binary", "", "generator executable override")
	bufTemplate := flags.String("buf-template", "buf.gen.yaml", "buf generation template")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("fitproto: unexpected positional arguments")
	}

	config, err := protofetch.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	var generator protofetch.Generator
	switch strings.ToLower(strings.TrimSpace(*generate)) {
	case "", "none":
	case "buf":
		binary := *generatorBinary
		if binary == "" {
			binary = "buf"
		}
		template, err := regularFile(*bufTemplate)
		if err != nil {
			return fmt.Errorf("fitproto: buf template: %w", err)
		}
		generator = protofetch.CommandGenerator{Executable: binary, Arguments: []string{"generate", "--template", template, "."}}
	case "protoc":
		binary := *generatorBinary
		generator = protofetch.ProtocGenerator{Executable: binary}
	default:
		return fmt.Errorf("fitproto: unsupported generator %q", *generate)
	}

	result, err := protofetch.Fetch(ctx, protofetch.Options{
		Mode:            mode,
		Specification:   config.APISpecifications,
		OutputDirectory: *output,
		OutputRoot:      *outputRoot,
		SourceDirectory: *source,
		ContractsPath:   *contracts,
		Generator:       generator,
	})
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func regularFile(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("must be a regular non-symlink file")
	}
	return absolute, nil
}
