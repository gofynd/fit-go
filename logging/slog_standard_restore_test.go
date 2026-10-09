// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package logging

import (
	"bytes"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"testing"
)

// These tests mutate process defaults and must not run in parallel.
func isolateStandardLog(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	baseline := slog.Default()
	writer, flags, prefix := log.Writer(), log.Flags(), log.Prefix()
	t.Cleanup(func() {
		slog.SetDefault(baseline)
		log.SetOutput(writer)
		log.SetFlags(flags)
		log.SetPrefix(prefix)
	})
	var output bytes.Buffer
	log.SetOutput(&output)
	log.SetFlags(log.LstdFlags)
	log.SetPrefix("baseline ")
	return baseline, &output
}

func standardRestoreLogger(t *testing.T) (*Logger, *bytes.Buffer) {
	t.Helper()
	var output bytes.Buffer
	logger, err := New(Options{Level: "info", Env: "production", Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	return logger, &output
}

func TestSetAsDefaultSlogRestoresStandardLogAcrossOwnerOrders(t *testing.T) {
	orders := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 0, 2}, {0, 2, 1}, {1, 2, 0}, {2, 0, 1}}
	for _, order := range orders {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			baseline, original := isolateStandardLog(t)
			for cycle := 0; cycle < 3; cycle++ {
				var restores []func()
				var sinks []*bytes.Buffer
				for i := 0; i < 3; i++ {
					logger, sink := standardRestoreLogger(t)
					restore := SetAsDefaultSlog(logger)
					t.Cleanup(restore)
					restores = append(restores, restore)
					sinks = append(sinks, sink)
				}
				log.Print("during")
				if !strings.Contains(sinks[2].String(), "during") {
					t.Fatal("standard logger did not use the active owner")
				}
				for _, index := range order {
					restores[index]()
					restores[index]() // restoration remains idempotent
				}
				if slog.Default() != baseline || log.Writer() != original || log.Flags() != log.LstdFlags || log.Prefix() != "baseline " {
					t.Fatal("original slog/standard log state was not restored")
				}
				log.Print("after-standard")
				slog.Info("after-slog")
				if !strings.Contains(original.String(), "after-standard") || !strings.Contains(original.String(), "after-slog") {
					t.Fatal("restored logging did not reach the original sink")
				}
				for _, sink := range sinks {
					if strings.Contains(sink.String(), "after-") {
						t.Fatal("restored logging reached a retired fit sink")
					}
				}
			}
		})
	}
}

func TestSetAsDefaultSlogPreservesIndependentStandardLogChanges(t *testing.T) {
	for _, field := range []string{"writer", "flags", "prefix", "all"} {
		t.Run(field, func(t *testing.T) {
			baseline, original := isolateStandardLog(t)
			logger, _ := standardRestoreLogger(t)
			restore := SetAsDefaultSlog(logger)
			t.Cleanup(restore)
			var external bytes.Buffer
			wantWriter, wantFlags, wantPrefix := original, log.LstdFlags, "baseline "
			if field == "writer" || field == "all" {
				log.SetOutput(&external)
				wantWriter = &external
			}
			if field == "flags" || field == "all" {
				log.SetFlags(log.Lshortfile)
				wantFlags = log.Lshortfile
			}
			if field == "prefix" || field == "all" {
				log.SetPrefix("external ")
				wantPrefix = "external "
			}
			restore()
			if slog.Default() != baseline || log.Writer() != wantWriter || log.Flags() != wantFlags || log.Prefix() != wantPrefix {
				t.Fatal("restore clobbered an independently changed standard log field")
			}
		})
	}
}

func TestSetAsDefaultSlogPreservesStandardLogChangesBetweenOwners(t *testing.T) {
	baseline, _ := isolateStandardLog(t)
	first, _ := standardRestoreLogger(t)
	restoreFirst := SetAsDefaultSlog(first)
	t.Cleanup(restoreFirst)
	var external bytes.Buffer
	log.SetOutput(&external)
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("external ")
	second, _ := standardRestoreLogger(t)
	restoreSecond := SetAsDefaultSlog(second)
	t.Cleanup(restoreSecond)
	restoreFirst()
	restoreSecond()
	if slog.Default() != baseline || log.Writer() != &external || log.Flags() != log.Lshortfile || log.Prefix() != "external " {
		t.Fatal("out-of-order restore lost a standard log replacement between owners")
	}
}

func TestSetAsDefaultSlogPreservesExternalSlogAndStandardLogger(t *testing.T) {
	isolateStandardLog(t)
	logger, _ := standardRestoreLogger(t)
	restore := SetAsDefaultSlog(logger)
	t.Cleanup(restore)
	var external bytes.Buffer
	externalSlog := slog.New(slog.NewTextHandler(&external, nil))
	slog.SetDefault(externalSlog)
	writer := log.Writer()
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("external ")
	restore()
	if slog.Default() != externalSlog || log.Writer() != writer || log.Flags() != log.Lshortfile || log.Prefix() != "external " {
		t.Fatal("restore clobbered independently replaced slog/standard logging")
	}
}

func TestSetAsDefaultSlogRestoresExternalBaselineAfterNewOwner(t *testing.T) {
	isolateStandardLog(t)
	first, _ := standardRestoreLogger(t)
	restoreFirst := SetAsDefaultSlog(first)
	t.Cleanup(restoreFirst)
	var external bytes.Buffer
	externalSlog := slog.New(slog.NewTextHandler(&external, nil))
	slog.SetDefault(externalSlog)
	writer := log.Writer()
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("external ")
	second, _ := standardRestoreLogger(t)
	restoreSecond := SetAsDefaultSlog(second)
	t.Cleanup(restoreSecond)
	restoreFirst()
	restoreSecond()
	if slog.Default() != externalSlog || log.Writer() != writer || log.Flags() != log.Lshortfile || log.Prefix() != "external " {
		t.Fatal("new owner did not restore the independently installed baseline")
	}
	log.Print("restored-external")
	if !strings.Contains(external.String(), "restored-external") {
		t.Fatal("standard logging no longer reaches the external baseline")
	}
}

type nonComparableLogWriter struct {
	output *bytes.Buffer
	marker []byte
}

func (writer nonComparableLogWriter) Write(data []byte) (int, error) {
	return writer.output.Write(data)
}

func TestSetAsDefaultSlogRestoresNonComparableStandardWriter(t *testing.T) {
	isolateStandardLog(t)
	var original bytes.Buffer
	log.SetOutput(nonComparableLogWriter{output: &original, marker: []byte{1}})
	first, _ := standardRestoreLogger(t)
	restoreFirst := SetAsDefaultSlog(first)
	t.Cleanup(restoreFirst)
	second, _ := standardRestoreLogger(t)
	restoreSecond := SetAsDefaultSlog(second)
	t.Cleanup(restoreSecond)
	restoreFirst()
	restoreSecond()
	log.Print("restored")
	if !strings.Contains(original.String(), "restored") {
		t.Fatal("non-comparable writer was not restored")
	}
}
