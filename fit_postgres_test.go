// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package fit

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/gofynd/fit-go/postgres"
)

func TestPostgresInitializationIsOptInAndOwnedByFramework(t *testing.T) {
	clearFitPostgresEnvironment(t)

	t.Run("disabled by default", func(t *testing.T) {
		resetFitMetricsTestState(t)
		framework, err := Init(context.Background())
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		if framework.Connections.Postgres != nil {
			t.Fatal("PostgreSQL initialized without WithPostgres")
		}
		if err := framework.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	})

	t.Run("enabled explicitly", func(t *testing.T) {
		resetFitMetricsTestState(t)
		framework, err := Init(context.Background(), WithPostgres(postgres.ConnectionOptions{}))
		if err != nil {
			t.Fatalf("Init() error = %v", err)
		}
		if framework.Postgres == nil || framework.Connections.Postgres == nil {
			t.Fatal("PostgreSQL was not initialized by WithPostgres")
		}
		if len(framework.Postgres.Services()) != 0 {
			t.Fatalf("PostgreSQL services = %d, want 0", len(framework.Postgres.Services()))
		}
		if problems := framework.Health.Check(); len(problems) != 0 {
			t.Fatalf("health problems = %v", problems)
		}
		if err := framework.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
		if framework.Postgres != nil || framework.Connections.Postgres != nil {
			t.Fatal("PostgreSQL client retained after Shutdown")
		}
	})
}

func clearFitPostgresEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if found && strings.HasPrefix(key, "POSTGRES_") {
			t.Setenv(key, "")
		}
	}
}
