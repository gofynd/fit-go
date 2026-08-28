// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestPoolMetricsRegistrationFailureIsReportedWithoutClosingPool(t *testing.T) {
	previousHandler := otel.GetErrorHandler()
	var reported error
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		reported = err
	}))
	t.Cleanup(func() {
		otel.SetErrorHandler(previousHandler)
	})

	configuration, err := pgxpool.ParseConfig("postgres://localhost/example?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig() error = %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), configuration)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig() error = %v", err)
	}
	t.Cleanup(pool.Close)

	client := &Client{
		services: map[string]*ServiceConnection{},
		poolMetricsRegistrar: func(string, string, *pgxpool.Pool) error {
			return errors.New("instrument conflict")
		},
	}
	client.tryRegisterPoolMetrics("orders", "write", pool)

	if reported == nil || !strings.Contains(reported.Error(), "pool metrics registration failed for orders_write") {
		t.Fatalf("reported error = %v", reported)
	}
	if pool.Stat().MaxConns() == 0 {
		t.Fatal("pool was closed after an optional metrics registration failure")
	}
}

func TestPoolMetricsUseBoundedIdentityAndUnregisterOnClose(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		_ = provider.Shutdown(context.Background())
	})

	configuration, err := pgxpool.ParseConfig("postgres://private-user:private-password@secret-host:5432/private_database?sslmode=disable")
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig() error = %v", err)
	}
	configuration.MinConns = 0
	configuration.MinIdleConns = 0
	configuration.MaxConns = 7
	pool, err := pgxpool.NewWithConfig(context.Background(), configuration)
	if err != nil {
		t.Fatalf("pgxpool.NewWithConfig() error = %v", err)
	}

	client := &Client{services: map[string]*ServiceConnection{
		"orders": {Read: pool, Write: pool},
	}}
	if err := client.registerPoolMetrics("orders", "write", pool); err != nil {
		pool.Close()
		t.Fatalf("registerPoolMetrics() error = %v", err)
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		_ = client.Close()
		t.Fatalf("Collect() error = %v", err)
	}

	foundMaximum := false
	for _, scope := range collected.ScopeMetrics {
		for _, measured := range scope.Metrics {
			if measured.Name != "pgxpool.max_connections" {
				continue
			}
			gauge, ok := measured.Data.(metricdata.Gauge[int64])
			if !ok || len(gauge.DataPoints) != 1 {
				t.Fatalf("max connections metric = %#v", measured.Data)
			}
			point := gauge.DataPoints[0]
			if point.Value != 7 {
				t.Fatalf("max connections = %d, want 7", point.Value)
			}
			if value, ok := point.Attributes.Value("db.client.connection.pool.name"); !ok || value.AsString() != "orders" {
				t.Fatalf("pool name attribute = %v, %v", value, ok)
			}
			if value, ok := point.Attributes.Value("fit.postgres.access_role"); !ok || value.AsString() != "write" {
				t.Fatalf("access role attribute = %v, %v", value, ok)
			}
			for _, keyValue := range point.Attributes.ToSlice() {
				for _, forbidden := range []string{"secret-host", "private_database", "private-user", "private-password"} {
					if keyValue.Value.Emit() == forbidden {
						t.Fatalf("metric attributes contain sensitive value %q", forbidden)
					}
				}
			}
			foundMaximum = true
		}
	}
	if !foundMaximum {
		t.Fatal("pgxpool.max_connections metric was not collected")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Client.Close() error = %v", err)
	}
	if len(client.metricRegistrations) != 0 {
		t.Fatalf("metric registrations after Close = %d", len(client.metricRegistrations))
	}
}
