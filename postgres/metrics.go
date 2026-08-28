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
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const postgresMeterName = "github.com/gofynd/fit-go/postgres"

// tryRegisterPoolMetrics keeps observability optional. A meter-provider or
// instrument registration failure is reported through OpenTelemetry's error
// handler, but it must never make a healthy PostgreSQL pool unavailable.
func (c *Client) tryRegisterPoolMetrics(serviceName, accessRole string, pool *pgxpool.Pool) {
	if c == nil || pool == nil {
		return
	}
	registrar := c.poolMetricsRegistrar
	if registrar == nil {
		registrar = c.registerPoolMetrics
	}
	if err := registrar(serviceName, accessRole, pool); err != nil {
		otel.Handle(fmt.Errorf("postgres: pool metrics registration failed for %s_%s: %w", serviceName, accessRole, err))
	}
}

// registerPoolMetrics publishes pgxpool statistics through the active global
// OpenTelemetry meter provider. With the default no-op provider this creates no
// exporter or network dependency. Every registration is owned by Client and is
// unregistered before its pool is closed.
func (c *Client) registerPoolMetrics(serviceName, accessRole string, pool *pgxpool.Pool) error {
	if pool == nil {
		return nil
	}

	meter := otel.GetMeterProvider().Meter(postgresMeterName)
	acquiredConnections, err := meter.Int64ObservableGauge(
		"pgxpool.acquired_connections",
		metric.WithDescription("Number of currently acquired PostgreSQL connections."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}
	idleConnections, err := meter.Int64ObservableGauge(
		"pgxpool.idle_connections",
		metric.WithDescription("Number of currently idle PostgreSQL connections."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}
	totalConnections, err := meter.Int64ObservableGauge(
		"pgxpool.total_connections",
		metric.WithDescription("Total number of PostgreSQL connections in the pool."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}
	maximumConnections, err := meter.Int64ObservableGauge(
		"pgxpool.max_connections",
		metric.WithDescription("Configured maximum number of PostgreSQL connections."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}
	acquireCount, err := meter.Int64ObservableCounter(
		"pgxpool.acquires",
		metric.WithDescription("Cumulative count of successful PostgreSQL pool acquisitions."),
		metric.WithUnit("{acquire}"),
	)
	if err != nil {
		return err
	}
	acquireDuration, err := meter.Int64ObservableCounter(
		"pgxpool.acquire_duration",
		metric.WithDescription("Cumulative time spent acquiring PostgreSQL connections."),
		metric.WithUnit("ns"),
	)
	if err != nil {
		return err
	}
	cancelledAcquireCount, err := meter.Int64ObservableCounter(
		"pgxpool.canceled_acquires",
		metric.WithDescription("Cumulative count of cancelled PostgreSQL pool acquisitions."),
		metric.WithUnit("{acquire}"),
	)
	if err != nil {
		return err
	}
	emptyAcquireCount, err := meter.Int64ObservableCounter(
		"pgxpool.empty_acquires",
		metric.WithDescription("Cumulative count of acquisitions that waited for a PostgreSQL connection."),
		metric.WithUnit("{acquire}"),
	)
	if err != nil {
		return err
	}
	emptyAcquireWaitTime, err := meter.Int64ObservableCounter(
		"pgxpool.empty_acquire_wait_time",
		metric.WithDescription("Cumulative time spent waiting for a PostgreSQL connection."),
		metric.WithUnit("ns"),
	)
	if err != nil {
		return err
	}
	newConnections, err := meter.Int64ObservableCounter(
		"pgxpool.new_connections",
		metric.WithDescription("Cumulative count of PostgreSQL connections created by the pool."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}
	maximumIdleDestroys, err := meter.Int64ObservableCounter(
		"pgxpool.max_idle_destroys",
		metric.WithDescription("Cumulative count of PostgreSQL connections closed after exceeding idle time."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}
	maximumLifetimeDestroys, err := meter.Int64ObservableCounter(
		"pgxpool.max_lifetime_destroys",
		metric.WithDescription("Cumulative count of PostgreSQL connections closed after exceeding lifetime."),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return err
	}

	attributes := metric.WithAttributes(
		attribute.String("db.client.connection.pool.name", serviceName),
		attribute.String("fit.postgres.access_role", accessRole),
	)
	registration, err := meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			statistics := pool.Stat()
			observer.ObserveInt64(acquiredConnections, int64(statistics.AcquiredConns()), attributes)
			observer.ObserveInt64(idleConnections, int64(statistics.IdleConns()), attributes)
			observer.ObserveInt64(totalConnections, int64(statistics.TotalConns()), attributes)
			observer.ObserveInt64(maximumConnections, int64(statistics.MaxConns()), attributes)
			observer.ObserveInt64(acquireCount, statistics.AcquireCount(), attributes)
			observer.ObserveInt64(acquireDuration, statistics.AcquireDuration().Nanoseconds(), attributes)
			observer.ObserveInt64(cancelledAcquireCount, statistics.CanceledAcquireCount(), attributes)
			observer.ObserveInt64(emptyAcquireCount, statistics.EmptyAcquireCount(), attributes)
			observer.ObserveInt64(emptyAcquireWaitTime, statistics.EmptyAcquireWaitTime().Nanoseconds(), attributes)
			observer.ObserveInt64(newConnections, statistics.NewConnsCount(), attributes)
			observer.ObserveInt64(maximumIdleDestroys, statistics.MaxIdleDestroyCount(), attributes)
			observer.ObserveInt64(maximumLifetimeDestroys, statistics.MaxLifetimeDestroyCount(), attributes)
			return nil
		},
		acquiredConnections,
		idleConnections,
		totalConnections,
		maximumConnections,
		acquireCount,
		acquireDuration,
		cancelledAcquireCount,
		emptyAcquireCount,
		emptyAcquireWaitTime,
		newConnections,
		maximumIdleDestroys,
		maximumLifetimeDestroys,
	)
	if err != nil {
		return err
	}
	c.metricRegistrations = append(c.metricRegistrations, registration)
	return nil
}
