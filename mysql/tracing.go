// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"database/sql"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/tracing"
)

// openSQLDB preserves database/sql's raw path while tracing is disabled. When
// enabled it uses otelsql, the maintained database/sql instrumentation derived
// from the OpenTelemetry Go contrib implementation and compatible with
// go-sql-driver/mysql.
//
// DisableQuery is mandatory here: SQL statements, bind parameters, DSNs, and
// credentials must never enter spans. The only static database attribute is the
// non-sensitive system name.
func openSQLDB(driverName, dsn string) (*sql.DB, error) {
	if tracer := tracing.Global(); tracer == nil || !tracer.IsEnabled() {
		return sql.Open(driverName, dsn)
	}
	return otelsql.Open(
		driverName,
		dsn,
		otelsql.WithTracerProvider(newPrivacyTracerProvider(otel.GetTracerProvider())),
		otelsql.WithAttributes(semconv.DBSystemNameMySQL),
		otelsql.WithSpanOptions(otelsql.SpanOptions{
			DisableQuery:   true,
			DisableErrSkip: true,
		}),
	)
}

const mysqlFailureStatus = "mysql operation failed"

// privacyTracerProvider keeps otelsql's lifecycle spans while replacing its raw
// error event/status handling. Driver errors can echo SQL, binds, DSNs, or
// backend payloads; callers still receive the original error unchanged.
type privacyTracerProvider struct {
	trace.TracerProvider
}

func newPrivacyTracerProvider(provider trace.TracerProvider) trace.TracerProvider {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &privacyTracerProvider{TracerProvider: provider}
}

func (p *privacyTracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return &privacyTracer{
		Tracer:   p.TracerProvider.Tracer(name, opts...),
		provider: p,
	}
}

type privacyTracer struct {
	trace.Tracer
	provider trace.TracerProvider
}

func (t *privacyTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx = tracing.ContextWithActiveGoroutine(ctx)
	ctx, span := t.Tracer.Start(ctx, name, opts...)
	wrapped := &privacySpan{Span: span, provider: t.provider}
	return trace.ContextWithSpan(ctx, wrapped), wrapped
}

type privacySpan struct {
	trace.Span
	provider trace.TracerProvider
}

// RecordError intentionally emits no exception event. otelsql follows this call
// with SetStatus, but setting it here also covers future versions that do not.
func (s *privacySpan) RecordError(err error, _ ...trace.EventOption) {
	if err != nil {
		s.Span.SetStatus(codes.Error, mysqlFailureStatus)
	}
}

func (s *privacySpan) SetStatus(code codes.Code, description string) {
	if code == codes.Error {
		description = mysqlFailureStatus
	}
	s.Span.SetStatus(code, description)
}

func (s *privacySpan) TracerProvider() trace.TracerProvider {
	return s.provider
}

// Open preserves database/sql's direct driver/DSN API while applying fit-go's
// PII-safe OpenTelemetry hook when tracing is enabled. Services that cannot use
// environment discovery should use this instead of sql.Open; DSN and pool
// configuration remain entirely caller-owned.
func Open(driverName, dsn string) (*sql.DB, error) {
	return openSQLDB(driverName, dsn)
}
