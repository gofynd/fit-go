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

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

type postgresTracingContextKey struct{}

type postgresTraceStartCase struct {
	name  string
	start func(*safePGXTracer, context.Context) context.Context
	end   func(*safePGXTracer, context.Context)
}

func postgresTraceStartCases() []postgresTraceStartCase {
	return []postgresTraceStartCase{
		{
			name: "query",
			start: func(tracer *safePGXTracer, ctx context.Context) context.Context {
				return tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
			},
			end: func(tracer *safePGXTracer, ctx context.Context) {
				tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})
			},
		},
		{
			name: "batch",
			start: func(tracer *safePGXTracer, ctx context.Context) context.Context {
				return tracer.TraceBatchStart(ctx, nil, pgx.TraceBatchStartData{})
			},
			end: func(tracer *safePGXTracer, ctx context.Context) {
				tracer.TraceBatchEnd(ctx, nil, pgx.TraceBatchEndData{})
			},
		},
		{
			name: "copy_from",
			start: func(tracer *safePGXTracer, ctx context.Context) context.Context {
				return tracer.TraceCopyFromStart(ctx, nil, pgx.TraceCopyFromStartData{})
			},
			end: func(tracer *safePGXTracer, ctx context.Context) {
				tracer.TraceCopyFromEnd(ctx, nil, pgx.TraceCopyFromEndData{})
			},
		},
		{
			name: "prepare",
			start: func(tracer *safePGXTracer, ctx context.Context) context.Context {
				return tracer.TracePrepareStart(ctx, nil, pgx.TracePrepareStartData{SQL: "SELECT 1"})
			},
			end: func(tracer *safePGXTracer, ctx context.Context) {
				tracer.TracePrepareEnd(ctx, nil, pgx.TracePrepareEndData{})
			},
		},
		{
			name: "connect",
			start: func(tracer *safePGXTracer, ctx context.Context) context.Context {
				return tracer.TraceConnectStart(ctx, pgx.TraceConnectStartData{})
			},
			end: func(tracer *safePGXTracer, ctx context.Context) {
				tracer.TraceConnectEnd(ctx, pgx.TraceConnectEndData{})
			},
		},
		{
			name: "acquire",
			start: func(tracer *safePGXTracer, ctx context.Context) context.Context {
				return tracer.TraceAcquireStart(ctx, nil, pgxpool.TraceAcquireStartData{})
			},
			end: func(tracer *safePGXTracer, ctx context.Context) {
				tracer.TraceAcquireEnd(ctx, nil, pgxpool.TraceAcquireEndData{})
			},
		},
	}
}

func newPostgresParentTestTracer(t *testing.T) (*safePGXTracer, *sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("tracer provider shutdown: %v", err)
		}
	})

	queryTracer := newQueryTracer()
	safeTracer, ok := queryTracer.(*safePGXTracer)
	if !ok {
		t.Fatalf("newQueryTracer returned %T, want *safePGXTracer", queryTracer)
	}
	return safeTracer, provider, exporter
}

func exportedPostgresSpan(t *testing.T, exporter *tracetest.InMemoryExporter, spanContext trace.SpanContext) tracetest.SpanStub {
	t.Helper()
	for _, span := range exporter.GetSpans() {
		if span.SpanContext.SpanID() == spanContext.SpanID() {
			return span
		}
	}
	t.Fatalf("span %s was not exported", spanContext.SpanID())
	return tracetest.SpanStub{}
}

func TestSafePGXTraceStartsInheritActiveGoroutineParent(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	for _, testCase := range postgresTraceStartCases() {
		t.Run(testCase.name, func(t *testing.T) {
			safeTracer, provider, exporter := newPostgresParentTestTracer(t)
			activeCtx, activeSpan := provider.Tracer("postgres-active-parent").Start(context.Background(), "active-parent")
			defer activeSpan.End()
			restore := tracing.InjectContextIntoGoroutine(activeCtx)
			defer restore()

			base := context.WithValue(context.Background(), postgresTracingContextKey{}, "preserved")
			base, cancel := context.WithCancel(base)
			operationCtx := testCase.start(safeTracer, base)
			operationSpanContext := trace.SpanFromContext(operationCtx).SpanContext()
			if !operationSpanContext.IsValid() {
				t.Fatal("trace start did not create an operation span")
			}
			testCase.end(safeTracer, operationCtx)

			if operationCtx.Value(postgresTracingContextKey{}) != "preserved" {
				t.Fatal("base context value was not preserved")
			}
			cancel()
			if !errors.Is(operationCtx.Err(), context.Canceled) {
				t.Fatalf("operation context error = %v, want context.Canceled", operationCtx.Err())
			}

			got := exportedPostgresSpan(t, exporter, operationSpanContext)
			wantParent := activeSpan.SpanContext()
			if got.Parent.SpanID() != wantParent.SpanID() || got.Parent.TraceID() != wantParent.TraceID() {
				t.Fatalf("parent = %s/%s, want active goroutine parent %s/%s",
					got.Parent.TraceID(), got.Parent.SpanID(), wantParent.TraceID(), wantParent.SpanID())
			}
		})
	}
}

func TestSafePGXTraceStartsExplicitParentWins(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	for _, testCase := range postgresTraceStartCases() {
		t.Run(testCase.name, func(t *testing.T) {
			safeTracer, provider, exporter := newPostgresParentTestTracer(t)
			activeCtx, activeSpan := provider.Tracer("postgres-active-parent").Start(context.Background(), "active-parent")
			defer activeSpan.End()
			restore := tracing.InjectContextIntoGoroutine(activeCtx)
			defer restore()

			explicitCtx, explicitSpan := provider.Tracer("postgres-explicit-parent").Start(context.Background(), "explicit-parent")
			defer explicitSpan.End()
			operationCtx := testCase.start(safeTracer, explicitCtx)
			operationSpanContext := trace.SpanFromContext(operationCtx).SpanContext()
			if !operationSpanContext.IsValid() {
				t.Fatal("trace start did not create an operation span")
			}
			testCase.end(safeTracer, operationCtx)

			got := exportedPostgresSpan(t, exporter, operationSpanContext)
			wantParent := explicitSpan.SpanContext()
			if got.Parent.SpanID() != wantParent.SpanID() || got.Parent.TraceID() != wantParent.TraceID() {
				t.Fatalf("parent = %s/%s, want explicit parent %s/%s",
					got.Parent.TraceID(), got.Parent.SpanID(), wantParent.TraceID(), wantParent.SpanID())
			}
			if got.Parent.SpanID() == activeSpan.SpanContext().SpanID() {
				t.Fatal("active goroutine parent overrode explicit parent")
			}
		})
	}
}

// sqlOperation must extract a PII-safe leading keyword and never the statement.
func TestSQLOperation(t *testing.T) {
	cases := map[string]string{
		"SELECT * FROM users WHERE email = $1":  "SELECT",
		"  insert into t (a) values ($1)":       "INSERT",
		"UPDATE t SET a = $1":                   "UPDATE",
		"delete from t":                         "DELETE",
		"BEGIN":                                 "BEGIN",
		"select(1)":                             "SELECT",
		"secret_function('token-canary')":       "query",
		"/* email=secret@example.com */ SELECT": "query",
		"":                                      "query",
		"   ":                                   "query",
	}
	for sql, want := range cases {
		if got := sqlOperation(sql); got != want {
			t.Errorf("sqlOperation(%q) = %q, want %q", sql, got, want)
		}
	}
}

// When tracing is off, no tracer is installed.
func TestNewQueryTracer_DisabledReturnsNil(t *testing.T) {
	if tracing.Global().IsEnabled() {
		t.Skip("global tracer cached enabled; disabled-path assertion N/A")
	}
	if newQueryTracer() != nil {
		t.Fatal("expected nil query tracer when tracing disabled")
	}
}

// When tracing is enabled, an otelpgx QueryTracer is installed (its span
// lifecycle is tested upstream in otelpgx; here we assert fit-go's wiring).
func TestNewQueryTracer_EnabledReturnsTracer(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	if newQueryTracer() == nil {
		t.Fatal("expected a query tracer when tracing enabled")
	}
}

func TestSafePGXTracerDoesNotExportSQLConnectionDetailsOrBackendErrors(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
	})

	queryTracer := newQueryTracer()
	safeTracer, ok := queryTracer.(*safePGXTracer)
	if !ok {
		t.Fatalf("newQueryTracer returned %T, want *safePGXTracer", queryTracer)
	}

	const (
		sqlCanary      = "SELECT password FROM private_accounts WHERE token = 'sql-token-canary'"
		argumentCanary = "argument-value-canary"
		errorCanary    = "postgres backend error password=backend-password-canary value=backend-value-canary"
		hostCanary     = "postgres-host-canary.internal"
		userCanary     = "postgres-user-canary"
		passwordCanary = "postgres-password-canary"
		databaseCanary = "postgres-database-canary"
		tableCanary    = "postgres-table-canary"
		prepareCanary  = "postgres-prepare-canary"
	)

	connConfig, err := pgx.ParseConfig("postgres://" + userCanary + ":" + passwordCanary + "@" + hostCanary + ":5432/" + databaseCanary)
	if err != nil {
		t.Fatalf("pgx.ParseConfig: %v", err)
	}
	backendErr := errors.New(errorCanary)
	parentCtx, parent := provider.Tracer("postgres-privacy-test").Start(context.Background(), "parent")

	queryCtx := safeTracer.TraceQueryStart(parentCtx, nil, pgx.TraceQueryStartData{
		SQL:  sqlCanary,
		Args: []any{argumentCanary},
	})
	safeTracer.TraceQueryEnd(queryCtx, nil, pgx.TraceQueryEndData{Err: backendErr})

	batch := &pgx.Batch{}
	batch.Queue(sqlCanary, argumentCanary)
	batchCtx := safeTracer.TraceBatchStart(parentCtx, nil, pgx.TraceBatchStartData{Batch: batch})
	safeTracer.TraceBatchQuery(batchCtx, nil, pgx.TraceBatchQueryData{
		SQL:  sqlCanary,
		Args: []any{argumentCanary},
		Err:  backendErr,
	})
	safeTracer.TraceBatchEnd(batchCtx, nil, pgx.TraceBatchEndData{Err: backendErr})

	copyCtx := safeTracer.TraceCopyFromStart(parentCtx, nil, pgx.TraceCopyFromStartData{
		TableName:   pgx.Identifier{tableCanary},
		ColumnNames: []string{"value-" + argumentCanary},
	})
	safeTracer.TraceCopyFromEnd(copyCtx, nil, pgx.TraceCopyFromEndData{Err: backendErr})

	prepareCtx := safeTracer.TracePrepareStart(parentCtx, nil, pgx.TracePrepareStartData{
		Name: prepareCanary,
		SQL:  sqlCanary,
	})
	safeTracer.TracePrepareEnd(prepareCtx, nil, pgx.TracePrepareEndData{Err: backendErr})

	connectCtx := safeTracer.TraceConnectStart(parentCtx, pgx.TraceConnectStartData{ConnConfig: connConfig})
	safeTracer.TraceConnectEnd(connectCtx, pgx.TraceConnectEndData{Err: backendErr})

	acquireCtx := safeTracer.TraceAcquireStart(parentCtx, nil, pgxpool.TraceAcquireStartData{})
	safeTracer.TraceAcquireEnd(acquireCtx, nil, pgxpool.TraceAcquireEndData{Err: backendErr})
	parent.End()

	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) < 8 {
		t.Fatalf("expected PostgreSQL operation spans, got %d", len(spans))
	}
	secrets := []string{
		sqlCanary,
		argumentCanary,
		errorCanary,
		"backend-password-canary",
		"backend-value-canary",
		hostCanary,
		userCanary,
		passwordCanary,
		databaseCanary,
		tableCanary,
		prepareCanary,
	}
	for _, span := range spans {
		assertSpanContainsNoCanaries(t, span, secrets)
		for _, attr := range span.Attributes {
			switch string(attr.Key) {
			case "db.query.text", "db.statement", "server.address", "server.port", "user.name", "db.user", "db.namespace", "db.name":
				t.Fatalf("span %q exported forbidden PostgreSQL attribute %q", span.Name, attr.Key)
			}
		}
	}
}

func assertSpanContainsNoCanaries(t *testing.T, span tracetest.SpanStub, canaries []string) {
	t.Helper()
	check := func(surface, value string) {
		t.Helper()
		for _, canary := range canaries {
			if strings.Contains(value, canary) {
				t.Fatalf("span %q leaked %q through %s", span.Name, canary, surface)
			}
		}
	}

	check("name", span.Name)
	check("status", span.Status.Description)
	for _, attr := range span.Attributes {
		check("attribute "+string(attr.Key), attr.Value.Emit())
	}
	for _, event := range span.Events {
		check("event name", event.Name)
		for _, attr := range event.Attributes {
			check("event attribute "+string(attr.Key), attr.Value.Emit())
		}
	}
}
