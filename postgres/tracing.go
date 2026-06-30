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

// tracing.go adds OpenTelemetry spans to the Postgres driver via a pgx
// QueryTracer — the Go equivalent of @opentelemetry/instrumentation-pg. One
// SpanKindClient span is opened per Query/QueryRow/Exec when tracing is enabled.
// To honor the platform "no PII in traces" rule it records only db.system and a
// db.operation derived from the leading SQL keyword (SELECT/INSERT/…) — never the
// full statement text or the query arguments.
package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/gofynd/fit-go/tracing"
)

// queryTracer implements pgx.QueryTracer.
type queryTracer struct{}

// pgSpanKey carries the in-flight span between TraceQueryStart and TraceQueryEnd.
type pgSpanKey struct{}

// newQueryTracer returns the tracer when tracing is enabled, else nil so the
// caller installs none (zero per-query overhead when disabled).
func newQueryTracer() pgx.QueryTracer {
	if t := tracing.Global(); t != nil && t.IsEnabled() {
		return queryTracer{}
	}
	return nil
}

// TraceQueryStart opens a client span and stashes it in the returned context,
// which pgx passes back to TraceQueryEnd.
func (queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return ctx
	}
	op := sqlOperation(data.SQL)
	ctx, span := tracer.StartSpan(ctx, "postgresql."+op, tracing.SpanKindClient)
	span.SetAttributes(map[string]any{
		"db.system":    "postgresql",
		"db.operation": op,
	})
	return context.WithValue(ctx, pgSpanKey{}, span)
}

// TraceQueryEnd closes the span started in TraceQueryStart, recording error status.
func (queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(pgSpanKey{}).(*tracing.Span)
	if !ok || span == nil {
		return
	}
	if data.Err != nil {
		span.SetStatus(tracing.StatusError, data.Err.Error())
	} else {
		span.SetStatus(tracing.StatusOK, "")
	}
	span.End()
}

// sqlOperation returns the leading SQL keyword (uppercased), e.g. "SELECT" — a
// PII-safe operation label. The statement text and args are deliberately omitted.
func sqlOperation(sql string) string {
	s := strings.TrimSpace(sql)
	if s == "" {
		return "query"
	}
	if i := strings.IndexAny(s, " \t\n\r("); i > 0 {
		s = s[:i]
	}
	return strings.ToUpper(s)
}
