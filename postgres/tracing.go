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

// tracing.go instruments the Postgres driver with OpenTelemetry query spans via
// the otelpgx package (pgx QueryTracer) — the Go equivalent of
// @opentelemetry/instrumentation-pg. otelpgx uses the global OTel TracerProvider
// that fit-go's tracing init installs, so it is wired only when tracing is
// enabled (zero per-query overhead when disabled).
//
// PII-safe configuration (platform "no PII in logs/traces" rule): otelpgx by
// default records SQL, connection details, and raw backend errors. The wrapper
// below strips caller-controlled inputs before delegating and replaces every
// backend error with a generic sentinel. This retains otelpgx's operation spans
// and metrics without exporting SQL, bind values, usernames, database/server
// details, or provider error text.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gofynd/fit-go/tracing"
)

var errPostgresOperationFailed = errors.New("postgresql operation failed")

// safePGXTracer implements every pgx tracer interface exposed by otelpgx. It
// sanitizes inputs before handing them to the upstream tracer, providing a
// defense in depth boundary even if an upstream option changes behavior.
type safePGXTracer struct {
	inner *otelpgx.Tracer
}

var (
	_ pgx.QueryTracer       = (*safePGXTracer)(nil)
	_ pgx.BatchTracer       = (*safePGXTracer)(nil)
	_ pgx.CopyFromTracer    = (*safePGXTracer)(nil)
	_ pgx.PrepareTracer     = (*safePGXTracer)(nil)
	_ pgx.ConnectTracer     = (*safePGXTracer)(nil)
	_ pgxpool.AcquireTracer = (*safePGXTracer)(nil)
)

// newQueryTracer returns an otelpgx QueryTracer when tracing is enabled, else nil
// so the caller installs none.
func newQueryTracer() pgx.QueryTracer {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return nil
	}
	return &safePGXTracer{inner: otelpgx.NewTracer(
		otelpgx.WithTrimSQLInSpanName(),
		otelpgx.WithDisableConnectionDetailsInAttributes(),
		otelpgx.WithDisableSQLStatementInAttributes(),
		otelpgx.WithSpanNameFunc(func(stmt string) string { return sqlOperation(stmt) }),
	)}
}

// sqlOperation extracts a PII-safe leading keyword (SELECT/INSERT/UPDATE/…) from
// a SQL statement — never the statement text or its arguments. Used for span
// names.
func sqlOperation(sql string) string {
	s := strings.TrimSpace(sql)
	if s == "" {
		return "query"
	}
	if i := strings.IndexAny(s, " \t\n\r("); i > 0 {
		s = s[:i]
	}
	s = strings.ToUpper(s)
	switch s {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "WITH",
		"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE",
		"COMMENT", "ANALYZE", "VACUUM", "EXPLAIN", "CALL", "DO",
		"COPY", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
		"SET", "SHOW", "RESET", "LISTEN", "UNLISTEN", "NOTIFY",
		"PREPARE", "EXECUTE", "DEALLOCATE":
		return s
	default:
		// The first SQL token is still caller-controlled. Unknown extensions or
		// malformed statements therefore use a fixed fallback instead of becoming
		// a span name.
		return "query"
	}
}

func safePostgresError(err error) error {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return err
	}
	return errPostgresOperationFailed
}

func safeSQL(sqlText string) string {
	return sqlOperation(sqlText)
}

func (t *safePGXTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	data.SQL = safeSQL(data.SQL)
	data.Args = nil
	return t.inner.TraceQueryStart(ctx, nil, data)
}

func (t *safePGXTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	data.Err = safePostgresError(data.Err)
	t.inner.TraceQueryEnd(ctx, nil, data)
}

func (t *safePGXTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	if data.Batch != nil {
		batch := &pgx.Batch{}
		for range data.Batch.Len() {
			batch.Queue("query")
		}
		data.Batch = batch
	}
	return t.inner.TraceBatchStart(ctx, nil, data)
}

func (t *safePGXTracer) TraceBatchQuery(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchQueryData) {
	data.SQL = safeSQL(data.SQL)
	data.Args = nil
	data.Err = safePostgresError(data.Err)
	t.inner.TraceBatchQuery(ctx, nil, data)
}

func (t *safePGXTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceBatchEndData) {
	data.Err = safePostgresError(data.Err)
	t.inner.TraceBatchEnd(ctx, nil, data)
}

func (t *safePGXTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromStartData) context.Context {
	data.TableName = pgx.Identifier{"table"}
	data.ColumnNames = nil
	return t.inner.TraceCopyFromStart(ctx, nil, data)
}

func (t *safePGXTracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceCopyFromEndData) {
	data.Err = safePostgresError(data.Err)
	t.inner.TraceCopyFromEnd(ctx, nil, data)
}

func (t *safePGXTracer) TracePrepareStart(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	data.Name = ""
	data.SQL = safeSQL(data.SQL)
	return t.inner.TracePrepareStart(ctx, nil, data)
}

func (t *safePGXTracer) TracePrepareEnd(ctx context.Context, _ *pgx.Conn, data pgx.TracePrepareEndData) {
	data.Err = safePostgresError(data.Err)
	t.inner.TracePrepareEnd(ctx, nil, data)
}

func (t *safePGXTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	return t.inner.TraceConnectStart(ctx, pgx.TraceConnectStartData{})
}

func (t *safePGXTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	data.Conn = nil
	data.Err = safePostgresError(data.Err)
	t.inner.TraceConnectEnd(ctx, data)
}

func (t *safePGXTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireStartData) context.Context {
	return t.inner.TraceAcquireStart(ctx, nil, data)
}

func (t *safePGXTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	data.Conn = nil
	data.Err = safePostgresError(data.Err)
	t.inner.TraceAcquireEnd(ctx, nil, data)
}
