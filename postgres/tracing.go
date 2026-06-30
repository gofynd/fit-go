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
// default records the full SQL both as the db.statement attribute AND as the
// span name. We suppress BOTH — WithDisableSQLStatementInAttributes drops the
// attribute, and WithSpanNameFunc names spans by the leading SQL keyword only
// (SELECT/INSERT/…). Query parameters are never included (default).
package postgres

import (
	"strings"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"

	"github.com/gofynd/fit-go/tracing"
)

// newQueryTracer returns an otelpgx QueryTracer when tracing is enabled, else nil
// so the caller installs none.
func newQueryTracer() pgx.QueryTracer {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return nil
	}
	return otelpgx.NewTracer(
		otelpgx.WithDisableSQLStatementInAttributes(),
		otelpgx.WithSpanNameFunc(func(stmt string) string { return sqlOperation(stmt) }),
	)
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
	return strings.ToUpper(s)
}
