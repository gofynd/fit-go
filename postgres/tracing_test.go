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
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// sqlOperation must extract a PII-safe leading keyword and never the statement.
func TestSQLOperation(t *testing.T) {
	cases := map[string]string{
		"SELECT * FROM users WHERE email = $1": "SELECT",
		"  insert into t (a) values ($1)":      "INSERT",
		"UPDATE t SET a = $1":                  "UPDATE",
		"delete from t":                        "DELETE",
		"BEGIN":                                "BEGIN",
		"select(1)":                            "SELECT",
		"":                                     "query",
		"   ":                                  "query",
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

// TraceQueryStart opens a span (stashed in the returned context) and TraceQueryEnd
// closes it without panic, on both success and error.
func TestQueryTracer_Lifecycle(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	qt := queryTracer{}

	// Success path.
	ctx := qt.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "SELECT 1"})
	if _, ok := ctx.Value(pgSpanKey{}).(*tracing.Span); !ok {
		t.Fatal("TraceQueryStart must stash a span in the context")
	}
	qt.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{}) // no panic

	// Error path.
	ctx2 := qt.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "INSERT INTO t VALUES ($1)"})
	qt.TraceQueryEnd(ctx2, nil, pgx.TraceQueryEndData{Err: errors.New("constraint violation")})

	// End with no span in context is a safe no-op.
	qt.TraceQueryEnd(context.Background(), nil, pgx.TraceQueryEndData{})
}
