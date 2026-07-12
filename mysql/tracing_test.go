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
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const otelTestDriverName = "fit-go-mysql-otel-test"

var registerOTelTestDriver sync.Once

type otelTestDriver struct{}

func (otelTestDriver) Open(string) (driver.Conn, error) { return otelTestConn{}, nil }

type otelTestConn struct{}

func (otelTestConn) Prepare(string) (driver.Stmt, error) { return otelTestStmt{}, nil }
func (otelTestConn) Close() error                        { return nil }
func (otelTestConn) Begin() (driver.Tx, error)           { return otelTestTx{}, nil }
func (otelTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(query, "FAIL") {
		return nil, errors.New("mysql provider failure: password=driver-secret")
	}
	return driver.RowsAffected(1), nil
}

type otelTestStmt struct{}

func (otelTestStmt) Close() error                               { return nil }
func (otelTestStmt) NumInput() int                              { return -1 }
func (otelTestStmt) Exec([]driver.Value) (driver.Result, error) { return driver.RowsAffected(1), nil }
func (otelTestStmt) Query([]driver.Value) (driver.Rows, error)  { return nil, driver.ErrSkip }

type otelTestTx struct{}

func (otelTestTx) Commit() error   { return nil }
func (otelTestTx) Rollback() error { return nil }

func TestOpenSQLDB_InstrumentsWithoutSQLOrCredentials(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	registerOTelTestDriver.Do(func() { sql.Register(otelTestDriverName, otelTestDriver{}) })

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

	const dsn = "user:password@tcp(database.internal:3306)/customer"
	db, err := Open(otelTestDriverName, dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx, parent := provider.Tracer("mysql-test").Start(context.Background(), "parent")
	const query = "SELECT * FROM customers WHERE email = ?"
	const parameter = "secret@example.com"
	if _, err := db.ExecContext(ctx, query, parameter); err != nil {
		t.Fatalf("ExecContext: %v", err)
	}
	const failingQuery = "FAIL SELECT private_token FROM customers"
	const failingParameter = "bind-secret"
	if _, err := db.ExecContext(ctx, failingQuery, failingParameter); err == nil {
		t.Fatal("failing ExecContext unexpectedly succeeded")
	}
	parent.End()
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	foundDBSpan := false
	foundFailedDBSpan := false
	for _, span := range exporter.GetSpans() {
		if strings.HasPrefix(span.Name, "sql.") {
			foundDBSpan = true
		}
		if span.Status.Code == codes.Error {
			foundFailedDBSpan = true
			if span.Status.Description != mysqlFailureStatus {
				t.Fatalf("failed span status = %q, want %q", span.Status.Description, mysqlFailureStatus)
			}
			if len(span.Events) != 0 {
				t.Fatalf("failed span exported raw error events: %+v", span.Events)
			}
		}
		secrets := []string{query, parameter, failingQuery, failingParameter, dsn, "password", "driver-secret"}
		for _, secret := range secrets {
			if strings.Contains(span.Name, secret) {
				t.Fatalf("span name leaked sensitive value %q", secret)
			}
		}
		for _, secret := range secrets {
			if strings.Contains(span.Status.Description, secret) {
				t.Fatalf("span status leaked sensitive value %q", secret)
			}
		}
		for _, attr := range span.Attributes {
			key := string(attr.Key)
			value := attr.Value.String()
			if key == "db.statement" || key == "db.query.text" {
				t.Fatalf("SQL text attribute %q leaked into span", key)
			}
			for _, secret := range secrets {
				if strings.Contains(value, secret) {
					t.Fatalf("span attribute %q leaked sensitive value %q", key, secret)
				}
			}
		}
		for _, event := range span.Events {
			for _, attr := range event.Attributes {
				value := attr.Value.String()
				for _, secret := range secrets {
					if strings.Contains(value, secret) {
						t.Fatalf("span event attribute %q leaked sensitive value %q", attr.Key, secret)
					}
				}
			}
		}
	}
	if !foundDBSpan {
		t.Fatal("expected an instrumented database/sql span")
	}
	if !foundFailedDBSpan {
		t.Fatal("failed MySQL operation did not retain error span status")
	}
}

func TestOpenSQLDB_WrapsGoMySQLDriver(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	db, err := Open("mysql", "user:password@tcp(localhost:3306)/test")
	if err != nil {
		t.Fatalf("Open(mysql): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
