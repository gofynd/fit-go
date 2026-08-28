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

package fitgraphql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestOperationSpanDoesNotCapturePayloadsOrRawErrors(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := New(Options{TracerProvider: provider})

	op := &graphql.OperationContext{
		RawQuery:      `mutation Save($password: String!) { save(password: $password) }`,
		Variables:     map[string]any{"password": "secret-value"},
		OperationName: "Save",
		Operation:     &ast.OperationDefinition{Operation: ast.Mutation, Name: "Save"},
	}
	ctx := graphql.WithOperationContext(context.Background(), op)
	response := tracer.InterceptResponse(ctx, func(ctx context.Context) *graphql.Response {
		if !oteltrace.SpanFromContext(ctx).SpanContext().IsValid() {
			t.Fatal("operation context did not contain the GraphQL span")
		}
		return &graphql.Response{Errors: gqlerror.List{gqlerror.Errorf("password secret-value rejected")}}
	})
	if response == nil || len(response.Errors) != 1 {
		t.Fatal("response was changed by tracing")
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	assertNoSensitiveTelemetry(t, spans[0], "secret-value", "password", "mutation Save", "Save")
}

func TestOperationNameRequiresSafeExplicitMapping(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := New(Options{
		TracerProvider: provider,
		OperationName: func(name string) (string, bool) {
			if name == "ClientControlledSecret" {
				return "PersistedOperation42", true
			}
			return "", false
		},
	})
	op := &graphql.OperationContext{
		OperationName: "ClientControlledSecret",
		Operation:     &ast.OperationDefinition{Operation: ast.Query, Name: "ClientControlledSecret"},
	}
	ctx := graphql.WithOperationContext(context.Background(), op)
	tracer.InterceptResponse(ctx, func(context.Context) *graphql.Response { return &graphql.Response{} })
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "graphql.PersistedOperation42" {
		t.Fatalf("operation spans = %+v", spans)
	}
	assertNoSensitiveTelemetry(t, spans[0], "ClientControlledSecret")
}

func TestResolverSpanIsChildAndPrivacySafe(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := New(Options{TracerProvider: provider})

	ctx, parent := provider.Tracer("test").Start(context.Background(), "parent")
	field := &graphql.FieldContext{
		Object: "Mutation",
		Args:   map[string]any{"token": "private-token"},
		Field: graphql.CollectedField{Field: &ast.Field{
			Name:  "updateProfile",
			Alias: "private_token_alias",
		}},
	}
	ctx = graphql.WithFieldContext(ctx, field)
	wantErr := errors.New("provider included private-token")
	_, gotErr := tracer.InterceptField(ctx, func(context.Context) (any, error) {
		return map[string]any{"token": "private-token"}, wantErr
	})
	parent.End()
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("resolver error = %v, want original error", gotErr)
	}

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	resolver := spans[0]
	if resolver.Parent().SpanID() != spans[1].SpanContext().SpanID() {
		t.Fatal("resolver span is not a child of the active span")
	}
	assertNoSensitiveTelemetry(t, resolver, "private-token", "private_token_alias", "provider included")
}

func TestResolverSpansCanBeDisabledOrFiltered(t *testing.T) {
	disabled := false
	tracer := New(Options{FieldSpans: &disabled})
	called := false
	_, err := tracer.InterceptField(context.Background(), func(context.Context) (any, error) {
		called = true
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("disabled middleware changed resolver result: called=%v err=%v", called, err)
	}

	tracer = New(Options{FieldPredicate: func(*graphql.FieldContext) bool { return false }})
	ctx := graphql.WithFieldContext(context.Background(), &graphql.FieldContext{})
	called = false
	_, err = tracer.InterceptField(ctx, func(context.Context) (any, error) {
		called = true
		return nil, nil
	})
	if err != nil || !called {
		t.Fatalf("filtered middleware changed resolver result: called=%v err=%v", called, err)
	}
}

func TestOperationPanicIsMarkedWithoutCapturingValue(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	tracer := New(Options{TracerProvider: provider})
	ctx := graphql.WithOperationContext(context.Background(), &graphql.OperationContext{
		Operation: &ast.OperationDefinition{Operation: ast.Query},
	})
	panicValue := "private-panic-value"
	func() {
		defer func() {
			if recovered := recover(); recovered != panicValue {
				t.Fatalf("recovered = %#v", recovered)
			}
		}()
		tracer.InterceptResponse(ctx, func(context.Context) *graphql.Response { panic(panicValue) })
	}()
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Status().Code.String() != "Error" {
		t.Fatalf("panic span = %+v", spans)
	}
	assertNoSensitiveTelemetry(t, spans[0], panicValue)
}

func assertNoSensitiveTelemetry(t *testing.T, span sdktrace.ReadOnlySpan, forbidden ...string) {
	t.Helper()
	values := []string{span.Name(), span.Status().Description}
	for _, attr := range span.Attributes() {
		values = append(values, string(attr.Key), attr.Value.Emit())
	}
	for _, event := range span.Events() {
		values = append(values, event.Name)
		for _, attr := range event.Attributes {
			values = append(values, string(attr.Key), attr.Value.Emit())
		}
	}
	for _, value := range values {
		for _, secret := range forbidden {
			if strings.Contains(value, secret) {
				t.Fatalf("telemetry value %q contains forbidden text %q", value, secret)
			}
		}
	}
}
