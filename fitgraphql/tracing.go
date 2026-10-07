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

// Package fitgraphql provides privacy-safe OpenTelemetry instrumentation for
// gqlgen. It creates operation and resolver spans without exporting GraphQL
// documents, variables, resolver arguments, response values, or raw error text.
package fitgraphql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/gofynd/fit-go/fitgraphql"

var errGraphQLOperation = errors.New("graphql operation failed")
var errGraphQLPanic = errors.New("graphql operation panicked")

const maxTelemetryOperationNameLength = 128

// FieldPredicate decides whether a resolver span should be created.
type FieldPredicate func(*graphql.FieldContext) bool

// OperationNameMapper maps a client-supplied operation name to a bounded,
// allowlisted telemetry identity. Return false to omit it. An identity mapper
// should only be used when persisted queries enforce operation names.
type OperationNameMapper func(string) (string, bool)

// Options controls GraphQL tracing. Resolver spans are enabled by default.
// Payload capture is intentionally not configurable: application data does not
// belong in telemetry.
type Options struct {
	TracerProvider trace.TracerProvider
	FieldSpans     *bool
	FieldPredicate FieldPredicate
	OperationName  OperationNameMapper
}

// Tracer is a gqlgen extension that traces GraphQL operations and resolvers.
type Tracer struct {
	tracer         trace.Tracer
	fieldSpans     bool
	fieldPredicate FieldPredicate
	operationName  OperationNameMapper
}

var _ interface {
	graphql.HandlerExtension
	graphql.ResponseInterceptor
	graphql.FieldInterceptor
} = (*Tracer)(nil)

// New returns a gqlgen tracing extension. Register it with handler.Server.Use.
func New(opts Options) *Tracer {
	provider := opts.TracerProvider
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	fieldSpans := true
	if opts.FieldSpans != nil {
		fieldSpans = *opts.FieldSpans
	}
	return &Tracer{
		tracer:         provider.Tracer(instrumentationName),
		fieldSpans:     fieldSpans,
		fieldPredicate: opts.FieldPredicate,
		operationName:  opts.OperationName,
	}
}

// ExtensionName implements graphql.HandlerExtension.
func (*Tracer) ExtensionName() string { return "FitOpenTelemetry" }

// Validate implements graphql.HandlerExtension.
func (*Tracer) Validate(graphql.ExecutableSchema) error { return nil }

// InterceptResponse creates one internal span per GraphQL operation response.
// The surrounding HTTP or WebSocket instrumentation owns the server span.
func (t *Tracer) InterceptResponse(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
	if t == nil || !graphql.HasOperationContext(ctx) {
		return next(ctx)
	}
	op := graphql.GetOperationContext(ctx)
	name := t.telemetryOperationName(op)
	ctx, span := t.tracer.Start(ctx, operationSpanName(op, name), trace.WithSpanKind(trace.SpanKindInternal))
	defer endSpan(span)

	if op.Operation != nil {
		span.SetAttributes(attribute.String("graphql.operation.type", string(op.Operation.Operation)))
	}
	if name != "" {
		span.SetAttributes(attribute.String("graphql.operation.name", name))
	}

	resp := next(ctx)
	if resp != nil && len(resp.Errors) > 0 {
		markError(span, len(resp.Errors))
	}
	return resp
}

// InterceptField creates a child span for a resolver without recording its
// arguments or result.
func (t *Tracer) InterceptField(ctx context.Context, next graphql.Resolver) (any, error) {
	if t == nil || !t.fieldSpans {
		return next(ctx)
	}
	field := graphql.GetFieldContext(ctx)
	if field == nil || (t.fieldPredicate != nil && !t.fieldPredicate(field)) {
		return next(ctx)
	}

	ctx, span := t.tracer.Start(ctx, fieldSpanName(field), trace.WithSpanKind(trace.SpanKindInternal))
	defer endSpan(span)
	span.SetAttributes(
		attribute.String("graphql.field.name", field.Field.Name),
		attribute.String("graphql.field.object", field.Object),
	)

	result, err := next(ctx)
	if err != nil {
		markError(span, 1)
	}
	return result, err
}

func rawOperationName(op *graphql.OperationContext) string {
	if op == nil {
		return ""
	}
	if name := strings.TrimSpace(op.OperationName); name != "" {
		return name
	}
	if op.Operation != nil {
		return strings.TrimSpace(op.Operation.Name)
	}
	return ""
}

func (t *Tracer) telemetryOperationName(op *graphql.OperationContext) string {
	if t == nil || t.operationName == nil {
		return ""
	}
	mapped, include := t.operationName(rawOperationName(op))
	mapped = strings.TrimSpace(mapped)
	if !include || len(mapped) == 0 || len(mapped) > maxTelemetryOperationNameLength || !validTelemetryName(mapped) {
		return ""
	}
	return mapped
}

func operationSpanName(op *graphql.OperationContext, name string) string {
	if name != "" {
		return "graphql." + name
	}
	if op != nil && op.Operation != nil && op.Operation.Operation != "" {
		return "graphql." + string(op.Operation.Operation)
	}
	return "graphql.operation"
}

func validTelemetryName(value string) bool {
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return value != ""
}

func fieldSpanName(field *graphql.FieldContext) string {
	if field == nil {
		return "graphql.field"
	}
	object := strings.TrimSpace(field.Object)
	name := strings.TrimSpace(field.Field.Name)
	if object == "" {
		object = "unknown"
	}
	if name == "" {
		name = "field"
	}
	return "graphql." + object + "." + name
}

func markError(span trace.Span, count int) {
	span.SetStatus(codes.Error, "graphql operation failed")
	span.SetAttributes(
		attribute.Bool("graphql.error", true),
		attribute.Int("graphql.error.count", count),
	)
	span.RecordError(errGraphQLOperation)
}

func endSpan(span trace.Span) {
	if recovered := recover(); recovered != nil {
		span.SetStatus(codes.Error, "graphql operation panicked")
		span.SetAttributes(
			attribute.Bool("graphql.error", true),
			attribute.String("error.type", fmt.Sprintf("%T", recovered)),
		)
		span.RecordError(errGraphQLPanic)
		span.End()
		panic(recovered)
	}
	span.End()
}
