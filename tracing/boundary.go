// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package tracing

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// BoundaryType identifies a non-HTTP entry point.
type BoundaryType string

const (
	BoundaryWorker        BoundaryType = "worker"
	BoundaryCron          BoundaryType = "cron"
	BoundaryJob           BoundaryType = "job"
	BoundaryTask          BoundaryType = "task"
	boundaryTypeAttribute              = "fit.boundary.type"
	boundaryNameAttribute              = "fit.boundary.name"
)

// BoundaryOptions configure a worker, cron, job, or detached task span. Name
// must be a stable operation name, never a payload or user identifier.
type BoundaryOptions struct {
	Type       BoundaryType
	Name       string
	SpanKind   SpanKind
	Attributes SpanAttributes
	Tracer     *Tracer
}

// RunBoundary executes a non-HTTP entry point with an active span and
// goroutine-local logging/transport bridge. Errors are returned unchanged while
// span status remains privacy-safe. Panics are marked failed and rethrown.
func RunBoundary(ctx context.Context, options BoundaryOptions, operation func(context.Context) error) (err error) {
	if operation == nil {
		return errors.New("tracing: nil boundary operation")
	}
	ctx, span, restore, err := beginBoundary(ctx, options)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			if span != nil {
				span.SetAttribute("error.type", fmt.Sprintf("%T", recovered))
				span.SetStatus(StatusError, "boundary panicked")
			}
			restore()
			if span != nil {
				span.End()
			}
			panic(recovered)
		}
		if span != nil {
			if err != nil {
				span.SetAttribute("error.type", fmt.Sprintf("%T", err))
				span.SetStatus(StatusError, "boundary failed")
			} else {
				span.SetStatus(StatusOK, "")
			}
		}
		restore()
		if span != nil {
			span.End()
		}
	}()
	return operation(ctx)
}

// RunBoundaryWithResult is RunBoundary for operations returning a result.
func RunBoundaryWithResult[T any](ctx context.Context, options BoundaryOptions, operation func(context.Context) (T, error)) (result T, err error) {
	if operation == nil {
		return result, errors.New("tracing: nil boundary operation")
	}
	err = RunBoundary(ctx, options, func(boundaryContext context.Context) error {
		result, err = operation(boundaryContext)
		return err
	})
	return result, err
}

// WrapBoundary prepares a reusable boundary wrapper.
func WrapBoundary(options BoundaryOptions, operation func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		return RunBoundary(ctx, options, operation)
	}
}

func beginBoundary(ctx context.Context, options BoundaryOptions) (context.Context, *Span, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	options.Name = strings.TrimSpace(options.Name)
	if options.Name == "" {
		return nil, nil, nil, errors.New("tracing: boundary name is required")
	}
	if options.Type == "" {
		options.Type = BoundaryJob
	}
	switch options.Type {
	case BoundaryWorker, BoundaryCron, BoundaryJob, BoundaryTask:
	default:
		return nil, nil, nil, fmt.Errorf("tracing: invalid boundary type %q", options.Type)
	}
	for _, reserved := range []string{boundaryTypeAttribute, boundaryNameAttribute} {
		if _, exists := options.Attributes[reserved]; exists {
			return nil, nil, nil, fmt.Errorf("tracing: boundary attribute %q is reserved", reserved)
		}
	}
	tracer := options.Tracer
	if tracer == nil {
		tracer = Global()
	}
	if tracer == nil || !tracer.IsEnabled() {
		return ctx, nil, InjectContextIntoGoroutine(ctx), nil
	}
	spanName := string(options.Type) + " " + options.Name
	ctx, span := tracer.StartSpan(ctx, spanName, options.SpanKind)
	attributes := SpanAttributes{
		boundaryTypeAttribute: string(options.Type),
		boundaryNameAttribute: options.Name,
	}
	for key, value := range options.Attributes {
		attributes[key] = value
	}
	span.SetAttributes(attributes)
	return ctx, span, InjectContextIntoGoroutine(ctx), nil
}
