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

// tracing.go adds OpenTelemetry command spans to the Redis driver via a go-redis
// Hook — the Go equivalent of the @opentelemetry/instrumentation-ioredis
// auto-instrumentation fit.js (Node) enabled. One SpanKindClient span is opened
// per command (and per pipeline) when tracing is enabled. It records only
// db.system / db.operation (the command name) — never keys, args, or values
// (platform "no PII in logs/traces" rule). A redis.Nil "key miss" is not an error.
package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"github.com/gofynd/fit-go/tracing"
)

// tracingHook implements goredis.Hook to emit per-command spans.
type tracingHook struct{}

// DialHook is a passthrough — connection dials are not spanned.
func (tracingHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

// ProcessHook spans a single command.
func (tracingHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		spanCtx, span, ok := startRedisSpan(ctx, cmd.Name())
		if !ok {
			return next(ctx, cmd)
		}
		err := next(spanCtx, cmd)
		endRedisSpan(span, err)
		return err
	}
}

// ProcessPipelineHook spans a pipeline (multiple commands in one round trip).
func (tracingHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		spanCtx, span, ok := startRedisSpan(ctx, "pipeline")
		if !ok {
			return next(ctx, cmds)
		}
		span.SetAttribute("db.redis.num_commands", len(cmds))
		err := next(spanCtx, cmds)
		endRedisSpan(span, err)
		return err
	}
}

// startRedisSpan opens a client span named "redis.<op>" when tracing is enabled.
// The bool reports whether a span was created (false ⇒ caller fast-paths next).
func startRedisSpan(ctx context.Context, op string) (context.Context, *tracing.Span, bool) {
	tracer := tracing.Global()
	if tracer == nil || !tracer.IsEnabled() {
		return ctx, nil, false
	}
	ctx, span := tracer.StartSpan(ctx, "redis."+op, tracing.SpanKindClient)
	span.SetAttributes(map[string]any{
		"db.system":    "redis",
		"db.operation": op,
	})
	return ctx, span, true
}

// endRedisSpan records status and ends the span. redis.Nil (key miss) is OK.
func endRedisSpan(span *tracing.Span, err error) {
	if err != nil && err != goredis.Nil {
		span.SetStatus(tracing.StatusError, err.Error())
	} else {
		span.SetStatus(tracing.StatusOK, "")
	}
	span.End()
}

// attachTracingHook adds the command-span hook to a go-redis client, but only
// when tracing is enabled — so a disabled client carries zero per-command
// overhead (no hook in the chain at all).
func attachTracingHook(c interface{ AddHook(goredis.Hook) }) {
	if t := tracing.Global(); t != nil && t.IsEnabled() {
		c.AddHook(tracingHook{})
	}
}
