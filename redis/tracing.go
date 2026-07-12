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

// tracing.go instruments the Redis driver with OpenTelemetry command spans, the
// Go equivalent of the @opentelemetry/instrumentation-ioredis instrumentation
// fit.js enabled. The upstream redisotel hook suppresses command statements but
// still exports raw backend errors in exception events and status descriptions.
// This package-owned hook retains client spans and context propagation while
// exporting only fixed operation metadata and generic failure status.
package redis

import (
	"context"
	"errors"
	"net"
	"strings"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/tracing"
)

const redisInstrumentationName = "github.com/gofynd/fit-go/redis"

// attachTracingHook instruments c with privacy-safe command spans, but only when
// tracing is enabled — so a disabled client gets no hook in the chain at all.
// All concrete go-redis client types (standalone, cluster, failover) satisfy
// goredis.UniversalClient.
func attachTracingHook(c goredis.UniversalClient) {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return
	}
	c.AddHook(newSafeTracingHook(otel.GetTracerProvider()))
}

type safeTracingHook struct {
	tracer trace.Tracer
}

var _ goredis.Hook = (*safeTracingHook)(nil)

func newSafeTracingHook(provider trace.TracerProvider) *safeTracingHook {
	if provider == nil {
		provider = otel.GetTracerProvider()
	}
	return &safeTracingHook{tracer: provider.Tracer(redisInstrumentationName)}
}

func (h *safeTracingHook) start(ctx context.Context, name, operation string, extra ...trace.SpanStartOption) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemNameRedis,
			semconv.DBOperationName(operation),
		),
	}
	opts = append(opts, extra...)
	return h.tracer.Start(ctx, name, opts...)
}

func setRedisSpanStatus(span trace.Span, err error) {
	if err != nil && !errors.Is(err, goredis.Nil) {
		// Redis and proxy errors can echo keys, values, scripts, credentials, or
		// complete commands. Preserve the caller-visible error but never copy it
		// into telemetry.
		span.SetStatus(codes.Error, "redis operation failed")
	}
}

func (h *safeTracingHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, span := h.start(ctx, "redis.dial", "dial")
		defer span.End()
		conn, err := next(ctx, network, addr)
		setRedisSpanStatus(span, err)
		return conn, err
	}
}

func (h *safeTracingHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		verb := safeRedisCommandVerb(cmd)
		ctx, span := h.start(ctx, verb, verb)
		defer span.End()
		err := next(ctx, cmd)
		setRedisSpanStatus(span, err)
		return err
	}
}

// safeRedisCommandVerb retains the legacy ioredis command-level signal without
// reading command arguments. Only one bounded command token is accepted; custom
// or malformed command names fall back to a fixed label.
func safeRedisCommandVerb(cmd goredis.Cmder) string {
	if cmd == nil {
		return "redis.command"
	}
	verb := strings.ToLower(strings.TrimSpace(cmd.Name()))
	if verb == "" || len(verb) > 64 {
		return "redis.command"
	}
	for _, r := range verb {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			continue
		}
		return "redis.command"
	}
	return verb
}

func (h *safeTracingHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		ctx, span := h.start(
			ctx,
			"redis.pipeline",
			"pipeline",
			trace.WithAttributes(semconv.DBOperationBatchSize(len(cmds))),
		)
		defer span.End()
		err := next(ctx, cmds)
		setRedisSpanStatus(span, err)
		return err
	}
}
