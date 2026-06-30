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

package redis

import (
	"context"
	"errors"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// The ProcessHook must call through and, when tracing is enabled, pass a
// span-bearing context to the next hook; when disabled it is a transparent
// passthrough (no span on the context).
func TestTracingHook_ProcessHook_SpanPresenceMatchesTracingState(t *testing.T) {
	var gotCtx context.Context
	called := false
	next := func(ctx context.Context, cmd goredis.Cmder) error {
		called = true
		gotCtx = ctx
		return nil
	}
	wrapped := tracingHook{}.ProcessHook(next)

	cmd := goredis.NewStatusCmd(context.Background(), "set", "k", "v")
	if err := wrapped(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessHook returned error: %v", err)
	}
	if !called {
		t.Fatal("next hook was not called")
	}

	span := tracing.SpanFromContext(gotCtx)
	if tracing.Global().IsEnabled() {
		if span == nil {
			t.Fatal("tracing enabled: next should receive a span-bearing context")
		}
	} else if span != nil {
		t.Fatal("tracing disabled: context must not carry a span (passthrough)")
	}
}

// A real command error propagates unchanged.
func TestTracingHook_ProcessHook_PropagatesError(t *testing.T) {
	wantErr := errors.New("boom")
	wrapped := tracingHook{}.ProcessHook(func(ctx context.Context, cmd goredis.Cmder) error {
		return wantErr
	})
	err := wrapped(context.Background(), goredis.NewStatusCmd(context.Background(), "get", "k"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("error not propagated: got %v", err)
	}
}

// redis.Nil (key miss) propagates to the caller AND must mark the span OK, not
// Error — the whole point of endRedisSpan's special-casing. With tracing enabled
// we capture the span and assert its status, so the behaviour is actually tested.
func TestTracingHook_ProcessHook_RedisNilIsNotAnError(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	var gotCtx context.Context
	wrapped := tracingHook{}.ProcessHook(func(ctx context.Context, cmd goredis.Cmder) error {
		gotCtx = ctx
		return goredis.Nil
	})
	err := wrapped(context.Background(), goredis.NewStringCmd(context.Background(), "get", "missing"))
	if !errors.Is(err, goredis.Nil) {
		t.Fatalf("redis.Nil should propagate, got %v", err)
	}
	span := tracing.SpanFromContext(gotCtx)
	if span == nil {
		t.Fatal("expected a command span on the context when tracing enabled")
	}
	if span.Status() != tracing.StatusOK {
		t.Fatalf("redis.Nil must leave the span status OK, got %v", span.Status())
	}
}

// With tracing enabled the next hook receives a span-bearing context.
func TestTracingHook_ProcessHook_EnabledCreatesSpan(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	var gotCtx context.Context
	wrapped := tracingHook{}.ProcessHook(func(ctx context.Context, cmd goredis.Cmder) error {
		gotCtx = ctx
		return nil
	})
	_ = wrapped(context.Background(), goredis.NewStatusCmd(context.Background(), "get", "k"))
	if tracing.SpanFromContext(gotCtx) == nil {
		t.Fatal("expected a command span on the context when tracing enabled")
	}
}
