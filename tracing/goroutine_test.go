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

package tracing

import (
	"context"
	"os"
	"sync"
	"testing"
)

func TestInjectContextIntoGoroutine(t *testing.T) {
	ctx := ContextWithTrace(context.Background(), "trace-abc", "span-def")

	cleanup := InjectContextIntoGoroutine(ctx)
	defer cleanup()

	got := ContextFromGoroutine()
	if got == nil {
		t.Fatal("ContextFromGoroutine returned nil after injection")
	}
	if TraceIDFromContext(got) != "trace-abc" {
		t.Errorf("trace ID = %q, want trace-abc", TraceIDFromContext(got))
	}
	if SpanIDFromContext(got) != "span-def" {
		t.Errorf("span ID = %q, want span-def", SpanIDFromContext(got))
	}
}

func TestInjectContextIntoGoroutine_Cleanup(t *testing.T) {
	ctx := ContextWithTrace(context.Background(), "trace-123", "span-456")

	cleanup := InjectContextIntoGoroutine(ctx)
	cleanup()

	got := ContextFromGoroutine()
	if got != nil {
		t.Error("ContextFromGoroutine should return nil after cleanup")
	}
}

func TestContextFromGoroutine_NoInjection(t *testing.T) {
	got := ContextFromGoroutine()
	if got != nil {
		t.Error("ContextFromGoroutine should return nil when nothing injected")
	}
}

func TestInjectContextIntoGoroutine_CrossGoroutine(t *testing.T) {
	ctx := ContextWithTrace(context.Background(), "trace-parent", "span-parent")

	cleanup := InjectContextIntoGoroutine(ctx)
	defer cleanup()

	done := make(chan context.Context, 1)
	go func() {
		done <- ContextFromGoroutine()
	}()
	got := <-done

	if got != nil {
		t.Error("ContextFromGoroutine in a different goroutine should return nil")
	}
}

func TestInjectContextIntoGoroutine_Concurrent(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx := ContextWithTrace(context.Background(), "trace-"+string(rune('A'+idx%26)), "span")
			cleanup := InjectContextIntoGoroutine(ctx)
			defer cleanup()

			got := ContextFromGoroutine()
			if got == nil {
				t.Errorf("goroutine %d: context was nil", idx)
			}
		}(i)
	}
	wg.Wait()
}

func TestPackageLevelShutdown(t *testing.T) {
	resetGlobalTracer()
	os.Unsetenv("TRACING_ENABLED")

	if err := Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on nil global tracer should not error, got: %v", err)
	}

	Init()
	if err := Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown error = %v", err)
	}
	resetGlobalTracer()
}
