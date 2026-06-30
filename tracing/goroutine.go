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

	"github.com/gofynd/fit-go/internal/goroutinectx"
)

// InjectContextIntoGoroutine stores ctx in goroutine-scoped storage so that
// loggers and other infrastructure can retrieve trace/span IDs without explicit
// context threading. Returns a cleanup function that must be deferred; nested
// calls on the same goroutine compose correctly.
//
//	cleanup := tracing.InjectContextIntoGoroutine(ctx)
//	defer cleanup()
func InjectContextIntoGoroutine(ctx context.Context) func() {
	return goroutinectx.Inject(ctx)
}

// ContextFromGoroutine retrieves the context previously stored by
// InjectContextIntoGoroutine for the current goroutine. Returns nil if none.
func ContextFromGoroutine() context.Context {
	return goroutinectx.Load()
}
