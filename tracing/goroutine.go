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
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
)

var goroutineContexts sync.Map

// InjectContextIntoGoroutine stores the trace context from ctx in
// goroutine-scoped storage so that loggers and other infrastructure can
// retrieve trace/span IDs without explicit context threading.
// Returns a cleanup function that must be deferred.
//
//	cleanup := tracing.InjectContextIntoGoroutine(ctx)
//	defer cleanup()
func InjectContextIntoGoroutine(ctx context.Context) func() {
	gid := goroutineID()
	goroutineContexts.Store(gid, ctx)
	return func() {
		goroutineContexts.Delete(gid)
	}
}

// ContextFromGoroutine retrieves the context previously stored by
// InjectContextIntoGoroutine for the current goroutine.
// Returns nil if no context was injected.
func ContextFromGoroutine() context.Context {
	gid := goroutineID()
	if v, ok := goroutineContexts.Load(gid); ok {
		return v.(context.Context)
	}
	return nil
}

func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := bytes.Fields(buf[:n])
	if len(fields) < 2 {
		return 0
	}
	id, _ := strconv.ParseUint(string(fields[1]), 10, 64)
	return id
}
