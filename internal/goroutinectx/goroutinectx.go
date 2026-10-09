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

// Package goroutinectx provides goroutine-local storage of the active context,
// so loggers can pick up the current trace/span without explicit context
// threading. It lives in internal/ so both the tracing and logging packages can
// use it without an import cycle (tracing already imports logging).
package goroutinectx

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
)

var store sync.Map // goroutine id -> context.Context

// Inject stores ctx as the active context for the current goroutine and returns
// a cleanup that restores the previous value (so nested Injects on the same
// goroutine compose correctly). Each returned cleanup must be called once.
func Inject(ctx context.Context) func() {
	gid := goroutineID()
	var prev context.Context
	if v, ok := store.Load(gid); ok {
		prev = v.(context.Context)
	}
	store.Store(gid, ctx)
	return func() {
		if prev != nil {
			store.Store(gid, prev)
		} else {
			store.Delete(gid)
		}
	}
}

// Active returns the active goroutine-local context, or context.Background() if
// none is stored.
func Active() context.Context {
	if v, ok := store.Load(goroutineID()); ok {
		return v.(context.Context)
	}
	return context.Background()
}

// Load returns the active goroutine-local context, or nil if none is stored.
func Load() context.Context {
	if v, ok := store.Load(goroutineID()); ok {
		return v.(context.Context)
	}
	return nil
}

// goroutineID parses the current goroutine's id from its stack header. It is
// only used to key short-lived per-goroutine context, never for correctness of
// trace data.
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
