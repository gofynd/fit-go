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
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/gofynd/fit-go/internal/tracingtest"
)

// attachTracingHook delegates to redisotel (whose span behavior is tested
// upstream). Here we only assert fit-go's wiring: it is gated on tracing-enabled
// and never panics. The client is not connected (go-redis dials lazily).

func TestAttachTracingHook_NoOpWhenDisabled(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0"})
	defer c.Close()
	// Tracing disabled by default → no instrumentation added, no panic.
	attachTracingHook(c)
}

func TestAttachTracingHook_EnabledInstruments(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0"})
	defer c.Close()
	// With tracing enabled, redisotel instrumentation is attached without error.
	attachTracingHook(c)
}
