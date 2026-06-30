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

// tracing.go instruments the Redis driver with OpenTelemetry command spans via
// the OFFICIAL redisotel package (shipped in the go-redis repo) — the Go
// equivalent of the @opentelemetry/instrumentation-ioredis auto-instrumentation
// fit.js (Node) enabled. redisotel uses the global OTel TracerProvider that
// fit-go's tracing init installs, so it is wired only when tracing is enabled
// (a disabled client carries zero per-command overhead). WithDBStatement(false)
// keeps raw commands/keys/args/values out of spans (platform "no PII in
// logs/traces" rule) — only the db.system / operation labels are recorded.
package redis

import (
	redisotel "github.com/redis/go-redis/extra/redisotel/v9"
	goredis "github.com/redis/go-redis/v9"

	"github.com/gofynd/fit-go/tracing"
)

// attachTracingHook instruments c with redisotel command spans, but only when
// tracing is enabled — so a disabled client gets no hook in the chain at all.
// All concrete go-redis client types (standalone, cluster, failover) satisfy
// goredis.UniversalClient.
func attachTracingHook(c goredis.UniversalClient) {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return
	}
	// PII-safe: WithDBStatement(false) suppresses the raw-command attribute.
	_ = redisotel.InstrumentTracing(c, redisotel.WithDBStatement(false))
}
