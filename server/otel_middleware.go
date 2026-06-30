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

// OTel HTTP server tracing for Gin, via the official otelgin contrib package —
// the Go equivalent of the OTel express auto-instrumentation fit.js enabled.
package server

import (
	"os"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/gofynd/fit-go/tracing"
)

// OTelMiddleware returns a Gin middleware that opens a server span per request
// using the official otelgin instrumentation. otelgin uses the global OTel
// TracerProvider + W3C propagator that fit-go's tracing init installs (extracts
// inbound traceparent, parents the span to it).
//
//   - When tracing is disabled this returns a passthrough — zero overhead, no
//     otelgin work per request.
//   - /_healthz and /_readyz are skipped via tracing.ShouldTrace, matching the
//     legacy fit.js ignoreIncomingRequestHook.
//   - PII-safe: otelgin records http.route (the gin route template), method,
//     scheme and url.path — never the query string.
func OTelMiddleware() gin.HandlerFunc {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return func(c *gin.Context) { c.Next() }
	}
	return otelgin.Middleware(
		os.Getenv("SERVICE_NAME"),
		// WithGinFilter skips a request when the filter returns false.
		otelgin.WithGinFilter(func(c *gin.Context) bool {
			return tracing.ShouldTrace(c.Request.URL.Path)
		}),
	)
}

// GoroutineContextMiddleware stores the request context (carrying the otelgin
// server span set by OTelMiddleware) in goroutine-local storage for the duration
// of the handler, so plain logging.* calls in handlers carry the trace without
// explicit context threading. Installed right after OTelMiddleware. Passthrough
// (zero cost) when tracing is disabled.
func GoroutineContextMiddleware() gin.HandlerFunc {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		cleanup := tracing.InjectContextIntoGoroutine(c.Request.Context())
		defer cleanup()
		c.Next()
	}
}
