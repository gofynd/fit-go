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
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"

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
	instrumentRequest := otelgin.Middleware(
		// The first otelgin argument is the primary HTTP server address, not the
		// OTel resource service.name. Empty makes otelgin derive server.address
		// from Request.Host instead of incorrectly exporting SERVICE_NAME there.
		"",
		// WithGinFilter skips a request when the filter returns false.
		otelgin.WithGinFilter(func(c *gin.Context) bool {
			return tracing.ShouldTrace(c.Request.URL.Path)
		}),
	)
	return func(c *gin.Context) {
		normalizeW3CTraceHeaders(c.Request.Header)
		instrumentRequest(c)
	}
}

// normalizeW3CTraceHeaders combines repeated W3C trace-context field-lines
// before propagation extracts them. HTTP defines repeated field-lines as one
// comma-separated field value, but propagation.HeaderCarrier.Get otherwise
// exposes only the first value. Combining them preserves every tracestate
// member and causes an invalid repeated traceparent to be rejected as a whole.
func normalizeW3CTraceHeaders(header http.Header) {
	for _, name := range []string{"traceparent", "tracestate"} {
		values := header.Values(name)
		if len(values) > 1 {
			header.Set(name, strings.Join(values, ", "))
		}
	}
}

// OTelRouteMiddleware updates the active HTTP server span after Gin resolves the
// matched route. Install it on every Gin engine that owns concrete routes. This
// matters when a fit-go root engine delegates through NoRoute/gin.WrapH to a
// nested Gin engine: the outer engine only sees a fallback and cannot know the
// child's route template when otelgin starts the span.
func OTelRouteMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		route := strings.TrimSpace(c.FullPath())
		// A multi-type fit-go server mounts nested engines under an outer
		// `/<server-type>/*path` route. That wildcard is only a delegation
		// boundary; the nested engine has already finalized the concrete route.
		// Leaving the wildcard untouched preserves the useful child template.
		if route == "" || strings.HasSuffix(route, "/*path") {
			return
		}
		span := oteltrace.SpanFromContext(c.Request.Context())
		if !span.SpanContext().IsValid() {
			return
		}
		span.SetName(c.Request.Method + " " + route)
		span.SetAttributes(attribute.String("http.route", route))
	}
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
