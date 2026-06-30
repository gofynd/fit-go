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

// OTel HTTP tracing middleware for Gin.
//
// Creates a server span for each incoming HTTP request, extracts W3C
// traceparent from request headers, sets HTTP semantic convention
// attributes, and injects the span context into the response.
//
// Ignored paths (/_healthz, /_readyz) are skipped automatically via
// tracing.ShouldTrace.
package server

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/tracing"
)

// OTelMiddleware returns a Gin middleware that instruments each request
// with an OpenTelemetry span. When tracing is disabled (TRACING_ENABLED=false),
// the middleware is a no-op passthrough.
func OTelMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tracer := tracing.Global()
		if tracer == nil || !tracer.IsEnabled() {
			c.Next()
			return
		}

		path := c.Request.URL.Path
		if !tracing.ShouldTrace(path) {
			c.Next()
			return
		}

		// Extract W3C traceparent from incoming request.
		traceparent := c.GetHeader("traceparent")
		ctx := c.Request.Context()
		if traceparent != "" {
			traceID, spanID, sampled := tracing.ExtractTraceContext(traceparent)
			if traceID != "" {
				ctx = tracing.ContextWithTrace(ctx, traceID, spanID, sampled)
			}
		}

		spanName := fmt.Sprintf("%s %s", c.Request.Method, normalizeRoutePath(path))
		ctx, span := tracer.StartSpan(ctx, spanName, tracing.SpanKindServer)
		defer span.End()

		// Set HTTP semantic convention attributes. http.url is scheme+host+path
		// only — never URL.String(), which would capture the query string (PII:
		// emails/tokens routinely ride in query params) into the span.
		span.SetAttributes(map[string]any{
			"http.method":     c.Request.Method,
			"http.url":        httpScheme(c.Request) + "://" + c.Request.Host + path,
			"http.target":     path,
			"http.host":       c.Request.Host,
			"http.scheme":     httpScheme(c.Request),
			"http.user_agent": c.Request.UserAgent(),
			"http.request_id": c.GetHeader("x-request-id"),
			"net.peer.ip":     c.ClientIP(),
			"http.route":      normalizeRoutePath(path),
		})

		// Propagate trace context into the request for downstream handlers.
		c.Request = c.Request.WithContext(ctx)

		// Inject traceparent into response headers for client correlation.
		if span.TraceID() != "" && span.SpanID() != "" {
			c.Header("traceparent", tracing.FormatTraceparent(span.TraceID(), span.SpanID(), span.IsSampled()))
		}

		c.Next()

		// Set response attributes and status after handler completes.
		statusCode := c.Writer.Status()
		span.SetAttribute("http.status_code", statusCode)

		if statusCode >= http.StatusInternalServerError {
			span.SetStatus(tracing.StatusError, fmt.Sprintf("HTTP %d", statusCode))
		} else {
			span.SetStatus(tracing.StatusOK, "")
		}
	}
}

func httpScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		return fwd
	}
	return "http"
}
