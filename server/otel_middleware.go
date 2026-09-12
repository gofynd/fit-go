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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// OTelMiddlewareConfig controls HTTP trace enrichment. Sensitive and
// high-cardinality request metadata is excluded unless explicitly requested.
type OTelMiddlewareConfig struct {
	Tracer           *tracing.Tracer
	CaptureFullURL   bool
	CaptureUserAgent bool
	CaptureClientIP  bool
}

// OTelMiddleware returns a Gin middleware that instruments each request
// with an OpenTelemetry span. When tracing is disabled (TRACING_ENABLED=false),
// the middleware is a no-op passthrough.
func OTelMiddleware() gin.HandlerFunc {
	return OTelMiddlewareWithConfig(OTelMiddlewareConfig{})
}

// OTelMiddlewareWithConfig returns configurable Gin OpenTelemetry middleware.
func OTelMiddlewareWithConfig(config OTelMiddlewareConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		tracer := config.Tracer
		if tracer == nil {
			tracer = tracing.Global()
		}
		if tracer == nil || !tracer.IsEnabled() {
			c.Next()
			return
		}

		path := c.Request.URL.Path
		if !tracing.ShouldTrace(path) {
			c.Next()
			return
		}

		ctx := otel.GetTextMapPropagator().Extract(
			c.Request.Context(),
			propagation.HeaderCarrier(c.Request.Header),
		)

		route := c.FullPath()
		if route == "" {
			route = "/unmatched"
		}
		spanName := fmt.Sprintf("%s %s", c.Request.Method, route)
		ctx, span := tracer.StartSpan(ctx, spanName, tracing.SpanKindServer)
		defer span.End()

		// Set HTTP semantic convention attributes.
		attributes := map[string]any{
			"http.request.method": c.Request.Method,
			"url.scheme":          httpScheme(c.Request),
			"http.route":          route,
			"http.request_id":     RequestIDFromContext(c.Request.Context()),
		}
		if config.CaptureFullURL {
			attributes["url.full"] = c.Request.URL.String()
		}
		if config.CaptureUserAgent {
			attributes["user_agent.original"] = c.Request.UserAgent()
		}
		if config.CaptureClientIP {
			attributes["client.address"] = c.ClientIP()
		}
		span.SetAttributes(attributes)

		// Propagate trace context into the request for downstream handlers.
		c.Request = c.Request.WithContext(ctx)

		// Inject the real sampled state for client correlation.
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(c.Writer.Header()))

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
