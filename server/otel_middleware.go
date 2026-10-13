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

// OTel HTTP server tracing for Gin. Sensitive and high-cardinality request
// attributes remain opt-in so route parameters, user agents, and client
// addresses are not exported by default.
package server

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/tracing"
)

// OTelMiddlewareConfig controls HTTP trace enrichment. Sensitive and
// high-cardinality request metadata is excluded unless explicitly requested.
type OTelMiddlewareConfig struct {
	Tracer           *tracing.Tracer
	CaptureFullURL   bool
	CaptureUserAgent bool
	CaptureClientIP  bool
}

// OTelMiddlewareAdvancedConfig contains opt-in controls added after the
// original OTelMiddlewareConfig contract.
// Deprecated: use OTelMiddlewareOptions.
type OTelMiddlewareAdvancedConfig struct {
	OTelMiddlewareConfig
	PropagateResponseHeaders *bool
}

// OTelMiddleware returns a Gin middleware that instruments each request with
// an OpenTelemetry span. When tracing is disabled, it is a no-op passthrough.
func OTelMiddleware() gin.HandlerFunc {
	return OTelMiddlewareWithConfig(OTelMiddlewareConfig{})
}

// OTelMiddlewareWithConfig returns configurable Gin OpenTelemetry middleware.
func OTelMiddlewareWithConfig(config OTelMiddlewareConfig) gin.HandlerFunc {
	return otelMiddleware(OTelMiddlewareAdvancedConfig{OTelMiddlewareConfig: config}, false)
}

// OTelMiddlewareWithAdvancedConfig returns configurable Gin OpenTelemetry
// middleware with post-main opt-in controls.
// Deprecated: use OTelMiddlewareWithOptions.
func OTelMiddlewareWithAdvancedConfig(config OTelMiddlewareAdvancedConfig) gin.HandlerFunc {
	return otelMiddleware(config, true)
}

func otelMiddleware(config OTelMiddlewareAdvancedConfig, advanced bool) gin.HandlerFunc {
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

		if advanced {
			normalizeW3CTraceHeaders(c.Request.Header)
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

		attributes := map[string]any{
			"http.request.method": c.Request.Method,
			"url.scheme":          httpScheme(c.Request),
			"http.route":          route,
			"http.request_id":     RequestIDFromContext(c.Request.Context()),
		}
		if advanced {
			if host := requestHost(c.Request); host != "" {
				attributes["server.address"] = host
			}
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

		c.Request = c.Request.WithContext(ctx)
		propagateResponseHeaders := true
		if config.PropagateResponseHeaders != nil {
			propagateResponseHeaders = *config.PropagateResponseHeaders
		}
		if propagateResponseHeaders {
			otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(c.Writer.Header()))
		}

		c.Next()

		statusCode := c.Writer.Status()
		span.SetAttribute("http.status_code", statusCode)
		if statusCode >= http.StatusInternalServerError {
			span.SetStatus(tracing.StatusError, fmt.Sprintf("HTTP %d", statusCode))
		} else {
			span.SetStatus(tracing.StatusOK, "")
		}
	}
}

func requestHost(r *http.Request) string {
	host := strings.TrimSpace(r.Host)
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		return parsedHost
	}
	return strings.Trim(host, "[]")
}

// normalizeW3CTraceHeaders combines repeated W3C trace-context field-lines
// before propagation extracts them. Repeated traceparent values become one
// invalid combined value instead of silently trusting the first parent.
func normalizeW3CTraceHeaders(header http.Header) {
	for _, name := range []string{"traceparent", "tracestate"} {
		values := header.Values(name)
		if len(values) > 1 {
			header.Set(name, strings.Join(values, ", "))
		}
	}
}

// OTelRouteMiddleware updates the active HTTP server span after Gin resolves
// the matched route. This retains concrete child routes for nested Gin engines.
func OTelRouteMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		route := strings.TrimSpace(c.FullPath())
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

// GoroutineContextMiddleware stores the request context for the duration of
// the handler so plain logging calls can carry the active trace context.
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

func httpScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		return fwd
	}
	return "http"
}
