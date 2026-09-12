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

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/tracing"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOTelMiddleware_TracingDisabled_Passthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddleware())
	engine.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/test", nil)
	engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestOTelMiddleware_HealthCheckSkipped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddleware())
	engine.GET("/_healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "healthy")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/_healthz", nil)
	engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// No traceparent header should be set for health checks.
	assert.Empty(t, w.Header().Get("traceparent"))
}

func TestOTelMiddleware_ReadyzSkipped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddleware())
	engine.GET("/_readyz", func(c *gin.Context) {
		c.String(http.StatusOK, "ready")
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/_readyz", nil)
	engine.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("traceparent"))
}

func TestHttpScheme_HTTP(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/test", nil)
	assert.Equal(t, "http", httpScheme(req))
}

func TestHttpScheme_XForwardedProto(t *testing.T) {
	req := httptest.NewRequest("GET", "http://localhost/test", nil)
	req.Header.Set("X-Forwarded-Proto", "https")
	assert.Equal(t, "https", httpScheme(req))
}

func TestOTelMiddleware_ContinuesRemoteTraceAndUsesSafeRouteAttributes(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName:            "http-test",
		SampleRate:             1,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New() error = %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddlewareWithConfig(OTelMiddlewareConfig{Tracer: tracer}))
	engine.GET("/orders/:orderID", func(c *gin.Context) {
		ctx, child := tracer.StartSpan(c.Request.Context(), "load-order", tracing.SpanKindInternal)
		defer child.End()
		if ctx == nil {
			t.Fatal("child context is nil")
		}
		c.String(http.StatusOK, "ok")
	})

	request := httptest.NewRequest(http.MethodGet, "/orders/private-order?token=secret", nil)
	request.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	request.Header.Set("user-agent", "private-user-agent")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported spans = %d, want 2", len(spans))
	}

	var serverSpan tracetest.SpanStub
	for _, span := range spans {
		if span.Name == "GET /orders/:orderID" {
			serverSpan = span
		}
	}
	if serverSpan.Name == "" {
		t.Fatalf("server span not found: %#v", spans)
	}
	assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", serverSpan.SpanContext.TraceID().String())
	assert.Equal(t, "b7ad6b7169203331", serverSpan.Parent.SpanID().String())

	attributes := make(map[string]string)
	for _, item := range serverSpan.Attributes {
		attributes[string(item.Key)] = item.Value.Emit()
	}
	assert.Equal(t, "/orders/:orderID", attributes["http.route"])
	assert.NotContains(t, attributes, "http.url")
	assert.NotContains(t, attributes, "http.user_agent")
	assert.NotContains(t, attributes, "net.peer.ip")
	for _, value := range attributes {
		assert.NotContains(t, value, "secret")
		assert.NotContains(t, value, "private-order")
	}
}

func TestOTelMiddleware_ResponseTraceparentKeepsUnsampledFlag(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName:  "unsampled-http-test",
		SampleRate:   0,
		SpanExporter: tracetest.NewInMemoryExporter(),
	})
	if err != nil {
		t.Fatalf("tracing.New() error = %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddlewareWithConfig(OTelMiddlewareConfig{Tracer: tracer}))
	engine.GET("/test", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/test", nil))

	traceparent := response.Header().Get("traceparent")
	assert.NotEmpty(t, traceparent)
	assert.True(t, len(traceparent) >= 2)
	assert.Equal(t, "00", traceparent[len(traceparent)-2:])
}

func TestOTelMiddleware_UsesGeneratedRequestID(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName:            "request-id-test",
		SampleRate:             1,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New() error = %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(RequestID())
	engine.Use(OTelMiddlewareWithConfig(OTelMiddlewareConfig{Tracer: tracer}))
	engine.GET("/test", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/test", nil))

	requestID := response.Header().Get("X-Request-ID")
	assert.NotEmpty(t, requestID)
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	for _, item := range spans[0].Attributes {
		if string(item.Key) == "http.request_id" {
			assert.Equal(t, requestID, item.Value.AsString())
			return
		}
	}
	t.Fatal("http.request_id attribute not found")
}
