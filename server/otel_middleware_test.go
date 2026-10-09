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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/tracing"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
)

// zeroTraceID is the string of an invalid/absent span context — i.e. no span.
const zeroTraceID = "00000000000000000000000000000000"

func TestOTelMiddleware_TracingDisabled_Passthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddleware()) // tracing disabled → passthrough, zero overhead
	engine.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest("GET", "/test", nil))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "ok", w.Body.String())
}

func TestOTelMiddleware_ResolvesTracerAtRequestTime(t *testing.T) {
	gin.SetMode(gin.TestMode)
	restoreEmpty := tracing.SetGlobal(nil)
	t.Cleanup(restoreEmpty)

	engine := gin.New()
	engine.Use(OTelMiddleware())
	engine.GET("/late", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName: "late-init", Enabled: &enabled, Sampler: "always_on",
		SpanExporter: exporter, UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restoreTracer := tracing.SetGlobal(tracer)
	t.Cleanup(func() {
		restoreTracer()
		_ = tracer.Shutdown(context.Background())
	})

	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/late", nil))
	if response.Code != http.StatusNoContent || len(exporter.GetSpans()) != 1 {
		t.Fatalf("late tracer request status=%d spans=%d", response.Code, len(exporter.GetSpans()))
	}
}

func TestOTelRouteMiddleware_FinalizesNestedRouteAndRequestHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("SERVICE_NAME", "must-not-be-server-address")

	previousProvider := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:            "route-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := tracing.SetGlobal(tracer)
	t.Cleanup(func() {
		restore()
		_ = tracer.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
	})

	outer := gin.New()
	outer.Use(OTelMiddlewareWithAdvancedConfig(OTelMiddlewareAdvancedConfig{}))
	child := gin.New()
	child.Use(OTelRouteMiddleware())
	child.GET("/company/:company_id/item/:item_id", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	outer.NoRoute(gin.WrapH(child))

	req := httptest.NewRequest(http.MethodGet, "http://api.example.test/company/42/item/secret", nil)
	w := httptest.NewRecorder()
	outer.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	span := spans[0]
	if span.Name != "GET /company/:company_id/item/:item_id" {
		t.Fatalf("span name = %q", span.Name)
	}
	attrs := map[string]any{}
	for _, attr := range span.Attributes {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	if attrs["http.route"] != "/company/:company_id/item/:item_id" {
		t.Fatalf("http.route = %#v", attrs["http.route"])
	}
	if attrs["server.address"] != "api.example.test" {
		t.Fatalf("server.address = %#v, want request host", attrs["server.address"])
	}
	if attrs["server.address"] == "must-not-be-server-address" {
		t.Fatal("SERVICE_NAME leaked into server.address")
	}
}

func TestServerInit_MultiTypeKeepsNestedRouteTemplate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("SERVER_TYPE", "platform,partner")
	t.Setenv("UNIFY_SERVER", "false")

	previousProvider := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:            "multi-type-route-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := tracing.SetGlobal(tracer)
	t.Cleanup(func() {
		restore()
		_ = tracer.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
	})

	newRouter := func() http.Handler {
		engine := gin.New()
		engine.Use(OTelRouteMiddleware())
		engine.GET("/company/:company_id/item/:item_id", func(c *gin.Context) {
			c.Status(http.StatusNoContent)
		})
		return engine
	}
	server := NewAdvanced(AdvancedConfig{})
	if err := server.Init(map[ServerType]http.Handler{
		ServerTypePlatform: newRouter(),
		ServerTypePartner:  newRouter(),
	}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}

	recorder := httptest.NewRecorder()
	server.Router.ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet,
		"http://api.example.test/platform/company/42/item/private",
		nil,
	))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNoContent)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	if got, want := spans[0].Name, "GET /company/:company_id/item/:item_id"; got != want {
		t.Fatalf("span name = %q, want %q", got, want)
	}
}

// With tracing enabled, OTelMiddleware opens a span for normal routes but the
// ShouldTrace filter skips /_healthz and /_readyz (legacy fit.js parity). The
// handler echoes its context trace id, so "no span" shows up as the zero id.
func TestOTelMiddleware_EnabledSkipsHealthPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tracingtest.EnabledGlobal(t)
	t.Setenv("SERVICE_NAME", "fit-test")

	engine := gin.New()
	engine.Use(OTelMiddleware())
	echo := func(c *gin.Context) {
		c.String(http.StatusOK, trace.SpanContextFromContext(c.Request.Context()).TraceID().String())
	}
	engine.GET("/api/thing", echo)
	engine.GET("/_healthz", echo)
	engine.GET("/_readyz", echo)

	traced := httptest.NewRecorder()
	engine.ServeHTTP(traced, httptest.NewRequest("GET", "/api/thing", nil))
	assert.Equal(t, http.StatusOK, traced.Code)
	assert.NotEqual(t, zeroTraceID, traced.Body.String(), "normal route must get a server span")

	for _, p := range []string{"/_healthz", "/_readyz"} {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", p, nil))
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, zeroTraceID, w.Body.String(), p+" must be filtered (no span)")
	}
}

func TestNormalizeW3CTraceHeaders(t *testing.T) {
	header := http.Header{}
	header.Add("traceparent", "first-parent")
	header.Add("traceparent", "second-parent")
	header.Add("tracestate", "first=one")
	header.Add("tracestate", "second=two")
	header.Add("baggage", "first=one")
	header.Add("baggage", "second=two")

	normalizeW3CTraceHeaders(header)

	assert.Equal(t, []string{"first-parent, second-parent"}, header.Values("traceparent"))
	assert.Equal(t, []string{"first=one, second=two"}, header.Values("tracestate"))
	assert.Equal(t, []string{"first=one", "second=two"}, header.Values("baggage"))
}

func TestOTelMiddleware_RepeatedW3CTraceHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tracingtest.EnabledGlobal(t)

	const (
		firstTraceID  = "11111111111111111111111111111111"
		secondTraceID = "33333333333333333333333333333333"
		firstParent   = "00-" + firstTraceID + "-2222222222222222-01"
		secondParent  = "00-" + secondTraceID + "-4444444444444444-01"
	)

	tests := []struct {
		name             string
		traceparents     []string
		tracestates      []string
		wantTraceID      string
		unwantedTraceIDs []string
		wantTraceparent  string
		wantTracestate   string
		wantRequestState string
	}{
		{
			name:            "single parent continues trace",
			traceparents:    []string{firstParent},
			wantTraceID:     firstTraceID,
			wantTraceparent: firstParent,
		},
		{
			name:             "distinct parents start new trace",
			traceparents:     []string{firstParent, secondParent},
			unwantedTraceIDs: []string{firstTraceID, secondTraceID},
			wantTraceparent:  firstParent + ", " + secondParent,
		},
		{
			name:             "reversed parents start new trace",
			traceparents:     []string{secondParent, firstParent},
			unwantedTraceIDs: []string{firstTraceID, secondTraceID},
			wantTraceparent:  secondParent + ", " + firstParent,
		},
		{
			name:             "identical parents start new trace",
			traceparents:     []string{firstParent, firstParent},
			unwantedTraceIDs: []string{firstTraceID},
			wantTraceparent:  firstParent + ", " + firstParent,
		},
		{
			name:             "repeated state members are retained",
			traceparents:     []string{firstParent},
			tracestates:      []string{"vendor1=one", "vendor2=two"},
			wantTraceID:      firstTraceID,
			wantTraceparent:  firstParent,
			wantTracestate:   "vendor1=one,vendor2=two",
			wantRequestState: "vendor1=one, vendor2=two",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotTraceContext trace.SpanContext
			var gotTraceparent string
			var gotTracestate string
			engine := gin.New()
			engine.Use(OTelMiddlewareWithAdvancedConfig(OTelMiddlewareAdvancedConfig{}))
			engine.GET("/test", func(c *gin.Context) {
				gotTraceContext = trace.SpanContextFromContext(c.Request.Context())
				gotTraceparent = c.Request.Header.Get("traceparent")
				gotTracestate = c.Request.Header.Get("tracestate")
				c.Status(http.StatusNoContent)
			})

			request := httptest.NewRequest(http.MethodGet, "/test", nil)
			for _, value := range tt.traceparents {
				request.Header.Add("traceparent", value)
			}
			for _, value := range tt.tracestates {
				request.Header.Add("tracestate", value)
			}
			recorder := httptest.NewRecorder()
			engine.ServeHTTP(recorder, request)

			assert.Equal(t, http.StatusNoContent, recorder.Code)
			assert.True(t, gotTraceContext.IsValid())
			if tt.wantTraceID != "" {
				assert.Equal(t, tt.wantTraceID, gotTraceContext.TraceID().String())
			}
			for _, unwantedTraceID := range tt.unwantedTraceIDs {
				assert.NotEqual(t, unwantedTraceID, gotTraceContext.TraceID().String())
			}
			assert.Equal(t, tt.wantTraceparent, gotTraceparent)
			assert.Equal(t, tt.wantTracestate, gotTraceContext.TraceState().String())
			assert.Equal(t, tt.wantRequestState, gotTracestate)
		})
	}
}

func TestOTelMiddleware_ContinuesRemoteTraceAndUsesSafeRouteAttributes(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:            "http-test",
		SampleRate:             1,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.NewAdvanced() error = %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddlewareWithConfig(OTelMiddlewareConfig{Tracer: tracer}))
	engine.GET("/orders/:orderID", func(c *gin.Context) {
		_ = c.Error(errors.New("raw-handler-secret"))
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
	request.RemoteAddr = "203.0.113.10:4321"
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
	assert.NotContains(t, attributes, "url.full")
	assert.NotContains(t, attributes, "user_agent.original")
	assert.NotContains(t, attributes, "client.address")
	for _, value := range attributes {
		assert.NotContains(t, value, "secret")
		assert.NotContains(t, value, "private-order")
	}
	for _, event := range serverSpan.Events {
		assert.NotContains(t, event.Name, "raw-handler-secret")
		for _, item := range event.Attributes {
			assert.NotContains(t, item.Value.Emit(), "raw-handler-secret")
		}
	}
}

func TestOTelMiddleware_ResponseTraceparentKeepsUnsampledFlag(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:  "unsampled-http-test",
		SampleRate:   0,
		SpanExporter: tracetest.NewInMemoryExporter(),
	})
	if err != nil {
		t.Fatalf("tracing.NewAdvanced() error = %v", err)
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

func TestOTelMiddleware_CanDisableResponsePropagation(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:  "no-response-propagation-test",
		SampleRate:   1,
		SpanExporter: tracetest.NewInMemoryExporter(),
	})
	if err != nil {
		t.Fatalf("tracing.NewAdvanced() error = %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	propagate := false
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(OTelMiddlewareWithAdvancedConfig(OTelMiddlewareAdvancedConfig{
		OTelMiddlewareConfig:     OTelMiddlewareConfig{Tracer: tracer},
		PropagateResponseHeaders: &propagate,
	}))
	engine.GET("/test", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	response := httptest.NewRecorder()
	engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/test", nil))

	assert.Empty(t, response.Header().Get("traceparent"))
}

func TestOTelMiddleware_UsesGeneratedRequestID(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	exporter := tracetest.NewInMemoryExporter()
	tracer, err := tracing.NewAdvanced(context.Background(), tracing.AdvancedOptions{
		ServiceName:            "request-id-test",
		SampleRate:             1,
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.NewAdvanced() error = %v", err)
	}
	t.Cleanup(func() { _ = tracer.Shutdown(context.Background()) })

	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(RequestID())
	engine.Use(OTelMiddlewareWithConfig(OTelMiddlewareConfig{Tracer: tracer}))
	var inboundRequestID string
	engine.GET("/test", func(c *gin.Context) {
		inboundRequestID = c.GetHeader("X-Request-ID")
		c.String(http.StatusOK, "ok")
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	engine.ServeHTTP(response, request)

	requestID := response.Header().Get("X-Request-ID")
	assert.NotEmpty(t, requestID)
	assert.Empty(t, inboundRequestID)
	assert.Empty(t, request.Header.Get("X-Request-ID"))
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
