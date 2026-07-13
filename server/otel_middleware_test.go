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
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
	fittracing "github.com/gofynd/fit-go/tracing"
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

func TestOTelRouteMiddleware_FinalizesNestedRouteAndRequestHost(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("OTEL_SDK_DISABLED", "false")
	t.Setenv("SERVICE_NAME", "must-not-be-server-address")

	previousProvider := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := fittracing.New(context.Background(), fittracing.Options{
		ServiceName:            "route-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := fittracing.SetGlobal(tracer)
	t.Cleanup(func() {
		restore()
		_ = tracer.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
	})

	outer := gin.New()
	outer.Use(OTelMiddleware())
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
	tracer, err := fittracing.New(context.Background(), fittracing.Options{
		ServiceName:            "multi-type-route-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := fittracing.SetGlobal(tracer)
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
	server := New(Config{})
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

// With tracing enabled, otelgin opens a span for normal routes but the
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
