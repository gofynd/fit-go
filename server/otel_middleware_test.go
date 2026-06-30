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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
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
