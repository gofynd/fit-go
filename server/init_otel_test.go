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
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
)

// Init must install OTelMiddleware (otelgin) in the default chain so every
// fit-go service gets per-request server spans with no per-service wiring (the
// Node OTel-express equivalent). The handler echoes the trace id from its
// request context, so a single request proves the middleware is in the chain AND
// that otelgin extracted + continued the inbound W3C traceparent.
func TestInit_InstallsOTelMiddlewareInDefaultChain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tracingtest.EnabledGlobal(t)
	t.Setenv("SERVER_TYPE", "internal") // single type → mounted at root "/"
	t.Setenv("SERVICE_NAME", "fit-test")

	inner := gin.New()
	inner.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, trace.SpanContextFromContext(c.Request.Context()).TraceID().String())
	})

	s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := s.Init(map[ServerType]http.Handler{ServerTypeInternal: inner}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}

	const inboundTrace = "11111111111111111111111111111111"
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set("traceparent", "00-"+inboundTrace+"-2222222222222222-01")
	rec := httptest.NewRecorder()
	s.engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("request through default chain: got %d, want 200", rec.Code)
	}
	// The handler's context must carry the inbound trace id — proving otelgin is
	// in the chain (server span created) and continued the upstream trace.
	if rec.Body.String() != inboundTrace {
		t.Fatalf("handler context trace id = %q, want %q — otelgin not wired or not extracting traceparent",
			rec.Body.String(), inboundTrace)
	}
}
