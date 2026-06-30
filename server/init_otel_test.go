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
	"github.com/gofynd/fit-go/internal/tracingtest"
)

// Init must install OTelMiddleware in the default chain so every fit-go service
// gets per-request server spans with no per-service wiring (the Node OTel-express
// equivalent). A normal request must serve cleanly; when tracing is enabled the
// response carries a `traceparent` header (the middleware's observable signal),
// proving the middleware is in the chain.
func TestInit_InstallsOTelMiddlewareInDefaultChain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tracingtest.EnabledGlobal(t)
	t.Setenv("SERVER_TYPE", "internal") // single type → mounted at root "/"
	t.Setenv("SERVICE_NAME", "fit-test")

	inner := gin.New()
	inner.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	s := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := s.Init(map[ServerType]http.Handler{ServerTypeInternal: inner}, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}

	rec := httptest.NewRecorder()
	s.engine.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ping", nil))

	// The full default chain (incl. the new OTel middleware) must serve normally.
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("request through default chain: got %d %q, want 200 \"ok\"", rec.Code, rec.Body.String())
	}

	// OTelMiddleware sets a traceparent response header when tracing is enabled —
	// its proof-of-presence in the default chain. The enabled tracer is installed
	// for this test (enabledGlobalTracer), so this strong assertion always runs.
	if rec.Header().Get("traceparent") == "" {
		t.Fatal("tracing enabled but no traceparent header — OTelMiddleware not in the default chain")
	}
}
