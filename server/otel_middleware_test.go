// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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
