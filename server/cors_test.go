package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCORS_ReflectSkipAndPreflight(t *testing.T) {
	gin.SetMode(gin.TestMode)
	allow := func(_ *gin.Context, origin string) bool { return strings.HasSuffix(origin, ".fynd.com") }
	r := gin.New()
	r.Use(DynamicCORS(CORSOptions{AllowOrigin: allow, AllowHeaders: "content-type", SkipHeader: "X-Skip-Cors"}))
	r.GET("/qr", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	do := func(method, origin, skip string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/qr", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		if skip != "" {
			req.Header.Set("X-Skip-Cors", skip)
		}
		r.ServeHTTP(w, req)
		return w
	}

	// allowed origin reflected on the actual request
	if w := do(http.MethodGet, "https://x.fynd.com", ""); w.Header().Get("Access-Control-Allow-Origin") != "https://x.fynd.com" || w.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Errorf("allowed origin not reflected: %+v", w.Header())
	}
	// disallowed origin: no ACAO
	if w := do(http.MethodGet, "https://evil.com", ""); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("disallowed origin must not be reflected")
	}
	// preflight to the GET-only path is answered 204 with headers (engine-level → runs on no-route)
	if w := do(http.MethodOptions, "https://x.fynd.com", ""); w.Code != http.StatusNoContent || w.Header().Get("Access-Control-Allow-Headers") != "content-type" || w.Header().Get("Access-Control-Max-Age") != "86400" {
		t.Errorf("preflight = %d headers=%q maxage=%q", w.Code, w.Header().Get("Access-Control-Allow-Headers"), w.Header().Get("Access-Control-Max-Age"))
	}
	// The zero-value compatibility mode also terminates a disallowed preflight.
	if w := do(http.MethodOptions, "https://evil.com", ""); w.Code != http.StatusNoContent {
		t.Errorf("default disallowed preflight = %d, want 204", w.Code)
	}
	// X-Skip-Cors bypass
	if w := do(http.MethodGet, "https://x.fynd.com", "true"); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("X-Skip-Cors must bypass")
	}
}

func TestCORS_PassThroughDisallowedPreflight(t *testing.T) {
	gin.SetMode(gin.TestMode)
	allow := func(_ *gin.Context, origin string) bool { return strings.HasSuffix(origin, ".fynd.com") }
	r := gin.New()
	r.Use(DynamicCORS(CORSOptions{
		AllowOrigin:                    allow,
		AllowHeaders:                   "content-type",
		PassThroughDisallowedPreflight: true,
	}))
	r.OPTIONS("/qr", func(c *gin.Context) { c.String(http.StatusTeapot, "route handled") })

	do := func(origin string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodOptions, "/qr", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		r.ServeHTTP(w, req)
		return w
	}

	for _, origin := range []string{"", "https://evil.com"} {
		w := do(origin)
		if w.Code != http.StatusTeapot || w.Body.String() != "route handled" {
			t.Errorf("origin %q = (%d, %q), want route response", origin, w.Code, w.Body.String())
		}
		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("origin %q unexpectedly reflected as %q", origin, got)
		}
	}

	// An allowed preflight retains the normal CORS short-circuit even in the
	// pass-through mode.
	w := do("https://x.fynd.com")
	if w.Code != http.StatusNoContent {
		t.Fatalf("allowed preflight = %d, want 204", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://x.fynd.com" {
		t.Errorf("allowed preflight origin = %q", got)
	}
}

// TestConfig_CORS_MountedViaInit verifies the integration wiring: setting Config.CORS
// installs DynamicCORS engine-level in Init so a preflight to a platform route is
// answered (204 + reflected origin) through the fully-built server, and a nil
// Config.CORS mounts nothing.
func TestConfig_CORS_MountedViaInit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	os.Setenv("SERVER_TYPE", "platform")
	os.Setenv("DISABLE_RESPONSE_MIDDLEWARES", "true")
	defer os.Unsetenv("SERVER_TYPE")
	defer os.Unsetenv("DISABLE_RESPONSE_MIDDLEWARES")

	routers := map[ServerType]http.Handler{
		ServerTypePlatform: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
	}

	// With Config.CORS set: preflight is answered 204 with a reflected allowed origin.
	s := New(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		CORS: &CORSOptions{
			AllowOrigin:  func(_ *gin.Context, origin string) bool { return strings.HasSuffix(origin, ".fynd.com") },
			AllowHeaders: "content-type",
			SkipHeader:   "X-Skip-Cors",
		},
	})
	if err := s.Init(routers, nil, nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/anything", nil)
	req.Header.Set("Origin", "https://x.fynd.com")
	s.App.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("preflight via Init = %d, want 204", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "https://x.fynd.com" {
		t.Errorf("preflight should reflect the origin, got %q", w.Header().Get("Access-Control-Allow-Origin"))
	}

	// With nil Config.CORS: no CORS headers.
	s2 := New(Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := s2.Init(routers, nil, nil); err != nil {
		t.Fatalf("Init(nil CORS): %v", err)
	}
	w2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Origin", "https://x.fynd.com")
	s2.App.ServeHTTP(w2, req2)
	if w2.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("nil Config.CORS must not set CORS headers")
	}
}
