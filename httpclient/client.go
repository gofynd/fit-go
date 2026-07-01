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

// Package httpclient is the Go equivalent of Node fit's `fit/axios`: a pre-wired
// outbound HTTP client that propagates distributed traces, forwards/generates a
// request id, logs requests/responses safely, and honors fit's proxy convention
// (HTTPS_PROXY + FORCE_PROXY_DOMAINS). All of it is implemented as an
// http.RoundTripper so it composes with any *http.Client.
//
// Tracing is gated on tracing.IsEnabled() and is a no-op when off. Logging is
// opt-in (WithLogger). Nothing here ever logs request/response bodies, query
// strings, or headers — only method, scheme+host+path, status, duration and the
// request id (platform "no PII in logs/traces" rule).
package httpclient

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gofynd/fit-go/redact"
	"github.com/gofynd/fit-go/tracing"
)

const (
	requestIDHeader   = "x-request-id"
	traceparentHeader = "traceparent"
)

// Option configures the instrumented transport.
type Option func(*transport)

// WithLogger attaches a slog logger that records one line per outbound request
// (safe fields only). When nil (default) no request logging is emitted.
func WithLogger(l *slog.Logger) Option { return func(t *transport) { t.logger = l } }

// MetricsRecorder records one outbound HTTP call's metrics. status is the
// numeric HTTP status (0 on transport error). Fields match
// metrics.HTTPClientMetrics so callers can forward directly to the metrics
// registry without this package importing it.
type MetricsRecorder func(method, host string, status int, duration time.Duration)

// WithMetrics records per-request outbound metrics (method/host/status/duration).
// When nil (default) no metrics are recorded.
func WithMetrics(rec MetricsRecorder) Option { return func(t *transport) { t.metrics = rec } }

// WrapTransport wraps base with trace propagation, request-id forwarding and
// optional logging. A nil base uses http.DefaultTransport. Use this to instrument
// an existing/custom transport; use NewHTTPClient for a ready-to-use client.
func WrapTransport(base http.RoundTripper, opts ...Option) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	t := &transport{base: base}
	for _, o := range opts {
		o(t)
	}
	return t
}

// NewHTTPClient returns an *http.Client whose transport adds trace propagation,
// request-id, optional logging, and fit's proxy behaviour (FORCE_PROXY_DOMAINS
// then HTTP(S)_PROXY/NO_PROXY).
func NewHTTPClient(opts ...Option) *http.Client {
	base := &http.Transport{Proxy: ProxyFromEnvWithForce}
	return &http.Client{Transport: WrapTransport(base, opts...)}
}

// NewHTTPClientWithTimeout is NewHTTPClient with a request timeout — the common
// drop-in for service clients that previously used &http.Client{Timeout: d}.
func NewHTTPClientWithTimeout(timeout time.Duration, opts ...Option) *http.Client {
	c := NewHTTPClient(opts...)
	c.Timeout = timeout
	return c
}

type transport struct {
	base    http.RoundTripper
	logger  *slog.Logger
	metrics MetricsRecorder
}

// RoundTrip clones the request (per the RoundTripper contract — must not mutate
// the caller's request), ensures a request id, starts a client span + injects
// traceparent when tracing is enabled, performs the call, then records span
// status and an optional safe log line.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())

	reqID := req.Header.Get(requestIDHeader)
	if reqID == "" {
		reqID = newRequestID()
		if reqID != "" {
			req.Header.Set(requestIDHeader, reqID)
		}
	}

	// Safe URL for spans/logs: scheme+host+path only (no query string / userinfo).
	safeURL := redact.SafeURL(req.URL)

	// NOTE: this client span is intentionally hand-rolled rather than delegated to
	// otelhttp (unlike redis/postgres/gin, which use redisotel/otelpgx/otelgin).
	// otelhttp records url.full = req.URL.String() — the FULL URL including the
	// query string (it strips only userinfo) — and exposes no option to redact it.
	// Query params in this platform routinely carry PII (emails/tokens), so
	// otelhttp would violate the "no PII in traces" rule. We keep this PII-safe
	// scheme+host+path span; the wrapper exists for the fit/axios parity behaviours
	// (proxy, x-request-id, safe logging) regardless. Do not swap to otelhttp.
	var span *tracing.Span
	if tracer := tracing.Global(); tracer != nil && tracer.IsEnabled() {
		ctx, s := tracer.StartSpan(req.Context(), "HTTP "+req.Method, tracing.SpanKindClient)
		span = s
		req = req.WithContext(ctx)
		if span.TraceID() != "" && span.SpanID() != "" {
			req.Header.Set(traceparentHeader, tracing.FormatTraceparent(span.TraceID(), span.SpanID(), span.IsSampled()))
		}
		span.SetAttributes(map[string]any{
			"http.method":     req.Method,
			"http.url":        safeURL,
			"http.host":       req.URL.Host,
			"http.request_id": reqID,
		})
	}

	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	duration := time.Since(start)

	status := 0
	if resp != nil {
		status = resp.StatusCode
	}

	if span != nil {
		span.SetAttribute("http.status_code", status)
		switch {
		case err != nil:
			span.SetStatus(tracing.StatusError, err.Error())
		case status >= http.StatusInternalServerError:
			span.SetStatus(tracing.StatusError, http.StatusText(status))
		default:
			span.SetStatus(tracing.StatusOK, "")
		}
		span.End()
	}

	if t.logger != nil {
		if err != nil {
			t.logger.Error("httpclient: request failed",
				"method", req.Method, "url", safeURL,
				"request_id", reqID, "duration_ms", duration.Milliseconds(),
				"error", err.Error())
		} else {
			t.logger.Info("httpclient: request",
				"method", req.Method, "url", safeURL, "status", status,
				"request_id", reqID, "duration_ms", duration.Milliseconds())
		}
	}

	if t.metrics != nil {
		t.metrics(req.Method, req.URL.Host, status, duration)
	}

	return resp, err
}

// newRequestID returns a random 128-bit hex id (matches fit/axios's per-request
// UUID purpose without adding a uuid dependency). Empty on the (unreachable)
// crypto/rand failure, in which case no header is set.
func newRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// ProxyFromEnvWithForce mirrors legacy fit/axios proxy handling
// (services/sentinel/node_modules/fit/dist/modules/axios/index.js): for an
// https request whose hostname is an EXACT member of the comma-separated
// FORCE_PROXY_DOMAINS, the request is forced through HTTPS_PROXY (bypassing
// NO_PROXY). Every other request falls back to the standard HTTP(S)_PROXY /
// NO_PROXY behaviour. Two details are faithful to fit/axios and deliberate:
//   - HTTPS-only: the force block runs only when the URL scheme is https
//     (legacy gates on `HTTPS_PROXY && url.startsWith("https://")`).
//   - Exact hostname membership, NOT subdomain-suffix or substring
//     (legacy uses `FORCE_PROXY_DOMAINS.split(",").includes(hostname)`).
//
// If forcing http targets or subdomains is ever wanted, add it as an explicit
// opt-in option rather than changing this default — it would diverge from fit.js.
func ProxyFromEnvWithForce(req *http.Request) (*url.URL, error) {
	if req.URL.Scheme == "https" {
		if force := os.Getenv("FORCE_PROXY_DOMAINS"); force != "" {
			proxyRaw := firstNonEmpty(os.Getenv("HTTPS_PROXY"), os.Getenv("https_proxy"))
			if proxyRaw != "" && forceProxyDomainsContains(force, req.URL.Hostname()) {
				if u, err := url.Parse(proxyRaw); err == nil {
					return u, nil
				}
			}
		}
	}
	return http.ProxyFromEnvironment(req)
}

// forceProxyDomainsContains reports whether hostname is an exact member of the
// comma-separated domain list, matching legacy fit/axios
// `FORCE_PROXY_DOMAINS.split(",").includes(hostname)`. The hostname is
// lower-cased to mirror JS `new URL(url).hostname` (the WHATWG URL parser
// lower-cases the host); list entries are compared verbatim, as fit/axios does
// (no trimming, no subdomain matching).
func forceProxyDomainsContains(force, hostname string) bool {
	hostname = strings.ToLower(hostname)
	for _, d := range strings.Split(force, ",") {
		if d == hostname {
			return true
		}
	}
	return false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
