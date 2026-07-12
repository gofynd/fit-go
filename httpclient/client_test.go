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

package httpclient

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/metrics"
	"github.com/gofynd/fit-go/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type markerPropagator struct{}

type contextCaptureHandler struct{ ctx context.Context }

func (*contextCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *contextCaptureHandler) Handle(ctx context.Context, _ slog.Record) error {
	h.ctx = ctx
	return nil
}
func (h *contextCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *contextCaptureHandler) WithGroup(string) slog.Handler      { return h }

func (markerPropagator) Inject(_ context.Context, carrier propagation.TextMapCarrier) {
	carrier.Set("x-fit-propagator", "used")
}
func (markerPropagator) Extract(ctx context.Context, _ propagation.TextMapCarrier) context.Context {
	return ctx
}
func (markerPropagator) Fields() []string { return []string{"x-fit-propagator"} }

// fakeRT captures the request the wrapped transport forwards.
type fakeRT struct {
	got    *http.Request
	status int
}

type errorRT struct{ err error }

func (f errorRT) RoundTrip(*http.Request) (*http.Response, error) { return nil, f.err }

func (f *fakeRT) RoundTrip(r *http.Request) (*http.Response, error) {
	f.got = r
	st := f.status
	if st == 0 {
		st = http.StatusOK
	}
	return &http.Response{StatusCode: st, Body: http.NoBody, Header: http.Header{}}, nil
}

func do(t *testing.T, rt http.RoundTripper, req *http.Request) {
	t.Helper()
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func TestRoundTrip_GeneratesRequestID(t *testing.T) {
	f := &fakeRT{}
	rt := WrapTransport(f)
	do(t, rt, httptest.NewRequest(http.MethodGet, "http://svc/internal/x", nil))

	if got := f.got.Header.Get(requestIDHeader); got == "" {
		t.Fatal("expected a generated x-request-id header")
	}
}

func TestRoundTrip_InitializesNilHeaders(t *testing.T) {
	f := &fakeRT{}
	req := httptest.NewRequest(http.MethodGet, "http://svc/internal/x", nil)
	req.Header = nil
	do(t, WrapTransport(f), req)
	if got := f.got.Header.Get(requestIDHeader); got == "" {
		t.Fatal("expected request id with a nil caller header map")
	}
}

func TestRoundTrip_PreservesRequestID(t *testing.T) {
	f := &fakeRT{}
	rt := WrapTransport(f)
	req := httptest.NewRequest(http.MethodGet, "http://svc/x", nil)
	req.Header.Set(requestIDHeader, "caller-123")
	do(t, rt, req)

	if got := f.got.Header.Get(requestIDHeader); got != "caller-123" {
		t.Fatalf("request-id: want caller-123 (forwarded), got %q", got)
	}
}

func TestRoundTrip_DoesNotMutateCallerRequest(t *testing.T) {
	f := &fakeRT{}
	rt := WrapTransport(f)
	req := httptest.NewRequest(http.MethodGet, "http://svc/x", nil)
	do(t, rt, req)

	// The contract: the transport must clone, not mutate the caller's request.
	if req.Header.Get(requestIDHeader) != "" {
		t.Fatal("caller request was mutated; transport must clone before adding headers")
	}
}

func TestRoundTrip_NoTraceparentWhenDisabled(t *testing.T) {
	f := &fakeRT{}
	rt := WrapTransport(f)
	do(t, rt, httptest.NewRequest(http.MethodGet, "http://svc/x", nil))

	if tracing.Global().IsEnabled() {
		t.Skip("tracing globally enabled in this run; disabled-path assertion N/A")
	}
	if got := f.got.Header.Get(traceparentHeader); got != "" {
		t.Fatalf("no traceparent expected when tracing disabled, got %q", got)
	}
}

// With tracing enabled, the client injects a traceparent header (outbound
// propagation).
func TestRoundTrip_InjectsTraceparentWhenEnabled(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	f := &fakeRT{}
	rt := WrapTransport(f)
	do(t, rt, httptest.NewRequest(http.MethodGet, "http://svc/x", nil))

	if got := f.got.Header.Get(traceparentHeader); got == "" {
		t.Fatal("expected traceparent header injected when tracing enabled")
	}
}

func TestRoundTrip_InjectsGlobalTraceStateAndBaggage(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	state, err := trace.ParseTraceState("vendor=value")
	if err != nil {
		t.Fatalf("ParseTraceState: %v", err)
	}
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
		TraceState: state,
		Remote:     true,
	})
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatalf("NewMember: %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("New baggage: %v", err)
	}
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), parent)
	ctx = baggage.ContextWithBaggage(ctx, bag)

	f := &fakeRT{}
	req := httptest.NewRequest(http.MethodGet, "http://svc/x", nil).WithContext(ctx)
	do(t, WrapTransport(f), req)

	if got := f.got.Header.Get("tracestate"); got != "vendor=value" {
		t.Fatalf("tracestate = %q, want vendor=value", got)
	}
	if got := f.got.Header.Get("baggage"); got != "tenant=acme" {
		t.Fatalf("baggage = %q, want tenant=acme", got)
	}
}

func TestRoundTrip_ReplacesStalePropagationHeadersCaseInsensitively(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	f := &fakeRT{}
	req := httptest.NewRequest(http.MethodGet, "http://svc/x", nil)
	req.Header = http.Header{
		"Traceparent":   {"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"},
		"traceparent":   {"00-cccccccccccccccccccccccccccccccc-dddddddddddddddd-01"},
		"TRACESTATE":    {"stale=state"},
		"bAgGaGe":       {"tenant=stale"},
		"X-Unrelated":   {"keep", "both"},
		"Authorization": {"Bearer unchanged"},
	}

	do(t, WrapTransport(f), req)

	traceparents := headerValuesEqualFold(f.got.Header, "traceparent")
	if len(traceparents) != 1 {
		t.Fatalf("traceparent values = %v, want exactly one freshly injected value", traceparents)
	}
	if strings.Contains(traceparents[0], "aaaaaaaa") || strings.Contains(traceparents[0], "cccccccc") {
		t.Fatalf("stale traceparent was forwarded: %q", traceparents[0])
	}
	if values := headerValuesEqualFold(f.got.Header, "tracestate"); len(values) != 0 {
		t.Fatalf("stale tracestate was forwarded: %v", values)
	}
	if values := headerValuesEqualFold(f.got.Header, "baggage"); len(values) != 0 {
		t.Fatalf("stale baggage was forwarded: %v", values)
	}
	if got := f.got.Header.Values("X-Unrelated"); len(got) != 2 || got[0] != "keep" || got[1] != "both" {
		t.Fatalf("unrelated header changed: %v", got)
	}
	if got := f.got.Header.Get("Authorization"); got != "Bearer unchanged" {
		t.Fatalf("authorization header changed: %q", got)
	}
	if got := headerValuesEqualFold(req.Header, "tracestate"); len(got) != 1 || got[0] != "stale=state" {
		t.Fatalf("caller request was mutated, tracestate = %v", got)
	}
}

func TestRoundTrip_UsesConfiguredGlobalPropagator(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(markerPropagator{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	f := &fakeRT{}
	req := httptest.NewRequest(http.MethodGet, "http://svc/x", nil)
	req.Header = http.Header{
		"X-Fit-Propagator": {"stale-one"},
		"x-fit-propagator": {"stale-two"},
		"Traceparent":      {"stale-standard-field"},
		"X-Unrelated":      {"keep"},
	}
	do(t, WrapTransport(f), req)
	if got := headerValuesEqualFold(f.got.Header, "x-fit-propagator"); len(got) != 1 || got[0] != "used" {
		t.Fatalf("custom propagator values = %v, want one fresh value", got)
	}
	if got := headerValuesEqualFold(f.got.Header, "traceparent"); len(got) != 0 {
		t.Fatalf("standard stale propagation field survived custom injection: %v", got)
	}
	if got := f.got.Header.Get("X-Unrelated"); got != "keep" {
		t.Fatalf("unrelated header changed: %q", got)
	}
}

func TestRoundTrip_NoopPropagatorRemovesStalePropagation(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	f := &fakeRT{}
	req := httptest.NewRequest(http.MethodGet, "http://svc/x", nil)
	req.Header = http.Header{
		"TRACEPARENT":    {"stale-parent"},
		"TraceState":     {"stale=state"},
		"BAGGAGE":        {"tenant=stale"},
		"b3":             {"stale-b3"},
		"X-B3-TraceId":   {"stale-trace"},
		"X-B3-SpanId":    {"stale-span"},
		"Uber-Trace-Id":  {"stale-jaeger"},
		"Uberctx-Tenant": {"stale-baggage"},
		"X-Unrelated":    {"keep"},
	}
	do(t, WrapTransport(f), req)

	for _, field := range []string{
		"traceparent", "tracestate", "baggage", "b3",
		"x-b3-traceid", "x-b3-spanid", "uber-trace-id", "uberctx-tenant",
	} {
		if got := headerValuesEqualFold(f.got.Header, field); len(got) != 0 {
			t.Fatalf("no-op propagator forwarded stale %s: %v", field, got)
		}
	}
	if got := f.got.Header.Get("X-Unrelated"); got != "keep" {
		t.Fatalf("unrelated header changed: %q", got)
	}
}

func headerValuesEqualFold(header http.Header, name string) []string {
	var values []string
	for key, current := range header {
		if strings.EqualFold(key, name) {
			values = append(values, current...)
		}
	}
	return values
}

func TestNewHTTPClient_ClonesDefaultTransport(t *testing.T) {
	want := http.DefaultTransport.(*http.Transport)
	client := NewHTTPClient()
	wrapper, ok := client.Transport.(*transport)
	if !ok {
		t.Fatalf("transport = %T, want *transport", client.Transport)
	}
	got, ok := wrapper.base.(*http.Transport)
	if !ok {
		t.Fatalf("base transport = %T, want *http.Transport", wrapper.base)
	}
	if got == want {
		t.Fatal("NewHTTPClient must clone, not mutate, http.DefaultTransport")
	}
	if got.DialContext == nil || got.MaxIdleConns != want.MaxIdleConns ||
		got.IdleConnTimeout != want.IdleConnTimeout ||
		got.TLSHandshakeTimeout != want.TLSHandshakeTimeout ||
		got.ExpectContinueTimeout != want.ExpectContinueTimeout ||
		got.ForceAttemptHTTP2 != want.ForceAttemptHTTP2 {
		t.Fatal("cloned transport did not preserve net/http default tuning")
	}
}

func TestHTTPClientStatusIsError(t *testing.T) {
	tests := map[int]bool{0: true, 99: true, 100: false, 204: false, 399: false, 400: true, 503: true}
	for status, want := range tests {
		if got := httpClientStatusIsError(status); got != want {
			t.Errorf("httpClientStatusIsError(%d) = %t, want %t", status, got, want)
		}
	}
}

func TestRoundTrip_SanitizesTransportErrorInLogsAndSpans(t *testing.T) {
	const rawURL = "https://user:password@provider.example/send?email=jane@example.com&token=api-secret"
	transportErr := &url.Error{Op: "post", URL: rawURL, Err: errors.New("provider echoed api-secret jane@example.com")}
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName: "http-privacy", Enabled: &enabled, Sampler: "always_on",
		SpanExporter: exporter, UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	restore := tracing.SetGlobal(tracer)
	defer restore()
	defer tracer.Shutdown(context.Background())

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	req, err := http.NewRequest(http.MethodPost, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	_, gotErr := WrapTransport(errorRT{err: transportErr}, WithLogger(logger)).RoundTrip(req)
	if !errors.Is(gotErr, transportErr) {
		t.Fatalf("RoundTrip error = %v, want original", gotErr)
	}

	var observed tracetest.SpanStub
	found := false
	for _, span := range exporter.GetSpans() {
		if span.Name == "HTTP POST" {
			observed = span
			found = true
			break
		}
	}
	if !found {
		t.Fatal("HTTP client span was not exported")
	}
	serialized := logs.String() + observed.Status.Description
	for _, secret := range []string{"password", "jane@example.com", "api-secret", "provider echoed"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("telemetry leaked %q: %s", secret, serialized)
		}
	}
}

// forceProxyDomainsContains matches legacy fit/axios exact hostname membership:
// subdomains, parent domains and substring look-alikes must NOT match.
func TestForceProxyDomainsContains(t *testing.T) {
	const force = "api.internal.svc,partner.example"

	for _, h := range []string{"api.internal.svc", "API.Internal.SVC", "partner.example"} {
		if !forceProxyDomainsContains(force, h) {
			t.Errorf("expected %q to match (exact, case-insensitive host)", h)
		}
	}
	for _, h := range []string{
		"deep.api.internal.svc",              // subdomain — legacy is exact only
		"internal.svc",                       // parent domain
		"evil-api.internal.svc.attacker.net", // forced domain as a left substring
		"notapi.internal.svc",                // no dot boundary
		"api.internal.svc.attacker.com",      // forced domain as a non-suffix substring
		"other.example",
	} {
		if forceProxyDomainsContains(force, h) {
			t.Errorf("expected %q NOT to match (exact membership only)", h)
		}
	}
}

func TestProxyFromEnvWithForce(t *testing.T) {
	t.Setenv("FORCE_PROXY_DOMAINS", "api.internal.svc,partner.example")
	t.Setenv("HTTPS_PROXY", "http://proxy.local:3128")
	t.Setenv("HTTP_PROXY", "")
	t.Setenv("NO_PROXY", "")

	// https + exact forced host → forced through HTTPS_PROXY (force path reads
	// env live, so this is independent of http.ProxyFromEnvironment's caching).
	u, err := ProxyFromEnvWithForce(httptest.NewRequest(http.MethodGet, "https://api.internal.svc/x", nil))
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	if u == nil || u.Host != "proxy.local:3128" {
		t.Fatalf("forced https host should use HTTPS_PROXY, got %v", u)
	}

	// http + forced host → the force block is HTTPS-only, so it must NOT be force
	// proxied (legacy parity: gated on url.startsWith("https://")). http with an
	// empty HTTP_PROXY resolves to no proxy.
	u2, err := ProxyFromEnvWithForce(httptest.NewRequest(http.MethodGet, "http://api.internal.svc/x", nil))
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	if u2 != nil && u2.Host == "proxy.local:3128" {
		t.Fatalf("http forced host must NOT be force-proxied (HTTPS-only), got %v", u2)
	}
}

// WithMetrics records method/host/status/duration for each outbound call.
func TestRoundTrip_RecordsMetrics(t *testing.T) {
	f := &fakeRT{status: 503}
	var gotMethod, gotHost string
	var gotStatus int
	called := false
	rt := WrapTransport(f, WithMetrics(func(m, h string, s int, _ time.Duration) {
		gotMethod, gotHost, gotStatus, called = m, h, s, true
	}))
	do(t, rt, httptest.NewRequest(http.MethodGet, "http://svc.internal:8080/x", nil))

	if !called {
		t.Fatal("metrics recorder was not called")
	}
	if gotMethod != "GET" || gotHost != "svc.internal" || gotStatus != 503 {
		t.Fatalf("metrics = %q %q %d, want GET svc.internal 503", gotMethod, gotHost, gotStatus)
	}
}

func TestWrapTransport_IdempotentAndPreservesOptions(t *testing.T) {
	base := &fakeRT{}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	metricCalls := 0

	first := WrapTransport(base, WithLogger(logger))
	second := WrapTransport(first, WithMetrics(func(string, string, int, time.Duration) {
		metricCalls++
	}))
	wrapper, ok := second.(*transport)
	if !ok {
		t.Fatalf("transport = %T, want *transport", second)
	}
	if wrapper.base != base {
		t.Fatalf("re-wrapped base = %T, want original %T", wrapper.base, base)
	}
	if wrapper.logger != logger {
		t.Fatal("re-wrapping discarded the existing logger option")
	}

	do(t, second, httptest.NewRequest(http.MethodGet, "http://svc.internal/path", nil))
	if metricCalls != 1 {
		t.Fatalf("metric calls = %d, want 1", metricCalls)
	}
	if !strings.Contains(logs.String(), "httpclient: request") {
		t.Fatalf("existing logger option did not run: %s", logs.String())
	}
}

func TestKnownOTelHTTPTransportType(t *testing.T) {
	if !isKnownOTelHTTPTransportType(
		"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp",
		"Transport",
	) {
		t.Fatal("official otelhttp.Transport was not recognized")
	}
	for _, test := range []struct{ packagePath, name string }{
		{"example.com/custom/otelhttp", "Transport"},
		{"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp", "Other"},
	} {
		if isKnownOTelHTTPTransportType(test.packagePath, test.name) {
			t.Fatalf("unrelated transport %s.%s was recognized", test.packagePath, test.name)
		}
	}
}

func TestRoundTrip_PassesRequestSpanContextToLogger(t *testing.T) {
	tracer := tracingtest.EnabledGlobal(t)
	ctx, parent := tracer.StartSpan(context.Background(), "request", tracing.SpanKindServer)
	defer parent.End()
	handler := &contextCaptureHandler{}
	req := httptest.NewRequest(http.MethodGet, "http://svc.internal/x", nil).WithContext(ctx)
	do(t, WrapTransport(&fakeRT{}, WithLogger(slog.New(handler))), req)

	logged := trace.SpanContextFromContext(handler.ctx)
	if !logged.IsValid() || logged.TraceID().String() != parent.TraceID() {
		t.Fatalf("logger context = %v, want trace %s", logged, parent.TraceID())
	}
}

func TestRoundTrip_UsesProcessDefaultMetrics(t *testing.T) {
	registry, err := metrics.New(metrics.Options{
		HTTPClientEnabled:  true,
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	defer registry.Shutdown()
	restore := metrics.SetDefault(registry)
	defer restore()

	do(t, WrapTransport(&fakeRT{status: 201}), httptest.NewRequest(http.MethodPost, "http://svc.internal:8080/x", nil))
	output := registry.GetMetricsOutput()
	if !strings.Contains(output, `host="svc.internal"`) || !strings.Contains(output, `status_code="201"`) {
		t.Fatalf("default metrics were not recorded:\n%s", output)
	}
}

func TestRoundTrip_ExplicitNilMetricsDisablesProcessDefault(t *testing.T) {
	registry, err := metrics.New(metrics.Options{
		HTTPClientEnabled:  true,
		PrometheusRegistry: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	defer registry.Shutdown()
	restore := metrics.SetDefault(registry)
	defer restore()

	do(t, WrapTransport(&fakeRT{}, WithMetrics(nil)), httptest.NewRequest(http.MethodGet, "http://svc/x", nil))
	if output := registry.GetMetricsOutput(); output != "" {
		t.Fatalf("WithMetrics(nil) should disable default metrics, got:\n%s", output)
	}
}
