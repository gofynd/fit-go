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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

// fakeRT captures the request the wrapped transport forwards.
type fakeRT struct {
	got    *http.Request
	status int
}

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
