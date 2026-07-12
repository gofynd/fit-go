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

package tracing

import (
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// neverSampler stands in for a ratio sampler that has decided "drop" — it lets us
// prove the entry-point rule overrides the configured sampler for entry points, and
// ONLY for entry points.
type neverSampler struct{}

func (neverSampler) ShouldSample(sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
}
func (neverSampler) Description() string { return "neverSampler" }

func parentCtx(t *testing.T, remote, sampled bool) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}
	flags := trace.TraceFlags(0)
	if sampled {
		flags = trace.FlagsSampled
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     remote,
	})
	if remote {
		return trace.ContextWithRemoteSpanContext(context.Background(), sc)
	}
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func hasEntryPointAttr(res sdktrace.SamplingResult) bool {
	for _, kv := range res.Attributes {
		if string(kv.Key) == EntryPointSpanAttribute && kv.Value.AsBool() {
			return true
		}
	}
	return false
}

// TestServiceEntryPointSampler_LocalRoot: a span with no parent is this service's
// entry point → always sampled, even though the fallback says drop. Without this,
// OTEL_TRACES_SAMPLER_ARG=0.25 makes ~75% of locally-rooted requests invisible.
func TestServiceEntryPointSampler_LocalRoot(t *testing.T) {
	s := serviceEntryPointSampler{fallback: neverSampler{}}

	res := s.ShouldSample(sdktrace.SamplingParameters{ParentContext: context.Background()})
	if res.Decision != sdktrace.RecordAndSample {
		t.Fatalf("local root decision = %v, want RecordAndSample (entry point must always be sampled)", res.Decision)
	}
	if !hasEntryPointAttr(res) {
		t.Errorf("missing %q attribute on the entry-point span", EntryPointSpanAttribute)
	}
}

// TestServiceEntryPointSampler_RemoteParent: the first span after an inbound
// request/message is also an entry point. Faithful to traceclue, this force-samples
// even when the upstream traceparent said sampled=0, so a service can always account
// for the requests it received.
func TestServiceEntryPointSampler_RemoteParent(t *testing.T) {
	s := serviceEntryPointSampler{fallback: neverSampler{}}

	for _, sampled := range []bool{true, false} {
		res := s.ShouldSample(sdktrace.SamplingParameters{ParentContext: parentCtx(t, true, sampled)})
		if res.Decision != sdktrace.RecordAndSample {
			t.Errorf("remote parent (upstream sampled=%v): decision = %v, want RecordAndSample",
				sampled, res.Decision)
		}
		if !hasEntryPointAttr(res) {
			t.Errorf("remote parent (upstream sampled=%v): missing %q attribute",
				sampled, EntryPointSpanAttribute)
		}
	}
}

// TestServiceEntryPointSampler_LocalParentDelegates: an INTERNAL span (local parent)
// is not an entry point, so the configured sampler decides. This is what keeps the
// interior of a trace thinned at the configured ratio.
func TestServiceEntryPointSampler_LocalParentDelegates(t *testing.T) {
	s := serviceEntryPointSampler{fallback: neverSampler{}}

	res := s.ShouldSample(sdktrace.SamplingParameters{ParentContext: parentCtx(t, false, true)})
	if res.Decision != sdktrace.Drop {
		t.Fatalf("local-parent (internal) span decision = %v, want the fallback's Drop", res.Decision)
	}
	if hasEntryPointAttr(res) {
		t.Error("internal span must NOT be tagged as an entry point")
	}
}

func (s serviceEntryPointSampler) descriptionForTest() string { return s.Description() }

// TestServiceEntryPointSampler_Description surfaces the wrapped sampler, as traceclue does.
func TestServiceEntryPointSampler_Description(t *testing.T) {
	s := serviceEntryPointSampler{fallback: neverSampler{}}
	if d := s.descriptionForTest(); !strings.Contains(d, "neverSampler") {
		t.Errorf("Description() = %q, want it to name the wrapped fallback", d)
	}
}

// TestBuildSampler_EntryPointGatedByEnv: the wrapper is applied only when the platform
// asks for it (fik sets this true fleet-wide). Unset ⇒ zero behaviour change.
func TestBuildSampler_EntryPointGatedByEnv(t *testing.T) {
	opts := Options{Sampler: "parentbased_traceidratio", SampleRate: 0.25}

	t.Setenv(AlwaysSampleEntryPointsEnv, "")
	if d := buildSampler(opts).Description(); strings.Contains(d, "ServiceEntryPointSampler") {
		t.Errorf("flag unset: sampler = %q, want no entry-point wrapping", d)
	}

	t.Setenv(AlwaysSampleEntryPointsEnv, "true")
	if d := buildSampler(opts).Description(); !strings.Contains(d, "ServiceEntryPointSampler") {
		t.Errorf("flag=true: sampler = %q, want the entry-point wrapper (fik sets this env)", d)
	}
}

// TestBuildBaseSampler_ZeroRateIsNeverSample pins the trap behind the fit.Init bug:
// a zero SampleRate collapses to NeverSample. fit.Init used to construct a bare
// Options{} (SampleRate 0), so any service booting through it produced NO
// locally-rooted traces. fit.Init now merges over DefaultOptions().
func TestBuildBaseSampler_ZeroRateIsNeverSample(t *testing.T) {
	s := buildBaseSampler(Options{}) // zero value: Sampler "", SampleRate 0.0
	res := s.ShouldSample(sdktrace.SamplingParameters{ParentContext: context.Background()})
	if res.Decision != sdktrace.Drop {
		t.Fatalf("zero-value Options decision = %v, want Drop — this is the trap fit.Init must avoid", res.Decision)
	}
}

// TestDefaultOptions_ReadsSamplerEnv: DefaultOptions must pick up the OTel-standard env
// the platform sets, so fit.Init's merge actually restores real sampling.
func TestDefaultOptions_ReadsSamplerEnv(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")

	opts := DefaultOptions()
	if opts.Sampler != "parentbased_traceidratio" {
		t.Errorf("Sampler = %q, want parentbased_traceidratio", opts.Sampler)
	}
	if opts.SampleRate != 0.25 {
		t.Errorf("SampleRate = %v, want 0.25", opts.SampleRate)
	}
}
