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
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// EntryPointSpanAttribute marks a span that the ServiceEntryPointSampler force-sampled
// because it is this service's entry point. Matches traceclue's attribute name exactly
// so existing dashboards/queries keep working.
const EntryPointSpanAttribute = "traceclue.is_entry_point_span"

// AlwaysSampleEntryPointsEnv gates the ServiceEntryPointSampler. The platform (fik)
// already sets this to "true" fleet-wide for the Node/Python services; Go ignored it
// until this sampler existed.
const AlwaysSampleEntryPointsEnv = "TRACECLUE_ALWAYS_SAMPLE_SERVICE_ENTRY_POINTS"

// serviceEntryPointSampler ports traceclue's ServiceEntryPointSampler
// (traceclue/traces/sampler/service_entry_point.py and its tracecluenode JS twin,
// which are semantically identical).
//
// Rule: ALWAYS record-and-sample this service's ENTRY POINT span — the span that has
// no parent (a local root) or whose parent is REMOTE (the first span after an inbound
// request/message). Every other span (internal/downstream, i.e. a local parent) falls
// back to the env-configured sampler.
//
// Why: with OTEL_TRACES_SAMPLER=parentbased_traceidratio and ARG=0.25 — the platform
// default — a plain ratio sampler drops ~75% of locally-rooted requests entirely, so
// they are invisible in traces and RED metrics. This sampler keeps 100% of entry
// points while still thinning the interior of each trace at the configured ratio.
//
// NOTE (faithful to legacy): a REMOTE parent is force-sampled even when the upstream
// traceparent says sampled=0. That is deliberately what traceclue does — it guarantees
// each service can always account for the requests it received.
type serviceEntryPointSampler struct {
	fallback sdktrace.Sampler
}

// ShouldSample implements sdktrace.Sampler.
func (s serviceEntryPointSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(p.ParentContext)

	// Local root (no/invalid parent) OR first span after a remote parent → entry point.
	if !psc.IsValid() || psc.IsRemote() {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Attributes: []attribute.KeyValue{attribute.Bool(EntryPointSpanAttribute, true)},
			Tracestate: psc.TraceState(),
		}
	}

	// Internal / downstream span → whatever the env-configured sampler says.
	return s.fallback.ShouldSample(p)
}

// Description implements sdktrace.Sampler.
func (s serviceEntryPointSampler) Description() string {
	return fmt.Sprintf("ServiceEntryPointSampler(%s)", s.fallback.Description())
}

// alwaysSampleEntryPoints reports whether the platform asked for entry-point sampling.
func alwaysSampleEntryPoints() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv(AlwaysSampleEntryPointsEnv)), "true")
}

// wrapWithEntryPointSampler wraps base with the traceclue entry-point sampler when
// TRACECLUE_ALWAYS_SAMPLE_SERVICE_ENTRY_POINTS=true; otherwise base is returned as-is
// (zero behaviour change when the flag is unset).
func wrapWithEntryPointSampler(base sdktrace.Sampler) sdktrace.Sampler {
	if !alwaysSampleEntryPoints() {
		return base
	}
	return serviceEntryPointSampler{fallback: base}
}
