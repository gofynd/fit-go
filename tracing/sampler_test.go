// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"strings"
	"testing"
)

// buildSampler must honor OTEL_TRACES_SAMPLER + the ratio from SampleRate, instead
// of the old behaviour that hardcoded SampleRate=1.0 and always sampled.
func TestBuildSampler(t *testing.T) {
	cases := []struct {
		name       string
		sampler    string
		rate       float64
		wantHas    []string
		wantHasNot []string
	}{
		{"empty -> parentbased ratio", "", 0.25, []string{"ParentBased", "TraceIDRatioBased"}, nil},
		{"platform config (parentbased_traceidratio 0.25)", "parentbased_traceidratio", 0.25, []string{"ParentBased", "TraceIDRatioBased"}, nil},
		{"traceidratio is NOT parent-based", "traceidratio", 0.5, []string{"TraceIDRatioBased"}, []string{"ParentBased"}},
		{"always_on", "always_on", 0.25, []string{"AlwaysOnSampler"}, nil},
		{"always_off", "always_off", 1.0, []string{"AlwaysOffSampler"}, nil},
		{"ratio>=1 collapses to always (old default preserved)", "parentbased_traceidratio", 1.0, []string{"AlwaysOnSampler"}, []string{"TraceIDRatioBased"}},
		{"unknown falls back to parentbased ratio", "bogus", 0.25, []string{"ParentBased", "TraceIDRatioBased"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := buildSampler(Options{Sampler: tc.sampler, SampleRate: tc.rate}).Description()
			for _, s := range tc.wantHas {
				if !strings.Contains(got, s) {
					t.Fatalf("sampler=%q rate=%v -> %q, want contains %q", tc.sampler, tc.rate, got, s)
				}
			}
			for _, s := range tc.wantHasNot {
				if strings.Contains(got, s) {
					t.Fatalf("sampler=%q rate=%v -> %q, want NOT contains %q", tc.sampler, tc.rate, got, s)
				}
			}
		})
	}
}

// DefaultOptions must read OTEL_TRACES_SAMPLER_ARG (was ignored -> always 1.0).
func TestSampleRateFromEnv(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	if got := sampleRateFromEnv(); got != 0.25 {
		t.Fatalf("sampleRateFromEnv = %v, want 0.25", got)
	}
	if got := DefaultOptions().SampleRate; got != 0.25 {
		t.Fatalf("DefaultOptions().SampleRate = %v, want 0.25", got)
	}
}

func TestSampleRateFromEnv_DefaultsWhenUnset(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
	if got := sampleRateFromEnv(); got != 1.0 {
		t.Fatalf("sampleRateFromEnv (unset) = %v, want 1.0", got)
	}
}
