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

package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"
)

func newTestSlog(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	lg, err := New(Options{Level: "info", Env: "production", Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return slog.New(NewSlogHandler(lg)), &buf
}

// A plain log/slog call must use the fit logger's selected JSON format.
func TestSlogHandler_RoutesThroughFitLogger(t *testing.T) {
	sl, buf := newTestSlog(t)
	sl.Info("via slog", "key", "val")
	out := buf.String()
	for _, want := range []string{"via slog", `"info"`, `"key"`, "val"} {
		if !strings.Contains(out, want) {
			t.Fatalf("slog output missing %q; got: %s", want, out)
		}
	}
}

// slog.*Context carries the OTel span's trace id into the fit log line.
func TestSlogHandler_CarriesTraceFromContext(t *testing.T) {
	sl, buf := newTestSlog(t)
	tid, _ := oteltrace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	sid, _ := oteltrace.SpanIDFromHex("0123456789abcdef")
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: tid, SpanID: sid})
	ctx := oteltrace.ContextWithSpanContext(context.Background(), sc)

	sl.InfoContext(ctx, "with span")
	if !strings.Contains(buf.String(), "0123456789abcdef0123456789abcdef") {
		t.Fatalf("missing trace_id from slog context: %s", buf.String())
	}
}

// Below-threshold records are dropped.
func TestSlogHandler_LevelFilter(t *testing.T) {
	sl, buf := newTestSlog(t)
	sl.Debug("dropped")
	if strings.Contains(buf.String(), "dropped") {
		t.Fatalf("debug should be filtered at info level: %s", buf.String())
	}
}

// WithAttrs/WithGroup are honored, group prefixing nested keys.
func TestSlogHandler_WithAttrsAndGroup(t *testing.T) {
	sl, buf := newTestSlog(t)
	sl.With("svc", "x").WithGroup("g").Info("msg", "k", 1)
	out := buf.String()
	if !strings.Contains(out, `"svc"`) {
		t.Fatalf("WithAttrs key missing: %s", out)
	}
	if !strings.Contains(out, `"g.k"`) {
		t.Fatalf("WithGroup-prefixed key missing: %s", out)
	}
}

func TestSetAsDefaultSlogRestoresBaselineAfterOutOfOrderOwners(t *testing.T) {
	baseline := slog.Default()
	first, err := New(Options{Level: "info", Env: "production", Output: &bytes.Buffer{}})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	second, err := New(Options{Level: "info", Env: "production", Output: &bytes.Buffer{}})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	restoreFirst := SetAsDefaultSlog(first)
	secondInstalled := SetAsDefaultSlog(second)
	current := slog.Default()

	restoreFirst()
	if slog.Default() != current {
		t.Fatal("restoring the older owner clobbered the newer slog default")
	}
	secondInstalled()
	if slog.Default() != baseline {
		t.Fatal("newer owner restored the inactive older logger instead of the baseline")
	}
}

func TestSetAsDefaultSlogDoesNotClobberExternalReplacement(t *testing.T) {
	baseline := slog.Default()
	t.Cleanup(func() { slog.SetDefault(baseline) })
	logger, err := New(Options{Level: "info", Env: "production", Output: &bytes.Buffer{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	restore := SetAsDefaultSlog(logger)
	external := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	slog.SetDefault(external)
	restore()
	if slog.Default() != external {
		t.Fatal("restore clobbered an independently installed slog default")
	}
}

func TestSetAsDefaultSlogStartsNewChainAfterExternalReplacement(t *testing.T) {
	baseline := slog.Default()
	t.Cleanup(func() { slog.SetDefault(baseline) })
	first, err := New(Options{Level: "info", Env: "production", Output: &bytes.Buffer{}})
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	second, err := New(Options{Level: "info", Env: "production", Output: &bytes.Buffer{}})
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	restoreFirst := SetAsDefaultSlog(first)
	external := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	slog.SetDefault(external)
	restoreSecond := SetAsDefaultSlog(second)

	restoreSecond()
	if slog.Default() != external {
		t.Fatal("new owner revived an obsolete fit logger instead of the external baseline")
	}
	restoreFirst()
	if slog.Default() != external {
		t.Fatal("obsolete owner clobbered the external baseline")
	}
}
