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
	"os"
	"testing"
)

// TestOTELSDKDisabled_KillsTracing pins the OTel-standard kill switch. traceclue and
// pyfit honour OTEL_SDK_DISABLED; fit-go ignored it entirely, so an operator disabling
// telemetry fleet-wide via the standard env had NO effect on Go services.
//
// It must win over BOTH TRACING_ENABLED and an explicit Options.Enabled — otherwise it
// is not a kill switch.
func TestOTELSDKDisabled_KillsTracing(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	t.Setenv("OTEL_SDK_DISABLED", "true")

	enabled := true
	tr, err := New(context.Background(), Options{ServiceName: "svc", Enabled: &enabled})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if tr.IsEnabled() {
		t.Fatal("OTEL_SDK_DISABLED=true did not disable tracing — it must override both " +
			"TRACING_ENABLED and an explicit Options.Enabled")
	}
}

// TestOTELSDKDisabled_UnsetLeavesTracingOn: the kill switch must not change behaviour
// when it is absent.
func TestOTELSDKDisabled_UnsetLeavesTracingOn(t *testing.T) {
	t.Setenv("TRACING_ENABLED", "true")
	os.Unsetenv("OTEL_SDK_DISABLED")

	tr, err := New(context.Background(), Options{ServiceName: "svc"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !tr.IsEnabled() {
		t.Fatal("tracing should be enabled when OTEL_SDK_DISABLED is unset")
	}
}

func TestPyfitActivationModeEnablesWhenSDKDisableEnvIsAbsent(t *testing.T) {
	os.Unsetenv("OTEL_SDK_DISABLED")
	t.Setenv("TRACING_ENABLED", "")
	if !tracingEnabled(Options{ActivationMode: "pyfit"}) {
		t.Fatal("pyfit compatibility mode should initialize when OTEL_SDK_DISABLED is absent")
	}
}

func TestPyfitActivationModePreservesLegacyPresenceSemantics(t *testing.T) {
	// pyfit historically treated any configured value, including the string
	// "false", as disabled. Keep this behavior opt-in and isolated from the
	// OTel-standard default mode.
	t.Setenv("OTEL_SDK_DISABLED", "false")
	if tracingEnabled(Options{ActivationMode: "pyfit"}) {
		t.Fatal("pyfit compatibility mode should preserve legacy env-presence behavior")
	}
	if !tracingEnabled(Options{ActivationMode: "explicit", Enabled: boolPointer(true)}) {
		t.Fatal("standard explicit mode should not treat OTEL_SDK_DISABLED=false as disabled")
	}
}

func TestPyfitActivationModeTreatsEmptySDKDisableAsEnabled(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "")
	if !tracingEnabled(Options{ActivationMode: "pyfit"}) {
		t.Fatal("pyfit compatibility mode should treat an empty OTEL_SDK_DISABLED as enabled")
	}
}

func boolPointer(value bool) *bool { return &value }
