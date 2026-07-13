// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"os"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

func resourceValues(res *resource.Resource) map[string]attribute.Value {
	values := make(map[string]attribute.Value)
	for _, attr := range res.Attributes() {
		values[string(attr.Key)] = attr.Value
	}
	return values
}

func TestBuildResourceDefaultsAndPrecedence(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "env-service")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "custom.key=env,env.only=present,service.instance.id=env-instance")

	res := buildResource(context.Background(), Options{
		ServiceName: "explicit-service",
		Env:         "sit",
		Attributes: map[string]string{
			"custom.key":          "option",
			"service.instance.id": "option-instance",
		},
	})
	values := resourceValues(res)

	wantStrings := map[string]string{
		"service.name":           "explicit-service",
		"deployment.environment": "sit",
		"custom.key":             "option",
		"env.only":               "present",
		"service.instance.id":    "option-instance",
		"telemetry.sdk.name":     "opentelemetry",
		"telemetry.sdk.language": "go",
	}
	for key, want := range wantStrings {
		got, ok := values[key]
		if !ok || got.AsString() != want {
			t.Errorf("resource[%q] = %v, want %q", key, got, want)
		}
	}
	for _, key := range []string{
		"telemetry.sdk.version",
		"process.pid",
		"process.executable.name",
		"process.executable.path",
		"process.runtime.name",
		"process.runtime.version",
		"process.runtime.description",
		"host.name",
	} {
		if _, ok := values[key]; !ok {
			t.Errorf("resource missing default detector attribute %q", key)
		}
	}
	for _, key := range []string{"process.command_args", "process.owner"} {
		if _, ok := values[key]; ok {
			t.Errorf("resource unexpectedly exports secret/PII-prone attribute %q", key)
		}
	}
	if got := values["process.pid"].AsInt64(); got != int64(os.Getpid()) {
		t.Errorf("process.pid = %d, want %d", got, os.Getpid())
	}
}

func TestBuildResourceKeepsPartialEnvironmentResource(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "valid.key=kept,malformed")
	res := buildResource(context.Background(), Options{ServiceName: "service", Env: "test"})
	values := resourceValues(res)
	if got := values["valid.key"].AsString(); got != "kept" {
		t.Fatalf("valid partial resource attribute = %q, want kept", got)
	}
	if got := values["service.name"].AsString(); got != "service" {
		t.Fatalf("service.name = %q after partial detector error", got)
	}
}

func TestBuildResourceAddsReleaseRevisionAndDeploymentIdentity(t *testing.T) {
	t.Setenv("SENTRY_RELEASE", "release-from-env")
	t.Setenv("GITSHA", "0123456789abcdef")
	t.Setenv("SENTRY_ENVIRONMENT", "fyndz0")
	// OTel's standard resource input must retain precedence over inferred values.
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.version=release-from-resource")

	values := resourceValues(buildResource(context.Background(), Options{
		ServiceName: "service",
		Env:         "production",
	}))
	want := map[string]string{
		"service.version":             "release-from-resource",
		"vcs.ref.head.revision":       "0123456789abcdef",
		"deployment.environment":      "production",
		"deployment.environment.name": "fyndz0",
	}
	for key, expected := range want {
		value, ok := values[key]
		if !ok || value.AsString() != expected {
			t.Errorf("resource[%q] = %v, want %q", key, value, expected)
		}
	}
}

func TestBuildResourceServiceNamePrecedence(t *testing.T) {
	t.Run("resource service survives without explicit service", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "")
		t.Setenv("SERVICE_NAME", "")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=resource-service")
		values := resourceValues(buildResource(context.Background(), Options{}))
		if got := values["service.name"].AsString(); got != "resource-service" {
			t.Fatalf("service.name = %q, want resource-service", got)
		}
	})

	t.Run("otel service wins over legacy and resource", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "otel-service")
		t.Setenv("SERVICE_NAME", "legacy-service")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=resource-service")
		values := resourceValues(buildResource(context.Background(), DefaultOptions()))
		if got := values["service.name"].AsString(); got != "otel-service" {
			t.Fatalf("service.name = %q, want otel-service", got)
		}
	})

	t.Run("resource service wins over legacy fallback", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "")
		t.Setenv("SERVICE_NAME", "legacy-service")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=resource-service")
		values := resourceValues(buildResource(context.Background(), DefaultOptions()))
		if got := values["service.name"].AsString(); got != "resource-service" {
			t.Fatalf("service.name = %q, want resource-service", got)
		}
	})

	t.Run("legacy service is used only as fallback", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "")
		t.Setenv("SERVICE_NAME", "legacy-service")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
		values := resourceValues(buildResource(context.Background(), DefaultOptions()))
		if got := values["service.name"].AsString(); got != "legacy-service" {
			t.Fatalf("service.name = %q, want legacy-service", got)
		}
	})

	t.Run("unknown is only a final fallback", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "")
		t.Setenv("SERVICE_NAME", "")
		t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
		values := resourceValues(buildResource(context.Background(), Options{}))
		if got := values["service.name"].AsString(); got != "unknown_service" {
			t.Fatalf("service.name = %q, want unknown_service", got)
		}
	})
}
