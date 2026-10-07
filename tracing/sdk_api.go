// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"

	"go.opentelemetry.io/otel/sdk/resource"
)

// SDKOptions configures the complete OpenTelemetry SDK lifecycle while the
// original Options type retains its released layout and defaults.
type SDKOptions = AdvancedOptions

// DefaultSDKOptions returns the environment-derived OpenTelemetry SDK options.
func DefaultSDKOptions() SDKOptions {
	return DefaultAdvancedOptions()
}

// NewSDK creates an independently owned tracer SDK.
func NewSDK(ctx context.Context, opts SDKOptions) (*Tracer, error) {
	return NewAdvanced(ctx, opts)
}

// ResourceFromSDKOptions builds an OpenTelemetry resource from SDKOptions.
func ResourceFromSDKOptions(ctx context.Context, opts SDKOptions) *resource.Resource {
	return ResourceFromAdvancedOptions(ctx, opts)
}

// InitSDK initializes and installs the process-global tracer SDK.
func InitSDK(opts SDKOptions) (*Tracer, error) {
	return InitWithAdvancedOptions(opts)
}
