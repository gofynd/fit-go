// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package profiling

// RuntimeConfig includes requested and effective profiler runtime settings.
type RuntimeConfig = AdvancedConfig

// DefaultRuntimeConfig returns the profiler's environment-derived runtime configuration.
func DefaultRuntimeConfig() RuntimeConfig {
	return DefaultAdvancedConfig()
}

// NewRuntime creates a profiler with complete runtime controls.
func NewRuntime(cfg RuntimeConfig) *Profiler {
	return NewAdvanced(cfg)
}

// GetRuntimeConfig returns the requested and effective profiler configuration.
func (p *Profiler) GetRuntimeConfig() RuntimeConfig {
	return p.GetAdvancedConfig()
}
