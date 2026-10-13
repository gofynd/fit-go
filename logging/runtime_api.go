// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package logging

// RuntimeOptions configures schema selection, resource attributes, and
// TraceClue-compatible formatting without changing the original Options type.
type RuntimeOptions = AdvancedOptions

// NewRuntime creates a logger with the complete runtime logging controls.
func NewRuntime(opts RuntimeOptions) (*Logger, error) {
	return NewAdvanced(opts)
}
