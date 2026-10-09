// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package metrics

// TextfileOptions configures Prometheus collectors and the optional textfile
// lifecycle without changing the original Options layout.
type TextfileOptions = AdvancedOptions

// NewTextfileRegistry creates a registry with textfile lifecycle controls.
func NewTextfileRegistry(opts TextfileOptions) (*Registry, error) {
	return NewAdvanced(opts)
}
