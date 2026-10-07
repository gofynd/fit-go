// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package encryption

// ManagerOptions configures accepted cross-language AES-GCM nonce sizes.
type ManagerOptions = ManagerAdvancedOptions

// NewManagerWithOptions creates an encryption manager using explicit compatibility options.
func NewManagerWithOptions(opts ManagerOptions) *Manager {
	return NewManagerAdvanced(opts)
}
