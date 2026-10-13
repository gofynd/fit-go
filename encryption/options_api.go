// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package encryption

// ManagerOptions configures accepted cross-language AES-GCM nonce sizes.
// It does not make the inherited fixed-IV format safe for new encryption.
// Prefer a versioned format with a fresh random nonce for every ciphertext.
type ManagerOptions = ManagerAdvancedOptions

// NewManagerWithOptions creates an encryption manager using explicit compatibility options.
func NewManagerWithOptions(opts ManagerOptions) *Manager {
	return NewManagerAdvanced(opts)
}
