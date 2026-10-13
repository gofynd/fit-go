// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package mysql

// InstrumentedConnectionOptions enables explicit MySQL command tracing.
type InstrumentedConnectionOptions = ConnectionAdvancedOptions

// InitInstrumented initializes MySQL with explicit command tracing controls.
func InitInstrumented(opts InstrumentedConnectionOptions) (*Client, error) {
	return InitAdvanced(opts)
}
