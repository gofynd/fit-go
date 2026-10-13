// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package mongo

// DefaultInstrumentedDialFunc returns the default MongoDB dialer with command tracing.
func DefaultInstrumentedDialFunc() DialFunc {
	return DefaultAdvancedDialFunc()
}
