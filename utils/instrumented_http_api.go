// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package utils

// NewInstrumentedHTTPClient creates an HTTP client with tracing, request-ID
// propagation, and process-default metrics.
func NewInstrumentedHTTPClient(opts HTTPClientOptions) *HTTPClient {
	return NewHTTPClientAdvanced(opts)
}
