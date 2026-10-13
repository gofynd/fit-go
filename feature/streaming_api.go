// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

// InitStreaming creates the FeatureHub SSE client from process environment.
func InitStreaming() (*Client, error) {
	return InitAdvanced()
}
