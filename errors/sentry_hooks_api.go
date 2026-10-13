// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package errors

// SentryHooksConfig adds sanitized event hooks to the original SentryConfig.
type SentryHooksConfig = SentryAdvancedConfig

// SentryHooksInitializer is implemented by reporters that support sanitized event hooks.
type SentryHooksInitializer interface {
	InitWithHooks(SentryHooksConfig) error
}

var _ SentryHooksInitializer = (*sentrySdk)(nil)

// InitSentryWithHooks initializes Sentry with sanitized event hooks.
func InitSentryWithHooks(cfg SentryHooksConfig) error {
	if reporter, ok := Sentry.(SentryHooksInitializer); ok {
		return reporter.InitWithHooks(cfg)
	}
	return InitSentryWithAdvancedConfig(cfg)
}

// InitWithHooks initializes the default reporter with sanitized event hooks.
func (s *sentrySdk) InitWithHooks(cfg SentryHooksConfig) error {
	return s.InitWithAdvancedConfig(cfg)
}
