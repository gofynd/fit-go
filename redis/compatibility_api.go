// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redis

import (
	"context"
	"time"
)

// DialSettings configures a standalone Redis connection, protocol, and retry policy.
type DialSettings = AdvancedDialOptions

// ClusterDialSettings configures a Redis Cluster connection, protocol, and retry policy.
type ClusterDialSettings = AdvancedClusterDialOptions

// SentinelDialSettings configures a Redis Sentinel connection, protocol, and retry policy.
type SentinelDialSettings = AdvancedSentinelDialOptions

// ConfiguredDialFunc creates a standalone connection from DialSettings.
type ConfiguredDialFunc func(context.Context, *DialSettings) (Connection, error)

// ConfiguredClusterDialFunc creates a cluster connection from ClusterDialSettings.
type ConfiguredClusterDialFunc func(context.Context, *ClusterDialSettings) (Connection, error)

// ConfiguredSentinelDialFunc creates a Sentinel connection from SentinelDialSettings.
type ConfiguredSentinelDialFunc func(context.Context, *SentinelDialSettings) (Connection, error)

// CompatibilityOptions configures RESP selection, retry controls, and optional
// ioredis wire/lifecycle compatibility without exposing migration-era names.
type CompatibilityOptions struct {
	Dial                  DialFunc
	ClusterDial           ClusterDialFunc
	SentinelDial          SentinelDialFunc
	DefaultConnectTimeout time.Duration
	Context               context.Context
	DialWithSettings      ConfiguredDialFunc
	ClusterWithSettings   ConfiguredClusterDialFunc
	SentinelWithSettings  ConfiguredSentinelDialFunc
	ProtocolByService     map[string]RedisProtocol
	IORedisCompatibility  map[string]IORedisCompatibilityProfile
}

// InitWithCompatibility initializes Redis with protocol, retry, and ioredis controls.
func InitWithCompatibility(opts CompatibilityOptions) (*Client, error) {
	return InitAdvanced(deprecatedCompatibilityOptions(opts))
}

func deprecatedCompatibilityOptions(opts CompatibilityOptions) AdvancedConnectionOptions {
	return AdvancedConnectionOptions{
		Dial:                  opts.Dial,
		ClusterDial:           opts.ClusterDial,
		SentinelDial:          opts.SentinelDial,
		DefaultConnectTimeout: opts.DefaultConnectTimeout,
		Context:               opts.Context,
		AdvancedDial:          AdvancedDialFunc(opts.DialWithSettings),
		AdvancedClusterDial:   AdvancedClusterDialFunc(opts.ClusterWithSettings),
		AdvancedSentinelDial:  AdvancedSentinelDialFunc(opts.SentinelWithSettings),
		ProtocolByService:     opts.ProtocolByService,
		IORedisCompatibility:  opts.IORedisCompatibility,
	}
}

// DefaultConfiguredDialFunc returns the default standalone dialer with protocol and retry controls.
func DefaultConfiguredDialFunc() ConfiguredDialFunc {
	return ConfiguredDialFunc(DefaultAdvancedDialFunc())
}

// DefaultConfiguredClusterDialFunc returns the default cluster dialer with protocol and retry controls.
func DefaultConfiguredClusterDialFunc() ConfiguredClusterDialFunc {
	return ConfiguredClusterDialFunc(DefaultAdvancedClusterDialFunc())
}

// DefaultConfiguredSentinelDialFunc returns the default Sentinel dialer with protocol and retry controls.
func DefaultConfiguredSentinelDialFunc() ConfiguredSentinelDialFunc {
	return ConfiguredSentinelDialFunc(DefaultAdvancedSentinelDialFunc())
}
