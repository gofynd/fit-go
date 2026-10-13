// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redis

import (
	"context"
	"testing"
	"time"
)

func TestCompatibilityOptionsPreserveEveryControl(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "context")
	dial := func(context.Context, *DialSettings) (Connection, error) { return nil, nil }
	cluster := func(context.Context, *ClusterDialSettings) (Connection, error) { return nil, nil }
	sentinel := func(context.Context, *SentinelDialSettings) (Connection, error) { return nil, nil }
	protocols := map[string]RedisProtocol{"cache": RedisProtocolRESP2}
	profiles := map[string]IORedisCompatibilityProfile{"cache": IORedisCompatibilityV5}

	converted := deprecatedCompatibilityOptions(CompatibilityOptions{
		DefaultConnectTimeout: 3 * time.Second,
		Context:               ctx,
		DialWithSettings:      dial,
		ClusterWithSettings:   cluster,
		SentinelWithSettings:  sentinel,
		ProtocolByService:     protocols,
		IORedisCompatibility:  profiles,
	})

	if converted.Context != ctx || converted.DefaultConnectTimeout != 3*time.Second {
		t.Fatalf("base controls were not preserved: %+v", converted)
	}
	if converted.AdvancedDial == nil || converted.AdvancedClusterDial == nil || converted.AdvancedSentinelDial == nil {
		t.Fatal("configured dialers were not preserved")
	}
	if converted.ProtocolByService["cache"] != RedisProtocolRESP2 ||
		converted.IORedisCompatibility["cache"] != IORedisCompatibilityV5 {
		t.Fatalf("compatibility maps were not preserved: %+v", converted)
	}
}
