// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package redis

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// The upstream-default entry points (Init, InitDefault) must never construct
// the opt-in ioredis compatibility transport, even if the service would be
// eligible for it under InitWithCompatibility.
func TestLegacyEntryPointsNeverConstructIORedisTransport(t *testing.T) {
	for _, env := range os.Environ() {
		if key, _, _ := strings.Cut(env, "="); envPattern.MatchString(key) {
			t.Setenv(key, "")
		}
	}

	advanced := advancedConnectionOptions(ConnectionOptions{
		Dial: DefaultDialFunc(), ClusterDial: DefaultClusterDialFunc(), SentinelDial: DefaultSentinelDialFunc(),
	})
	if advanced.IORedisCompatibility != nil || advanced.AdvancedDial != nil {
		t.Fatalf("legacy options enable ioredis/advanced dialing: %+v", advanced)
	}
	if profile, enabled := ioredisCompatibilityProfile(advanced, "guard"); enabled {
		t.Fatalf("legacy options select ioredis profile %q", profile)
	}

	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "HELLO":
			return "-ERR unknown command 'HELLO'\r\n", false
		case "PING":
			return "+PONG\r\n", false
		default:
			return "+OK\r\n", false
		}
	})
	defer server.stop()
	t.Setenv("REDIS_GUARD_READ_WRITE", "redis://"+server.addr)

	assertGoRedis := func(name string, client *Client) {
		t.Helper()
		defer client.Close()
		service := client.Service("guard")
		if service == nil || service.Write == nil {
			t.Fatalf("%s: guard service missing", name)
		}
		if _, isIORedis := service.Write.(*ioredisConnection); isIORedis {
			t.Fatalf("%s constructed an ioredis connection", name)
		}
		if _, ok := service.Write.Raw().(*goredis.Client); !ok {
			t.Fatalf("%s Raw() = %T; want *goredis.Client", name, service.Write.Raw())
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dials atomic.Int32
	defaultDial := DefaultDialFunc()
	client, err := Init(ConnectionOptions{
		Context: ctx,
		Dial: func(ctx context.Context, opts *DialOptions) (Connection, error) {
			dials.Add(1)
			return defaultDial(ctx, opts)
		},
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	assertGoRedis("Init", client)
	if dials.Load() != 1 {
		t.Fatalf("Init used caller DialFunc %d times; want 1", dials.Load())
	}

	client, err = InitDefault(ctx)
	if err != nil {
		t.Fatalf("InitDefault: %v", err)
	}
	assertGoRedis("InitDefault", client)
}
