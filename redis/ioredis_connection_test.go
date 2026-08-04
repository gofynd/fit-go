// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitIORedisV4CompatibilityIsServiceScoped(t *testing.T) {
	restore := isolateRedisEnvironment(t)
	defer restore()

	server := startRetryLoopbackRedis(t, "")
	defer server.stop(t)
	t.Setenv("REDIS_ORBIS_READ_WRITE", "redis://"+server.addr+"/0")
	t.Setenv("REDIS_CACHE_READ_WRITE", "redis://cache.example:6379/0")

	var defaultDialCalls atomic.Int32
	client, err := Init(ConnectionOptions{
		Dial: func(context.Context, *DialOptions) (Connection, error) {
			defaultDialCalls.Add(1)
			return &mockConnection{}, nil
		},
		Context: context.Background(),
		IORedisCompatibility: map[string]IORedisCompatibilityProfile{
			"ORBIS": IORedisCompatibilityV4,
		},
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer client.Close()

	orbis := client.Service("orbis")
	if orbis == nil || orbis.Write == nil {
		t.Fatal("orbis compatibility connection is missing")
	}
	if _, ok := orbis.Write.Raw().(*IORedisCompatClient); !ok {
		t.Fatalf("orbis raw type = %T, want *IORedisCompatClient", orbis.Write.Raw())
	}
	cache := client.Service("cache")
	if cache == nil || cache.Write == nil {
		t.Fatal("non-selected cache connection is missing")
	}
	if _, ok := cache.Write.Raw().(*mockConnection); !ok {
		t.Fatalf("cache raw type = %T, want existing default dialer", cache.Write.Raw())
	}
	if got := defaultDialCalls.Load(); got != 1 {
		t.Fatalf("default dial calls = %d, want 1 for only the non-selected service", got)
	}
}

func TestInitIORedisV4CompatibilityRejectsUnprovenTopologies(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{name: "sentinel", uri: "redis-sentinel://127.0.0.1:26379?master=main", want: "does not support Sentinel"},
		{name: "cluster", uri: "redis://127.0.0.1:6379,127.0.0.2:6379", want: "does not support Cluster"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := isolateRedisEnvironment(t)
			defer restore()
			t.Setenv("REDIS_ORBIS_READ_WRITE", test.uri)
			_, err := Init(ConnectionOptions{
				Dial: mockDial,
				IORedisCompatibility: map[string]IORedisCompatibilityProfile{
					"orbis": IORedisCompatibilityV4,
				},
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Init error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestInitIORedisV4CompatibilityQueuesAcrossOutageAfterWaitCancellation(t *testing.T) {
	restore := isolateRedisEnvironment(t)
	defer restore()

	server := startRetryLoopbackRedis(t, "")
	addr := server.addr
	t.Setenv("REDIS_ORBIS_READ_WRITE", "redis://"+addr+"/0")
	client, err := Init(ConnectionOptions{
		Dial: mockDial,
		IORedisCompatibility: map[string]IORedisCompatibilityProfile{
			"orbis": IORedisCompatibilityV4,
		},
	})
	if err != nil {
		server.stop(t)
		t.Fatalf("Init: %v", err)
	}
	defer client.Close()
	raw := client.Service("orbis").Write.Raw().(*IORedisCompatClient)

	server.stop(t)
	future := raw.Submit("SETEX", "file:job:proof", "86400", `{"progress":0}`)
	waitCtx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	_, err = future.Wait(waitCtx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first wait error = %v, want caller-only deadline", err)
	}

	restarted := startRetryLoopbackRedis(t, addr)
	defer restarted.stop(t)
	settleCtx, settleCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer settleCancel()
	result, err := future.Wait(settleCtx)
	if err != nil {
		t.Fatalf("accepted SETEX did not resume after reconnect: %v", err)
	}
	if len(result.Replies) != 1 || result.Replies[0].Error != nil || result.Replies[0].Value != "OK" {
		t.Fatalf("SETEX result = %+v, want one OK reply", result)
	}
}

func isolateRedisEnvironment(t *testing.T) func() {
	t.Helper()
	type entry struct{ key, value string }
	var saved []entry
	for _, env := range os.Environ() {
		key, value, found := strings.Cut(env, "=")
		if !found || !strings.HasPrefix(key, "REDIS_") {
			continue
		}
		saved = append(saved, entry{key: key, value: value})
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
	return func() {
		for _, env := range os.Environ() {
			key, _, found := strings.Cut(env, "=")
			if found && strings.HasPrefix(key, "REDIS_") {
				_ = os.Unsetenv(key)
			}
		}
		for _, original := range saved {
			_ = os.Setenv(original.key, original.value)
		}
	}
}
