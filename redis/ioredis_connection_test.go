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
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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

func TestInitIORedisV4CompatibilityRejectsReplicaTopologies(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{name: "sentinel", uri: "redis-sentinel://127.0.0.1:26379?master=main", want: "does not support Sentinel read replicas"},
		{name: "cluster", uri: "redis://127.0.0.1:6379,127.0.0.2:6379", want: "does not support Cluster replica reads"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := isolateRedisEnvironment(t)
			defer restore()
			t.Setenv("REDIS_ORBIS_READ_ONLY", test.uri)
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

func TestInitIORedisV4CompatibilitySentinelResolvesMaster(t *testing.T) {
	restore := isolateRedisEnvironment(t)
	defer restore()

	master := newIORedisTopologyDataServer(t)
	masterStopped := false
	defer func() {
		if !masterStopped {
			master.stop()
		}
	}()
	failoverMaster := newIORedisTopologyDataServer(t)
	defer failoverMaster.stop()
	var currentMaster atomic.Value
	currentMaster.Store(master.addr)
	sentinel := startSlingshotRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "INFO":
			return "$11\r\nloading:0\r\n\r\n", false
		case "SENTINEL":
			if len(command) != 3 || !strings.EqualFold(command[1], "get-master-addr-by-name") || command[2] != "galvatron-main" {
				return "-ERR unexpected sentinel command\r\n", false
			}
			masterHost, masterPort := splitTopologyAddress(t, currentMaster.Load().(string))
			return encodeTopologyRESP([]any{masterHost, int64(masterPort)}), false
		default:
			return "+OK\r\n", false
		}
	})
	defer sentinel.stop()
	t.Setenv("REDIS_ORBIS_READ_WRITE", "redis-sentinel://"+sentinel.addr+"/0?master=galvatron-main")

	client, err := Init(ConnectionOptions{
		Context: context.Background(),
		Dial:    mockDial,
		IORedisCompatibility: map[string]IORedisCompatibilityProfile{
			"orbis": IORedisCompatibilityV4,
		},
	})
	if err != nil {
		t.Fatalf("Init Sentinel: %v", err)
	}
	defer client.Close()
	connection := client.Service("orbis").Write
	if connection.IsCluster() {
		t.Fatal("Sentinel connection reported Cluster topology")
	}
	raw := connection.Raw().(*IORedisCompatClient)
	assertIORedisTopologyFuture(t, raw.Submit("SETEX", "file:job:sentinel", "86400", `{"progress":0}`), "OK")
	assertIORedisTopologyFuture(t, raw.Submit("GET", "file:job:sentinel"), `{"progress":0}`)
	if got := master.value("file:job:sentinel"); got != `{"progress":0}` {
		t.Fatalf("master value = %q", got)
	}
	if !topologyCommandsContain(sentinel.commandsSnapshot(), "SENTINEL", "get-master-addr-by-name", "galvatron-main") {
		t.Fatalf("Sentinel commands = %#v", sentinel.commandsSnapshot())
	}

	// Sentinel has already elected the replacement when the old master's
	// connection closes. The accepted command must remain queued while the
	// transport rediscovers the current master, then execute there.
	currentMaster.Store(failoverMaster.addr)
	master.stop()
	masterStopped = true
	assertIORedisTopologyFuture(t, raw.Submit("SETEX", "file:job:after-failover", "86400", `{"progress":50}`), "OK")
	if got := failoverMaster.value("file:job:after-failover"); got != `{"progress":50}` {
		t.Fatalf("failover master value = %q", got)
	}
	if got := topologyCommandCount(sentinel.commandsSnapshot(), "SENTINEL", "get-master-addr-by-name", "galvatron-main"); got < 2 {
		t.Fatalf("Sentinel discovery calls = %d, want at least initial plus failover", got)
	}
}

func TestInitIORedisV4CompatibilityClusterRoutesByHashSlot(t *testing.T) {
	restore := isolateRedisEnvironment(t)
	defer restore()

	first := newIORedisTopologyDataServer(t)
	defer first.stop()
	second := newIORedisTopologyDataServer(t)
	secondStopped := false
	defer func() {
		if !secondStopped {
			second.stop()
		}
	}()
	replacement := newIORedisTopologyDataServer(t)
	defer replacement.stop()
	firstHost, firstPort := splitTopologyAddress(t, first.addr)
	var secondRangeMaster atomic.Value
	secondRangeMaster.Store(second.addr)
	seed := startSlingshotRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "INFO":
			return "$11\r\nloading:0\r\n\r\n", false
		case "CLUSTER":
			if len(command) != 2 || !strings.EqualFold(command[1], "slots") {
				return "-ERR unexpected cluster command\r\n", false
			}
			secondHost, secondPort := splitTopologyAddress(t, secondRangeMaster.Load().(string))
			return encodeTopologyRESP([]any{
				[]any{int64(0), int64(8191), []any{firstHost, int64(firstPort), "node-a"}},
				[]any{int64(8192), int64(16383), []any{secondHost, int64(secondPort), "node-b"}},
			}), false
		default:
			return "+OK\r\n", false
		}
	})
	defer seed.stop()
	t.Setenv("REDIS_ORBIS_READ_WRITE", "redis://"+seed.addr+"/0?sharded_db=true")

	client, err := Init(ConnectionOptions{
		Context: context.Background(),
		Dial:    mockDial,
		IORedisCompatibility: map[string]IORedisCompatibilityProfile{
			"orbis": IORedisCompatibilityV4,
		},
	})
	if err != nil {
		t.Fatalf("Init Cluster: %v", err)
	}
	defer client.Close()
	connection := client.Service("orbis").Write
	if !connection.IsCluster() {
		t.Fatal("Cluster connection did not report Cluster topology")
	}
	raw := connection.Raw().(*IORedisCompatClient)
	firstKey := keyInTopologySlotRange(0, 8191)
	secondKey := keyInTopologySlotRange(8192, 16383)
	assertIORedisTopologyFuture(t, raw.Submit("SETEX", firstKey, "1800", "first"), "OK")
	assertIORedisTopologyFuture(t, raw.Submit("SETEX", secondKey, "1800", "second"), "OK")
	assertIORedisTopologyFuture(t, raw.Submit("GET", firstKey), "first")
	assertIORedisTopologyFuture(t, raw.Submit("GET", secondKey), "second")
	if got := first.value(firstKey); got != "first" {
		t.Fatalf("first slot value = %q", got)
	}
	if got := second.value(secondKey); got != "second" {
		t.Fatalf("second slot value = %q", got)
	}
	if !topologyCommandsContain(seed.commandsSnapshot(), "CLUSTER", "slots") {
		t.Fatalf("seed commands = %#v", seed.commandsSnapshot())
	}

	secondRangeMaster.Store(replacement.addr)
	second.stop()
	secondStopped = true
	assertIORedisTopologyFuture(t, raw.Submit("SETEX", secondKey, "1800", "after-reshard"), "OK")
	if got := replacement.value(secondKey); got != "after-reshard" {
		t.Fatalf("replacement slot value = %q", got)
	}
	if got := topologyCommandCount(seed.commandsSnapshot(), "CLUSTER", "slots"); got < 2 {
		t.Fatalf("Cluster slot refresh calls = %d, want at least initial plus node replacement", got)
	}
}

func TestIORedisClusterHashSlots(t *testing.T) {
	if got := ioredisClusterSlot("foo"); got != 12182 {
		t.Fatalf("slot(foo) = %d, want 12182", got)
	}
	if got := ioredisClusterSlot("bar"); got != 5061 {
		t.Fatalf("slot(bar) = %d, want 5061", got)
	}
	if got, want := ioredisClusterSlot("{user1000}.following"), ioredisClusterSlot("{user1000}.followers"); got != want {
		t.Fatalf("hash-tag slots differ: %d != %d", got, want)
	}
}

// TestIORedisV4CompatibilityLiveTopologies is the opt-in runtime harness for
// real Redis Sentinel and Cluster deployments. Unit tests above pin routing
// deterministically; this test proves the same transport against redis-server.
//
//	FIT_GO_REDIS_SENTINEL_LIVE_URI='redis-sentinel://127.0.0.1:26379/0?master=main' \
//	FIT_GO_REDIS_CLUSTER_LIVE_URI='redis://127.0.0.1:7000/0?sharded_db=true' \
//	go test ./redis -run TestIORedisV4CompatibilityLiveTopologies -count=1
func TestIORedisV4CompatibilityLiveTopologies(t *testing.T) {
	tests := []struct {
		name      string
		env       string
		isCluster bool
	}{
		{name: "sentinel", env: "FIT_GO_REDIS_SENTINEL_LIVE_URI"},
		{name: "cluster", env: "FIT_GO_REDIS_CLUSTER_LIVE_URI", isCluster: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uri := strings.TrimSpace(os.Getenv(test.env))
			if uri == "" {
				t.Skip(test.env + " is not configured")
			}
			restore := isolateRedisEnvironment(t)
			defer restore()
			if err := os.Setenv("REDIS_ORBIS_READ_WRITE", uri); err != nil {
				t.Fatalf("set live URI: %v", err)
			}
			client, err := Init(ConnectionOptions{
				Context: context.Background(),
				Dial:    mockDial,
				IORedisCompatibility: map[string]IORedisCompatibilityProfile{
					"orbis": IORedisCompatibilityV4,
				},
			})
			if err != nil {
				t.Fatalf("Init %s: %v", test.name, err)
			}
			defer client.Close()
			connection := client.Service("orbis").Write
			if connection.IsCluster() != test.isCluster {
				t.Fatalf("IsCluster = %v, want %v", connection.IsCluster(), test.isCluster)
			}
			raw := connection.Raw().(*IORedisCompatClient)
			key := "fit-go:topology-live:" + test.name
			assertIORedisTopologyFuture(t, raw.Submit("SETEX", key, "60", "verified"), "OK")
			assertIORedisTopologyFuture(t, raw.Submit("GET", key), "verified")
		})
	}
}

type ioredisTopologyDataServer struct {
	server *slingshotRESPScenarioServer
	addr   string
	mu     sync.Mutex
	values map[string]string
}

func newIORedisTopologyDataServer(t *testing.T) *ioredisTopologyDataServer {
	t.Helper()
	data := &ioredisTopologyDataServer{values: make(map[string]string)}
	data.server = startSlingshotRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "INFO":
			return "$11\r\nloading:0\r\n\r\n", false
		case "PING":
			return "+PONG\r\n", false
		case "SETEX":
			if len(command) != 4 {
				return "-ERR wrong number of arguments\r\n", false
			}
			data.mu.Lock()
			data.values[command[1]] = command[3]
			data.mu.Unlock()
			return "+OK\r\n", false
		case "GET":
			if len(command) != 2 {
				return "-ERR wrong number of arguments\r\n", false
			}
			data.mu.Lock()
			value, ok := data.values[command[1]]
			data.mu.Unlock()
			if !ok {
				return "$-1\r\n", false
			}
			return encodeTopologyRESP(value), false
		default:
			return "+OK\r\n", false
		}
	})
	data.addr = data.server.addr
	return data
}

func (s *ioredisTopologyDataServer) value(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key]
}

func (s *ioredisTopologyDataServer) stop() { s.server.stop() }

func splitTopologyAddress(t *testing.T, address string) (string, int) {
	t.Helper()
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split %q: %v", address, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatalf("parse port %q: %v", rawPort, err)
	}
	return host, port
}

func encodeTopologyRESP(value any) string {
	switch typed := value.(type) {
	case nil:
		return "$-1\r\n"
	case string:
		return fmt.Sprintf("$%d\r\n%s\r\n", len(typed), typed)
	case int64:
		return fmt.Sprintf(":%d\r\n", typed)
	case []any:
		var builder strings.Builder
		fmt.Fprintf(&builder, "*%d\r\n", len(typed))
		for _, element := range typed {
			builder.WriteString(encodeTopologyRESP(element))
		}
		return builder.String()
	default:
		panic(fmt.Sprintf("unsupported test RESP value %T", value))
	}
}

func keyInTopologySlotRange(first, last int) string {
	for index := 0; ; index++ {
		key := fmt.Sprintf("topology:%d", index)
		slot := ioredisClusterSlot(key)
		if slot >= first && slot <= last {
			return key
		}
	}
}

func assertIORedisTopologyFuture(t *testing.T, future *IORedisFuture, want any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := future.Wait(ctx)
	if err != nil {
		t.Fatalf("future: %v", err)
	}
	if len(result.Replies) != 1 || result.Replies[0].Error != nil || result.Replies[0].Value != want {
		t.Fatalf("future result = %#v, want %#v", result, want)
	}
}

func topologyCommandsContain(commands [][]string, want ...string) bool {
	return topologyCommandCount(commands, want...) > 0
}

func topologyCommandCount(commands [][]string, want ...string) int {
	count := 0
	for _, command := range commands {
		if len(command) != len(want) {
			continue
		}
		matched := true
		for index := range command {
			if !strings.EqualFold(command[index], want[index]) {
				matched = false
				break
			}
		}
		if matched {
			count++
		}
	}
	return count
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
