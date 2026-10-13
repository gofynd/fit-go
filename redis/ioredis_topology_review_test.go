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
	"strconv"
	"strings"
	"testing"
)

func reviewClusterTransport() *ioredisClusterTransport {
	cluster := &ioredisClusterTransport{first: "first"}
	for slot := range cluster.slots {
		cluster.slots[slot] = "key-node"
	}
	return cluster
}

func TestIORedisClusterKeyCountBounds(t *testing.T) {
	cluster := reviewClusterTransport()
	maxInt := uint64(^uint(0) >> 1)
	for _, verb := range []string{"EVAL", "EVALSHA", "FCALL", "FCALL_RO", "BLMPOP", "BZMPOP"} {
		t.Run(verb, func(t *testing.T) {
			countedPop := verb == "BLMPOP" || verb == "BZMPOP"
			prefix := []string{verb, "synthetic-script"}
			if countedPop {
				prefix[1] = "1"
			}
			for _, count := range []string{"-1", strconv.FormatUint(maxInt, 10), strconv.FormatUint(maxInt+1, 10), "2"} {
				command := append(append([]string(nil), prefix...), count, "{review}one")
				if _, err := cluster.route(command); err == nil {
					t.Errorf("count %s accepted with one argument", count)
				}
			}
			zero := append(append([]string(nil), prefix...), "0")
			address, err := cluster.route(zero)
			if countedPop {
				if err == nil {
					t.Fatal("zero pop keys accepted")
				}
			} else if err != nil || address != "first" {
				t.Fatalf("zero script keys = %q, %v", address, err)
			}
			for _, keys := range [][]string{{"{review}one"}, {"{review}one", "{review}two"}} {
				command := append(append([]string(nil), prefix...), strconv.Itoa(len(keys)))
				command = append(command, keys...)
				if address, err := cluster.route(command); err != nil || address != "key-node" {
					t.Fatalf("exact key count boundary = %q, %v", address, err)
				}
			}
			if _, err := cluster.route(prefix); err == nil {
				t.Fatal("missing count accepted")
			}
		})
	}
}

func TestIORedisXReadPrefixRoutingAndBlocking(t *testing.T) {
	cluster := reviewClusterTransport()
	for _, test := range []struct {
		name      string
		command   []string
		exclusive bool
	}{
		{"group named STREAMS", []string{"XREADGROUP", "GROUP", "STREAMS", "consumer", "STREAMS", "{review}key", ">"}, false},
		{"consumer named STREAMS", []string{"XREADGROUP", "GROUP", "group", "STREAMS", "STREAMS", "{review}key", ">"}, false},
		{"group named BLOCK", []string{"XREADGROUP", "GROUP", "BLOCK", "STREAMS", "COUNT", "2", "BLOCK", "10", "NOACK", "STREAMS", "{review}key", ">"}, true},
		{"options reordered", []string{"xreadgroup", "group", "STREAMS", "STREAMS", "block", "1", "count", "1", "streams", "{review}key", ">"}, true},
		{"COUNT before GROUP", []string{"XREADGROUP", "COUNT", "1", "GROUP", "group", "consumer", "STREAMS", "{review}key", ">"}, false},
		{"BLOCK before GROUP", []string{"XREADGROUP", "BLOCK", "1", "GROUP", "group", "consumer", "STREAMS", "{review}key", ">"}, true},
		{"NOACK before GROUP", []string{"XREADGROUP", "NOACK", "GROUP", "group", "consumer", "STREAMS", "{review}key", ">"}, false},
		{"repeated GROUP", []string{"XREADGROUP", "GROUP", "old-group", "old-consumer", "COUNT", "1", "GROUP", "group", "consumer", "STREAMS", "{review}key", ">"}, false},
		{"all options before GROUP with keyword names", []string{"xreadgroup", "count", "1", "block", "1", "noack", "group", "STREAMS", "BLOCK", "streams", "{review}key", ">"}, true},
		{"stream named BLOCK", []string{"XREAD", "STREAMS", "BLOCK", "0"}, false},
		{"stream named STREAMS", []string{"XREAD", "COUNT", "1", "BLOCK", "1", "STREAMS", "STREAMS", "$"}, true},
		{"two streams", []string{"XREADGROUP", "GROUP", "STREAMS", "STREAMS", "STREAMS", "{review}one", "{review}two", ">", ">"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := validateIORedisQueuedCommand(test.command); err != nil || got != test.exclusive {
				t.Fatalf("queue exclusive = %v, %v; want %v", got, err, test.exclusive)
			}
			if address, err := cluster.route(test.command); err != nil || address != "key-node" {
				t.Fatalf("route = %q, %v", address, err)
			}
		})
	}
	for _, timeout := range []string{"0", "-1", "NaN", "Inf", "synthetic-private-value"} {
		command := []string{"XREADGROUP", "GROUP", "STREAMS", "STREAMS", "COUNT", "1", "BLOCK", timeout, "STREAMS", "{review}key", ">"}
		if _, err := validateIORedisQueuedCommand(command); err == nil || strings.Contains(err.Error(), timeout) {
			t.Errorf("unsafe timeout %q rejection = %v", timeout, err)
		}
	}
	for _, command := range [][]string{
		{"XREAD", "BLOCK"},
		{"XREAD", "COUNT", "STREAMS", "synthetic-private-value", "$"},
		{"XREADGROUP", "GROUP", "STREAMS", "synthetic-private-value"},
		{"XREADGROUP", "COUNT", "1", "GROUP", "synthetic-private-value"},
		{"XREADGROUP", "COUNT", "1", "STREAMS", "synthetic-private-value", ">"},
		{"XREAD", "GROUP", "group", "synthetic-private-value", "STREAMS", "key", "$"},
		{"XREADGROUP", "GROUP", "group", "consumer", "BLOCK", "1", "BLOCK", "0", "STREAMS", "key", ">"},
		{"XREADGROUP", "BLOCK", "0", "GROUP", "group", "consumer", "STREAMS", "key", ">"},
	} {
		if _, err := validateIORedisQueuedCommand(command); err == nil || strings.Contains(err.Error(), "synthetic-private-value") {
			t.Errorf("invalid prefix rejection = %v", err)
		}
	}
	for _, command := range [][]string{
		{"XREAD", "STREAMS"},
		{"XREAD", "STREAMS", "key"},
		{"XREADGROUP", "GROUP", "STREAMS", "consumer", "STREAMS", "key", "other", ">"},
	} {
		if _, err := cluster.route(command); err == nil {
			t.Errorf("missing/mismatched stream list accepted: %v", command)
		}
	}
}

func TestIORedisXReadGroupOptionPermutations(t *testing.T) {
	cluster := reviewClusterTransport()
	clauses := [][]string{{"GROUP", "STREAMS", "BLOCK"}, {"COUNT", "1"}, {"BLOCK", "1"}, {"NOACK"}}
	cases := 0
	var visit func([]string, []bool, int)
	visit = func(prefix []string, used []bool, depth int) {
		if depth == len(clauses) {
			command := append(append([]string(nil), prefix...), "STREAMS", "{review}BLOCK", "{review}STREAMS", ">", ">")
			if exclusive, err := validateIORedisQueuedCommand(command); err != nil || !exclusive {
				t.Fatalf("valid option permutation queue = %v, %v: %v", exclusive, err, command)
			}
			if address, err := cluster.route(command); err != nil || address != "key-node" {
				t.Fatalf("valid option permutation route = %q, %v: %v", address, err, command)
			}
			cases++
			return
		}
		for index, clause := range clauses {
			if used[index] {
				continue
			}
			used[index] = true
			visit(append(prefix, clause...), used, depth+1)
			used[index] = false
		}
	}
	visit([]string{"XREADGROUP"}, make([]bool, len(clauses)), 0)
	if cases != 24 {
		t.Fatalf("checked %d permutations, want 24", cases)
	}
}

func TestIORedisTopologyHostPortFormatting(t *testing.T) {
	for _, test := range []struct{ host, want string }{
		{"::1", "[::1]:6379"},
		{"2001:db8::123", "[2001:db8::123]:6379"},
		{"fe80::1%lo", "[fe80::1%lo]:6379"},
		{"127.0.0.1", "127.0.0.1:6379"},
		{"synthetic-redis.example", "synthetic-redis.example:6379"},
	} {
		t.Run(test.host, func(t *testing.T) {
			address, err := ioredisSentinelMasterAddress([]any{test.host, "6379"})
			if err != nil || address != test.want {
				t.Fatalf("Sentinel address = %q, %v", address, err)
			}
			for _, host := range []any{test.host, "", nil} {
				ranges, err := parseIORedisClusterSlots([]any{[]any{int64(0), int64(16383), []any{host, int64(6379)}}}, test.want)
				if err != nil || len(ranges) != 1 || ranges[0].address != test.want {
					t.Fatalf("Cluster address = %#v, %v", ranges, err)
				}
			}
		})
	}
	if _, err := ioredisSentinelMasterAddress([]any{"", "6379"}); err == nil {
		t.Fatal("empty Sentinel host accepted")
	}
	if _, err := parseIORedisClusterSlots([]any{[]any{int64(0), int64(16383), []any{"", int64(6379)}}}, "synthetic-private-value"); err == nil || strings.Contains(err.Error(), "synthetic-private-value") {
		t.Fatalf("invalid seed fallback = %v", err)
	}
}
