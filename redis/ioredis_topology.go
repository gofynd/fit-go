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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ioredisSentinelOptions struct {
	SentinelAddrs            []string
	MasterName               string
	Username                 string
	Password                 string
	SentinelUsername         string
	SentinelPassword         string
	DB                       int
	ConnectionName           string
	TLSConfig                *tls.Config
	ConnectTimeout           time.Duration
	SocketTimeout            time.Duration
	KeepAlive                time.Duration
	DisableClientInfo        bool
	clientInfoLibraryVersion string
}

type ioredisClusterOptions struct {
	SeedAddrs                []string
	Username                 string
	Password                 string
	DB                       int
	ConnectionName           string
	TLSConfig                *tls.Config
	ConnectTimeout           time.Duration
	SocketTimeout            time.Duration
	KeepAlive                time.Duration
	DisableClientInfo        bool
	clientInfoLibraryVersion string
}

func dialIORedisCompatibleSentinel(ctx context.Context, profile IORedisCompatibilityProfile, options ioredisSentinelOptions) (Connection, error) {
	resolved, err := resolveIORedisRESPCompatibilityProfile(profile)
	if err != nil {
		return nil, err
	}
	options.DisableClientInfo = resolved.disableClientInfo
	options.clientInfoLibraryVersion = resolved.clientInfoLibraryVersion
	factory := &ioredisSentinelFactory{options: options}
	client, err := NewIORedisCompatClientReady(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &ioredisConnection{client: client}, nil
}

type ioredisSentinelFactory struct {
	options ioredisSentinelOptions
}

func (f *ioredisSentinelFactory) Connect(ctx context.Context) (IORedisTransport, error) {
	if f == nil || len(f.options.SentinelAddrs) == 0 || strings.TrimSpace(f.options.MasterName) == "" {
		return nil, errors.New("redis: ioredis Sentinel addresses and master name are required")
	}
	failures := 0
	for _, address := range f.options.SentinelAddrs {
		sentinel, err := connectIORedisRESP(ctx, IORedisRESPOptions{
			Addr:                     address,
			Username:                 f.options.SentinelUsername,
			Password:                 f.options.SentinelPassword,
			ConnectionName:           f.options.ConnectionName,
			TLSConfig:                f.options.TLSConfig,
			ConnectTimeout:           f.options.ConnectTimeout,
			SocketTimeout:            f.options.SocketTimeout,
			KeepAlive:                f.options.KeepAlive,
			DisableClientInfo:        f.options.DisableClientInfo,
			clientInfoLibraryVersion: f.options.clientInfoLibraryVersion,
		})
		if err != nil {
			failures++
			continue
		}
		exchange := sentinel.Exchange(ctx, [][]string{{"sentinel", "get-master-addr-by-name", f.options.MasterName}})
		_ = sentinel.Close()
		if exchange.Error != nil {
			failures++
			continue
		}
		if len(exchange.Replies) != 1 || exchange.Replies[0].Error != nil {
			failures++
			continue
		}
		masterAddress, err := ioredisSentinelMasterAddress(exchange.Replies[0].Value)
		if err != nil {
			failures++
			continue
		}
		master, connectErr := connectIORedisRESP(ctx, IORedisRESPOptions{
			Addr:                     masterAddress,
			Username:                 f.options.Username,
			Password:                 f.options.Password,
			DB:                       f.options.DB,
			ConnectionName:           f.options.ConnectionName,
			TLSConfig:                f.options.TLSConfig,
			ConnectTimeout:           f.options.ConnectTimeout,
			SocketTimeout:            f.options.SocketTimeout,
			KeepAlive:                f.options.KeepAlive,
			DisableClientInfo:        f.options.DisableClientInfo,
			clientInfoLibraryVersion: f.options.clientInfoLibraryVersion,
		})
		if connectErr != nil {
			return nil, newIORedisSafeError("redis: resolved Sentinel master connection failed", connectErr)
		}
		return master, nil
	}
	return nil, fmt.Errorf("redis: no Sentinel could resolve the configured master after %d attempts", failures)
}

func ioredisSentinelMasterAddress(value any) (string, error) {
	parts, ok := value.([]any)
	if !ok || len(parts) != 2 {
		return "", errors.New("redis: invalid Sentinel master reply")
	}
	host, ok := parts[0].(string)
	if !ok || strings.TrimSpace(host) == "" {
		return "", errors.New("redis: invalid Sentinel master host")
	}
	port, ok := ioredisReplyInt(parts[1])
	if !ok || port <= 0 || port > 65535 {
		return "", errors.New("redis: invalid Sentinel master port")
	}
	return net.JoinHostPort(host, strconv.FormatInt(port, 10)), nil
}

func dialIORedisCompatibleCluster(ctx context.Context, profile IORedisCompatibilityProfile, options ioredisClusterOptions) (Connection, error) {
	resolved, err := resolveIORedisRESPCompatibilityProfile(profile)
	if err != nil {
		return nil, err
	}
	options.DisableClientInfo = resolved.disableClientInfo
	options.clientInfoLibraryVersion = resolved.clientInfoLibraryVersion
	factory := &ioredisClusterFactory{options: options}
	client, err := NewIORedisCompatClientReady(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &ioredisConnection{client: client, cluster: true}, nil
}

type ioredisClusterFactory struct {
	options ioredisClusterOptions
}

func (f *ioredisClusterFactory) Connect(ctx context.Context) (IORedisTransport, error) {
	if f == nil || len(f.options.SeedAddrs) == 0 {
		return nil, errors.New("redis: ioredis Cluster seed address is required")
	}
	failures := 0
	for _, address := range f.options.SeedAddrs {
		seed, err := f.connectNode(ctx, address)
		if err != nil {
			failures++
			continue
		}
		exchange := seed.Exchange(ctx, [][]string{{"cluster", "slots"}})
		_ = seed.Close()
		if exchange.Error != nil {
			failures++
			continue
		}
		if len(exchange.Replies) != 1 || exchange.Replies[0].Error != nil {
			failures++
			continue
		}
		ranges, err := parseIORedisClusterSlots(exchange.Replies[0].Value, address)
		if err != nil {
			failures++
			continue
		}
		return newIORedisClusterTransport(ctx, f, ranges)
	}
	return nil, fmt.Errorf("redis: no Cluster seed returned slots after %d attempts", failures)
}

func (f *ioredisClusterFactory) connectNode(ctx context.Context, address string) (IORedisTransport, error) {
	return connectIORedisRESP(ctx, IORedisRESPOptions{
		Addr:                     address,
		Username:                 f.options.Username,
		Password:                 f.options.Password,
		DB:                       f.options.DB,
		ConnectionName:           f.options.ConnectionName,
		TLSConfig:                f.options.TLSConfig,
		ConnectTimeout:           f.options.ConnectTimeout,
		SocketTimeout:            f.options.SocketTimeout,
		KeepAlive:                f.options.KeepAlive,
		DisableClientInfo:        f.options.DisableClientInfo,
		clientInfoLibraryVersion: f.options.clientInfoLibraryVersion,
	})
}

func connectIORedisRESP(ctx context.Context, options IORedisRESPOptions) (IORedisTransport, error) {
	factory, err := NewIORedisRESPTransportFactory(options)
	if err != nil {
		return nil, err
	}
	return factory.Connect(ctx)
}

type ioredisClusterSlotRange struct {
	first   int
	last    int
	address string
}

func parseIORedisClusterSlots(value any, seedAddress string) ([]ioredisClusterSlotRange, error) {
	rows, ok := value.([]any)
	if !ok || len(rows) == 0 {
		return nil, errors.New("redis: invalid CLUSTER SLOTS reply")
	}
	ranges := make([]ioredisClusterSlotRange, 0, len(rows))
	for _, rawRow := range rows {
		row, ok := rawRow.([]any)
		if !ok || len(row) < 3 {
			return nil, errors.New("redis: invalid CLUSTER SLOTS row")
		}
		first, firstOK := ioredisReplyInt(row[0])
		last, lastOK := ioredisReplyInt(row[1])
		node, nodeOK := row[2].([]any)
		if !firstOK || !lastOK || !nodeOK || len(node) < 2 || first < 0 || last < first || last >= 16384 {
			return nil, errors.New("redis: invalid CLUSTER SLOTS row")
		}
		host, hostOK := node[0].(string)
		port, portOK := ioredisReplyInt(node[1])
		if !hostOK || strings.TrimSpace(host) == "" {
			seedHost, _, splitErr := net.SplitHostPort(seedAddress)
			if splitErr != nil || strings.TrimSpace(seedHost) == "" {
				return nil, newIORedisSafeError("redis: invalid Cluster seed address", splitErr)
			}
			host = seedHost
		}
		if !portOK || port <= 0 || port > 65535 {
			return nil, errors.New("redis: invalid CLUSTER SLOTS node")
		}
		ranges = append(ranges, ioredisClusterSlotRange{
			first: int(first), last: int(last), address: net.JoinHostPort(host, strconv.FormatInt(port, 10)),
		})
	}
	return ranges, nil
}

func ioredisReplyInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

type ioredisClusterTransport struct {
	mu         sync.RWMutex
	factory    ioredisClusterNodeFactory
	nodes      map[string]IORedisTransport
	slots      [16384]string
	first      string
	closed     chan struct{}
	stop       chan struct{}
	closedOnce sync.Once
	stopOnce   sync.Once
	stopped    bool
}

type ioredisClusterNodeFactory interface {
	connectNode(context.Context, string) (IORedisTransport, error)
}

func newIORedisClusterTransport(ctx context.Context, factory *ioredisClusterFactory, ranges []ioredisClusterSlotRange) (*ioredisClusterTransport, error) {
	transport := &ioredisClusterTransport{
		factory: factory, nodes: make(map[string]IORedisTransport),
		closed: make(chan struct{}), stop: make(chan struct{}),
	}
	for _, slotRange := range ranges {
		if transport.first == "" {
			transport.first = slotRange.address
		}
		for slot := slotRange.first; slot <= slotRange.last; slot++ {
			transport.slots[slot] = slotRange.address
		}
		if _, exists := transport.nodes[slotRange.address]; exists {
			continue
		}
		node, err := factory.connectNode(ctx, slotRange.address)
		if err != nil {
			_ = transport.Close()
			return nil, newIORedisSafeError("redis: Cluster node connection failed", err)
		}
		transport.nodes[slotRange.address] = node
		go transport.watch(node)
	}
	if transport.first == "" || len(transport.nodes) == 0 {
		return nil, errors.New("redis: CLUSTER SLOTS returned no master nodes")
	}
	return transport, nil
}

func (t *ioredisClusterTransport) watch(node IORedisTransport) {
	select {
	case <-node.Closed():
		t.closedOnce.Do(func() { close(t.closed) })
	case <-t.stop:
	}
}

func (t *ioredisClusterTransport) Closed() <-chan struct{} { return t.closed }

func (t *ioredisClusterTransport) Close() error {
	if t == nil {
		return nil
	}
	failures := 0
	t.stopOnce.Do(func() { close(t.stop) })
	t.closedOnce.Do(func() { close(t.closed) })
	t.mu.Lock()
	t.stopped = true
	nodes := make(map[string]IORedisTransport, len(t.nodes))
	for address, node := range t.nodes {
		nodes[address] = node
	}
	t.mu.Unlock()
	for _, node := range nodes {
		if err := node.Close(); err != nil {
			failures++
		}
	}
	if failures > 0 {
		return fmt.Errorf("redis: %d Cluster node connections failed to close", failures)
	}
	return nil
}

func (t *ioredisClusterTransport) Exchange(ctx context.Context, commands [][]string) IORedisExchange {
	if t == nil || len(commands) == 0 {
		return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: errors.New("redis: empty Cluster exchange")}
	}
	type commandRoute struct {
		index   int
		command []string
	}
	groups := make(map[string][]commandRoute)
	order := make([]string, 0)
	for index, command := range commands {
		address, err := t.route(command)
		if err != nil {
			return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: newIORedisTerminalError(err)}
		}
		if _, exists := groups[address]; !exists {
			order = append(order, address)
		}
		groups[address] = append(groups[address], commandRoute{index: index, command: command})
	}
	// Sending separate fragments sequentially cannot preserve a single Redis
	// pipeline failure boundary: a later-node failure would either misassociate
	// its local replies with earlier commands or replay writes already executed
	// on the earlier node. Reject before any write and require callers to group
	// pipelines by Cluster node explicitly.
	if len(order) > 1 {
		return IORedisExchange{
			WriteDisposition: IORedisNotWritten,
			Error: newIORedisTerminalError(fmt.Errorf(
				"redis: ioredis Cluster pipeline spans %d nodes; split it by hash slot before submitting", len(order),
			)),
		}
	}
	replies := make([]IORedisReply, len(commands))
	for _, address := range order {
		node, err := t.nodeForAddress(ctx, address)
		if err != nil {
			return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: newIORedisTerminalError(err)}
		}
		routes := groups[address]
		nodeCommands := make([][]string, len(routes))
		for index := range routes {
			nodeCommands[index] = routes[index].command
		}
		exchange := node.Exchange(ctx, nodeCommands)
		if exchange.Error != nil {
			return exchange
		}
		if len(exchange.Replies) != len(routes) {
			return IORedisExchange{WriteDisposition: IORedisFullyWritten, MayHaveExecuted: true, Error: fmt.Errorf("redis: Cluster node returned %d replies for %d commands", len(exchange.Replies), len(routes))}
		}
		for index, route := range routes {
			replies[route.index] = exchange.Replies[index]
		}
		for index, route := range routes {
			if redirect, ok := parseIORedisClusterRedirect(exchange.Replies[index].Error); ok {
				if len(commands) == 1 {
					return t.followSingleRedirect(ctx, route.command, redirect)
				}
				// The node answered every command in the pipeline, so each reply is
				// authoritative: the redirected command did not execute and the
				// others report their own outcome. The connection is healthy and the
				// reply stream is aligned, so the aggregate transport is kept; only
				// ambiguous outcomes (ioredisRedirectFailure with mayHaveExecuted)
				// retire it.
				return IORedisExchange{
					Replies:          replies,
					WriteDisposition: IORedisFullyWritten,
					MayHaveExecuted:  len(routes) > 1,
					Error: newIORedisAuthoritativeRedirectError(
						errors.New("redis: ioredis Cluster pipeline redirect is not replayed automatically"), len(routes) > 1,
					),
				}
			}
		}
	}
	return IORedisExchange{Replies: replies, WriteDisposition: IORedisFullyWritten, MayHaveExecuted: true}
}

func (t *ioredisClusterTransport) route(command []string) (string, error) {
	if len(command) == 0 {
		return "", errors.New("redis: Cluster command is empty")
	}
	verb := strings.ToUpper(command[0])
	switch verb {
	case "SCAN", "KEYS", "FLUSHDB", "FLUSHALL", "RANDOMKEY":
		// Cluster-wide commands would silently cover only one node's keyspace.
		// The error names only the verb, never caller arguments.
		return "", fmt.Errorf(
			"redis: ioredis Cluster command %s is cluster-wide and unsupported; execute it explicitly on each intended node",
			verb,
		)
	}
	if ioredisClusterNodeCommand(verb) || verb == "SCRIPT" {
		return t.routeFirstNode()
	}
	if verb == "EVAL" || verb == "EVALSHA" {
		return t.routeEval(command, 2)
	}
	if verb == "FCALL" || verb == "FCALL_RO" {
		return t.routeEval(command, 2)
	}
	if verb == "XREAD" || verb == "XREADGROUP" {
		return t.routeXRead(command)
	}
	switch verb {
	case "OBJECT", "XGROUP", "XINFO":
		return t.routeSubcommandKey(command)
	case "MEMORY":
		if len(command) < 2 {
			return "", errors.New("redis: ioredis Cluster command MEMORY is missing its subcommand")
		}
		subcommand := strings.ToUpper(command[1])
		if subcommand == "USAGE" {
			return t.routeKeyAt(command, 2)
		}
		switch subcommand {
		case "DOCTOR", "HELP", "MALLOC-STATS", "PURGE", "STATS":
			return t.routeFirstNode()
		default:
			return "", errors.New("redis: ioredis Cluster command MEMORY has no supported subcommand routing rule")
		}
	case "BLPOP", "BRPOP", "BZPOPMIN", "BZPOPMAX":
		if len(command) < 3 {
			return "", fmt.Errorf("redis: ioredis Cluster command %s is missing keys or timeout", verb)
		}
		return t.routeKeys(command[1 : len(command)-1])
	case "BRPOPLPUSH", "BLMOVE":
		if len(command) < 2 {
			return "", fmt.Errorf("redis: ioredis Cluster command %s is missing its source key", verb)
		}
		return t.routeKey(command[1])
	case "BLMPOP", "BZMPOP":
		return t.routeCountedKeys(command, 2, 3)
	}
	if len(command) < 2 {
		return "", fmt.Errorf("redis: ioredis Cluster command %s has no routable key", verb)
	}
	return t.routeKey(command[1])
}

func (t *ioredisClusterTransport) routeFirstNode() (string, error) {
	t.mu.RLock()
	first := t.first
	t.mu.RUnlock()
	if first == "" {
		return "", errors.New("redis: ioredis Cluster has no available node")
	}
	return first, nil
}

func (t *ioredisClusterTransport) routeSubcommandKey(command []string) (string, error) {
	verb := strings.ToUpper(command[0])
	if len(command) < 2 {
		return "", fmt.Errorf("redis: ioredis Cluster command %s is missing its subcommand", verb)
	}
	subcommand := strings.ToUpper(command[1])
	if subcommand == "HELP" {
		return t.routeFirstNode()
	}
	if !ioredisClusterKeyedSubcommand(verb, subcommand) {
		return "", fmt.Errorf("redis: ioredis Cluster command %s has no supported subcommand routing rule", verb)
	}
	return t.routeKeyAt(command, 2)
}

func ioredisClusterKeyedSubcommand(verb, subcommand string) bool {
	switch verb {
	case "OBJECT":
		switch subcommand {
		case "ENCODING", "FREQ", "IDLETIME", "REFCOUNT":
			return true
		}
	case "XGROUP":
		switch subcommand {
		case "CREATE", "CREATECONSUMER", "DELCONSUMER", "DESTROY", "SETID":
			return true
		}
	case "XINFO":
		switch subcommand {
		case "CONSUMERS", "GROUPS", "STREAM":
			return true
		}
	}
	return false
}

func (t *ioredisClusterTransport) routeKeyAt(command []string, index int) (string, error) {
	verb := strings.ToUpper(command[0])
	if len(command) <= index {
		return "", fmt.Errorf("redis: ioredis Cluster command %s is missing its key", verb)
	}
	return t.routeKey(command[index])
}

func ioredisClusterNodeCommand(verb string) bool {
	switch verb {
	case "PING", "ECHO", "INFO", "TIME", "LASTSAVE", "ROLE", "COMMAND", "CLIENT", "CLUSTER", "DBSIZE":
		return true
	default:
		return false
	}
}

func (t *ioredisClusterTransport) routeEval(command []string, keyCountIndex int) (string, error) {
	verb := strings.ToUpper(command[0])
	if len(command) <= keyCountIndex {
		return "", fmt.Errorf("redis: ioredis Cluster command %s is missing its key count", verb)
	}
	keyCount, err := strconv.Atoi(command[keyCountIndex])
	if err != nil || keyCount < 0 {
		return "", fmt.Errorf("redis: ioredis Cluster command %s has an invalid key count", verb)
	}
	firstKeyIndex := keyCountIndex + 1
	if keyCount > len(command)-firstKeyIndex {
		return "", fmt.Errorf("redis: ioredis Cluster command %s declares %d keys but provides %d arguments", verb, keyCount, len(command)-firstKeyIndex)
	}
	if keyCount == 0 {
		return t.routeFirstNode()
	}
	firstSlot := ioredisClusterSlot(command[firstKeyIndex])
	for index := firstKeyIndex + 1; index < firstKeyIndex+keyCount; index++ {
		if slot := ioredisClusterSlot(command[index]); slot != firstSlot {
			return "", fmt.Errorf("redis: ioredis Cluster command %s keys span hash slots %d and %d", verb, firstSlot, slot)
		}
	}
	return t.routeSlot(firstSlot)
}

func (t *ioredisClusterTransport) routeXRead(command []string) (string, error) {
	verb := strings.ToUpper(command[0])
	streamsIndex, _, _, err := parseIORedisXReadPrefix(command)
	if err != nil {
		return "", err
	}
	remaining := len(command) - streamsIndex - 1
	if remaining%2 != 0 {
		return "", fmt.Errorf("redis: ioredis Cluster command %s has mismatched stream keys and IDs", verb)
	}
	keyCount := remaining / 2
	if keyCount == 0 {
		return "", fmt.Errorf("redis: ioredis Cluster command %s has no stream key", verb)
	}
	return t.routeKeys(command[streamsIndex+1 : streamsIndex+1+keyCount])
}

func (t *ioredisClusterTransport) routeCountedKeys(command []string, keyCountIndex, firstKeyIndex int) (string, error) {
	verb := strings.ToUpper(command[0])
	if len(command) <= keyCountIndex {
		return "", fmt.Errorf("redis: ioredis Cluster command %s is missing its key count", verb)
	}
	keyCount, err := strconv.Atoi(command[keyCountIndex])
	if err != nil || keyCount <= 0 || firstKeyIndex > len(command) || keyCount > len(command)-firstKeyIndex {
		return "", fmt.Errorf("redis: ioredis Cluster command %s has an invalid key count", verb)
	}
	return t.routeKeys(command[firstKeyIndex : firstKeyIndex+keyCount])
}

func (t *ioredisClusterTransport) routeKeys(keys []string) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("redis: ioredis Cluster command has no routable key")
	}
	firstSlot := ioredisClusterSlot(keys[0])
	for _, key := range keys[1:] {
		if slot := ioredisClusterSlot(key); slot != firstSlot {
			return "", fmt.Errorf("redis: ioredis Cluster command keys span hash slots %d and %d", firstSlot, slot)
		}
	}
	return t.routeSlot(firstSlot)
}

func (t *ioredisClusterTransport) routeKey(key string) (string, error) {
	return t.routeSlot(ioredisClusterSlot(key))
}

func (t *ioredisClusterTransport) routeSlot(slot int) (string, error) {
	t.mu.RLock()
	address := t.slots[slot]
	t.mu.RUnlock()
	if address == "" {
		return "", fmt.Errorf("redis: Cluster slot %d is not mapped", slot)
	}
	return address, nil
}

type ioredisClusterRedirectInfo struct {
	kind    string
	slot    int
	address string
}

const ioredisClusterMaxRedirects = 16

func parseIORedisClusterRedirect(err error) (ioredisClusterRedirectInfo, bool) {
	if err == nil {
		return ioredisClusterRedirectInfo{}, false
	}
	fields := strings.Fields(err.Error())
	if len(fields) != 3 {
		return ioredisClusterRedirectInfo{}, false
	}
	kind := strings.ToUpper(fields[0])
	if kind != "MOVED" && kind != "ASK" {
		return ioredisClusterRedirectInfo{}, false
	}
	slot, parseErr := strconv.Atoi(fields[1])
	if parseErr != nil || slot < 0 || slot >= 16384 || strings.TrimSpace(fields[2]) == "" {
		return ioredisClusterRedirectInfo{}, false
	}
	return ioredisClusterRedirectInfo{kind: kind, slot: slot, address: fields[2]}, true
}

func (t *ioredisClusterTransport) followSingleRedirect(ctx context.Context, command []string, redirect ioredisClusterRedirectInfo) IORedisExchange {
	for attempt := 0; attempt < ioredisClusterMaxRedirects; attempt++ {
		if redirect.kind == "ASK" {
			node, err := t.nodeForAddress(ctx, redirect.address)
			if err != nil {
				return ioredisRedirectFailure(err, false)
			}
			exchange := node.Exchange(ctx, [][]string{{"asking"}, command})
			if exchange.Error != nil {
				return ioredisRedirectFailure(exchange.Error, exchange.MayHaveExecuted)
			}
			if len(exchange.Replies) != 2 || exchange.Replies[0].Error != nil {
				// ASKING and the command share a pipeline. Redis can still process
				// the second item after rejecting the first, so side effects cannot
				// be ruled out here.
				return ioredisRedirectFailure(errors.New("redis: ASKING failed before the redirected command completed"), true)
			}
			if next, redirectedAgain := parseIORedisClusterRedirect(exchange.Replies[1].Error); redirectedAgain {
				redirect = next
				continue
			}
			return IORedisExchange{Replies: []IORedisReply{exchange.Replies[1]}, WriteDisposition: IORedisFullyWritten, MayHaveExecuted: true}
		}

		if err := t.refreshSlots(ctx, redirect.address); err != nil {
			return ioredisRedirectFailure(err, false)
		}
		address, err := t.route(command)
		if err != nil {
			return ioredisRedirectFailure(err, false)
		}
		node, err := t.nodeForAddress(ctx, address)
		if err != nil {
			return ioredisRedirectFailure(err, false)
		}
		exchange := node.Exchange(ctx, [][]string{command})
		if exchange.Error != nil {
			return ioredisRedirectFailure(exchange.Error, exchange.MayHaveExecuted)
		}
		if len(exchange.Replies) != 1 {
			return ioredisRedirectFailure(fmt.Errorf("redis: redirected Cluster node returned %d replies for one command", len(exchange.Replies)), true)
		}
		if next, redirectedAgain := parseIORedisClusterRedirect(exchange.Replies[0].Error); redirectedAgain {
			redirect = next
			continue
		}
		return IORedisExchange{Replies: exchange.Replies, WriteDisposition: IORedisFullyWritten, MayHaveExecuted: true}
	}
	// Every attempt ended in an authoritative redirect response, so the command
	// bytes were written but command side effects are known not to have occurred.
	return ioredisRedirectFailure(errors.New("redis: Cluster redirect limit reached"), false)
}

func ioredisRedirectFailure(err error, mayHaveExecuted bool) IORedisExchange {
	safe := newIORedisSafeError("redis: ioredis Cluster redirect could not be completed safely", err)
	return IORedisExchange{
		WriteDisposition: IORedisFullyWritten,
		MayHaveExecuted:  mayHaveExecuted,
		Error:            newIORedisRedirectOutcomeError(safe, mayHaveExecuted),
	}
}

func (t *ioredisClusterTransport) refreshSlots(ctx context.Context, address string) error {
	node, err := t.nodeForAddress(ctx, address)
	if err != nil {
		return err
	}
	exchange := node.Exchange(ctx, [][]string{{"cluster", "slots"}})
	if exchange.Error != nil {
		return newIORedisSafeError("redis: CLUSTER SLOTS refresh failed", exchange.Error)
	}
	if len(exchange.Replies) != 1 {
		return fmt.Errorf("redis: CLUSTER SLOTS returned %d replies", len(exchange.Replies))
	}
	if exchange.Replies[0].Error != nil {
		return newIORedisSafeError("redis: CLUSTER SLOTS refresh was rejected", exchange.Replies[0].Error)
	}
	ranges, err := parseIORedisClusterSlots(exchange.Replies[0].Value, address)
	if err != nil {
		return err
	}
	var slots [16384]string
	first := ""
	for _, slotRange := range ranges {
		if first == "" {
			first = slotRange.address
		}
		for slot := slotRange.first; slot <= slotRange.last; slot++ {
			slots[slot] = slotRange.address
		}
	}
	t.mu.Lock()
	t.slots = slots
	t.first = first
	t.mu.Unlock()
	return nil
}

func (t *ioredisClusterTransport) nodeForAddress(ctx context.Context, address string) (IORedisTransport, error) {
	t.mu.RLock()
	stopped := t.stopped
	node := t.nodes[address]
	t.mu.RUnlock()
	if stopped {
		return nil, IORedisConnectionClosedError{}
	}
	if node != nil {
		return node, nil
	}
	if t.factory == nil {
		return nil, errors.New("redis: Cluster node is not connected")
	}
	connected, err := t.factory.connectNode(ctx, address)
	if err != nil {
		return nil, newIORedisSafeError("redis: Cluster node connection failed", err)
	}
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		_ = connected.Close()
		return nil, IORedisConnectionClosedError{}
	}
	if existing := t.nodes[address]; existing != nil {
		t.mu.Unlock()
		_ = connected.Close()
		return existing, nil
	}
	t.nodes[address] = connected
	t.mu.Unlock()
	go t.watch(connected)
	return connected, nil
}

// ioredisClusterSlot implements Redis Cluster's CRC16/XMODEM hash-slot rule,
// including the first non-empty {...} hash tag.
func ioredisClusterSlot(key string) int {
	if open := strings.IndexByte(key, '{'); open >= 0 {
		if closeIndex := strings.IndexByte(key[open+1:], '}'); closeIndex > 0 {
			key = key[open+1 : open+1+closeIndex]
		}
	}
	var crc uint16
	for index := 0; index < len(key); index++ {
		crc ^= uint16(key[index]) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return int(crc % 16384)
}
