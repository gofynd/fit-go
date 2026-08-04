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
	SentinelAddrs    []string
	MasterName       string
	Username         string
	Password         string
	SentinelUsername string
	SentinelPassword string
	DB               int
	ConnectionName   string
	TLSConfig        *tls.Config
	ConnectTimeout   time.Duration
	SocketTimeout    time.Duration
	KeepAlive        time.Duration
}

type ioredisClusterOptions struct {
	SeedAddrs      []string
	Username       string
	Password       string
	DB             int
	ConnectionName string
	TLSConfig      *tls.Config
	ConnectTimeout time.Duration
	SocketTimeout  time.Duration
	KeepAlive      time.Duration
}

func dialIORedisCompatibleSentinel(ctx context.Context, profile IORedisCompatibilityProfile, options ioredisSentinelOptions) (Connection, error) {
	if profile != IORedisCompatibilityV4 {
		return nil, fmt.Errorf("redis: unsupported ioredis compatibility profile %q", profile)
	}
	factory := &ioredisSentinelFactory{options: options}
	client, err := NewSlingshotIORedisCompatClientReady(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &ioredisConnection{client: client}, nil
}

type ioredisSentinelFactory struct {
	options ioredisSentinelOptions
}

func (f *ioredisSentinelFactory) Connect(ctx context.Context) (SlingshotIORedisTransport, error) {
	if f == nil || len(f.options.SentinelAddrs) == 0 || strings.TrimSpace(f.options.MasterName) == "" {
		return nil, errors.New("redis: ioredis Sentinel addresses and master name are required")
	}
	var failures []string
	for _, address := range f.options.SentinelAddrs {
		sentinel, err := connectIORedisRESP(ctx, IORedisRESPOptions{
			Addr:              address,
			Username:          f.options.SentinelUsername,
			Password:          f.options.SentinelPassword,
			ConnectionName:    f.options.ConnectionName,
			TLSConfig:         f.options.TLSConfig,
			ConnectTimeout:    f.options.ConnectTimeout,
			SocketTimeout:     f.options.SocketTimeout,
			KeepAlive:         f.options.KeepAlive,
			DisableClientInfo: true,
		})
		if err != nil {
			failures = append(failures, address+": "+err.Error())
			continue
		}
		exchange := sentinel.Exchange(ctx, [][]string{{"sentinel", "get-master-addr-by-name", f.options.MasterName}})
		_ = sentinel.Close()
		if exchange.Error != nil {
			failures = append(failures, address+": "+exchange.Error.Error())
			continue
		}
		if len(exchange.Replies) != 1 || exchange.Replies[0].Error != nil {
			var replyErr error
			if len(exchange.Replies) == 1 {
				replyErr = exchange.Replies[0].Error
			}
			failures = append(failures, address+": "+fmt.Sprint(replyErr))
			continue
		}
		masterAddress, err := ioredisSentinelMasterAddress(exchange.Replies[0].Value)
		if err != nil {
			failures = append(failures, address+": "+err.Error())
			continue
		}
		return connectIORedisRESP(ctx, IORedisRESPOptions{
			Addr:              masterAddress,
			Username:          f.options.Username,
			Password:          f.options.Password,
			DB:                f.options.DB,
			ConnectionName:    f.options.ConnectionName,
			TLSConfig:         f.options.TLSConfig,
			ConnectTimeout:    f.options.ConnectTimeout,
			SocketTimeout:     f.options.SocketTimeout,
			KeepAlive:         f.options.KeepAlive,
			DisableClientInfo: true,
		})
	}
	return nil, fmt.Errorf("redis: no Sentinel could resolve master %q: %s", f.options.MasterName, strings.Join(failures, "; "))
}

func ioredisSentinelMasterAddress(value any) (string, error) {
	parts, ok := value.([]any)
	if !ok || len(parts) != 2 {
		return "", fmt.Errorf("redis: invalid Sentinel master reply %T", value)
	}
	host, ok := parts[0].(string)
	if !ok || strings.TrimSpace(host) == "" {
		return "", fmt.Errorf("redis: invalid Sentinel master host %v", parts[0])
	}
	port, ok := ioredisReplyInt(parts[1])
	if !ok || port <= 0 || port > 65535 {
		return "", fmt.Errorf("redis: invalid Sentinel master port %v", parts[1])
	}
	return host + ":" + strconv.FormatInt(port, 10), nil
}

func dialIORedisCompatibleCluster(ctx context.Context, profile IORedisCompatibilityProfile, options ioredisClusterOptions) (Connection, error) {
	if profile != IORedisCompatibilityV4 {
		return nil, fmt.Errorf("redis: unsupported ioredis compatibility profile %q", profile)
	}
	factory := &ioredisClusterFactory{options: options}
	client, err := NewSlingshotIORedisCompatClientReady(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &ioredisConnection{client: client, cluster: true}, nil
}

type ioredisClusterFactory struct {
	options ioredisClusterOptions
}

func (f *ioredisClusterFactory) Connect(ctx context.Context) (SlingshotIORedisTransport, error) {
	if f == nil || len(f.options.SeedAddrs) == 0 {
		return nil, errors.New("redis: ioredis Cluster seed address is required")
	}
	var failures []string
	for _, address := range f.options.SeedAddrs {
		seed, err := f.connectNode(ctx, address)
		if err != nil {
			failures = append(failures, address+": "+err.Error())
			continue
		}
		exchange := seed.Exchange(ctx, [][]string{{"cluster", "slots"}})
		_ = seed.Close()
		if exchange.Error != nil {
			failures = append(failures, address+": "+exchange.Error.Error())
			continue
		}
		if len(exchange.Replies) != 1 || exchange.Replies[0].Error != nil {
			var replyErr error
			if len(exchange.Replies) == 1 {
				replyErr = exchange.Replies[0].Error
			}
			failures = append(failures, address+": "+fmt.Sprint(replyErr))
			continue
		}
		ranges, err := parseIORedisClusterSlots(exchange.Replies[0].Value, address)
		if err != nil {
			failures = append(failures, address+": "+err.Error())
			continue
		}
		return newIORedisClusterTransport(ctx, f, ranges)
	}
	return nil, fmt.Errorf("redis: no Cluster seed returned slots: %s", strings.Join(failures, "; "))
}

func (f *ioredisClusterFactory) connectNode(ctx context.Context, address string) (SlingshotIORedisTransport, error) {
	return connectIORedisRESP(ctx, IORedisRESPOptions{
		Addr:              address,
		Username:          f.options.Username,
		Password:          f.options.Password,
		DB:                f.options.DB,
		ConnectionName:    f.options.ConnectionName,
		TLSConfig:         f.options.TLSConfig,
		ConnectTimeout:    f.options.ConnectTimeout,
		SocketTimeout:     f.options.SocketTimeout,
		KeepAlive:         f.options.KeepAlive,
		DisableClientInfo: true,
	})
}

func connectIORedisRESP(ctx context.Context, options IORedisRESPOptions) (SlingshotIORedisTransport, error) {
	factory, err := NewSlingshotIORedisRESPTransportFactory(options)
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
		return nil, fmt.Errorf("redis: invalid CLUSTER SLOTS reply %T", value)
	}
	ranges := make([]ioredisClusterSlotRange, 0, len(rows))
	for _, rawRow := range rows {
		row, ok := rawRow.([]any)
		if !ok || len(row) < 3 {
			return nil, fmt.Errorf("redis: invalid CLUSTER SLOTS row %v", rawRow)
		}
		first, firstOK := ioredisReplyInt(row[0])
		last, lastOK := ioredisReplyInt(row[1])
		node, nodeOK := row[2].([]any)
		if !firstOK || !lastOK || !nodeOK || len(node) < 2 || first < 0 || last < first || last >= 16384 {
			return nil, fmt.Errorf("redis: invalid CLUSTER SLOTS row %v", rawRow)
		}
		host, hostOK := node[0].(string)
		port, portOK := ioredisReplyInt(node[1])
		if !hostOK || strings.TrimSpace(host) == "" {
			seedHost, _, splitErr := net.SplitHostPort(seedAddress)
			if splitErr != nil || strings.TrimSpace(seedHost) == "" {
				return nil, fmt.Errorf("redis: invalid Cluster seed address %q: %w", seedAddress, splitErr)
			}
			host = seedHost
		}
		if !portOK || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("redis: invalid CLUSTER SLOTS node %v", node)
		}
		ranges = append(ranges, ioredisClusterSlotRange{
			first: int(first), last: int(last), address: host + ":" + strconv.FormatInt(port, 10),
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
	factory    *ioredisClusterFactory
	nodes      map[string]SlingshotIORedisTransport
	slots      [16384]string
	first      string
	closed     chan struct{}
	stop       chan struct{}
	closedOnce sync.Once
	stopOnce   sync.Once
}

func newIORedisClusterTransport(ctx context.Context, factory *ioredisClusterFactory, ranges []ioredisClusterSlotRange) (*ioredisClusterTransport, error) {
	transport := &ioredisClusterTransport{
		factory: factory, nodes: make(map[string]SlingshotIORedisTransport),
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
			return nil, err
		}
		transport.nodes[slotRange.address] = node
		go transport.watch(node)
	}
	if transport.first == "" || len(transport.nodes) == 0 {
		return nil, errors.New("redis: CLUSTER SLOTS returned no master nodes")
	}
	return transport, nil
}

func (t *ioredisClusterTransport) watch(node SlingshotIORedisTransport) {
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
	var failures []string
	t.stopOnce.Do(func() { close(t.stop) })
	t.closedOnce.Do(func() { close(t.closed) })
	for address, node := range t.nodes {
		if err := node.Close(); err != nil {
			failures = append(failures, address+": "+err.Error())
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func (t *ioredisClusterTransport) Exchange(ctx context.Context, commands [][]string) SlingshotIORedisExchange {
	if t == nil || len(commands) == 0 {
		return SlingshotIORedisExchange{WriteDisposition: SlingshotIORedisNotWritten, Error: errors.New("redis: empty Cluster exchange")}
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
			return SlingshotIORedisExchange{WriteDisposition: SlingshotIORedisNotWritten, Error: err}
		}
		if _, exists := groups[address]; !exists {
			order = append(order, address)
		}
		groups[address] = append(groups[address], commandRoute{index: index, command: command})
	}
	replies := make([]SlingshotIORedisReply, len(commands))
	for _, address := range order {
		node := t.nodes[address]
		if node == nil {
			return SlingshotIORedisExchange{WriteDisposition: SlingshotIORedisNotWritten, Error: fmt.Errorf("redis: Cluster node %s is not connected", address)}
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
			return SlingshotIORedisExchange{WriteDisposition: SlingshotIORedisFullyWritten, MayHaveExecuted: true, Error: fmt.Errorf("redis: Cluster node returned %d replies for %d commands", len(exchange.Replies), len(routes))}
		}
		for index, route := range routes {
			if ioredisClusterRedirect(exchange.Replies[index].Error) {
				return SlingshotIORedisExchange{WriteDisposition: SlingshotIORedisFullyWritten, Error: exchange.Replies[index].Error}
			}
			replies[route.index] = exchange.Replies[index]
		}
	}
	return SlingshotIORedisExchange{Replies: replies, WriteDisposition: SlingshotIORedisFullyWritten, MayHaveExecuted: true}
}

func (t *ioredisClusterTransport) route(command []string) (string, error) {
	if len(command) == 0 {
		return "", errors.New("redis: Cluster command is empty")
	}
	verb := strings.ToUpper(command[0])
	if verb == "PING" {
		return t.first, nil
	}
	if len(command) < 2 {
		return "", fmt.Errorf("redis: Cluster command %s has no key", verb)
	}
	slot := ioredisClusterSlot(command[1])
	address := t.slots[slot]
	if address == "" {
		return "", fmt.Errorf("redis: Cluster slot %d is not mapped", slot)
	}
	return address, nil
}

func ioredisClusterRedirect(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToUpper(err.Error())
	return strings.HasPrefix(message, "MOVED ") || strings.HasPrefix(message, "ASK ")
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
