// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIORedisRESPDecoderRejectsResourceExhaustionFrames(t *testing.T) {
	tests := []struct {
		name string
		wire string
		want string
	}{
		{
			name: "bulk length",
			wire: fmt.Sprintf("$%d\r\n", ioredisRESPMaxBulkBytes+1),
			want: "RESP bulk length",
		},
		{
			name: "array length",
			wire: fmt.Sprintf("*%d\r\n", ioredisRESPMaxArrayElements+1),
			want: "RESP array length",
		},
		{
			name: "nesting depth",
			wire: strings.Repeat("*1\r\n", ioredisRESPMaxDepth+2) + "+OK\r\n",
			want: "RESP nesting depth",
		},
		{
			name: "line length",
			wire: "+" + strings.Repeat("x", ioredisRESPMaxLineBytes) + "\r\n",
			want: "RESP line exceeds",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := readIORedisRESPValue(bufio.NewReader(strings.NewReader(test.wire)))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("decoder error = %v, want substring %q", err, test.want)
			}
		})
	}

	budget := &ioredisRESPDecodeBudget{bytesRemaining: 7, valuesRemaining: 2}
	if _, _, err := readIORedisRESPValueWithBudget(bufio.NewReader(strings.NewReader("$3\r\nfoo\r\n")), budget, 0); err == nil || !strings.Contains(err.Error(), "aggregate size limit") {
		t.Fatalf("aggregate budget error = %v", err)
	}
}

func TestIORedisRESPDeclaredPayloadsAllocateIncrementally(t *testing.T) {
	for _, wire := range []string{
		fmt.Sprintf("$%d\r\n", ioredisRESPMaxBulkBytes),
		fmt.Sprintf("*%d\r\n", ioredisRESPMaxArrayElements-1),
	} {
		_, _, err := readIORedisRESPValue(bufio.NewReader(strings.NewReader(wire)))
		if !errors.Is(err, io.EOF) {
			t.Fatalf("header-only frame error = %v; want EOF without eager allocation", err)
		}
	}
}

func TestIORedisRejectsConnectionModeCommandsBeforeQueueing(t *testing.T) {
	transport := newIORedisSafetyTransport(func(_ [][]string) IORedisExchange {
		return IORedisExchange{Replies: []IORedisReply{{Value: "OK"}}, WriteDisposition: IORedisFullyWritten}
	})
	client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		return transport, nil
	}))
	if err != nil {
		t.Fatalf("NewIORedisCompatClientReady: %v", err)
	}
	defer client.Disconnect()

	commands := [][]string{
		{"SUBSCRIBE", "one", "two"},
		{"MONITOR"},
		{"CLIENT", "REPLY", "OFF"},
		{"BLPOP", "queue", "0"},
		{"XREAD", "BLOCK", "0", "STREAMS", "events", "$"},
		{"MULTI"},
		{"HELLO", "3"},
	}
	for _, command := range commands {
		result, submitErr := waitIORedisFuture(t, client.Submit(command...))
		if submitErr == nil || !strings.Contains(submitErr.Error(), "does not support") {
			t.Fatalf("%v result = %#v, error = %v; want local unsupported error", command, result, submitErr)
		}
	}
	if got := transport.exchanges.Load(); got != 0 {
		t.Fatalf("unsupported commands reached transport %d times", got)
	}
	assertIORedisFutureOK(t, client.Submit("SET", "safe", "1"), "OK", 0, 0)
	assertIORedisFutureOK(t, client.Submit("BLPOP", "queue", "0.01"), "OK", 0, 0)
	assertIORedisFutureOK(t, client.Submit("XREAD", "STREAMS", "block", "0"), "OK", 0, 0)
	assertIORedisFutureOK(t, client.Submit("XREAD", "BLOCK", "10", "STREAMS", "events", "$"), "OK", 0, 0)
	assertIORedisFutureOK(t, client.Submit("HELLO", "2"), "OK", 0, 0)
}

func TestIORedisTerminalRoutingErrorSettlesWithoutReconnectOrQueueStall(t *testing.T) {
	transport := newIORedisSafetyTransport(func(commands [][]string) IORedisExchange {
		if strings.EqualFold(commands[0][0], "BADROUTE") {
			return IORedisExchange{WriteDisposition: IORedisNotWritten, Error: newIORedisTerminalError(errors.New("no route"))}
		}
		return IORedisExchange{Replies: []IORedisReply{{Value: "after"}}, WriteDisposition: IORedisFullyWritten}
	})
	var connects atomic.Int32
	client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		connects.Add(1)
		return transport, nil
	}))
	if err != nil {
		t.Fatalf("NewIORedisCompatClientReady: %v", err)
	}
	defer client.Disconnect()

	bad := client.Submit("BADROUTE")
	after := client.Submit("GET", "key")
	if _, err := waitIORedisFuture(t, bad); err == nil || err.Error() != "no route" {
		t.Fatalf("routing error = %v, want no route", err)
	}
	assertIORedisFutureOK(t, after, "after", 0, 0)
	if got := connects.Load(); got != 1 {
		t.Fatalf("factory connections = %d, want one", got)
	}
}

func TestIORedisTerminalPipelineErrorPreservesCompletedReplies(t *testing.T) {
	transport := newIORedisSafetyTransport(func([][]string) IORedisExchange {
		return IORedisExchange{
			Replies: []IORedisReply{
				{Value: int64(1)},
				{Error: errors.New("MOVED 42 redis-b:6379")},
			},
			WriteDisposition: IORedisFullyWritten,
			MayHaveExecuted:  true,
			Error:            newIORedisRedirectError(errors.New("pipeline redirect is not replayed")),
		}
	})
	client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		return transport, nil
	}))
	if err != nil {
		t.Fatalf("NewIORedisCompatClientReady: %v", err)
	}
	defer client.Disconnect()

	result, waitErr := waitIORedisFuture(t, client.SubmitPipeline(
		[]string{"INCR", "{orders}one"}, []string{"INCR", "{orders}two"},
	))
	if waitErr != nil {
		t.Fatalf("pipeline terminal error escaped the per-reply contract: %v", waitErr)
	}
	if len(result.Replies) != 2 || result.Replies[0].Value != int64(1) || result.Replies[1].Error == nil {
		t.Fatalf("terminal pipeline replies = %#v; want completed prefix and redirect error", result.Replies)
	}
}

func TestIORedisClusterRoutingAndPipelineSafety(t *testing.T) {
	first := newIORedisSafetyTransport(func(commands [][]string) IORedisExchange {
		replies := make([]IORedisReply, len(commands))
		for index := range replies {
			replies[index].Value = "first"
		}
		return IORedisExchange{Replies: replies, WriteDisposition: IORedisFullyWritten}
	})
	second := newIORedisSafetyTransport(func(commands [][]string) IORedisExchange {
		replies := make([]IORedisReply, len(commands))
		for index := range replies {
			replies[index].Value = "second"
		}
		return IORedisExchange{Replies: replies, WriteDisposition: IORedisFullyWritten}
	})
	cluster := &ioredisClusterTransport{
		nodes:  map[string]IORedisTransport{"first": first, "second": second},
		first:  "first",
		closed: make(chan struct{}),
		stop:   make(chan struct{}),
	}
	for slot := range cluster.slots {
		if slot < len(cluster.slots)/2 {
			cluster.slots[slot] = "first"
		} else {
			cluster.slots[slot] = "second"
		}
	}

	firstKey := keyInTopologySlotRange(0, 8191)
	secondKey := keyInTopologySlotRange(8192, 16383)
	address, err := cluster.route([]string{"EVAL", "return redis.call('GET',KEYS[1])", "1", secondKey})
	if err != nil || address != "second" {
		t.Fatalf("EVAL route = (%q, %v), want second key's node", address, err)
	}
	if address, err = cluster.route([]string{"INFO"}); err != nil || address != "first" {
		t.Fatalf("INFO route = (%q, %v), want first node", address, err)
	}
	if address, err = cluster.route([]string{"SCRIPT", "LOAD", "return 1"}); err != nil || address != "first" {
		t.Fatalf("SCRIPT LOAD route = (%q, %v), want first node", address, err)
	}
	if address, err = cluster.route([]string{"DBSIZE"}); err != nil || address != "first" {
		t.Fatalf("DBSIZE route = (%q, %v), want first node", address, err)
	}
	if address, err = cluster.route([]string{"XREAD", "STREAMS", "block", "0"}); err != nil {
		t.Fatalf("XREAD stream named block route error = %v", err)
	} else if want, routeErr := cluster.routeKey("block"); routeErr != nil || address != want {
		t.Fatalf("XREAD stream named block route = %q, want %q (error %v)", address, want, routeErr)
	}
	for _, command := range [][]string{{"DBSIZE"}, {"SCRIPT", "LOAD", "return 1"}} {
		exchange := cluster.Exchange(context.Background(), [][]string{command})
		if exchange.Error != nil || len(exchange.Replies) != 1 || exchange.Replies[0].Value != "first" {
			t.Fatalf("node command %v exchange = %+v", command, exchange)
		}
	}
	sameNode := cluster.Exchange(context.Background(), [][]string{{"GET", "{same}one"}, {"GET", "{same}two"}})
	if sameNode.Error != nil || len(sameNode.Replies) != 2 || sameNode.Replies[0].Value != sameNode.Replies[1].Value {
		t.Fatalf("same-node pipeline exchange = %+v", sameNode)
	}
	firstBeforeCross, secondBeforeCross := first.exchanges.Load(), second.exchanges.Load()
	if firstBeforeCross+secondBeforeCross != 3 {
		t.Fatalf("node command and same-node pipeline exchanges: first=%d second=%d, want three", firstBeforeCross, secondBeforeCross)
	}

	exchange := cluster.Exchange(context.Background(), [][]string{{"INCR", firstKey}, {"INCR", secondKey}})
	if exchange.Error == nil || !strings.Contains(exchange.Error.Error(), "spans 2 nodes") || exchange.WriteDisposition != IORedisNotWritten {
		t.Fatalf("cross-node pipeline exchange = %+v", exchange)
	}
	if first.exchanges.Load() != firstBeforeCross || second.exchanges.Load() != secondBeforeCross {
		t.Fatalf("cross-node pipeline reached nodes: first=%d second=%d", first.exchanges.Load(), second.exchanges.Load())
	}
	for _, keyless := range [][]string{{"DBSIZE"}, {"SCRIPT", "LOAD", "return 1"}} {
		exchange = cluster.Exchange(context.Background(), [][]string{keyless, {"GET", secondKey}})
		if exchange.Error == nil || !strings.Contains(exchange.Error.Error(), "spans 2 nodes") || exchange.WriteDisposition != IORedisNotWritten {
			t.Fatalf("keyless/keyed cross-node pipeline exchange = %+v", exchange)
		}
		if first.exchanges.Load() != firstBeforeCross || second.exchanges.Load() != secondBeforeCross {
			t.Fatalf("keyless/keyed pipeline reached nodes: first=%d second=%d", first.exchanges.Load(), second.exchanges.Load())
		}
	}
}

func TestIORedisRESPProtocolLimitSettlesWithoutReconnectLoop(t *testing.T) {
	var oversized atomic.Int32
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "GET":
			oversized.Add(1)
			return fmt.Sprintf("*%d\r\n", ioredisRESPMaxArrayElements+1), false
		case "SET":
			return "+OK\r\n", false
		default:
			return "-ERR unexpected\r\n", false
		}
	})
	defer server.stop()
	client, err := NewIORedisRESPCompatClientReady(context.Background(), IORedisRESPOptions{
		Addr: server.addr, DisableClientInfo: true, DisableReadyCheck: true,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Disconnect()

	if _, err := waitIORedisFuture(t, client.Submit("GET", "oversized")); err == nil || !strings.Contains(err.Error(), "RESP array length") {
		t.Fatalf("oversized reply error = %v", err)
	}
	if got := oversized.Load(); got != 1 {
		t.Fatalf("oversized command executions = %d, want one without reconnect replay", got)
	}
	assertIORedisFutureOK(t, client.Submit("SET", "later", "1"), "OK", 0, 0)
}

func TestIORedisClusterSingleCommandMOVEDRefreshesAndReplays(t *testing.T) {
	var target *ioredisRESPScenarioServer
	target = startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "INFO":
			return "$11\r\nloading:0\r\n\r\n", false
		case "CLUSTER":
			host, port := splitTopologyAddress(t, target.addr)
			return encodeTopologyRESP([]any{
				[]any{int64(0), int64(16383), []any{host, int64(port), "target"}},
			}), false
		case "GET":
			return encodeTopologyRESP("moved-value"), false
		default:
			return "-ERR unexpected\r\n", false
		}
	})
	defer target.stop()

	key := "moved-key"
	slot := ioredisClusterSlot(key)
	source := newIORedisSafetyTransport(func(_ [][]string) IORedisExchange {
		return IORedisExchange{
			Replies:          []IORedisReply{{Error: fmt.Errorf("MOVED %d %s", slot, target.addr)}},
			WriteDisposition: IORedisFullyWritten,
		}
	})
	cluster := newIORedisRedirectTestCluster(source, &ioredisClusterFactory{options: ioredisClusterOptions{
		DisableClientInfo: true,
	}})
	defer cluster.Close()

	exchange := cluster.Exchange(context.Background(), [][]string{{"GET", key}})
	if exchange.Error != nil || len(exchange.Replies) != 1 || exchange.Replies[0].Value != "moved-value" {
		t.Fatalf("MOVED exchange = %+v", exchange)
	}
	if got := source.exchanges.Load(); got != 1 {
		t.Fatalf("source exchanges = %d, want one", got)
	}
	if got := target.commandNames(); !reflect.DeepEqual(got, []string{"INFO", "CLUSTER", "GET"}) {
		t.Fatalf("MOVED target commands = %v", got)
	}
}

func TestIORedisClusterSingleCommandASKUsesAsking(t *testing.T) {
	target := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "INFO":
			return "$11\r\nloading:0\r\n\r\n", false
		case "ASKING":
			return "+OK\r\n", false
		case "GET":
			return encodeTopologyRESP("ask-value"), false
		default:
			return "-ERR unexpected\r\n", false
		}
	})
	defer target.stop()

	key := "ask-key"
	slot := ioredisClusterSlot(key)
	source := newIORedisSafetyTransport(func(_ [][]string) IORedisExchange {
		return IORedisExchange{
			Replies:          []IORedisReply{{Error: fmt.Errorf("ASK %d %s", slot, target.addr)}},
			WriteDisposition: IORedisFullyWritten,
		}
	})
	cluster := newIORedisRedirectTestCluster(source, &ioredisClusterFactory{options: ioredisClusterOptions{
		DisableClientInfo: true,
	}})
	defer cluster.Close()

	exchange := cluster.Exchange(context.Background(), [][]string{{"GET", key}})
	if exchange.Error != nil || len(exchange.Replies) != 1 || exchange.Replies[0].Value != "ask-value" {
		t.Fatalf("ASK exchange = %+v", exchange)
	}
	if got := source.exchanges.Load(); got != 1 {
		t.Fatalf("source exchanges = %d, want one", got)
	}
	if got := target.commandNames(); !reflect.DeepEqual(got, []string{"INFO", "ASKING", "GET"}) {
		t.Fatalf("ASK target commands = %v", got)
	}
}

func TestIORedisClusterPipelineRedirectIsNeverReplayed(t *testing.T) {
	keyOne, keyTwo := "{pipeline}one", "{pipeline}two"
	slot := ioredisClusterSlot(keyOne)
	source := newIORedisSafetyTransport(func(commands [][]string) IORedisExchange {
		if len(commands) != 2 {
			return IORedisExchange{Error: fmt.Errorf("unexpected command count %d", len(commands))}
		}
		return IORedisExchange{
			Replies: []IORedisReply{
				{Value: "already-executed"},
				{Error: fmt.Errorf("MOVED %d 127.0.0.1:7002", slot)},
			},
			WriteDisposition: IORedisFullyWritten,
			MayHaveExecuted:  true,
		}
	})
	cluster := newIORedisRedirectTestCluster(source, nil)
	defer cluster.Close()

	exchange := cluster.Exchange(context.Background(), [][]string{{"INCR", keyOne}, {"INCR", keyTwo}})
	if exchange.Error == nil || !strings.Contains(exchange.Error.Error(), "not replayed automatically") || !exchange.MayHaveExecuted {
		t.Fatalf("pipeline redirect exchange = %+v", exchange)
	}
	if len(exchange.Replies) != 2 || exchange.Replies[0].Value != "already-executed" || exchange.Replies[1].Error == nil {
		t.Fatalf("pipeline redirect lost completed replies: %+v", exchange.Replies)
	}
	if got := source.exchanges.Load(); got != 1 {
		t.Fatalf("pipeline exchanges = %d, want one without replay", got)
	}
}

type blockingClusterNodeFactory struct {
	started   chan struct{}
	release   chan struct{}
	transport IORedisTransport
}

func (f *blockingClusterNodeFactory) connectNode(context.Context, string) (IORedisTransport, error) {
	close(f.started)
	<-f.release
	return f.transport, nil
}

func TestIORedisClusterConnectionFinishingDuringCloseIsNotLeaked(t *testing.T) {
	connected := newIORedisSafetyTransport(func([][]string) IORedisExchange { return IORedisExchange{} })
	factory := &blockingClusterNodeFactory{
		started: make(chan struct{}), release: make(chan struct{}), transport: connected,
	}
	cluster := &ioredisClusterTransport{
		factory: factory, nodes: make(map[string]IORedisTransport),
		closed: make(chan struct{}), stop: make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() {
		_, err := cluster.nodeForAddress(context.Background(), "new-node")
		result <- err
	}()
	<-factory.started
	if err := cluster.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(factory.release)
	if err := <-result; err == nil {
		t.Fatal("connection completed during Close without a closed error")
	}
	select {
	case <-connected.Closed():
	default:
		t.Fatal("connection completed during Close was leaked")
	}
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	if len(cluster.nodes) != 0 {
		t.Fatalf("closed cluster retained %d late nodes", len(cluster.nodes))
	}
}

func newIORedisRedirectTestCluster(source IORedisTransport, factory *ioredisClusterFactory) *ioredisClusterTransport {
	cluster := &ioredisClusterTransport{
		nodes:  map[string]IORedisTransport{"source": source},
		first:  "source",
		closed: make(chan struct{}),
		stop:   make(chan struct{}),
	}
	if factory != nil {
		cluster.factory = factory
	}
	for slot := range cluster.slots {
		cluster.slots[slot] = "source"
	}
	return cluster
}

func TestIORedisPerRequestRetryBudgetSurvivesSuccessfulReconnect(t *testing.T) {
	var failSlowCommand atomic.Bool
	failSlowCommand.Store(true)
	var exchanges atomic.Int32
	var connects atomic.Int32
	factory := IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		connects.Add(1)
		return newIORedisSafetyTransport(func(_ [][]string) IORedisExchange {
			exchanges.Add(1)
			if failSlowCommand.Load() {
				return IORedisExchange{
					WriteDisposition: IORedisFullyWritten,
					MayHaveExecuted:  true,
					Error:            errors.New("Socket timeout. Expecting data, but didn't receive any in 5ms."),
				}
			}
			return IORedisExchange{Replies: []IORedisReply{{Value: "recovered"}}, WriteDisposition: IORedisFullyWritten}
		}), nil
	})
	client := newFastIORedisClient(t, factory)
	defer client.Disconnect()

	result, err := waitIORedisFuture(t, client.Submit("GET", "slow"))
	var maxRetries IORedisMaxRetriesError
	if !errors.As(err, &maxRetries) {
		t.Fatalf("slow command error = %T %v, want max retries", err, err)
	}
	if result.ReplayCount != IORedisMaxRetriesPerRequest+1 || result.AmbiguousReplays != IORedisMaxRetriesPerRequest+1 {
		t.Fatalf("slow command replay counters = %+v", result)
	}
	if got := exchanges.Load(); got != IORedisMaxRetriesPerRequest+1 {
		t.Fatalf("slow command exchanges = %d, want %d", got, IORedisMaxRetriesPerRequest+1)
	}

	failSlowCommand.Store(false)
	assertIORedisFutureOK(t, client.Submit("GET", "independent"), "recovered", 0, 0)
	if got := connects.Load(); got < IORedisMaxRetriesPerRequest+1 {
		t.Fatalf("connections = %d, want successful reconnects to have occurred without resetting request budget", got)
	}
}

func TestIORedisBoundedBlockingSocketTimeoutIsNotReplayed(t *testing.T) {
	var slowBlockingCalls atomic.Int32
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "BLPOP":
			if len(command) > 1 && command[1] == "fast" {
				time.Sleep(2 * time.Millisecond)
				return "*-1\r\n", false
			}
			slowBlockingCalls.Add(1)
			time.Sleep(50 * time.Millisecond)
			return "*-1\r\n", false
		case "PING":
			return "+PONG\r\n", false
		default:
			return "-ERR unexpected\r\n", false
		}
	})
	defer server.stop()
	client, err := NewIORedisRESPCompatClientReady(context.Background(), IORedisRESPOptions{
		Addr: server.addr, DisableClientInfo: true, DisableReadyCheck: true, SocketTimeout: 15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Disconnect()

	assertIORedisFutureOK(t, client.Submit("BLPOP", "fast", "0.002"), nil, 0, 0)
	if _, err := waitIORedisFuture(t, client.Submit("BLPOP", "queue", "0.05")); err == nil || !strings.Contains(err.Error(), "not replayed") {
		t.Fatalf("bounded BLPOP error = %v", err)
	}
	if got := slowBlockingCalls.Load(); got != 1 {
		t.Fatalf("BLPOP executions = %d, want one", got)
	}
	assertIORedisFutureOK(t, client.Submit("PING"), "PONG", 0, 0)
}

func TestIORedisRESPWritesStopOnContextAndDisconnect(t *testing.T) {
	t.Run("startup context", func(t *testing.T) {
		clientSide, serverSide := net.Pipe()
		defer serverSide.Close()
		factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
			Addr:              "pipe:0",
			ConnectionName:    "blocked-startup",
			DisableClientInfo: true,
			DisableReadyCheck: true,
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return clientSide, nil
			},
		})
		if err != nil {
			t.Fatalf("factory: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if _, err := factory.Connect(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked startup error = %v, want context deadline", err)
		}
	})

	t.Run("disconnect", func(t *testing.T) {
		clientSide, serverSide := net.Pipe()
		defer serverSide.Close()
		factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
			Addr:              "pipe:0",
			DisableClientInfo: true,
			DisableReadyCheck: true,
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return clientSide, nil
			},
		})
		if err != nil {
			t.Fatalf("factory: %v", err)
		}
		client, err := NewIORedisCompatClientReady(context.Background(), factory)
		if err != nil {
			t.Fatalf("client: %v", err)
		}
		future := client.Submit("SET", "blocked", strings.Repeat("x", 1024))
		time.Sleep(10 * time.Millisecond)
		done := make(chan struct{})
		go func() {
			client.Disconnect()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Disconnect hung behind blocked socket write")
		}
		if _, err := waitIORedisFuture(t, future); err == nil {
			t.Fatal("blocked command unexpectedly succeeded")
		}
	})
}

type ioredisSafetyTransport struct {
	exchange  func([][]string) IORedisExchange
	exchanges atomic.Int32
	closed    chan struct{}
	closeOnce sync.Once
}

func newIORedisSafetyTransport(exchange func([][]string) IORedisExchange) *ioredisSafetyTransport {
	return &ioredisSafetyTransport{exchange: exchange, closed: make(chan struct{})}
}

func (t *ioredisSafetyTransport) Exchange(_ context.Context, commands [][]string) IORedisExchange {
	t.exchanges.Add(1)
	return t.exchange(commands)
}

func (t *ioredisSafetyTransport) Closed() <-chan struct{} { return t.closed }

func (t *ioredisSafetyTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}
