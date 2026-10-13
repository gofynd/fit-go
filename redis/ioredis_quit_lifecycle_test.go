// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redis

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type quitLifecycleTransport struct {
	exchange func(context.Context, [][]string) IORedisExchange
	closed   chan struct{}
	once     sync.Once
	closes   atomic.Int32
	closeErr error
}

func (t *quitLifecycleTransport) Exchange(ctx context.Context, commands [][]string) IORedisExchange {
	return t.exchange(ctx, commands)
}

func (t *quitLifecycleTransport) Closed() <-chan struct{} { return t.closed }

func (t *quitLifecycleTransport) Close() error {
	t.once.Do(func() {
		t.closes.Add(1)
		close(t.closed)
	})
	return t.closeErr
}

func readyQuitClient(t *testing.T, transport IORedisTransport) *IORedisCompatClient {
	t.Helper()
	client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		return transport, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Disconnect)
	return client
}

func assertQuitLifecycleFinished(t *testing.T, client *IORedisCompatClient) {
	t.Helper()
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("Quit left the closing client lifecycle running")
	}
	result, err := client.Submit("GET", "synthetic-post-quit-key").Wait(context.Background())
	var closed IORedisConnectionClosedError
	if !errors.As(err, &closed) || len(result.Replies) != 0 {
		t.Fatalf("post-Quit command = %v, %v; want closed", result, err)
	}
}

func TestIORedisQuitTerminalOutcomesFinishAndRepeat(t *testing.T) {
	for _, test := range []struct {
		name     string
		exchange IORedisExchange
		closeErr error
		wantErr  bool
	}{
		{"success", IORedisExchange{Replies: []IORedisReply{{Value: "OK"}}}, nil, false},
		{"server error", IORedisExchange{Replies: []IORedisReply{{Error: errors.New("ERR synthetic-private-reply")}}}, nil, true},
		{"terminal error", IORedisExchange{Error: newIORedisTerminalError(errors.New("synthetic-private-route"))}, nil, true},
		{"EOF", IORedisExchange{Error: io.EOF}, nil, true},
		{"missing reply", IORedisExchange{}, nil, true},
		{"malformed reply", IORedisExchange{Replies: []IORedisReply{{Value: []any{"synthetic-private-reply"}}}}, nil, true},
		{"close error", IORedisExchange{Replies: []IORedisReply{{Value: "OK"}}}, errors.New("synthetic-private-close"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var exchanges atomic.Int32
			transport := &quitLifecycleTransport{closed: make(chan struct{}), closeErr: test.closeErr,
				exchange: func(context.Context, [][]string) IORedisExchange { exchanges.Add(1); return test.exchange },
			}
			client := readyQuitClient(t, transport)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := client.Quit(ctx)
			if (err != nil) != test.wantErr || err != nil && strings.Contains(err.Error(), "synthetic-private") {
				t.Fatalf("Quit error = %v, want error %v and no raw reply", err, test.wantErr)
			}
			if test.name == "EOF" && !errors.Is(err, io.EOF) {
				t.Fatalf("Quit lost EOF identity: %v", err)
			}
			assertQuitLifecycleFinished(t, client)
			if repeated := client.Quit(context.Background()); repeated != err {
				t.Fatalf("repeated Quit = %v; want original result %v", repeated, err)
			}
			if exchanges.Load() != 1 || transport.closes.Load() != 1 {
				t.Fatalf("Quit exchanges=%d closes=%d; want one each", exchanges.Load(), transport.closes.Load())
			}
		})
	}
}

func TestIORedisClusterQuitDrainsAllNodesAndConcurrentCallers(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "one node rejects Quit"}[fail], func(t *testing.T) {
			var mu sync.Mutex
			var commands []string
			nodes := make(map[string]IORedisTransport)
			for _, name := range []string{"a", "b"} {
				nodes[name] = &quitLifecycleTransport{closed: make(chan struct{}), exchange: func(_ context.Context, batch [][]string) IORedisExchange {
					mu.Lock()
					defer mu.Unlock()
					replies := make([]IORedisReply, len(batch))
					for index, command := range batch {
						verb := strings.ToUpper(command[0])
						commands = append(commands, name+":"+verb)
						replies[index].Value = "OK"
						if verb == "GET" {
							replies[index].Value = "drained"
						}
						if fail && name == "a" && verb == "QUIT" {
							replies[index] = IORedisReply{Error: errors.New("ERR synthetic-private-reply")}
						}
					}
					return IORedisExchange{Replies: replies, WriteDisposition: IORedisFullyWritten}
				}}
			}
			cluster := &ioredisClusterTransport{nodes: nodes, first: "a", closed: make(chan struct{}), stop: make(chan struct{})}
			for slot := range cluster.slots {
				cluster.slots[slot] = "a"
			}
			client := readyQuitClient(t, cluster)
			set := client.Submit("SET", "synthetic-key", "drained")
			get := client.Submit("GET", "synthetic-key")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			results := make(chan error, 8)
			for range cap(results) {
				go func() { results <- client.Quit(ctx) }()
			}
			for range cap(results) {
				if err := <-results; (err != nil) != fail || err != nil && strings.Contains(err.Error(), "synthetic-private") {
					t.Fatalf("concurrent Quit = %v, want error %v", err, fail)
				}
			}
			assertIORedisFutureOK(t, set, "OK", 0, 0)
			assertIORedisFutureOK(t, get, "drained", 0, 0)
			assertQuitLifecycleFinished(t, client)
			mu.Lock()
			got := strings.Join(commands, ",")
			mu.Unlock()
			if got != "a:SET,a:GET,a:QUIT,b:QUIT" {
				t.Fatalf("Cluster drain order = %s", got)
			}
			for _, node := range nodes {
				if node.(*quitLifecycleTransport).closes.Load() != 1 {
					t.Fatal("Cluster Quit did not close every node exactly once")
				}
			}
		})
	}
}

func TestIORedisClusterQuitCancellationFinishesAllNodes(t *testing.T) {
	nodes := make(map[string]IORedisTransport)
	for _, name := range []string{"a", "b"} {
		nodes[name] = &quitLifecycleTransport{closed: make(chan struct{}), exchange: func(ctx context.Context, _ [][]string) IORedisExchange {
			<-ctx.Done()
			return IORedisExchange{Error: ctx.Err()}
		}}
	}
	cluster := &ioredisClusterTransport{nodes: nodes, first: "a", closed: make(chan struct{}), stop: make(chan struct{})}
	client := readyQuitClient(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Quit(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Quit deadline = %v", err)
	}
	assertQuitLifecycleFinished(t, client)
	if err := client.Quit(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated Quit lost cancellation outcome: %v", err)
	}
	for _, node := range nodes {
		if node.(*quitLifecycleTransport).closes.Load() != 1 {
			t.Fatal("canceled Cluster Quit leaked a node")
		}
	}
}

func TestIORedisQuitDeadlineDoesNotCancelAdmittedCommands(t *testing.T) {
	setStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	transport := &quitLifecycleTransport{closed: make(chan struct{}), exchange: func(ctx context.Context, commands [][]string) IORedisExchange {
		if err := ctx.Err(); err != nil {
			return IORedisExchange{Error: err}
		}
		switch strings.ToUpper(commands[0][0]) {
		case "SET":
			close(setStarted)
			select {
			case <-release:
				return IORedisExchange{Replies: []IORedisReply{{Value: "OK"}}}
			case <-ctx.Done():
				return IORedisExchange{Error: ctx.Err()}
			}
		case "GET":
			return IORedisExchange{Replies: []IORedisReply{{Value: "drained"}}}
		default:
			return IORedisExchange{Replies: []IORedisReply{{Value: "OK"}}}
		}
	}}
	client := readyQuitClient(t, transport)
	set := client.Submit("SET", "synthetic-key", "drained")
	get := client.Submit("GET", "synthetic-key")
	<-setStarted
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := client.Quit(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Quit waiting deadline = %v", err)
	}
	select {
	case <-set.state.done:
		t.Fatal("Quit deadline canceled an earlier admitted command")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	assertIORedisFutureOK(t, set, "OK", 0, 0)
	assertIORedisFutureOK(t, get, "drained", 0, 0)
	assertQuitLifecycleFinished(t, client)
	if err := client.Quit(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("final control result = %v; want expired Quit context", err)
	}
}

func TestIORedisQuitRetryExhaustionStopsOfflineClient(t *testing.T) {
	connectStarted := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		first.Do(func() {
			close(connectStarted)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
		return nil, io.EOF
	})
	client, err := newIORedisCompatClient(factory, ioredisPolicy{
		retryDelay: func(int) time.Duration { return 0 },
		wait:       func(ctx context.Context, _ time.Duration) bool { return ctx.Err() == nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect()
	<-connectStarted
	set := client.Submit("SET", "synthetic-key", "value")
	quitDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { quitDone <- client.Quit(ctx) }()
	// Ensure Quit joined the offline queue before allowing retry exhaustion.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		closing := client.closing
		client.mu.Unlock()
		if closing {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	var exhausted IORedisMaxRetriesError
	if err := <-quitDone; !errors.As(err, &exhausted) {
		t.Fatalf("offline Quit = %v; want retry exhaustion", err)
	}
	if _, err := set.Wait(ctx); !errors.As(err, &exhausted) {
		t.Fatalf("earlier queued command = %v; want retry exhaustion", err)
	}
	assertQuitLifecycleFinished(t, client)
}

func TestIORedisRESPQuitReplyFailuresDoNotReconnect(t *testing.T) {
	for _, test := range []struct {
		name, reply string
		close       bool
	}{
		{"server error", "-ERR synthetic-private-reply\r\n", false},
		{"lost reply", "", true},
		{"invalid reply", "+NOT-OK\r\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var quits atomic.Int32
			server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
				if strings.EqualFold(command[0], "QUIT") {
					quits.Add(1)
					return test.reply, test.close
				}
				return "+OK\r\n", false
			})
			defer server.stop()
			client, err := NewIORedisRESPCompatClientReady(context.Background(), IORedisRESPOptions{
				Addr: server.addr, DisableClientInfo: true, DisableReadyCheck: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Disconnect()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := client.Quit(ctx); err == nil || strings.Contains(err.Error(), "synthetic-private") {
				t.Fatalf("owned RESP Quit = %v", err)
			}
			assertQuitLifecycleFinished(t, client)
			if quits.Load() != 1 {
				t.Fatalf("Quit replayed %d times", quits.Load())
			}
		})
	}
}

func TestIORedisRESPQuitDeadlineClosesConnection(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	quitStarted := make(chan struct{})
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		if strings.EqualFold(command[0], "QUIT") {
			close(quitStarted)
			<-release
		}
		return "+OK\r\n", false
	})
	defer func() {
		releaseOnce.Do(func() { close(release) })
		server.stop()
	}()
	client, err := NewIORedisRESPCompatClientReady(context.Background(), IORedisRESPOptions{
		Addr: server.addr, DisableClientInfo: true, DisableReadyCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	quitDone := make(chan error, 1)
	go func() { quitDone <- client.Quit(ctx) }()
	select {
	case <-quitStarted:
	case <-time.After(time.Second):
		t.Fatal("QUIT was not written")
	}
	if err := <-quitDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("owned RESP Quit deadline = %v", err)
	}
	assertQuitLifecycleFinished(t, client)
	if err := client.Quit(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated owned RESP Quit = %v", err)
	}
}
