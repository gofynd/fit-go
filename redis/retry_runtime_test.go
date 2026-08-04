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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// retryLoopbackRedis is a disposable RESP server used to prove go-redis's
// runtime retry boundary without requiring Docker or an external Redis. It is
// intentionally small: only the handshake, PING and SET commands used by these
// tests are implemented.
type retryLoopbackRedis struct {
	mu             sync.Mutex
	listener       net.Listener
	addr           string
	connections    map[net.Conn]struct{}
	dropSETReplies bool
	setAttempts    map[string]int
	done           chan struct{}
}

func startRetryLoopbackRedis(t *testing.T, addr string) *retryLoopbackRedis {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}
	server := &retryLoopbackRedis{
		listener:    listener,
		addr:        listener.Addr().String(),
		connections: make(map[net.Conn]struct{}),
		setAttempts: make(map[string]int),
		done:        make(chan struct{}),
	}
	go server.serve()
	return server
}

func (s *retryLoopbackRedis) serve() {
	defer close(s.done)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.connections[conn] = struct{}{}
		s.mu.Unlock()
		go s.serveConn(conn)
	}
}

func (s *retryLoopbackRedis) serveConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
	}()
	reader := bufio.NewReader(conn)
	for {
		args, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		command := strings.ToUpper(args[0])
		switch command {
		case "HELLO":
			_, err = io.WriteString(conn, "%2\r\n$5\r\nproto\r\n:3\r\n$6\r\nserver\r\n$5\r\nredis\r\n")
		case "PING":
			_, err = io.WriteString(conn, "+PONG\r\n")
		case "CLIENT":
			_, err = io.WriteString(conn, "+OK\r\n")
		case "SET":
			key := ""
			if len(args) > 1 {
				key = args[1]
			}
			s.mu.Lock()
			s.setAttempts[key]++
			drop := s.dropSETReplies
			s.mu.Unlock()
			if drop {
				return
			}
			_, err = io.WriteString(conn, "+OK\r\n")
		default:
			_, err = io.WriteString(conn, "+OK\r\n")
		}
		if err != nil {
			return
		}
	}
}

func (s *retryLoopbackRedis) setDropSETReplies(drop bool) {
	s.mu.Lock()
	s.dropSETReplies = drop
	s.mu.Unlock()
}

func (s *retryLoopbackRedis) attempts(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setAttempts[key]
}

func (s *retryLoopbackRedis) stop(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	listener := s.listener
	connections := make([]net.Conn, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
	}
	s.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	for _, conn := range connections {
		_ = conn.Close()
	}
	select {
	case <-s.done:
	case <-time.After(time.Second):
		t.Fatal("loopback Redis did not stop")
	}
}

func readRESPCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP array header %q", line)
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count <= 0 {
		return nil, fmt.Errorf("invalid RESP array count %q", line)
	}
	args := make([]string, 0, count)
	for range count {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' {
			return nil, fmt.Errorf("invalid RESP bulk header %q", line)
		}
		length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || length < 0 {
			return nil, fmt.Errorf("invalid RESP bulk length %q", line)
		}
		payload := make([]byte, length+2)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, err
		}
		args = append(args, string(payload[:length]))
	}
	return args, nil
}

func newRetryRuntimeClient(t *testing.T, addr string, maxRetries int, retryBackoff time.Duration) (*standaloneConnection, *goredis.Client) {
	t.Helper()
	conn, err := DefaultDialFunc()(context.Background(), &DialOptions{
		Addr:               addr,
		ConnectTimeout:     25 * time.Millisecond,
		SocketTimeout:      100 * time.Millisecond,
		MaxRetries:         maxRetries,
		MinRetryBackoff:    retryBackoff,
		MaxRetryBackoff:    retryBackoff,
		DialerRetries:      1,
		DialerRetryTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("DefaultDialFunc: %v", err)
	}
	standalone, ok := conn.(*standaloneConnection)
	if !ok {
		t.Fatalf("connection type = %T, want *standaloneConnection", conn)
	}
	client := standalone.Raw().(*goredis.Client)
	t.Cleanup(func() { _ = standalone.Close() })
	return standalone, client
}

func TestStandaloneCommandRetryBudgetIsPerCommandNotOfflineQueue(t *testing.T) {
	server := startRetryLoopbackRedis(t, "")
	defer server.stop(t)
	_, client := newRetryRuntimeClient(t, server.addr, 20, time.Millisecond)
	server.setDropSETReplies(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, key := range []string{"first", "second"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			errs <- client.Set(ctx, key, "value", 0).Err()
		}(key)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil {
			t.Fatal("SET unexpectedly succeeded when every reply was dropped")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SET reached context deadline instead of exhausting retry budget: %v", err)
		}
	}

	// MaxRetries=20 means one initial attempt plus twenty retries for each
	// caller. Both commands were written 21 times independently. That is command
	// replay (and a duplicate-execution hazard after a lost reply), not ioredis's
	// single shared offline FIFO/reconnect lifecycle.
	if got := server.attempts("first"); got != 21 {
		t.Errorf("first SET attempts = %d, want 21", got)
	}
	if got := server.attempts("second"); got != 21 {
		t.Errorf("second SET attempts = %d, want 21", got)
	}
}

func TestStandaloneCommandRetryRecoversBeforeExhaustion(t *testing.T) {
	server := startRetryLoopbackRedis(t, "")
	addr := server.addr
	_, client := newRetryRuntimeClient(t, addr, 20, 10*time.Millisecond)
	if err := client.Set(context.Background(), "before-outage", "value", 0).Err(); err != nil {
		server.stop(t)
		t.Fatalf("initial SET: %v", err)
	}
	server.stop(t)

	result := make(chan error, 1)
	started := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result <- client.Set(ctx, "after-recovery", "value", 0).Err()
	}()

	// Let at least one connect attempt fail, then restore a disposable server on
	// the same loopback address while this caller still owns retry budget.
	time.Sleep(40 * time.Millisecond)
	recovered := startRetryLoopbackRedis(t, addr)
	defer recovered.stop(t)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("SET did not recover before retry exhaustion: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SET did not settle after Redis recovery")
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || elapsed >= 5*time.Second {
		t.Fatalf("recovery elapsed = %v, want >=40ms and <5s", elapsed)
	}
	if got := recovered.attempts("after-recovery"); got != 1 {
		t.Errorf("recovered server SET attempts = %d, want 1", got)
	}
}

func TestStandaloneCloseReturnsClientClosedInsteadOfOfflineQueueReplies(t *testing.T) {
	server := startRetryLoopbackRedis(t, "")
	conn, client := newRetryRuntimeClient(t, server.addr, 20, 50*time.Millisecond)
	server.stop(t)

	result := make(chan error, 1)
	go func() {
		result <- client.Set(context.Background(), "pending", "value", 0).Err()
	}()
	time.Sleep(20 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, goredis.ErrClosed) {
			t.Fatalf("pending SET error = %v, want redis.ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending SET did not observe client close")
	}
}
