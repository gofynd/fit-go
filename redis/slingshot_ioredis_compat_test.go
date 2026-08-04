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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSlingshotIORedisLockedDefaults(t *testing.T) {
	want := []time.Duration{
		50 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond,
		200 * time.Millisecond, 250 * time.Millisecond, 300 * time.Millisecond,
		350 * time.Millisecond, 400 * time.Millisecond, 450 * time.Millisecond,
		500 * time.Millisecond, 550 * time.Millisecond, 600 * time.Millisecond,
		650 * time.Millisecond, 700 * time.Millisecond, 750 * time.Millisecond,
		800 * time.Millisecond, 850 * time.Millisecond, 900 * time.Millisecond,
		950 * time.Millisecond, time.Second,
	}
	for index, expected := range want {
		if got := SlingshotIORedisRetryDelay(index + 1); got != expected {
			t.Fatalf("retry delay %d = %v, want %v", index+1, got, expected)
		}
	}
	if got := SlingshotIORedisRetryDelay(40); got != 2*time.Second {
		t.Fatalf("retry delay cap = %v, want 2s", got)
	}
	if got := (SlingshotIORedisMaxRetriesError{}).Error(); got != `Reached the max retries per request limit (which is 20). Refer to "maxRetriesPerRequest" option for details.` {
		t.Fatalf("max retries error = %q", got)
	}
}

func TestSlingshotIORedisSharedOfflineFIFORecoversOnLoopback(t *testing.T) {
	addr := reserveSlingshotLoopbackAddr(t)
	var attempts atomic.Int32
	factory := &slingshotLoopbackFactory{addr: addr, attempts: &attempts}
	client, err := NewSlingshotIORedisCompatClient(factory)
	if err != nil {
		t.Fatalf("NewSlingshotIORedisCompatClient: %v", err)
	}
	t.Cleanup(client.Disconnect)

	first := client.Submit("SET", "first", "1")
	second := client.Submit("SET", "second", "2")
	waitForSlingshotCondition(t, time.Second, func() bool { return attempts.Load() >= 2 })
	server := startSlingshotLoopbackServer(t, addr, nil)
	t.Cleanup(server.stop)

	assertSlingshotFutureOK(t, first, "OK", 0, 0)
	assertSlingshotFutureOK(t, second, "OK", 0, 0)
	if got := server.commandKeys(); strings.Join(got, ",") != "first,second" {
		t.Fatalf("recovered command order = %v, want [first second]", got)
	}
	if got := client.RetryAttempts(); got != 0 {
		t.Fatalf("retry attempts after ready = %d, want 0", got)
	}
}

func TestSlingshotIORedisLostReplyReplaysBeforeLaterFIFOItem(t *testing.T) {
	server := startSlingshotLoopbackServer(t, "", func(sequence int, _ []string) slingshotLoopbackAction {
		if sequence == 1 {
			return slingshotLoopbackCloseWithoutReply
		}
		return slingshotLoopbackReplyOK
	})
	defer server.stop()
	client := newFastSlingshotIORedisClient(t, &slingshotLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	first := client.Submit("SET", "first", "1")
	second := client.Submit("SET", "second", "2")
	assertSlingshotFutureOK(t, first, "OK", 1, 1)
	assertSlingshotFutureOK(t, second, "OK", 0, 0)
	if got := server.commandKeys(); strings.Join(got, ",") != "first,first,second" {
		t.Fatalf("lost-reply replay order = %v, want [first first second]", got)
	}
}

func TestSlingshotIORedisIdleCloseEntersSharedReconnectLifecycle(t *testing.T) {
	server := startSlingshotLoopbackServer(t, "", nil)
	defer server.stop()
	idle := newSignaledSlingshotTransport()
	var calls atomic.Int32
	factory := SlingshotIORedisTransportFactoryFunc(func(ctx context.Context) (SlingshotIORedisTransport, error) {
		if calls.Add(1) == 1 {
			return idle, nil
		}
		return (&slingshotLoopbackFactory{addr: server.addr}).Connect(ctx)
	})
	retryWait := make(chan struct{}, 1)
	releaseRetry := make(chan struct{})
	client, err := newSlingshotIORedisCompatClient(factory, slingshotIORedisPolicy{
		retryDelay: SlingshotIORedisRetryDelay,
		wait: func(ctx context.Context, _ time.Duration) bool {
			retryWait <- struct{}{}
			select {
			case <-releaseRetry:
				return true
			case <-ctx.Done():
				return false
			}
		},
	})
	if err != nil {
		t.Fatalf("newSlingshotIORedisCompatClient: %v", err)
	}
	defer client.Disconnect()
	idle.signalRemoteClose()
	select {
	case <-retryWait:
	case <-time.After(time.Second):
		t.Fatal("idle close did not enter reconnect delay")
	}
	future := client.Submit("SET", "after-idle-close", "1")
	close(releaseRetry)
	assertSlingshotFutureOK(t, future, "OK", 0, 0)
	if calls.Load() < 2 {
		t.Fatalf("factory calls = %d, want reconnect after idle close", calls.Load())
	}
}

func TestSlingshotIORedisPartialPipelineAbortsUnreadSuffix(t *testing.T) {
	server := startSlingshotLoopbackServer(t, "", func(sequence int, _ []string) slingshotLoopbackAction {
		if sequence == 2 {
			return slingshotLoopbackCloseWithoutReply
		}
		return slingshotLoopbackReplyOK
	})
	defer server.stop()
	client := newFastSlingshotIORedisClient(t, &slingshotLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	future := client.SubmitPipeline(
		[]string{"SET", "first", "1"},
		[]string{"SET", "second", "2"},
		[]string{"SET", "third", "3"},
	)
	result, err := waitSlingshotFuture(t, future)
	if err != nil {
		t.Fatalf("partial pipeline top-level error = %v, want nil", err)
	}
	if len(result.Replies) != 3 || result.Replies[0].Value != "OK" {
		t.Fatalf("partial pipeline replies = %#v", result.Replies)
	}
	for index := 1; index < 3; index++ {
		var abort SlingshotIORedisAbortError
		if !errors.As(result.Replies[index].Error, &abort) {
			t.Fatalf("reply %d error = %T %v, want SlingshotIORedisAbortError", index, result.Replies[index].Error, result.Replies[index].Error)
		}
	}
	if result.ReplayCount != 0 || result.AmbiguousReplays != 0 {
		t.Fatalf("partial pipeline replay counters = %+v, want zero", result)
	}

	// The third command bytes may or may not have reached the loopback socket
	// before the server close. ioredis aborts the unread suffix instead of
	// resending any pipeline fragment. A later command must reconnect and run
	// after that boundary.
	later := client.Submit("SET", "later", "4")
	assertSlingshotFutureOK(t, later, "OK", 0, 0)
	if got := server.commandKeys(); strings.Join(got, ",") != "first,second,later" {
		t.Fatalf("partial pipeline wire order = %v", got)
	}
}

func TestSlingshotIORedisTwentyFirstReconnectFlushesWholeQueue(t *testing.T) {
	release := make(chan struct{}, 64)
	var attempts atomic.Int32
	factory := SlingshotIORedisTransportFactoryFunc(func(ctx context.Context) (SlingshotIORedisTransport, error) {
		attempts.Add(1)
		select {
		case <-release:
			return nil, errors.New("loopback unavailable")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	policy := slingshotIORedisPolicy{
		retryDelay: SlingshotIORedisRetryDelay,
		wait:       func(context.Context, time.Duration) bool { return true },
	}
	client, err := newSlingshotIORedisCompatClient(factory, policy)
	if err != nil {
		t.Fatalf("newSlingshotIORedisCompatClient: %v", err)
	}
	defer client.Disconnect()

	direct := client.Submit("SET", "direct", "1")
	pipeline := client.SubmitPipeline([]string{"SET", "one", "1"}, []string{"SET", "two", "2"})
	for range SlingshotIORedisMaxRetriesPerRequest + 1 {
		release <- struct{}{}
	}

	_, directErr := waitSlingshotFuture(t, direct)
	var maxErr SlingshotIORedisMaxRetriesError
	if !errors.As(directErr, &maxErr) || directErr.Error() != (SlingshotIORedisMaxRetriesError{}).Error() {
		t.Fatalf("direct error = %T %v", directErr, directErr)
	}
	pipelineResult, pipelineErr := waitSlingshotFuture(t, pipeline)
	if pipelineErr != nil {
		t.Fatalf("pipeline top-level error = %v, want nil", pipelineErr)
	}
	if len(pipelineResult.Replies) != 2 {
		t.Fatalf("pipeline reply count = %d, want 2", len(pipelineResult.Replies))
	}
	for index, reply := range pipelineResult.Replies {
		if !errors.As(reply.Error, &maxErr) {
			t.Fatalf("pipeline reply %d error = %T %v", index, reply.Error, reply.Error)
		}
	}
	if got := attempts.Load(); got < 21 {
		t.Fatalf("connect attempts = %d, want at least 21", got)
	}
}

func TestSlingshotIORedisQuitDrainsAndPostCloseFails(t *testing.T) {
	server := startSlingshotLoopbackServer(t, "", nil)
	defer server.stop()
	client := newFastSlingshotIORedisClient(t, &slingshotLoopbackFactory{addr: server.addr})

	set := client.Submit("SET", "before-quit", "1")
	quitDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		quitDone <- client.Quit(ctx)
	}()
	assertSlingshotFutureOK(t, set, "OK", 0, 0)
	if err := <-quitDone; err != nil {
		t.Fatalf("Quit: %v", err)
	}
	if got := server.commandNames(); strings.Join(got, ",") != "SET,QUIT" {
		t.Fatalf("drain order = %v, want [SET QUIT]", got)
	}
	_, err := waitSlingshotFuture(t, client.Submit("SET", "after-quit", "2"))
	var closed SlingshotIORedisConnectionClosedError
	if !errors.As(err, &closed) || err.Error() != slingshotIORedisClosedMessage {
		t.Fatalf("post-quit error = %T %v", err, err)
	}
}

func TestSlingshotIORedisOfflineQuitWithEmptyQueueDoesNotReconnect(t *testing.T) {
	connectStarted := make(chan struct{}, 1)
	factory := SlingshotIORedisTransportFactoryFunc(func(ctx context.Context) (SlingshotIORedisTransport, error) {
		connectStarted <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	client, err := NewSlingshotIORedisCompatClient(factory)
	if err != nil {
		t.Fatalf("NewSlingshotIORedisCompatClient: %v", err)
	}
	select {
	case <-connectStarted:
	case <-time.After(time.Second):
		t.Fatal("eager connection did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Quit(ctx); err != nil {
		t.Fatalf("offline empty-queue Quit: %v", err)
	}
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("offline Quit did not stop reconnect lifecycle")
	}
}

func TestSlingshotIORedisServerErrorsRejectDirectButRemainPipelineReplies(t *testing.T) {
	server := startSlingshotLoopbackServer(t, "", func(_ int, _ []string) slingshotLoopbackAction {
		return slingshotLoopbackReplyError
	})
	defer server.stop()
	client := newFastSlingshotIORedisClient(t, &slingshotLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	directResult, directErr := waitSlingshotFuture(t, client.Submit("SET", "direct", "1"))
	if directErr == nil || directErr.Error() != "NOPERM denied" {
		t.Fatalf("direct server error = %v", directErr)
	}
	if len(directResult.Replies) != 1 || directResult.Replies[0].Error == nil {
		t.Fatalf("direct result = %#v", directResult)
	}
	pipelineResult, pipelineErr := waitSlingshotFuture(t, client.SubmitPipeline(
		[]string{"SET", "one", "1"}, []string{"SET", "two", "2"},
	))
	if pipelineErr != nil {
		t.Fatalf("pipeline server error rejected top level: %v", pipelineErr)
	}
	if len(pipelineResult.Replies) != 2 || pipelineResult.Replies[0].Error == nil || pipelineResult.Replies[1].Error == nil {
		t.Fatalf("pipeline server-error replies = %#v", pipelineResult.Replies)
	}
}

func TestSlingshotIORedisWaitCancellationDoesNotCancelAcceptedCommand(t *testing.T) {
	server := startSlingshotLoopbackServer(t, "", nil)
	defer server.stop()
	client := newFastSlingshotIORedisClient(t, &slingshotLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	future := client.Submit("SET", "survives-waiter", "1")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := future.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter error = %v, want context.Canceled", err)
	}
	assertSlingshotFutureOK(t, future, "OK", 0, 0)
	// Promise-like futures retain settlement for another observer.
	assertSlingshotFutureOK(t, future, "OK", 0, 0)
}

func newFastSlingshotIORedisClient(t *testing.T, factory SlingshotIORedisTransportFactory) *SlingshotIORedisCompatClient {
	t.Helper()
	client, err := newSlingshotIORedisCompatClient(factory, slingshotIORedisPolicy{
		retryDelay: SlingshotIORedisRetryDelay,
		wait: func(ctx context.Context, _ time.Duration) bool {
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		},
	})
	if err != nil {
		t.Fatalf("newSlingshotIORedisCompatClient: %v", err)
	}
	return client
}

func waitSlingshotFuture(t *testing.T, future *SlingshotIORedisFuture) (SlingshotIORedisResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return future.Wait(ctx)
}

func assertSlingshotFutureOK(t *testing.T, future *SlingshotIORedisFuture, want any, replays, ambiguous int) {
	t.Helper()
	result, err := waitSlingshotFuture(t, future)
	if err != nil {
		t.Fatalf("future error = %v", err)
	}
	if len(result.Replies) != 1 || result.Replies[0].Error != nil || result.Replies[0].Value != want {
		t.Fatalf("future replies = %#v, want one %v reply", result.Replies, want)
	}
	if result.ReplayCount != replays || result.AmbiguousReplays != ambiguous {
		t.Fatalf("future replay counters = %+v, want %d/%d", result, replays, ambiguous)
	}
}

func waitForSlingshotCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for Slingshot ioredis condition")
		}
		time.Sleep(time.Millisecond)
	}
}

type slingshotLoopbackFactory struct {
	addr     string
	attempts *atomic.Int32
}

func (f *slingshotLoopbackFactory) Connect(ctx context.Context) (SlingshotIORedisTransport, error) {
	if f.attempts != nil {
		f.attempts.Add(1)
	}
	dialer := net.Dialer{Timeout: 20 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "tcp", f.addr)
	if err != nil {
		return nil, err
	}
	return &slingshotRESPTransport{conn: conn, reader: bufio.NewReader(conn), closed: make(chan struct{})}, nil
}

type slingshotRESPTransport struct {
	conn      net.Conn
	reader    *bufio.Reader
	closed    chan struct{}
	closeOnce sync.Once
}

func (t *slingshotRESPTransport) Exchange(ctx context.Context, commands [][]string) SlingshotIORedisExchange {
	deadline := time.Now().Add(2 * time.Second)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	_ = t.conn.SetDeadline(deadline)
	var payload bytes.Buffer
	for _, command := range commands {
		writeSlingshotRESPCommand(&payload, command)
	}
	written, err := t.conn.Write(payload.Bytes())
	mayHaveExecuted := written > 0
	if err != nil || written != payload.Len() {
		if err == nil {
			err = io.ErrShortWrite
		}
		return SlingshotIORedisExchange{MayHaveExecuted: mayHaveExecuted, Error: err}
	}
	replies := make([]SlingshotIORedisReply, 0, len(commands))
	for range commands {
		reply, err := readSlingshotRESPReply(t.reader)
		if err != nil {
			return SlingshotIORedisExchange{Replies: replies, MayHaveExecuted: true, Error: err}
		}
		replies = append(replies, reply)
	}
	return SlingshotIORedisExchange{Replies: replies, MayHaveExecuted: true}
}

func (t *slingshotRESPTransport) Closed() <-chan struct{} { return t.closed }

func (t *slingshotRESPTransport) Close() error {
	err := t.conn.Close()
	t.closeOnce.Do(func() { close(t.closed) })
	return err
}

type signaledSlingshotTransport struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newSignaledSlingshotTransport() *signaledSlingshotTransport {
	return &signaledSlingshotTransport{closed: make(chan struct{})}
}

func (t *signaledSlingshotTransport) Exchange(context.Context, [][]string) SlingshotIORedisExchange {
	return SlingshotIORedisExchange{Error: errors.New("signaled transport cannot exchange")}
}

func (t *signaledSlingshotTransport) Closed() <-chan struct{} { return t.closed }

func (t *signaledSlingshotTransport) Close() error {
	t.signalRemoteClose()
	return nil
}

func (t *signaledSlingshotTransport) signalRemoteClose() {
	t.closeOnce.Do(func() { close(t.closed) })
}

func writeSlingshotRESPCommand(writer io.Writer, command []string) {
	_, _ = fmt.Fprintf(writer, "*%d\r\n", len(command))
	for _, argument := range command {
		_, _ = fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(argument), argument)
	}
}

func readSlingshotRESPReply(reader *bufio.Reader) (SlingshotIORedisReply, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return SlingshotIORedisReply{}, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return SlingshotIORedisReply{}, fmt.Errorf("invalid RESP reply %q", line)
	}
	payload := strings.TrimSuffix(line[1:], "\r\n")
	switch line[0] {
	case '+':
		return SlingshotIORedisReply{Value: payload}, nil
	case ':':
		value, parseErr := strconv.ParseInt(payload, 10, 64)
		return SlingshotIORedisReply{Value: value}, parseErr
	case '-':
		return SlingshotIORedisReply{Error: errors.New(payload)}, nil
	default:
		return SlingshotIORedisReply{}, fmt.Errorf("unsupported RESP reply prefix %q", line[0])
	}
}

type slingshotLoopbackAction int

const (
	slingshotLoopbackReplyOK slingshotLoopbackAction = iota
	slingshotLoopbackCloseWithoutReply
	slingshotLoopbackReplyError
)

type slingshotLoopbackServer struct {
	listener net.Listener
	addr     string
	action   func(int, []string) slingshotLoopbackAction

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	commands    [][]string
	done        chan struct{}
}

func reserveSlingshotLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback address: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release loopback address: %v", err)
	}
	return addr
}

func startSlingshotLoopbackServer(t *testing.T, addr string, action func(int, []string) slingshotLoopbackAction) *slingshotLoopbackServer {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}
	server := &slingshotLoopbackServer{
		listener:    listener,
		addr:        listener.Addr().String(),
		action:      action,
		connections: make(map[net.Conn]struct{}),
		done:        make(chan struct{}),
	}
	go server.serve()
	return server
}

func (s *slingshotLoopbackServer) serve() {
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

func (s *slingshotLoopbackServer) serveConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
	}()
	reader := bufio.NewReader(conn)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		cloned := append([]string(nil), command...)
		s.mu.Lock()
		s.commands = append(s.commands, cloned)
		sequence := len(s.commands)
		s.mu.Unlock()
		action := slingshotLoopbackReplyOK
		if s.action != nil {
			action = s.action(sequence, cloned)
		}
		if action == slingshotLoopbackCloseWithoutReply {
			return
		}
		reply := "+OK\r\n"
		if action == slingshotLoopbackReplyError {
			reply = "-NOPERM denied\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
		if strings.EqualFold(command[0], "QUIT") {
			return
		}
	}
}

func (s *slingshotLoopbackServer) commandKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.commands))
	for _, command := range s.commands {
		if len(command) > 1 {
			keys = append(keys, command[1])
		}
	}
	return keys
}

func (s *slingshotLoopbackServer) commandNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.commands))
	for _, command := range s.commands {
		names = append(names, strings.ToUpper(command[0]))
	}
	return names
}

func (s *slingshotLoopbackServer) stop() {
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
	}
}
