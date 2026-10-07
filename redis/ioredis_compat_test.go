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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIORedisLockedDefaults(t *testing.T) {
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
		if got := IORedisRetryDelay(index + 1); got != expected {
			t.Fatalf("retry delay %d = %v, want %v", index+1, got, expected)
		}
	}
	if got := IORedisRetryDelay(40); got != 2*time.Second {
		t.Fatalf("retry delay cap = %v, want 2s", got)
	}
	if got := (IORedisMaxRetriesError{}).Error(); got != `Reached the max retries per request limit (which is 20). Refer to "maxRetriesPerRequest" option for details.` {
		t.Fatalf("max retries error = %q", got)
	}
}

func TestIORedisFirstReadyRejectsFirstErrorAndStopsReconnect(t *testing.T) {
	want := errors.New("initial Redis unavailable")
	var attempts atomic.Int32
	client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		attempts.Add(1)
		return nil, want
	}))
	if client != nil || !errors.Is(err, want) {
		t.Fatalf("first-ready result = (%v, %v), want (nil, %v)", client, err, want)
	}
	time.Sleep(75 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts after rejected role boot = %d, want 1", got)
	}
}

func TestIORedisFirstReadyWaitsForDelayedSuccess(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	transport := newSignaledIORedisTestTransport()
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		close(started)
		select {
		case <-release:
			return transport, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	result := make(chan *IORedisCompatClient, 1)
	failures := make(chan error, 1)
	go func() {
		client, err := NewIORedisCompatClientReady(context.Background(), factory)
		result <- client
		failures <- err
	}()
	<-started
	select {
	case <-result:
		t.Fatal("first-ready constructor returned before delayed transport was ready")
	default:
	}
	close(release)
	client := <-result
	if err := <-failures; err != nil || client == nil {
		t.Fatalf("delayed first-ready result = (%v, %v)", client, err)
	}
	client.Disconnect()
}

func TestIORedisFirstReadyContextCancellationCleansLoop(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	var once sync.Once
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		client, err := NewIORedisCompatClientReady(ctx, factory)
		if client != nil {
			client.Disconnect()
		}
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled first-ready error = %v, want context.Canceled", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("cancelled first-ready factory goroutine did not stop")
	}
}

func TestIORedisFirstReadySuccessKeepsReconnectLifecycle(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", nil)
	defer server.stop()
	first := newSignaledIORedisTestTransport()
	var calls atomic.Int32
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}
		return (&ioredisLoopbackFactory{addr: server.addr}).Connect(ctx)
	})
	client, err := NewIORedisCompatClientReady(context.Background(), factory)
	if err != nil {
		t.Fatalf("NewIORedisCompatClientReady: %v", err)
	}
	defer client.Disconnect()
	first.signalRemoteClose()
	waitForIORedisCondition(t, time.Second, func() bool { return calls.Load() >= 2 })
	future := client.Submit("SET", "after-first-ready", "1")
	assertIORedisFutureOK(t, future, "OK", 0, 0)
	if calls.Load() < 2 {
		t.Fatalf("factory calls = %d, want reconnect after first-ready", calls.Load())
	}
}

func TestIORedisFirstReadyConcurrentWaitersObserveImmutableResult(t *testing.T) {
	transport := newSignaledIORedisTestTransport()
	client, err := NewIORedisCompatClient(IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		return transport, nil
	}))
	if err != nil {
		t.Fatalf("NewIORedisCompatClient: %v", err)
	}
	defer client.Disconnect()

	const waiters = 32
	results := make(chan error, waiters)
	var group sync.WaitGroup
	for range waiters {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- client.WaitForFirstReady(context.Background())
		}()
	}
	group.Wait()
	close(results)
	for waiterErr := range results {
		if waiterErr != nil {
			t.Fatalf("concurrent first-ready waiter error = %v", waiterErr)
		}
	}
	if err := client.WaitForFirstReady(context.Background()); err != nil {
		t.Fatalf("repeated first-ready waiter error = %v", err)
	}
}

func TestIORedisFirstReadyWaiterCancellationDoesNotStopClient(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	transport := newSignaledIORedisTestTransport()
	client, err := NewIORedisCompatClient(IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		close(started)
		select {
		case <-release:
			return transport, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	if err != nil {
		t.Fatalf("NewIORedisCompatClient: %v", err)
	}
	defer client.Disconnect()
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.WaitForFirstReady(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled observer error = %v, want context.Canceled", err)
	}
	close(release)
	if err := client.WaitForFirstReady(context.Background()); err != nil {
		t.Fatalf("client did not become ready after observer cancellation: %v", err)
	}
}

func TestIORedisDisconnectBeforeFirstReadySettlesAllWaiters(t *testing.T) {
	started := make(chan struct{})
	client, err := NewIORedisCompatClient(IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	if err != nil {
		t.Fatalf("NewIORedisCompatClient: %v", err)
	}
	<-started

	const waiters = 32
	results := make(chan error, waiters)
	for range waiters {
		go func() { results <- client.WaitForFirstReady(context.Background()) }()
	}
	client.Disconnect()
	for range waiters {
		var closed IORedisConnectionClosedError
		if waiterErr := <-results; !errors.As(waiterErr, &closed) {
			t.Fatalf("disconnect-before-ready waiter error = %v, want connection closed", waiterErr)
		}
	}
	var closed IORedisConnectionClosedError
	if err := client.WaitForFirstReady(context.Background()); !errors.As(err, &closed) {
		t.Fatalf("repeated disconnect-before-ready error = %v, want connection closed", err)
	}
}

func TestIORedisSharedOfflineFIFORecoversOnLoopback(t *testing.T) {
	addr := reserveIORedisLoopbackAddr(t)
	var attempts atomic.Int32
	factory := &ioredisLoopbackFactory{addr: addr, attempts: &attempts}
	client, err := NewIORedisCompatClient(factory)
	if err != nil {
		t.Fatalf("NewIORedisCompatClient: %v", err)
	}
	t.Cleanup(client.Disconnect)

	first := client.Submit("SET", "first", "1")
	second := client.Submit("SET", "second", "2")
	waitForIORedisCondition(t, time.Second, func() bool { return attempts.Load() >= 2 })
	server := startIORedisLoopbackServer(t, addr, nil)
	t.Cleanup(server.stop)

	assertIORedisFutureOK(t, first, "OK", 0, 0)
	assertIORedisFutureOK(t, second, "OK", 0, 0)
	if got := server.commandKeys(); strings.Join(got, ",") != "first,second" {
		t.Fatalf("recovered command order = %v, want [first second]", got)
	}
	if got := client.RetryAttempts(); got != 0 {
		t.Fatalf("retry attempts after ready = %d, want 0", got)
	}
}

func TestIORedisLostReplyReplaysBeforeLaterFIFOItem(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", func(sequence int, _ []string) ioredisLoopbackAction {
		if sequence == 1 {
			return ioredisLoopbackCloseWithoutReply
		}
		return ioredisLoopbackReplyOK
	})
	defer server.stop()
	client := newFastIORedisClient(t, &ioredisLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	first := client.Submit("SET", "first", "1")
	second := client.Submit("SET", "second", "2")
	assertIORedisFutureOK(t, first, "OK", 1, 1)
	assertIORedisFutureOK(t, second, "OK", 0, 0)
	if got := server.commandKeys(); strings.Join(got, ",") != "first,first,second" {
		t.Fatalf("lost-reply replay order = %v, want [first first second]", got)
	}
}

func TestIORedisDuplexPreservesPriorReplyAcrossLaterWriteFailure(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", nil)
	defer server.stop()
	firstTransport := newReplyBeforeWriteFailureIORedisTestTransport()
	var connections atomic.Int32
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		if connections.Add(1) == 1 {
			return firstTransport, nil
		}
		return (&ioredisLoopbackFactory{addr: server.addr}).Connect(ctx)
	})
	client := newFastIORedisClient(t, factory)
	defer client.Disconnect()

	first := client.Submit("SET", "first", "1")
	second := client.Submit("SET", "second", "2")
	assertIORedisFutureOK(t, first, "OK", 0, 0)
	assertIORedisFutureOK(t, second, "OK", 1, 1)
	if got := server.commandKeys(); !reflect.DeepEqual(got, []string{"second"}) {
		t.Fatalf("reconnected command keys = %v, want only uncertain second command", got)
	}
}

func TestIORedisIdleCloseEntersSharedReconnectLifecycle(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", nil)
	defer server.stop()
	idle := newSignaledIORedisTestTransport()
	var calls atomic.Int32
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		if calls.Add(1) == 1 {
			return idle, nil
		}
		return (&ioredisLoopbackFactory{addr: server.addr}).Connect(ctx)
	})
	retryWait := make(chan struct{}, 1)
	releaseRetry := make(chan struct{})
	client, err := newIORedisCompatClient(factory, ioredisPolicy{
		retryDelay: IORedisRetryDelay,
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
		t.Fatalf("newIORedisCompatClient: %v", err)
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
	assertIORedisFutureOK(t, future, "OK", 0, 0)
	if calls.Load() < 2 {
		t.Fatalf("factory calls = %d, want reconnect after idle close", calls.Load())
	}
}

func TestIORedisPartialPipelineAbortsUnreadSuffix(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", func(sequence int, _ []string) ioredisLoopbackAction {
		if sequence == 2 {
			return ioredisLoopbackCloseWithoutReply
		}
		return ioredisLoopbackReplyOK
	})
	defer server.stop()
	client := newFastIORedisClient(t, &ioredisLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	future := client.SubmitPipeline(
		[]string{"SET", "first", "1"},
		[]string{"SET", "second", "2"},
		[]string{"SET", "third", "3"},
	)
	result, err := waitIORedisFuture(t, future)
	if err != nil {
		t.Fatalf("partial pipeline top-level error = %v, want nil", err)
	}
	if len(result.Replies) != 3 || result.Replies[0].Value != "OK" {
		t.Fatalf("partial pipeline replies = %#v", result.Replies)
	}
	for index := 1; index < 3; index++ {
		var abort IORedisAbortError
		if !errors.As(result.Replies[index].Error, &abort) {
			t.Fatalf("reply %d error = %T %v, want IORedisAbortError", index, result.Replies[index].Error, result.Replies[index].Error)
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
	assertIORedisFutureOK(t, later, "OK", 0, 0)
	if got := server.commandKeys(); strings.Join(got, ",") != "first,second,later" {
		t.Fatalf("partial pipeline wire order = %v", got)
	}
}

func TestIORedisTwentyFirstReconnectFlushesWholeQueue(t *testing.T) {
	release := make(chan struct{}, 64)
	var attempts atomic.Int32
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		attempts.Add(1)
		select {
		case <-release:
			return nil, errors.New("loopback unavailable")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	policy := ioredisPolicy{
		retryDelay: IORedisRetryDelay,
		wait:       func(context.Context, time.Duration) bool { return true },
	}
	client, err := newIORedisCompatClient(factory, policy)
	if err != nil {
		t.Fatalf("newIORedisCompatClient: %v", err)
	}
	defer client.Disconnect()

	direct := client.Submit("SET", "direct", "1")
	pipeline := client.SubmitPipeline([]string{"SET", "one", "1"}, []string{"SET", "two", "2"})
	for range IORedisMaxRetriesPerRequest + 1 {
		release <- struct{}{}
	}

	_, directErr := waitIORedisFuture(t, direct)
	var maxErr IORedisMaxRetriesError
	if !errors.As(directErr, &maxErr) || directErr.Error() != (IORedisMaxRetriesError{}).Error() {
		t.Fatalf("direct error = %T %v", directErr, directErr)
	}
	pipelineResult, pipelineErr := waitIORedisFuture(t, pipeline)
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

func TestIORedisQuitDrainsAndPostCloseFails(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", nil)
	defer server.stop()
	client := newFastIORedisClient(t, &ioredisLoopbackFactory{addr: server.addr})

	set := client.Submit("SET", "before-quit", "1")
	quitDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		quitDone <- client.Quit(ctx)
	}()
	assertIORedisFutureOK(t, set, "OK", 0, 0)
	if err := <-quitDone; err != nil {
		t.Fatalf("Quit: %v", err)
	}
	if got := server.commandNames(); strings.Join(got, ",") != "SET,QUIT" {
		t.Fatalf("drain order = %v, want [SET QUIT]", got)
	}
	_, err := waitIORedisFuture(t, client.Submit("SET", "after-quit", "2"))
	var closed IORedisConnectionClosedError
	if !errors.As(err, &closed) || err.Error() != ioredisClosedMessage {
		t.Fatalf("post-quit error = %T %v", err, err)
	}
}

func TestIORedisOfflineQuitWithEmptyQueueDoesNotReconnect(t *testing.T) {
	connectStarted := make(chan struct{}, 1)
	factory := IORedisTransportFactoryFunc(func(ctx context.Context) (IORedisTransport, error) {
		connectStarted <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	client, err := NewIORedisCompatClient(factory)
	if err != nil {
		t.Fatalf("NewIORedisCompatClient: %v", err)
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

func TestIORedisServerErrorsRejectDirectButRemainPipelineReplies(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", func(_ int, _ []string) ioredisLoopbackAction {
		return ioredisLoopbackReplyError
	})
	defer server.stop()
	client := newFastIORedisClient(t, &ioredisLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	directResult, directErr := waitIORedisFuture(t, client.Submit("SET", "direct", "1"))
	if directErr == nil || directErr.Error() != "NOPERM denied" {
		t.Fatalf("direct server error = %v", directErr)
	}
	if len(directResult.Replies) != 1 || directResult.Replies[0].Error == nil {
		t.Fatalf("direct result = %#v", directResult)
	}
	pipelineResult, pipelineErr := waitIORedisFuture(t, client.SubmitPipeline(
		[]string{"SET", "one", "1"}, []string{"SET", "two", "2"},
	))
	if pipelineErr != nil {
		t.Fatalf("pipeline server error rejected top level: %v", pipelineErr)
	}
	if len(pipelineResult.Replies) != 2 || pipelineResult.Replies[0].Error == nil || pipelineResult.Replies[1].Error == nil {
		t.Fatalf("pipeline server-error replies = %#v", pipelineResult.Replies)
	}
}

func TestIORedisWaitCancellationDoesNotCancelAcceptedCommand(t *testing.T) {
	server := startIORedisLoopbackServer(t, "", nil)
	defer server.stop()
	client := newFastIORedisClient(t, &ioredisLoopbackFactory{addr: server.addr})
	defer client.Disconnect()

	future := client.Submit("SET", "survives-waiter", "1")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := future.Wait(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter error = %v, want context.Canceled", err)
	}
	assertIORedisFutureOK(t, future, "OK", 0, 0)
	// Promise-like futures retain settlement for another observer.
	assertIORedisFutureOK(t, future, "OK", 0, 0)
}

func newFastIORedisClient(t *testing.T, factory IORedisTransportFactory) *IORedisCompatClient {
	t.Helper()
	client, err := newIORedisCompatClient(factory, ioredisPolicy{
		retryDelay: IORedisRetryDelay,
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
		t.Fatalf("newIORedisCompatClient: %v", err)
	}
	return client
}

func waitIORedisFuture(t *testing.T, future *IORedisFuture) (IORedisResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return future.Wait(ctx)
}

func assertIORedisFutureOK(t *testing.T, future *IORedisFuture, want any, replays, ambiguous int) {
	t.Helper()
	result, err := waitIORedisFuture(t, future)
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

func waitForIORedisCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for ioredis condition")
		}
		time.Sleep(time.Millisecond)
	}
}

type ioredisLoopbackFactory struct {
	addr     string
	attempts *atomic.Int32
}

func (f *ioredisLoopbackFactory) Connect(ctx context.Context) (IORedisTransport, error) {
	if f.attempts != nil {
		f.attempts.Add(1)
	}
	dialer := net.Dialer{Timeout: 20 * time.Millisecond}
	conn, err := dialer.DialContext(ctx, "tcp", f.addr)
	if err != nil {
		return nil, err
	}
	return &testIORedisRESPTransport{conn: conn, reader: bufio.NewReader(conn), closed: make(chan struct{})}, nil
}

type testIORedisRESPTransport struct {
	conn      net.Conn
	reader    *bufio.Reader
	closed    chan struct{}
	closeOnce sync.Once
}

func (t *testIORedisRESPTransport) Exchange(ctx context.Context, commands [][]string) IORedisExchange {
	deadline := time.Now().Add(2 * time.Second)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	_ = t.conn.SetDeadline(deadline)
	var payload bytes.Buffer
	for _, command := range commands {
		writeIORedisRESPCommand(&payload, command)
	}
	written, err := t.conn.Write(payload.Bytes())
	mayHaveExecuted := written > 0
	if err != nil || written != payload.Len() {
		if err == nil {
			err = io.ErrShortWrite
		}
		return IORedisExchange{MayHaveExecuted: mayHaveExecuted, Error: err}
	}
	replies := make([]IORedisReply, 0, len(commands))
	for range commands {
		reply, err := readIORedisRESPReply(t.reader)
		if err != nil {
			return IORedisExchange{Replies: replies, MayHaveExecuted: true, Error: err}
		}
		replies = append(replies, reply)
	}
	return IORedisExchange{Replies: replies, MayHaveExecuted: true}
}

func (t *testIORedisRESPTransport) Closed() <-chan struct{} { return t.closed }

func (t *testIORedisRESPTransport) Close() error {
	err := t.conn.Close()
	t.closeOnce.Do(func() { close(t.closed) })
	return err
}

type signaledIORedisTestTransport struct {
	closed    chan struct{}
	closeOnce sync.Once
}

type replyBeforeWriteFailureIORedisTestTransport struct {
	closed      chan struct{}
	readStarted chan struct{}
	releaseRead chan struct{}
	closeOnce   sync.Once
	readOnce    sync.Once
	writes      atomic.Int32
}

func newReplyBeforeWriteFailureIORedisTestTransport() *replyBeforeWriteFailureIORedisTestTransport {
	return &replyBeforeWriteFailureIORedisTestTransport{
		closed:      make(chan struct{}),
		readStarted: make(chan struct{}),
		releaseRead: make(chan struct{}),
	}
}

func (t *replyBeforeWriteFailureIORedisTestTransport) Exchange(context.Context, [][]string) IORedisExchange {
	return IORedisExchange{Error: errors.New("duplex transport must not use serial Exchange")}
}

func (t *replyBeforeWriteFailureIORedisTestTransport) writeCommands(_ context.Context, commands [][]string) IORedisExchange {
	wire, err := encodeIORedisRESPCommands(commands)
	if err != nil {
		return IORedisExchange{Error: err}
	}
	if t.writes.Add(1) == 1 {
		return IORedisExchange{
			MayHaveExecuted: true, WriteDisposition: IORedisFullyWritten,
			BytesWritten: len(wire), BytesTotal: len(wire),
		}
	}
	<-t.readStarted
	return IORedisExchange{
		MayHaveExecuted: true, WriteDisposition: IORedisFullyWritten,
		BytesWritten: len(wire), BytesTotal: len(wire), Error: errors.New("later write failed"),
	}
}

func (t *replyBeforeWriteFailureIORedisTestTransport) readReply(context.Context) (IORedisReply, error) {
	t.readOnce.Do(func() { close(t.readStarted) })
	<-t.releaseRead
	return IORedisReply{Value: "OK"}, nil
}

func (*replyBeforeWriteFailureIORedisTestTransport) finishReplyWait() {}

func (t *replyBeforeWriteFailureIORedisTestTransport) Closed() <-chan struct{} { return t.closed }

func (t *replyBeforeWriteFailureIORedisTestTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.releaseRead)
		close(t.closed)
	})
	return nil
}

func newSignaledIORedisTestTransport() *signaledIORedisTestTransport {
	return &signaledIORedisTestTransport{closed: make(chan struct{})}
}

func (t *signaledIORedisTestTransport) Exchange(context.Context, [][]string) IORedisExchange {
	return IORedisExchange{Error: errors.New("signaled transport cannot exchange")}
}

func (t *signaledIORedisTestTransport) Closed() <-chan struct{} { return t.closed }

func (t *signaledIORedisTestTransport) Close() error {
	t.signalRemoteClose()
	return nil
}

func (t *signaledIORedisTestTransport) signalRemoteClose() {
	t.closeOnce.Do(func() { close(t.closed) })
}

func writeIORedisRESPCommand(writer io.Writer, command []string) {
	_, _ = fmt.Fprintf(writer, "*%d\r\n", len(command))
	for _, argument := range command {
		_, _ = fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(argument), argument)
	}
}

func readIORedisRESPReply(reader *bufio.Reader) (IORedisReply, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return IORedisReply{}, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return IORedisReply{}, fmt.Errorf("invalid RESP reply %q", line)
	}
	payload := strings.TrimSuffix(line[1:], "\r\n")
	switch line[0] {
	case '+':
		return IORedisReply{Value: payload}, nil
	case ':':
		value, parseErr := strconv.ParseInt(payload, 10, 64)
		return IORedisReply{Value: value}, parseErr
	case '-':
		return IORedisReply{Error: errors.New(payload)}, nil
	default:
		return IORedisReply{}, fmt.Errorf("unsupported RESP reply prefix %q", line[0])
	}
}

type ioredisLoopbackAction int

const (
	ioredisLoopbackReplyOK ioredisLoopbackAction = iota
	ioredisLoopbackCloseWithoutReply
	ioredisLoopbackReplyError
)

type ioredisLoopbackServer struct {
	listener net.Listener
	addr     string
	action   func(int, []string) ioredisLoopbackAction

	mu          sync.Mutex
	connections map[net.Conn]struct{}
	commands    [][]string
	done        chan struct{}
}

func reserveIORedisLoopbackAddr(t *testing.T) string {
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

func startIORedisLoopbackServer(t *testing.T, addr string, action func(int, []string) ioredisLoopbackAction) *ioredisLoopbackServer {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}
	server := &ioredisLoopbackServer{
		listener:    listener,
		addr:        listener.Addr().String(),
		action:      action,
		connections: make(map[net.Conn]struct{}),
		done:        make(chan struct{}),
	}
	go server.serve()
	return server
}

func (s *ioredisLoopbackServer) serve() {
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

func (s *ioredisLoopbackServer) serveConn(conn net.Conn) {
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
		action := ioredisLoopbackReplyOK
		if s.action != nil {
			action = s.action(sequence, cloned)
		}
		if action == ioredisLoopbackCloseWithoutReply {
			return
		}
		reply := "+OK\r\n"
		if action == ioredisLoopbackReplyError {
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

func (s *ioredisLoopbackServer) commandKeys() []string {
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

func (s *ioredisLoopbackServer) commandNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.commands))
	for _, command := range s.commands {
		names = append(names, strings.ToUpper(command[0]))
	}
	return names
}

func (s *ioredisLoopbackServer) stop() {
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
