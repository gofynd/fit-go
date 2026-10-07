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
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// IORedisMaxRetriesPerRequest is the ioredis 5.11.1 default.
const IORedisMaxRetriesPerRequest = 20

const ioredisClosedMessage = "Connection is closed."

// IORedisRetryDelay returns the ioredis 5.11.1 default retryStrategy delay:
// min(times*50ms, 2000ms).
func IORedisRetryDelay(times int) time.Duration {
	if times <= 0 {
		return 0
	}
	delay := time.Duration(times) * 50 * time.Millisecond
	if delay > 2*time.Second {
		return 2 * time.Second
	}
	return delay
}

// IORedisMaxRetriesError reports retry exhaustion using ioredis's message.
type IORedisMaxRetriesError struct{}

func (IORedisMaxRetriesError) Error() string {
	return `Reached the max retries per request limit (which is 20). Refer to "maxRetriesPerRequest" option for details.`
}

// IORedisConnectionClosedError reports a post-disconnect command.
type IORedisConnectionClosedError struct{}

func (IORedisConnectionClosedError) Error() string { return ioredisClosedMessage }

// IORedisAbortError reports an interrupted pipeline after a partial reply.
type IORedisAbortError struct{}

func (IORedisAbortError) Error() string { return "Command aborted due to connection close" }

// IORedisReply is one RESP command result.
type IORedisReply struct {
	Value any
	Error error
}

// IORedisExchange reports one ordered write/read attempt.
//
// A transport must make loss ambiguity explicit. MayHaveExecuted is true when
// at least one command byte may have reached Redis before a transport error.
// The state machine intentionally replays a direct command or a zero-reply
// pipeline in that case, matching ioredis's duplicate-execution risk. Once a
// pipeline reply has arrived, the remaining commands are aborted instead.
type IORedisExchange struct {
	Replies         []IORedisReply
	MayHaveExecuted bool
	// WriteDisposition is populated by the owned RESP transport. Custom
	// transports written before the owned transport was added may leave it at
	// IORedisWriteUnknown; the compatibility state machine continues
	// to use MayHaveExecuted as its replay decision for that reason.
	WriteDisposition IORedisWriteDisposition
	BytesWritten     int
	BytesTotal       int
	// NetworkBytesWritten counts bytes accepted by the underlying socket for
	// this exchange. For TLS it counts ciphertext, while BytesWritten counts
	// encoded RESP plaintext.
	NetworkBytesWritten int64
	Error               error
}

// IORedisWriteDisposition records how much of an encoded command
// batch crossed the socket boundary before an exchange failed. It separates a
// definitely unwritten request from partial-write ambiguity and a complete
// write whose reply was lost.
type IORedisWriteDisposition uint8

const (
	IORedisWriteUnknown IORedisWriteDisposition = iota
	IORedisNotWritten
	IORedisPartiallyWritten
	IORedisFullyWritten
)

// IORedisTransport is one ready standalone Redis connection. Connect
// readiness, authentication, database selection, TLS and RESP parsing belong
// to the factory. Server reply errors must not be returned as Exchange.Error.
//
// This deliberately does not adapt go-redis automatically: go-redis does not
// expose enough information to distinguish not-written, partial-write and
// written/lost-reply failures, so such an adapter would silently claim parity
// it cannot provide.
type IORedisTransport interface {
	Exchange(context.Context, [][]string) IORedisExchange
	// Closed must signal a remotely closed idle connection. Returning nil is a
	// contract violation and causes the state machine to discard the transport.
	Closed() <-chan struct{}
	Close() error
}

// ioredisDuplexTransport is limited to transports with independent I/O phases.
type ioredisDuplexTransport interface {
	IORedisTransport
	writeCommands(context.Context, [][]string) IORedisExchange
	readReply(context.Context) (IORedisReply, error)
	finishReplyWait()
}

// IORedisTransportFactory creates a fully ready transport.
type IORedisTransportFactory interface {
	Connect(context.Context) (IORedisTransport, error)
}

// IORedisTransportFactoryFunc adapts a function to IORedisTransportFactory.
type IORedisTransportFactoryFunc func(context.Context) (IORedisTransport, error)

func (f IORedisTransportFactoryFunc) Connect(ctx context.Context) (IORedisTransport, error) {
	return f(ctx)
}

// IORedisResult preserves the complete ordered result and records the
// duplicate-execution exposure of replay after a lost reply.
type IORedisResult struct {
	Replies          []IORedisReply
	ReplayCount      int
	AmbiguousReplays int
}

// IORedisFuture is returned immediately when a command is accepted
// into the connection-wide FIFO. Cancelling a Wait context stops only that
// waiter; it does not remove or cancel the accepted Redis command, matching a
// JavaScript Promise rather than a request-scoped Go operation.
type IORedisFuture struct {
	state *ioredisFutureState
}

type ioredisCompletion struct {
	result IORedisResult
	err    error
}

type ioredisFutureState struct {
	done       chan struct{}
	settleOnce sync.Once
	completion ioredisCompletion
}

func newIORedisFutureState() *ioredisFutureState {
	return &ioredisFutureState{done: make(chan struct{})}
}

func (s *ioredisFutureState) settle(completion ioredisCompletion) {
	s.settleOnce.Do(func() {
		s.completion = completion
		close(s.done)
	})
}

func completedIORedisFuture(result IORedisResult, err error) *IORedisFuture {
	state := newIORedisFutureState()
	state.settle(ioredisCompletion{result: result, err: err})
	return &IORedisFuture{state: state}
}

// Wait observes settlement without transferring cancellation into the shared
// queue. The settled value is retained, so multiple waiters observe the same
// result as multiple handlers attached to one JavaScript Promise.
func (f *IORedisFuture) Wait(ctx context.Context) (IORedisResult, error) {
	if f == nil || f.state == nil {
		return IORedisResult{}, errors.New("ioredis future is not configured")
	}
	select {
	case <-f.state.done:
		return f.state.completion.result, f.state.completion.err
	case <-ctx.Done():
		return IORedisResult{}, ctx.Err()
	}
}

type ioredisRequest struct {
	commands         [][]string
	pipeline         bool
	quit             bool
	replays          int
	ambiguousReplays int
	future           *ioredisFutureState
	span             trace.Span
}

type ioredisPolicy struct {
	retryDelay func(int) time.Duration
	wait       func(context.Context, time.Duration) bool
}

func defaultIORedisPolicy() ioredisPolicy {
	return ioredisPolicy{
		retryDelay: IORedisRetryDelay,
		wait: func(ctx context.Context, delay time.Duration) bool {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
				return true
			case <-ctx.Done():
				return false
			}
		},
	}
}

// IORedisCompatClient implements ioredis 5.x connection-owned retry semantics.
// Callers must supply a transport that reports write disposition.
type IORedisCompatClient struct {
	factory IORedisTransportFactory
	policy  ioredisPolicy

	ctx            context.Context
	cancel         context.CancelFunc
	wake           chan struct{}
	done           chan struct{}
	firstReady     chan struct{}
	firstReadyOnce sync.Once

	mu            sync.Mutex
	queue         []*ioredisRequest
	closing       bool
	disconnecting bool
	closed        bool
	ready         bool
	retryAttempts int
	firstReadyErr error
}

// NewIORedisCompatClient starts the eager connection and reconnect loop.
func NewIORedisCompatClient(factory IORedisTransportFactory) (*IORedisCompatClient, error) {
	return newIORedisCompatClient(factory, defaultIORedisPolicy())
}

// NewIORedisCompatClientReady waits for the first successful connection and
// disconnects the client if startup fails.
// Existing constructors remain asynchronous and unchanged.
func NewIORedisCompatClientReady(ctx context.Context, factory IORedisTransportFactory) (*IORedisCompatClient, error) {
	if ctx == nil {
		return nil, errors.New("ioredis first-ready context is required")
	}
	client, err := NewIORedisCompatClient(factory)
	if err != nil {
		return nil, err
	}
	if err := client.WaitForFirstReady(ctx); err != nil {
		client.Disconnect()
		return nil, err
	}
	return client, nil
}

func newIORedisCompatClient(factory IORedisTransportFactory, policy ioredisPolicy) (*IORedisCompatClient, error) {
	if factory == nil {
		return nil, errors.New("ioredis transport factory is required")
	}
	if policy.retryDelay == nil || policy.wait == nil {
		return nil, errors.New("ioredis retry policy is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &IORedisCompatClient{
		factory:    factory,
		policy:     policy,
		ctx:        ctx,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
		done:       make(chan struct{}),
		firstReady: make(chan struct{}),
	}
	go client.run()
	return client, nil
}

// WaitForFirstReady observes the first connection/startup result. It does not
// stop the client when the caller context ends; the role-safe Ready constructor
// above owns that cleanup. Once successful, later reconnects use the existing
// ioredis-compatible lifecycle and do not change this immutable result.
func (c *IORedisCompatClient) WaitForFirstReady(ctx context.Context) error {
	if c == nil || c.firstReady == nil {
		return errors.New("ioredis client is not configured")
	}
	if ctx == nil {
		return errors.New("ioredis first-ready context is required")
	}
	select {
	case <-c.firstReady:
		c.mu.Lock()
		err := c.firstReadyErr
		c.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *IORedisCompatClient) settleFirstReady(err error) {
	c.firstReadyOnce.Do(func() {
		c.mu.Lock()
		c.firstReadyErr = err
		c.mu.Unlock()
		close(c.firstReady)
	})
}

// Submit accepts one command into the shared FIFO.
func (c *IORedisCompatClient) Submit(command ...string) *IORedisFuture {
	return c.SubmitContext(context.Background(), command...)
}

// SubmitContext accepts one command and parents its optional tracing span to
// ctx. Command execution remains detached from caller cancellation, matching
// the JavaScript Promise semantics of Submit and Wait.
func (c *IORedisCompatClient) SubmitContext(ctx context.Context, command ...string) *IORedisFuture {
	return c.submit(ctx, [][]string{cloneIORedisCommand(command)}, false, false)
}

// SubmitPipeline accepts an ordered pipeline as one FIFO item. Redis reply
// errors are returned per reply. Transport failure before any reply causes a
// complete replay; failure after a prefix of replies aborts only the suffix.
func (c *IORedisCompatClient) SubmitPipeline(commands ...[]string) *IORedisFuture {
	return c.SubmitPipelineContext(context.Background(), commands...)
}

// SubmitPipelineContext is the context-aware counterpart of SubmitPipeline.
// It creates one privacy-safe pipeline span and does not transfer ctx
// cancellation into the accepted FIFO item.
func (c *IORedisCompatClient) SubmitPipelineContext(ctx context.Context, commands ...[]string) *IORedisFuture {
	cloned := make([][]string, len(commands))
	for index := range commands {
		cloned[index] = cloneIORedisCommand(commands[index])
	}
	return c.submit(ctx, cloned, true, false)
}

func cloneIORedisCommand(command []string) []string {
	cloned := append([]string(nil), command...)
	if len(cloned) > 0 {
		// ioredis Command normalizes only the command name. Arguments retain
		// their original bytes (notably CLIENT's SETINFO/LIB-NAME tokens).
		cloned[0] = strings.ToLower(cloned[0])
	}
	return cloned
}

func (c *IORedisCompatClient) submit(ctx context.Context, commands [][]string, pipeline, quit bool) *IORedisFuture {
	if c == nil {
		return completedIORedisFuture(IORedisResult{}, IORedisConnectionClosedError{})
	}
	if len(commands) == 0 {
		return completedIORedisFuture(IORedisResult{}, errors.New("ioredis command is empty"))
	}
	for _, command := range commands {
		if len(command) == 0 || command[0] == "" {
			return completedIORedisFuture(IORedisResult{}, errors.New("ioredis command is empty"))
		}
	}
	request := &ioredisRequest{
		commands: commands,
		pipeline: pipeline,
		quit:     quit,
		future:   newIORedisFutureState(),
		span:     startIORedisCompatibilitySpan(ctx, commands, pipeline),
	}
	c.mu.Lock()
	if c.closed || c.closing || c.disconnecting {
		c.mu.Unlock()
		closedErr := IORedisConnectionClosedError{}
		c.complete(request, nil, closedErr)
		return &IORedisFuture{state: request.future}
	}
	c.queue = append(c.queue, request)
	c.mu.Unlock()
	c.signal()
	return &IORedisFuture{state: request.future}
}

// Quit appends QUIT after every already accepted command and waits for its
// settlement. New commands are rejected once Quit begins. If no connection and
// no queued command exist, it resolves locally like ioredis's offline QUIT
// special case.
func (c *IORedisCompatClient) Quit(ctx context.Context) error {
	if c == nil {
		return nil
	}
	request := &ioredisRequest{
		commands: [][]string{{"quit"}},
		quit:     true,
		future:   newIORedisFutureState(),
		span:     startIORedisCompatibilitySpan(ctx, [][]string{{"quit"}}, false),
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		finishIORedisCompatibilitySpan(request.span, nil, nil)
		return nil
	}
	if c.closing || c.disconnecting {
		done := c.done
		c.mu.Unlock()
		finishIORedisCompatibilitySpan(request.span, nil, nil)
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.closing = true
	if len(c.queue) == 0 && !c.ready {
		c.disconnecting = true
		c.mu.Unlock()
		c.complete(request, []IORedisReply{{Value: "OK"}}, nil)
		c.cancel()
		c.signal()
		return nil
	}
	c.queue = append(c.queue, request)
	c.mu.Unlock()
	c.signal()
	future := &IORedisFuture{state: request.future}
	_, err := future.Wait(ctx)
	return err
}

// Disconnect immediately stops reconnecting and settles every accepted item
// with the exact ioredis connection-closed error. It is the counterpart of
// ioredis disconnect(), not quit().
func (c *IORedisCompatClient) Disconnect() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed || c.disconnecting {
		c.mu.Unlock()
		return
	}
	c.disconnecting = true
	c.mu.Unlock()
	c.settleFirstReady(IORedisConnectionClosedError{})
	c.cancel()
	c.signal()
	<-c.done
}

// RetryAttempts returns the connection-owned counter. It resets to zero only
// after the factory returns a ready transport.
func (c *IORedisCompatClient) RetryAttempts() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.retryAttempts
}

func (c *IORedisCompatClient) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *IORedisCompatClient) run() {
	defer close(c.done)
	var transport IORedisTransport
	defer func() {
		if transport != nil {
			_ = transport.Close()
		}
		c.finishClosed(IORedisConnectionClosedError{})
	}()

	needsRetryDelay := false
	for {
		if c.shouldDisconnect() {
			return
		}
		if transport == nil {
			if needsRetryDelay {
				delay := c.currentRetryDelay()
				if !c.policy.wait(c.ctx, delay) {
					return
				}
			}
			connected, err := c.factory.Connect(c.ctx)
			if err != nil || connected == nil {
				if err == nil {
					err = errors.New("ioredis transport factory returned nil transport")
				}
				c.settleFirstReady(err)
				c.connectionFailed(err)
				needsRetryDelay = true
				continue
			}
			if connected.Closed() == nil {
				_ = connected.Close()
				err := errors.New("ioredis transport returned nil close signal")
				c.settleFirstReady(err)
				c.connectionFailed(err)
				needsRetryDelay = true
				continue
			}
			transport = connected
			needsRetryDelay = false
			c.mu.Lock()
			c.ready = true
			c.retryAttempts = 0
			c.mu.Unlock()
			c.settleFirstReady(nil)
		}

		if duplex, ok := transport.(ioredisDuplexTransport); ok {
			stop, sessionErr := c.runDuplexSession(duplex)
			_ = transport.Close()
			transport = nil
			if stop {
				return
			}
			c.setNotReady()
			c.connectionFailed(sessionErr)
			needsRetryDelay = true
			continue
		}

		request := c.peek()
		if request == nil {
			select {
			case <-c.wake:
				continue
			case <-transport.Closed():
				_ = transport.Close()
				transport = nil
				c.setNotReady()
				c.connectionFailed(errors.New("ioredis transport closed"))
				needsRetryDelay = true
				continue
			case <-c.ctx.Done():
				return
			}
		}

		exchange := transport.Exchange(c.ctx, request.commands)
		if exchange.Error == nil && len(exchange.Replies) != len(request.commands) {
			exchange.Error = fmt.Errorf("ioredis transport returned %d replies for %d commands", len(exchange.Replies), len(request.commands))
		}
		if exchange.Error == nil {
			c.pop(request)
			var commandErr error
			if !request.pipeline && len(exchange.Replies) == 1 {
				commandErr = exchange.Replies[0].Error
			}
			c.complete(request, exchange.Replies, commandErr)
			if request.quit {
				_ = transport.Close()
				transport = nil
				return
			}
			continue
		}

		_ = transport.Close()
		transport = nil
		c.setNotReady()
		if request.pipeline && len(exchange.Replies) > 0 {
			replies := append([]IORedisReply(nil), exchange.Replies...)
			for len(replies) < len(request.commands) {
				replies = append(replies, IORedisReply{Error: IORedisAbortError{}})
			}
			c.pop(request)
			c.complete(request, replies, nil)
		} else {
			request.replays++
			if exchange.MayHaveExecuted {
				request.ambiguousReplays++
			}
		}
		c.connectionFailed(exchange.Error)
		needsRetryDelay = true
	}
}

type ioredisDuplexWriteResult struct {
	request  *ioredisRequest
	exchange IORedisExchange
}

type ioredisDuplexReadResult struct {
	reply IORedisReply
	err   error
}

type ioredisInFlight struct {
	request *ioredisRequest
	replies []IORedisReply
}

// runDuplexSession mirrors ioredis's connection-owned command queue: writes
// remain ordered but do not wait for earlier replies. The in-flight ledger is
// reconciled only after the read/write failure boundary is known, so every
// fully or partially written request without a reply is replayed after a lost
// connection. This is what exposes duplicate execution for more than one
// direct command on the same socket.
func (c *IORedisCompatClient) runDuplexSession(transport ioredisDuplexTransport) (bool, error) {
	writeRequests := make(chan *ioredisRequest)
	writeResults := make(chan ioredisDuplexWriteResult, 1)
	readRequests := make(chan struct{})
	readResults := make(chan ioredisDuplexReadResult, 1)
	sessionCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	go func() {
		for {
			select {
			case request := <-writeRequests:
				exchange := transport.writeCommands(sessionCtx, request.commands)
				writeResults <- ioredisDuplexWriteResult{request: request, exchange: exchange}
			case <-sessionCtx.Done():
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-readRequests:
				reply, err := transport.readReply(sessionCtx)
				readResults <- ioredisDuplexReadResult{reply: reply, err: err}
			case <-sessionCtx.Done():
				return
			}
		}
	}()

	scheduled := make(map[*ioredisRequest]bool)
	var writing *ioredisRequest
	var inFlight []*ioredisInFlight
	reading := false

	for {
		var nextWrite *ioredisRequest
		var writeRequestChannel chan *ioredisRequest
		if writing == nil {
			nextWrite = c.firstUnscheduled(scheduled)
			if nextWrite != nil {
				writeRequestChannel = writeRequests
			}
		}
		var readRequestChannel chan struct{}
		if !reading && ioredisOutstandingReplies(inFlight) > 0 {
			readRequestChannel = readRequests
		}

		select {
		case writeRequestChannel <- nextWrite:
			scheduled[nextWrite] = true
			writing = nextWrite
		case writeResult := <-writeResults:
			if writing == writeResult.request {
				writing = nil
			}
			if writeResult.exchange.Error != nil {
				_ = transport.Close()
				// A reply for an earlier request can race with the later socket
				// write failure. Preserve any reply already handed to the session
				// before classifying the remaining ledger as uncertain.
				if reading {
					readResult := <-readResults
					reading = false
					if readResult.err == nil {
						var stop bool
						inFlight, stop = c.applyDuplexReply(transport, inFlight, scheduled, readResult.reply)
						if stop {
							return true, nil
						}
					}
				}
				c.reconcileDuplexFailure(inFlight, writeResult.request, writeResult.exchange)
				return false, writeResult.exchange.Error
			}
			inFlight = append(inFlight, &ioredisInFlight{request: writeResult.request})
		case readRequestChannel <- struct{}{}:
			reading = true
		case readResult := <-readResults:
			reading = false
			if readResult.err != nil {
				_ = transport.Close()
				if writing != nil {
					writeResult := <-writeResults
					writing = nil
					if writeResult.exchange.Error == nil {
						inFlight = append(inFlight, &ioredisInFlight{request: writeResult.request})
					} else {
						c.reconcileDuplexFailure(inFlight, writeResult.request, writeResult.exchange)
						return false, readResult.err
					}
				}
				c.reconcileDuplexFailure(inFlight, nil, IORedisExchange{})
				return false, readResult.err
			}
			if len(inFlight) == 0 {
				_ = transport.Close()
				return false, errors.New("ioredis RESP returned a reply without an in-flight command")
			}
			var stop bool
			inFlight, stop = c.applyDuplexReply(transport, inFlight, scheduled, readResult.reply)
			if stop {
				return true, nil
			}
		case <-c.wake:
			continue
		case <-c.ctx.Done():
			return true, c.ctx.Err()
		}
	}
}

func (c *IORedisCompatClient) applyDuplexReply(transport ioredisDuplexTransport, inFlight []*ioredisInFlight, scheduled map[*ioredisRequest]bool, reply IORedisReply) ([]*ioredisInFlight, bool) {
	entry := inFlight[0]
	entry.replies = append(entry.replies, reply)
	if len(entry.replies) != len(entry.request.commands) {
		return inFlight, false
	}
	inFlight = inFlight[1:]
	delete(scheduled, entry.request)
	c.pop(entry.request)
	var commandErr error
	if !entry.request.pipeline && len(entry.replies) == 1 {
		commandErr = entry.replies[0].Error
	}
	c.complete(entry.request, entry.replies, commandErr)
	if ioredisOutstandingReplies(inFlight) == 0 {
		// socketTimeout applies only while commands await replies. ioredis
		// leaves an otherwise idle ready connection open indefinitely.
		transport.finishReplyWait()
	}
	return inFlight, entry.request.quit
}

func ioredisOutstandingReplies(inFlight []*ioredisInFlight) int {
	total := 0
	for _, entry := range inFlight {
		total += len(entry.request.commands) - len(entry.replies)
	}
	return total
}

func (c *IORedisCompatClient) firstUnscheduled(scheduled map[*ioredisRequest]bool) *ioredisRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, request := range c.queue {
		if !scheduled[request] {
			return request
		}
	}
	return nil
}

func (c *IORedisCompatClient) reconcileDuplexFailure(inFlight []*ioredisInFlight, writing *ioredisRequest, writeExchange IORedisExchange) {
	for _, entry := range inFlight {
		if entry.request.pipeline && len(entry.replies) > 0 {
			replies := append([]IORedisReply(nil), entry.replies...)
			for len(replies) < len(entry.request.commands) {
				replies = append(replies, IORedisReply{Error: IORedisAbortError{}})
			}
			c.pop(entry.request)
			c.complete(entry.request, replies, nil)
			continue
		}
		entry.request.replays++
		entry.request.ambiguousReplays++
	}
	if writing != nil {
		writing.replays++
		if writeExchange.MayHaveExecuted {
			writing.ambiguousReplays++
		}
	}
}

func (c *IORedisCompatClient) setNotReady() {
	c.mu.Lock()
	c.ready = false
	c.mu.Unlock()
}

func (c *IORedisCompatClient) shouldDisconnect() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.disconnecting
}

func (c *IORedisCompatClient) currentRetryDelay() time.Duration {
	c.mu.Lock()
	attempts := c.retryAttempts
	c.mu.Unlock()
	return c.policy.retryDelay(attempts)
}

func (c *IORedisCompatClient) connectionFailed(_ error) {
	c.mu.Lock()
	c.retryAttempts++
	flush := c.retryAttempts%(IORedisMaxRetriesPerRequest+1) == 0
	var requests []*ioredisRequest
	if flush {
		requests = c.queue
		c.queue = nil
	}
	c.mu.Unlock()
	if !flush {
		return
	}
	for _, request := range requests {
		if request.pipeline {
			replies := make([]IORedisReply, len(request.commands))
			for index := range replies {
				replies[index].Error = IORedisMaxRetriesError{}
			}
			c.complete(request, replies, nil)
			continue
		}
		c.complete(request, nil, IORedisMaxRetriesError{})
	}
}

func (c *IORedisCompatClient) peek() *ioredisRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) == 0 {
		return nil
	}
	return c.queue[0]
}

func (c *IORedisCompatClient) pop(request *ioredisRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.queue) > 0 && c.queue[0] == request {
		c.queue[0] = nil
		c.queue = c.queue[1:]
		if len(c.queue) == 0 {
			c.queue = nil
		}
	}
}

func (c *IORedisCompatClient) complete(request *ioredisRequest, replies []IORedisReply, err error) {
	result := IORedisResult{
		Replies:          append([]IORedisReply(nil), replies...),
		ReplayCount:      request.replays,
		AmbiguousReplays: request.ambiguousReplays,
	}
	finishIORedisCompatibilitySpan(request.span, replies, err)
	request.future.settle(ioredisCompletion{result: result, err: err})
}

func (c *IORedisCompatClient) finishClosed(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.ready = false
	requests := c.queue
	c.queue = nil
	c.mu.Unlock()
	c.settleFirstReady(err)
	for _, request := range requests {
		c.complete(request, nil, err)
	}
}
