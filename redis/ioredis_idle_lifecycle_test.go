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
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestIORedisRESPIdleCloseReconnectsWithoutCommands(t *testing.T) {
	peers := make(chan net.Conn, 4)
	var dials atomic.Int32
	client, err := NewIORedisRESPCompatClient(IORedisRESPOptions{
		Addr: "pipe:0", DisableClientInfo: true, DisableReadyCheck: true,
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			local, peer := net.Pipe()
			dials.Add(1)
			peers <- peer
			return local, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Disconnect)
	first := idleLifecyclePeer(t, peers)
	_ = first.Close()
	// Each dial creates a new socket. No command wakes the client here.
	second := idleLifecyclePeer(t, peers)
	t.Cleanup(func() { _ = second.Close() })
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want initial connection and one eager reconnect", dials.Load())
	}
	go func() {
		_, err := readRESPCommand(bufio.NewReader(second))
		if err == nil {
			_, _ = io.WriteString(second, "+PONG\r\n")
		}
	}()
	assertIORedisFutureOK(t, client.Submit("PING"), "PONG", 0, 0)
	client.Disconnect()
	idleLifecycleDone(t, client)
	if _, err := second.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("reconnected socket after Disconnect = %v, want EOF", err)
	}
	if dials.Load() != 2 {
		t.Fatalf("Disconnect reconnected: dials = %d", dials.Load())
	}
}

func TestIORedisRESPIdleCloseEmptyOfflineQuit(t *testing.T) {
	local, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	transport := newIORedisRESPTransport(local, 0, time.Second, nil)
	var connects atomic.Int32
	retryEntered := make(chan struct{}, 1)
	client, err := newIORedisCompatClient(IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		if connects.Add(1) == 1 {
			return transport, nil
		}
		return nil, errors.New("offline")
	}), ioredisPolicy{
		retryDelay: IORedisRetryDelay,
		wait: func(ctx context.Context, _ time.Duration) bool {
			retryEntered <- struct{}{}
			<-ctx.Done()
			return false
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Disconnect)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.WaitForFirstReady(ctx); err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
	select {
	case <-retryEntered:
	case <-ctx.Done():
		t.Fatal("idle remote EOF did not enter the offline retry wait")
	}
	if err := client.Quit(ctx); err != nil {
		t.Fatalf("empty offline Quit: %v", err)
	}
	idleLifecycleDone(t, client)
	if connects.Load() != 1 {
		t.Fatalf("Quit bypassed offline retry wait: connects = %d", connects.Load())
	}
}

func TestIORedisRESPIdleClosePreservesReplyBeforeEOF(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		name := "direct"
		if pipeline {
			name = "partial pipeline"
		}
		t.Run(name, func(t *testing.T) {
			local, peer := net.Pipe()
			t.Cleanup(func() { _ = peer.Close() })
			transport := &idleLifecycleReplyBeforeEOF{ioredisRESPTransport: newIORedisRESPTransport(local, 0, time.Second, nil)}
			var connects atomic.Int32
			reconnected := make(chan net.Conn, 1)
			client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
				if connects.Add(1) == 1 {
					return transport, nil
				}
				freshLocal, freshPeer := net.Pipe()
				reconnected <- freshPeer
				return newIORedisRESPTransport(freshLocal, 0, time.Second, nil), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Disconnect)
			go func() {
				reader := bufio.NewReader(peer)
				_, _ = readRESPCommand(reader)
				if pipeline {
					_, _ = readRESPCommand(reader)
				}
				_, _ = io.WriteString(peer, "+PONG\r\n")
				_ = peer.Close()
			}()
			if pipeline {
				result, err := waitIORedisFuture(t, client.SubmitPipeline([]string{"PING"}, []string{"PING"}))
				if err != nil || len(result.Replies) != 2 || result.Replies[0].Value != "PONG" || result.Replies[0].Error != nil {
					t.Fatalf("partial pipeline = %+v, %v", result, err)
				}
				var abort IORedisAbortError
				if !errors.As(result.Replies[1].Error, &abort) || result.ReplayCount != 0 || result.AmbiguousReplays != 0 {
					t.Fatalf("unread suffix was not aborted without replay: %+v", result)
				}
			} else {
				assertIORedisFutureOK(t, client.Submit("PING"), "PONG", 0, 0)
			}
			freshPeer := idleLifecyclePeer(t, reconnected)
			t.Cleanup(func() { _ = freshPeer.Close() })
			client.Disconnect()
			idleLifecycleDone(t, client)
		})
	}
}

func TestIORedisRESPIdleCloseKeepsInFlightReplayBoundary(t *testing.T) {
	var connects atomic.Int32
	commands := make(chan []string, 4)
	client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
		local, peer := net.Pipe()
		connection := connects.Add(1)
		go func() {
			defer peer.Close()
			reader := bufio.NewReader(peer)
			for index := 0; index < 2; index++ {
				command, err := readRESPCommand(reader)
				if err != nil {
					return
				}
				commands <- command
				if connection > 1 {
					_, _ = io.WriteString(peer, ":1\r\n")
				}
			}
			if connection > 1 {
				// Leave the replacement connection idle until Disconnect.
				_, _ = reader.ReadByte()
			}
		}()
		return newIORedisRESPTransport(local, 0, time.Second, nil), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Disconnect)
	first := client.Submit("INCR", "first")
	second := client.Submit("INCR", "second")
	assertIORedisFutureOK(t, first, int64(1), 1, 1)
	assertIORedisFutureOK(t, second, int64(1), 1, 1)
	for _, want := range []string{"first", "second", "first", "second"} {
		select {
		case command := <-commands:
			if len(command) != 2 || command[0] != "incr" || command[1] != want {
				t.Fatalf("wire command = %v, want INCR %s", command, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing wire command INCR %s", want)
		}
	}
	client.Disconnect()
	idleLifecycleDone(t, client)
	if connects.Load() != 2 {
		t.Fatalf("connections = %d, want exactly initial and replay connections", connects.Load())
	}
}

// Hold the successfully read reply until the actual RESP reader has published
// EOF and Closed. The session must still consume this reply before acting on
// the close signal, including when more pipeline replies remain outstanding.
type idleLifecycleReplyBeforeEOF struct {
	*ioredisRESPTransport
}

func (t *idleLifecycleReplyBeforeEOF) readReply(ctx context.Context) (IORedisReply, error) {
	reply, err := t.ioredisRESPTransport.readReply(ctx)
	if err == nil {
		select {
		case <-t.Closed():
		case <-ctx.Done():
			return IORedisReply{}, ctx.Err()
		}
	}
	return reply, err
}

func idleLifecyclePeer(t *testing.T, peers <-chan net.Conn) net.Conn {
	t.Helper()
	select {
	case peer := <-peers:
		return peer
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a fresh RESP connection")
		return nil
	}
}

func idleLifecycleDone(t *testing.T, client *IORedisCompatClient) {
	t.Helper()
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("client shutdown did not complete")
	}
}
