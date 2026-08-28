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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIORedisRESPCodec(t *testing.T) {
	encoded, err := encodeIORedisRESPCommands([][]string{{"SET", "snowman", "☃"}, {"GET", "snowman"}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wantEncoded := "*3\r\n$3\r\nSET\r\n$7\r\nsnowman\r\n$3\r\n☃\r\n*2\r\n$3\r\nGET\r\n$7\r\nsnowman\r\n"
	if string(encoded) != wantEncoded {
		t.Fatalf("encoded bytes = %q, want %q", encoded, wantEncoded)
	}
	if _, err := encodeIORedisRESPCommands([][]string{{}}); err == nil {
		t.Fatal("empty command accepted")
	}

	tests := []struct {
		name      string
		wire      string
		wantValue any
		wantReply string
		wantFatal string
	}{
		{name: "simple", wire: "+OK\r\n", wantValue: "OK"},
		{name: "reply error", wire: "-WRONGTYPE operation\r\n", wantReply: "WRONGTYPE operation"},
		{name: "integer", wire: ":42\r\n", wantValue: int64(42)},
		{name: "bulk", wire: "$3\r\nfoo\r\n", wantValue: "foo"},
		{name: "null bulk", wire: "$-1\r\n"},
		{name: "null array", wire: "*-1\r\n"},
		{name: "nested array", wire: "*3\r\n:1\r\n$3\r\nfoo\r\n*2\r\n+OK\r\n-ERR nested\r\n", wantValue: []any{int64(1), "foo", []any{"OK", errors.New("ERR nested")}}},
		{name: "unsupported RESP3", wire: "_\r\n", wantFatal: "unsupported RESP2 prefix '_'"},
		{name: "bad integer", wire: ":nan\r\n", wantFatal: "invalid RESP integer"},
		{name: "bad bulk length", wire: "$-2\r\n", wantFatal: "invalid RESP bulk length"},
		{name: "bad array length", wire: "*-2\r\n", wantFatal: "invalid RESP array length"},
		{name: "bad bulk terminator", wire: "$1\r\nxZZ", wantFatal: "invalid RESP bulk terminator"},
		{name: "bad line terminator", wire: "+OK\n", wantFatal: "invalid RESP line terminator"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, replyErr, fatalErr := readIORedisRESPValue(bufio.NewReader(strings.NewReader(test.wire)))
			if test.wantFatal != "" {
				if fatalErr == nil || !strings.Contains(fatalErr.Error(), test.wantFatal) {
					t.Fatalf("fatal error = %v, want substring %q", fatalErr, test.wantFatal)
				}
				return
			}
			if fatalErr != nil {
				t.Fatalf("fatal error: %v", fatalErr)
			}
			if test.wantReply != "" {
				if replyErr == nil || replyErr.Error() != test.wantReply {
					t.Fatalf("reply error = %v, want %q", replyErr, test.wantReply)
				}
				return
			}
			if !reflect.DeepEqual(value, test.wantValue) {
				t.Fatalf("value = %#v, want %#v", value, test.wantValue)
			}
		})
	}
}

func TestIORedisRESPOptionsFailClosed(t *testing.T) {
	tests := []struct {
		name    string
		options IORedisRESPOptions
	}{
		{name: "missing address"},
		{name: "negative database", options: IORedisRESPOptions{Addr: "localhost:6379", DB: -1}},
		{name: "negative connect timeout", options: IORedisRESPOptions{Addr: "localhost:6379", ConnectTimeout: -1}},
		{name: "negative socket timeout", options: IORedisRESPOptions{Addr: "localhost:6379", SocketTimeout: -1}},
		{name: "negative keepalive", options: IORedisRESPOptions{Addr: "localhost:6379", KeepAlive: -1}},
		{name: "negative loading retry", options: IORedisRESPOptions{Addr: "localhost:6379", MaxLoadingRetryTime: -1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewIORedisRESPTransportFactory(test.options); err == nil {
				t.Fatal("invalid options accepted")
			}
		})
	}
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{Addr: "localhost:6379"})
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if factory.options.ConnectTimeout != 10*time.Second || factory.options.MaxLoadingRetryTime != 10*time.Second || factory.options.readyWait == nil {
		t.Fatalf("defaults = %+v", factory.options)
	}
	if _, err := (*IORedisRESPTransportFactory)(nil).Connect(context.Background()); err == nil {
		t.Fatal("nil factory connected")
	}
}

func TestIORedisRESPConnectTimeoutAndIdleClose(t *testing.T) {
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr: "ignored:0", ConnectTimeout: 20 * time.Millisecond,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if _, err := factory.Connect(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connect error = %v, want deadline", err)
	}

	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		if strings.EqualFold(command[0], "INFO") {
			return "$11\r\nloading:0\r\n\r\n", true
		}
		return "+OK\r\n", false
	})
	defer server.stop()
	transportFactory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr: server.addr, DisableClientInfo: true,
	})
	if err != nil {
		t.Fatalf("transport factory: %v", err)
	}
	transport, err := transportFactory.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	select {
	case <-transport.Closed():
	case <-time.After(time.Second):
		t.Fatal("idle remote close was not signaled")
	}
	_ = transport.Close()
	_ = transport.Close()
}

func TestIORedisRESPConvenienceConstructorRemainsExplicit(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	go func() {
		reader := bufio.NewReader(serverSide)
		for {
			command, err := readRESPCommand(reader)
			if err != nil {
				return
			}
			if strings.EqualFold(command[0], "PING") {
				_, _ = io.WriteString(serverSide, "+PONG\r\n")
			}
		}
	}()
	client, err := NewIORedisRESPCompatClient(IORedisRESPOptions{
		Addr: "pipe:0", DisableClientInfo: true, DisableReadyCheck: true,
		DialContext: func(context.Context, string, string) (net.Conn, error) { return clientSide, nil },
	})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	assertIORedisFutureOK(t, client.Submit("PING"), "PONG", 0, 0)
	client.Disconnect()
}

func TestIORedisRESPSocketTimeoutDoesNotCloseIdleReadyClient(t *testing.T) {
	var infoCalls atomic.Int32
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		if strings.EqualFold(command[0], "INFO") {
			infoCalls.Add(1)
			return "$11\r\nloading:0\r\n\r\n", false
		}
		if strings.EqualFold(command[0], "PING") {
			return "+PONG\r\n", false
		}
		return "+OK\r\n", false
	})
	defer server.stop()
	client, err := NewIORedisRESPCompatClient(IORedisRESPOptions{
		Addr: server.addr, DisableClientInfo: true, SocketTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}
	defer client.Disconnect()
	assertIORedisFutureOK(t, client.Submit("PING"), "PONG", 0, 0)
	time.Sleep(60 * time.Millisecond)
	assertIORedisFutureOK(t, client.Submit("PING"), "PONG", 0, 0)
	if got := infoCalls.Load(); got != 1 {
		t.Fatalf("ready-check INFO calls = %d, want one connection across idle socket timeout", got)
	}
}

func TestIORedisRESPStartupMatchesFITJSOrderAndTolerance(t *testing.T) {
	var infoCalls int
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "AUTH", "SELECT":
			return "+OK\r\n", false
		case "CLIENT":
			return "-ERR unknown CLIENT subcommand\r\n", false
		case "INFO":
			infoCalls++
			if infoCalls == 1 {
				return "$36\r\nloading:1\r\nloading_eta_seconds:0\r\n\r\n\r\n", false
			}
			return "$11\r\nloading:0\r\n\r\n", false
		case "PING":
			return "+PONG\r\n", false
		default:
			return "-ERR unexpected\r\n", false
		}
	})
	defer server.stop()

	var delays []time.Duration
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr:           server.addr,
		Username:       "legacy-user",
		Password:       "legacy-pass",
		DB:             4,
		ConnectionName: "uat-cache",
		readyWait: func(_ context.Context, delay time.Duration) bool {
			delays = append(delays, delay)
			return true
		},
	})
	if err != nil {
		t.Fatalf("NewIORedisRESPTransportFactory: %v", err)
	}
	transport, err := factory.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer transport.Close()

	exchange := transport.Exchange(context.Background(), [][]string{{"PING"}})
	if exchange.Error != nil || len(exchange.Replies) != 1 || exchange.Replies[0].Value != "PONG" {
		t.Fatalf("PING exchange = %+v", exchange)
	}
	wantCommands := [][]string{
		{"auth", "legacy-user", "legacy-pass"},
		{"select", "4"},
		{"client", "setname", "uat-cache"},
		{"client", "SETINFO", "LIB-NAME", "ioredis"},
		{"client", "SETINFO", "LIB-VER", "5.11.1"},
		{"info"}, {"info"}, {"PING"},
	}
	if got := server.commandsSnapshot(); !reflect.DeepEqual(got, wantCommands) {
		t.Fatalf("startup commands = %#v, want %#v", got, wantCommands)
	}
	if !reflect.DeepEqual(delays, []time.Duration{0}) {
		t.Fatalf("loading delays = %v, want [0s]", delays)
	}
}

func TestIORedisRESPStartupErrorBoundaries(t *testing.T) {
	tests := []struct {
		name       string
		options    IORedisRESPOptions
		reply      func([]string) string
		wantErr    string
		wantAccept bool
	}{
		{
			name:    "auth rejected",
			options: IORedisRESPOptions{Password: "wrong", DisableClientInfo: true},
			reply: func(command []string) string {
				if strings.EqualFold(command[0], "AUTH") {
					return "-WRONGPASS invalid username-password pair\r\n"
				}
				return "+OK\r\n"
			},
			wantErr: "WRONGPASS invalid username-password pair",
		},
		{
			name:    "auth unnecessary warning tolerated",
			options: IORedisRESPOptions{Password: "unused", DisableClientInfo: true},
			reply: func(command []string) string {
				if strings.EqualFold(command[0], "AUTH") {
					return "-ERR Client sent AUTH, but no password is set\r\n"
				}
				return "$11\r\nloading:0\r\n\r\n"
			},
			wantAccept: true,
		},
		{
			name:    "select rejects FIT startup",
			options: IORedisRESPOptions{DB: 99, DisableClientInfo: true},
			reply: func(command []string) string {
				if strings.EqualFold(command[0], "SELECT") {
					return "-ERR DB index is out of range\r\n"
				}
				return "+OK\r\n"
			},
			wantErr: "ERR DB index is out of range",
		},
		{
			name:    "info noperm is ready",
			options: IORedisRESPOptions{DisableClientInfo: true},
			reply: func([]string) string {
				return "-NOPERM this user has no permissions to run the 'info' command\r\n"
			},
			wantAccept: true,
		},
		{
			name:    "info other error rejects",
			options: IORedisRESPOptions{DisableClientInfo: true},
			reply: func([]string) string {
				return "-ERR ready check failed\r\n"
			},
			wantErr: "ERR ready check failed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
				return test.reply(command), false
			})
			defer server.stop()
			test.options.Addr = server.addr
			factory, err := NewIORedisRESPTransportFactory(test.options)
			if err != nil {
				t.Fatalf("factory: %v", err)
			}
			transport, err := factory.Connect(context.Background())
			if test.wantAccept {
				if err != nil {
					t.Fatalf("Connect: %v", err)
				}
				_ = transport.Close()
				return
			}
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("Connect error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestIORedisRESPWriteDispositionFaultMatrix(t *testing.T) {
	t.Run("not written", func(t *testing.T) {
		client, peer := net.Pipe()
		defer peer.Close()
		factory := newPipeIORedisRESPFactory(t, &ioredisWriteFaultConn{Conn: client, failAfter: 0, err: errors.New("before write")})
		transport, err := factory.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		exchange := transport.Exchange(context.Background(), [][]string{{"SET", "key", "value"}})
		if exchange.WriteDisposition != IORedisNotWritten || exchange.MayHaveExecuted || exchange.BytesWritten != 0 || exchange.NetworkBytesWritten != 0 || exchange.Error == nil {
			t.Fatalf("not-written exchange = %+v", exchange)
		}
	})

	t.Run("partial write", func(t *testing.T) {
		client, peer := net.Pipe()
		defer peer.Close()
		go io.Copy(io.Discard, peer)
		factory := newPipeIORedisRESPFactory(t, &ioredisWriteFaultConn{Conn: client, failAfter: 7, err: errors.New("partial write")})
		transport, err := factory.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		exchange := transport.Exchange(context.Background(), [][]string{{"SET", "key", "value"}})
		if exchange.WriteDisposition != IORedisPartiallyWritten || !exchange.MayHaveExecuted || exchange.BytesWritten != 7 || exchange.NetworkBytesWritten != 7 || exchange.BytesTotal <= exchange.BytesWritten || exchange.Error == nil {
			t.Fatalf("partial-write exchange = %+v", exchange)
		}
	})

	t.Run("fully written lost reply", func(t *testing.T) {
		client, peer := net.Pipe()
		factory := newPipeIORedisRESPFactory(t, client)
		go func() {
			_, _ = readRESPCommand(bufio.NewReader(peer))
			_ = peer.Close()
		}()
		transport, err := factory.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		exchange := transport.Exchange(context.Background(), [][]string{{"INCR", "counter"}})
		if exchange.WriteDisposition != IORedisFullyWritten || !exchange.MayHaveExecuted || exchange.BytesWritten != exchange.BytesTotal || exchange.NetworkBytesWritten != int64(exchange.BytesTotal) || len(exchange.Replies) != 0 || exchange.Error == nil {
			t.Fatalf("lost-reply exchange = %+v", exchange)
		}
	})

	t.Run("partial pipeline reply", func(t *testing.T) {
		client, peer := net.Pipe()
		factory := newPipeIORedisRESPFactory(t, client)
		go func() {
			reader := bufio.NewReader(peer)
			_, _ = readRESPCommand(reader)
			_, _ = readRESPCommand(reader)
			_, _ = io.WriteString(peer, "+OK\r\n")
			_ = peer.Close()
		}()
		transport, err := factory.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		exchange := transport.Exchange(context.Background(), [][]string{{"SET", "one", "1"}, {"SET", "two", "2"}})
		if exchange.WriteDisposition != IORedisFullyWritten || len(exchange.Replies) != 1 || exchange.Replies[0].Value != "OK" || exchange.Error == nil {
			t.Fatalf("partial-reply exchange = %+v", exchange)
		}
	})
}

func TestIORedisRESPSocketTimeoutAndPartialReadActivity(t *testing.T) {
	t.Run("exact timeout error", func(t *testing.T) {
		client, peer := net.Pipe()
		defer peer.Close()
		factory := newPipeIORedisRESPFactory(t, client)
		factory.options.SocketTimeout = 20 * time.Millisecond
		go func() {
			_, _ = readRESPCommand(bufio.NewReader(peer))
			<-time.After(time.Second)
		}()
		transport, err := factory.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		exchange := transport.Exchange(context.Background(), [][]string{{"GET", "never-replies"}})
		want := "Socket timeout. Expecting data, but didn't receive any in 20ms."
		if exchange.Error == nil || exchange.Error.Error() != want || exchange.WriteDisposition != IORedisFullyWritten {
			t.Fatalf("timeout exchange = %+v, want %q", exchange, want)
		}
	})

	t.Run("partial bytes refresh timeout", func(t *testing.T) {
		client, peer := net.Pipe()
		factory := newPipeIORedisRESPFactory(t, client)
		factory.options.SocketTimeout = 25 * time.Millisecond
		go func() {
			_, _ = readRESPCommand(bufio.NewReader(peer))
			for _, fragment := range []string{"$4\r\n", "P", "O", "N", "G\r\n"} {
				_, _ = io.WriteString(peer, fragment)
				time.Sleep(15 * time.Millisecond)
			}
		}()
		transport, err := factory.Connect(context.Background())
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		defer transport.Close()
		exchange := transport.Exchange(context.Background(), [][]string{{"PING"}})
		if exchange.Error != nil || len(exchange.Replies) != 1 || exchange.Replies[0].Value != "PONG" {
			t.Fatalf("partial activity exchange = %+v", exchange)
		}
	})
}

func TestIORedisRESPTLSAndQuit(t *testing.T) {
	certificate := newIORedisTestCertificate(t)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	clientTLS := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12} // test-only certificate
	server := startIORedisRESPScenarioServer(t, serverTLS, func(command []string) (string, bool) {
		switch strings.ToUpper(command[0]) {
		case "INFO":
			return "$11\r\nloading:0\r\n\r\n", false
		case "QUIT":
			return "+OK\r\n", true
		default:
			return "+OK\r\n", false
		}
	})
	defer server.stop()
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr: server.addr, TLSConfig: clientTLS, DisableClientInfo: true,
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	client := newFastIORedisClient(t, factory)
	assertIORedisFutureOK(t, client.Submit("PING"), "OK", 0, 0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Quit(ctx); err != nil {
		t.Fatalf("TLS Quit: %v", err)
	}
	if got := server.commandNames(); !reflect.DeepEqual(got, []string{"INFO", "PING", "QUIT"}) {
		t.Fatalf("TLS command order = %v", got)
	}
}

func TestIORedisRESPTransportReplaysCompleteUnfulfilledSet(t *testing.T) {
	var connectionNumber int
	var mu sync.Mutex
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		if strings.EqualFold(command[0], "INFO") {
			connectionNumber++
			return "$11\r\nloading:0\r\n\r\n", false
		}
		if strings.EqualFold(command[0], "INCR") {
			if connectionNumber == 1 {
				// Withhold the first reply until the second direct command reaches the
				// same connection, then close without either reply. This is the exact
				// multi-in-flight boundary exposed by the pinned ioredis live oracle.
				return "", len(command) > 1 && command[1] == "second"
			}
			return ":1\r\n", false
		}
		return "+OK\r\n", false
	})
	defer server.stop()
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr: server.addr, ConnectionName: "cache",
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	client := newFastIORedisClient(t, factory)
	defer client.Disconnect()
	first := client.Submit("INCR", "first")
	second := client.Submit("INCR", "second")
	assertIORedisFutureOK(t, first, int64(1), 1, 1)
	assertIORedisFutureOK(t, second, int64(1), 1, 1)
	wantCommands := [][]string{
		{"client", "setname", "cache"},
		{"client", "SETINFO", "LIB-NAME", "ioredis"},
		{"client", "SETINFO", "LIB-VER", "5.11.1"},
		{"info"},
		{"incr", "first"},
		{"incr", "second"},
		{"client", "setname", "cache"},
		{"client", "SETINFO", "LIB-NAME", "ioredis"},
		{"client", "SETINFO", "LIB-VER", "5.11.1"},
		{"info"},
		{"incr", "first"},
		{"incr", "second"},
	}
	if got := server.commandsSnapshot(); !reflect.DeepEqual(got, wantCommands) {
		t.Fatalf("lost-reply wire commands = %#v, want pinned ioredis oracle %#v", got, wantCommands)
	}
}

func TestIORedisRESPDuplexPartialPipelineAbortsUnreadSuffix(t *testing.T) {
	var connectionNumber int
	var mu sync.Mutex
	server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		if strings.EqualFold(command[0], "INFO") {
			connectionNumber++
			return "$11\r\nloading:0\r\n\r\n", false
		}
		if connectionNumber == 1 && strings.EqualFold(command[0], "SET") {
			if len(command) > 1 && command[1] == "first" {
				return "+OK\r\n", false
			}
			return "", true
		}
		return "+OK\r\n", false
	})
	defer server.stop()
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr: server.addr, DisableClientInfo: true,
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	client := newFastIORedisClient(t, factory)
	defer client.Disconnect()

	result, err := waitIORedisFuture(t, client.SubmitPipeline(
		[]string{"SET", "first", "1"},
		[]string{"SET", "second", "2"},
		[]string{"SET", "third", "3"},
	))
	if err != nil {
		t.Fatalf("pipeline top-level error = %v, want nil", err)
	}
	if len(result.Replies) != 3 || result.Replies[0].Value != "OK" || result.Replies[0].Error != nil {
		t.Fatalf("pipeline replies = %#v", result.Replies)
	}
	for index := 1; index < len(result.Replies); index++ {
		var abort IORedisAbortError
		if !errors.As(result.Replies[index].Error, &abort) {
			t.Fatalf("pipeline reply %d error = %T %v, want IORedisAbortError", index, result.Replies[index].Error, result.Replies[index].Error)
		}
	}
	if result.ReplayCount != 0 || result.AmbiguousReplays != 0 {
		t.Fatalf("pipeline replay counters = %+v, want zero", result)
	}
	assertIORedisFutureOK(t, client.Submit("SET", "later", "4"), "OK", 0, 0)

	var keys []string
	for _, command := range server.commandsSnapshot() {
		if strings.EqualFold(command[0], "SET") {
			keys = append(keys, command[1])
		}
	}
	if !reflect.DeepEqual(keys, []string{"first", "second", "later"}) {
		t.Fatalf("pipeline failure wire keys = %v", keys)
	}
}

func newPipeIORedisRESPFactory(t *testing.T, connection net.Conn) *IORedisRESPTransportFactory {
	t.Helper()
	factory, err := NewIORedisRESPTransportFactory(IORedisRESPOptions{
		Addr: "pipe:0", DisableClientInfo: true, DisableReadyCheck: true,
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return connection, nil
		},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	return factory
}

type ioredisWriteFaultConn struct {
	net.Conn
	failAfter int
	err       error
	written   int
}

func (c *ioredisWriteFaultConn) Write(buffer []byte) (int, error) {
	remaining := c.failAfter - c.written
	if remaining <= 0 {
		return 0, c.err
	}
	if remaining < len(buffer) {
		count, _ := c.Conn.Write(buffer[:remaining])
		c.written += count
		return count, c.err
	}
	count, err := c.Conn.Write(buffer)
	c.written += count
	return count, err
}

type ioredisRESPScenarioServer struct {
	listener net.Listener
	addr     string
	tls      *tls.Config
	handler  func([]string) (string, bool)

	mu          sync.Mutex
	commands    [][]string
	connections map[net.Conn]struct{}
	done        chan struct{}
}

func startIORedisRESPScenarioServer(t *testing.T, tlsConfig *tls.Config, handler func([]string) (string, bool)) *ioredisRESPScenarioServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := &ioredisRESPScenarioServer{
		listener: listener, addr: listener.Addr().String(), tls: tlsConfig, handler: handler,
		connections: make(map[net.Conn]struct{}), done: make(chan struct{}),
	}
	go server.serve()
	return server
}

func (s *ioredisRESPScenarioServer) serve() {
	defer close(s.done)
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		if s.tls != nil {
			connection = tls.Server(connection, s.tls)
		}
		s.mu.Lock()
		s.connections[connection] = struct{}{}
		s.mu.Unlock()
		go s.serveConnection(connection)
	}
}

func (s *ioredisRESPScenarioServer) serveConnection(connection net.Conn) {
	defer func() {
		_ = connection.Close()
		s.mu.Lock()
		delete(s.connections, connection)
		s.mu.Unlock()
	}()
	reader := bufio.NewReader(connection)
	for {
		command, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.commands = append(s.commands, append([]string(nil), command...))
		s.mu.Unlock()
		reply, closeConnection := s.handler(command)
		if reply != "" {
			if _, err := io.WriteString(connection, reply); err != nil {
				return
			}
		}
		if closeConnection {
			return
		}
	}
}

func (s *ioredisRESPScenarioServer) commandsSnapshot() [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	commands := make([][]string, len(s.commands))
	for index := range s.commands {
		commands[index] = append([]string(nil), s.commands[index]...)
	}
	return commands
}

func (s *ioredisRESPScenarioServer) commandNames() []string {
	commands := s.commandsSnapshot()
	names := make([]string, len(commands))
	for index := range commands {
		names[index] = strings.ToUpper(commands[index][0])
	}
	return names
}

func (s *ioredisRESPScenarioServer) stop() {
	_ = s.listener.Close()
	s.mu.Lock()
	connections := make([]net.Conn, 0, len(s.connections))
	for connection := range s.connections {
		connections = append(connections, connection)
	}
	s.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	select {
	case <-s.done:
	case <-time.After(time.Second):
	}
}

func newIORedisTestCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{certificateDER}, PrivateKey: privateKey}
}
