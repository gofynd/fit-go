// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestIORedisBootstrapAndProtocolErrorsOmitRawData(t *testing.T) {
	t.Parallel()

	for name, probe := range map[string]func() error{
		"sentinel host": func() error {
			_, err := ioredisSentinelMasterAddress([]any{"secret-host", "secret-port"})
			return err
		},
		"cluster row": func() error {
			_, err := parseIORedisClusterSlots([]any{[]any{"secret-start", "secret-end", "secret-node"}}, "seed:6379")
			return err
		},
		"integer": func() error {
			_, _, err := readIORedisRESPValue(bufio.NewReader(strings.NewReader(":secret-value\r\n")))
			return err
		},
		"bulk length": func() error {
			_, _, err := readIORedisRESPValue(bufio.NewReader(strings.NewReader("$secret-value\r\n")))
			return err
		},
		"array length": func() error {
			_, _, err := readIORedisRESPValue(bufio.NewReader(strings.NewReader("*secret-value\r\n")))
			return err
		},
		"prefix": func() error {
			_, _, err := readIORedisRESPValue(bufio.NewReader(strings.NewReader("?secret-value\r\n")))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := probe()
			if err == nil {
				t.Fatal("expected an error")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaked raw data: %q", err)
			}
		})
	}
}

func TestIORedisRESPStartupErrorsOmitServerText(t *testing.T) {
	tests := []struct {
		name    string
		options IORedisRESPOptions
		verb    string
		want    string
	}{
		{name: "auth", options: IORedisRESPOptions{Password: "wrong", DisableClientInfo: true}, verb: "AUTH", want: "redis: ioredis AUTH failed"},
		{name: "select", options: IORedisRESPOptions{DB: 2, DisableClientInfo: true}, verb: "SELECT", want: "redis: ioredis SELECT failed"},
		{name: "ready", options: IORedisRESPOptions{DisableClientInfo: true}, verb: "INFO", want: "redis: ioredis INFO readiness check failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startIORedisRESPScenarioServer(t, nil, func(command []string) (string, bool) {
				if strings.EqualFold(command[0], test.verb) {
					return "-ERR secret-tenant-value\r\n", false
				}
				return "+OK\r\n", false
			})
			defer server.stop()
			test.options.Addr = server.addr
			factory, err := NewIORedisRESPTransportFactory(test.options)
			if err != nil {
				t.Fatal(err)
			}
			_, err = factory.Connect(context.Background())
			if err == nil || err.Error() != test.want || strings.Contains(err.Error(), "secret-tenant-value") {
				t.Fatalf("startup error = %v, want safe %q", err, test.want)
			}
		})
	}
}

func TestIORedisPINGErrorsOmitRawReplies(t *testing.T) {
	for name, reply := range map[string]IORedisReply{
		"server error":     {Error: errors.New("ERR secret-value-123")},
		"unexpected value": {Value: "secret-value-123"},
	} {
		t.Run(name, func(t *testing.T) {
			transport := newIORedisSafetyTransport(func([][]string) IORedisExchange {
				return IORedisExchange{Replies: []IORedisReply{reply}, WriteDisposition: IORedisFullyWritten}
			})
			client, err := NewIORedisCompatClientReady(context.Background(), IORedisTransportFactoryFunc(func(context.Context) (IORedisTransport, error) {
				return transport, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Disconnect()
			err = (&ioredisConnection{client: client}).Ping(context.Background())
			if err == nil || strings.Contains(err.Error(), "secret-value-123") {
				t.Fatalf("PING error = %v; raw reply must be omitted", err)
			}
		})
	}
}

func TestIORedisRefreshPreservesSafeCauseWithoutReplyText(t *testing.T) {
	transport := newIORedisSafetyTransport(func([][]string) IORedisExchange {
		return IORedisExchange{Error: io.EOF}
	})
	cluster := &ioredisClusterTransport{
		nodes:  map[string]IORedisTransport{"node": transport},
		first:  "node",
		closed: make(chan struct{}),
		stop:   make(chan struct{}),
	}
	err := cluster.refreshSlots(context.Background(), "node")
	if !errors.Is(err, io.EOF) {
		t.Fatalf("refresh error = %v; want errors.Is(io.EOF)", err)
	}
}

func TestIORedisRedirectOutcomeIsProgrammaticallyInspectable(t *testing.T) {
	t.Parallel()

	for _, mayHaveExecuted := range []bool{false, true} {
		exchange := ioredisRedirectFailure(io.EOF, mayHaveExecuted)
		var redirectErr *IORedisRedirectError
		if !errors.As(exchange.Error, &redirectErr) {
			t.Fatalf("redirect error %T is not inspectable", exchange.Error)
		}
		if redirectErr.MayHaveExecuted != mayHaveExecuted || redirectErr.WriteDisposition != IORedisFullyWritten {
			t.Fatalf("redirect outcome = %+v; want mayHaveExecuted=%v and fully written", redirectErr, mayHaveExecuted)
		}
		if !errors.Is(exchange.Error, io.EOF) {
			t.Fatalf("redirect outcome lost EOF identity: %v", exchange.Error)
		}
		terminal, ok := asIORedisTerminalError(exchange.Error)
		if !ok || terminal.closeTransport != mayHaveExecuted {
			t.Fatalf("terminal close policy = (%v, %v); want %v", terminal.closeTransport, ok, mayHaveExecuted)
		}
	}
}

func TestIORedisRoutingErrorsDoNotEchoArguments(t *testing.T) {
	cluster := &ioredisClusterTransport{first: "node"}
	for _, command := range [][]string{
		{"EVAL", "return 1", "secret-key-count"},
		{"FCALL", "fn", "secret-key-count"},
	} {
		_, err := cluster.route(command)
		if err == nil || strings.Contains(err.Error(), "secret-key-count") {
			t.Fatalf("route(%v) error = %v; argument must be omitted", command[:2], err)
		}
	}
}

func TestIORedisClusterRoutesSubcommandKeys(t *testing.T) {
	cluster := &ioredisClusterTransport{first: "first"}
	for slot := range cluster.slots {
		cluster.slots[slot] = "first"
	}
	key := keyInTopologySlotRange(8192, 16383)
	cluster.slots[ioredisClusterSlot(key)] = "key-node"

	for _, command := range [][]string{
		{"OBJECT", "ENCODING", key},
		{"MEMORY", "USAGE", key},
		{"XGROUP", "CREATE", key, "group", "$"},
		{"XINFO", "GROUPS", key},
	} {
		address, err := cluster.route(command)
		if err != nil || address != "key-node" {
			t.Fatalf("route(%v) = (%q, %v), want key-node", command[:2], address, err)
		}
	}

	for _, command := range [][]string{{"OBJECT", "HELP"}, {"MEMORY", "STATS"}, {"XGROUP", "HELP"}, {"XINFO", "HELP"}} {
		address, err := cluster.route(command)
		if err != nil || address != "first" {
			t.Fatalf("route(%v) = (%q, %v), want first", command, address, err)
		}
	}

	for _, command := range [][]string{{"OBJECT", "secret-subcommand", "secret-key"}, {"XGROUP", "secret-subcommand", "secret-key"}, {"MEMORY", "secret-subcommand", "secret-key"}} {
		_, err := cluster.route(command)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("route(%s) error = %v; want argument-safe rejection", command[0], err)
		}
	}
}

func TestIORedisUnsupportedErrorsDoNotEchoKeys(t *testing.T) {
	t.Parallel()

	for _, command := range [][]string{
		{"BLPOP", "secret-user-key", "0"},
		{"SELECT", "secret-database"},
		{"QUIT", "secret-value"},
	} {
		_, err := validateIORedisQueuedCommand(command)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("validate(%s) error = %v; arguments must be omitted", command[0], err)
		}
	}
}
