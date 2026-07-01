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
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// isClientCommandUnsupported must match ONLY the server rejecting the CLIENT
// command (so the standalone dial fallback drops the client name + identity), and
// must NOT match network/auth/other-unknown-command errors (which should surface
// to the caller's health check unchanged).
func TestIsClientCommandUnsupported(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			// The exact error observed in the SIT pods.
			"client setname rejected",
			errors.New("ERR unknown command `client`, with args beginning with: `setname`, `fplt-highbrow-app-srvr-dply`"),
			true,
		},
		{
			"client setinfo rejected",
			errors.New("ERR unknown command 'CLIENT', with args beginning with: 'SETINFO'"),
			true,
		},
		{
			// Redis < 7.2 supports CLIENT but not the SETINFO SUBCOMMAND that go-redis
			// sends for lib identity — rejected as "unknown subcommand ... setinfo".
			"setinfo subcommand rejected (redis < 7.2)",
			errors.New("ERR Unknown subcommand or wrong number of arguments for 'SETINFO'. Try CLIENT HELP."),
			true,
		},
		{
			"setname subcommand rejected",
			errors.New("ERR Unknown subcommand or wrong number of arguments for 'SETNAME'."),
			true,
		},
		{
			// A different subcommand rejected (not setinfo/setname) must NOT match.
			"different subcommand rejected",
			errors.New("ERR Unknown subcommand or wrong number of arguments for 'GETNAME'. Try CLIENT HELP."),
			false,
		},
		{"connection refused", errors.New("dial tcp 10.0.0.1:6379: connect: connection refused"), false},
		{"auth required", errors.New("NOAUTH Authentication required."), false},
		{"different unknown command", errors.New("ERR unknown command `subscribe`"), false},
		// A different command rejected, with "client" only in its ARGS, must NOT match.
		{"client only in args", errors.New("ERR unknown command `foo`, with args beginning with: `client`"), false},
		{"wrongtype", errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isClientCommandUnsupported(tc.err); got != tc.want {
				t.Fatalf("isClientCommandUnsupported(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// fakePinger lets us drive clientHandshakeRejected without a real server.
type fakePinger struct{ err error }

func (f fakePinger) Ping(ctx context.Context) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx, "ping")
	cmd.SetErr(f.err)
	return cmd
}

// clientHandshakeRejected (used by all three dial paths — standalone, cluster,
// sentinel) returns true ONLY for the CLIENT-rejection error.
func TestClientHandshakeRejected(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"rejected", errors.New("ERR unknown command `client`, with args beginning with: `setname`"), true},
		{"setinfo subcommand rejected", errors.New("ERR Unknown subcommand or wrong number of arguments for 'SETINFO'. Try CLIENT HELP."), true},
		{"nil/healthy", nil, false},
		{"other error", errors.New("dial tcp: connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientHandshakeRejected(context.Background(), fakePinger{err: tc.err}); got != tc.want {
				t.Fatalf("clientHandshakeRejected(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
