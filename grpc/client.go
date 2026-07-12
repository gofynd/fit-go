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

// gRPC client dialing with OpenTelemetry instrumentation.
//
// fit-go instrumented the gRPC SERVER (otelgrpc.NewServerHandler) but shipped no
// client helper, so every outbound RPC was untraced AND did not propagate context:
// the callee started a brand-new trace. Legacy traceclue auto-instruments both
// directions (@opentelemetry/instrumentation-grpc patches client and server).
package grpc

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/gofynd/fit-go/tracing"
)

// TracingDialOptions returns the grpc.DialOptions that install OpenTelemetry client
// instrumentation: a per-RPC client span, and injection of the trace context into
// the outgoing gRPC metadata so the callee CONTINUES this trace.
//
// Returns nil when tracing is disabled, so it is safe to always append:
//
//	opts := append(myOpts, fitgrpc.TracingDialOptions()...)
//	conn, err := grpc.NewClient(target, opts...)
func TracingDialOptions() []grpc.DialOption {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
}

// NewClient dials target with OpenTelemetry client instrumentation already installed,
// plus any caller-supplied options. It is a thin wrapper over grpc.NewClient — use it
// (or TracingDialOptions) instead of grpc.NewClient directly, or outbound RPCs will
// be invisible in traces and will break the trace chain at the service boundary.
func NewClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, append(TracingDialOptions(), opts...)...)
}
