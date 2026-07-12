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
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"github.com/gofynd/fit-go/tracing"
)

// TracingDialOptions returns the grpc.DialOptions that install OpenTelemetry client
// instrumentation: a per-RPC client span, and injection of the trace context into
// the outgoing gRPC metadata so the callee CONTINUES this trace.
//
// Returns nil when tracing is disabled. Append these options after existing
// options so FIT can observe and defer to a caller-owned OTel stats handler:
//
//	opts := append(myOpts, fitgrpc.TracingDialOptions()...)
//	conn, err := grpc.NewClient(target, opts...)
func TracingDialOptions() []grpc.DialOption {
	if t := tracing.Global(); t == nil || !t.IsEnabled() {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(markUnaryInstrumentationBaseline),
		grpc.WithChainStreamInterceptor(markStreamInstrumentationBaseline),
		grpc.WithStatsHandler(newDeduplicatingClientHandler(otelgrpc.NewClientHandler())),
	}
}

// NewClient dials target with OpenTelemetry client instrumentation already installed,
// plus any caller-supplied options. It is a thin wrapper over grpc.NewClient — use it
// (or TracingDialOptions) instead of grpc.NewClient directly, or outbound RPCs will
// be invisible in traces and will break the trace chain at the service boundary.
func NewClient(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// Caller options come first so a supplied fit/otelgrpc stats handler remains
	// authoritative. The FIT handler is last and suppresses itself when an earlier
	// handler has already installed a per-RPC span; non-tracing options are kept.
	traceOpts := TracingDialOptions()
	allOpts := make([]grpc.DialOption, 0, len(opts)+len(traceOpts))
	allOpts = append(allOpts, opts...)
	allOpts = append(allOpts, traceOpts...)
	return grpc.NewClient(target, allOpts...)
}

type clientInstrumentationBaselineKey struct{}

func markInstrumentationBaseline(ctx context.Context) context.Context {
	if _, ok := ctx.Value(clientInstrumentationBaselineKey{}).(oteltrace.SpanContext); ok {
		return ctx
	}
	return context.WithValue(
		ctx,
		clientInstrumentationBaselineKey{},
		oteltrace.SpanContextFromContext(ctx),
	)
}

func markUnaryInstrumentationBaseline(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	return invoker(markInstrumentationBaseline(ctx), method, req, reply, cc, opts...)
}

func markStreamInstrumentationBaseline(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return streamer(markInstrumentationBaseline(ctx), desc, cc, method, opts...)
}

type deduplicatingClientHandler struct {
	next stats.Handler
}

func newDeduplicatingClientHandler(next stats.Handler) stats.Handler {
	return &deduplicatingClientHandler{next: next}
}

func (h *deduplicatingClientHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	baseline, marked := ctx.Value(clientInstrumentationBaselineKey{}).(oteltrace.SpanContext)
	current := oteltrace.SpanContextFromContext(ctx)
	if marked && current.IsValid() && !current.Equal(baseline) {
		return context.WithValue(ctx, h, false)
	}
	ctx = h.next.TagRPC(ctx, info)
	return context.WithValue(ctx, h, true)
}

func (h *deduplicatingClientHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	instrumented, _ := ctx.Value(h).(bool)
	if instrumented {
		h.next.HandleRPC(ctx, event)
	}
}

func (h *deduplicatingClientHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return h.next.TagConn(ctx, info)
}

func (h *deduplicatingClientHandler) HandleConn(ctx context.Context, event stats.ConnStats) {
	h.next.HandleConn(ctx, event)
}
