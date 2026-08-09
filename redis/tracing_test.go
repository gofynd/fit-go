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
	"net"
	"strings"
	"sync"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/tracingtest"
	"github.com/gofynd/fit-go/tracing"
)

func recordingRedisProvider(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Errorf("tracer provider shutdown: %v", err)
		}
	})
	return provider, exporter
}

func recordingRedisTracer(t *testing.T) (*tracing.Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	t.Setenv("OTEL_SDK_DISABLED", "false")

	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	enabled := true
	tracer, err := tracing.New(context.Background(), tracing.Options{
		ServiceName:            "ioredis-compatibility-test",
		Enabled:                &enabled,
		Sampler:                "always_on",
		SpanExporter:           exporter,
		UseSimpleSpanProcessor: true,
	})
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	t.Cleanup(func() {
		if err := tracer.Shutdown(context.Background()); err != nil {
			t.Errorf("tracer shutdown: %v", err)
		}
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return tracer, exporter
}

func exportedRedisSpan(t *testing.T, exporter *tracetest.InMemoryExporter, name string) tracetest.SpanStub {
	t.Helper()
	for _, span := range exporter.GetSpans() {
		if span.Name == name {
			return span
		}
	}
	t.Fatalf("exported span %q not found", name)
	return tracetest.SpanStub{}
}

// attachTracingHook is gated on tracing-enabled and never dials eagerly. Detailed
// span behavior is pinned by the exporter test below.

func TestAttachTracingHook_NoOpWhenDisabled(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0"})
	defer c.Close()
	// Tracing disabled by default → no instrumentation added, no panic.
	attachTracingHook(c)
}

func TestAttachTracingHook_EnabledInstruments(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:0"})
	defer c.Close()
	// With tracing enabled, privacy-safe instrumentation is attached without error.
	attachTracingHook(c)
}

func TestSafeRedisCommandVerb(t *testing.T) {
	tests := []struct {
		name string
		cmd  goredis.Cmder
		want string
	}{
		{name: "standard", cmd: goredis.NewStatusCmd(context.Background(), "SET", "private-key", "private-value"), want: "set"},
		{name: "module", cmd: goredis.NewStatusCmd(context.Background(), "JSON.GET", "private-key"), want: "json.get"},
		{name: "malformed", cmd: goredis.NewStatusCmd(context.Background(), "set private-key", "private-value"), want: "redis.command"},
		{name: "nil", want: "redis.command"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := safeRedisCommandVerb(test.cmd); got != test.want {
				t.Fatalf("safeRedisCommandVerb() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSafeTracingHookInheritsActiveGoroutineParent(t *testing.T) {
	provider, exporter := recordingRedisProvider(t)
	hook := newSafeTracingHook(provider)
	activeCtx, active := provider.Tracer("redis-parent-test").Start(context.Background(), "active-handler")
	defer active.End()
	restoreActive := tracing.InjectContextIntoGoroutine(activeCtx)
	defer restoreActive()

	base, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := goredis.NewStringCmd(base, "get", "private-key")
	err := hook.ProcessHook(func(ctx context.Context, got goredis.Cmder) error {
		if got != cmd {
			t.Fatalf("process command changed: got %p want %p", got, cmd)
		}
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("process context error = %v, want caller cancellation", ctx.Err())
		}
		return nil
	})(base, cmd)
	if err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}

	child := exportedRedisSpan(t, exporter, "get")
	if got := child.Parent.SpanID(); got != active.SpanContext().SpanID() {
		t.Fatalf("Redis span parent = %s, want active goroutine span %s", got, active.SpanContext().SpanID())
	}
}

func TestSafeTracingHookExplicitContextParentWins(t *testing.T) {
	provider, exporter := recordingRedisProvider(t)
	hook := newSafeTracingHook(provider)
	activeCtx, active := provider.Tracer("redis-parent-test").Start(context.Background(), "active-handler")
	defer active.End()
	explicitCtx, explicit := provider.Tracer("redis-parent-test").Start(context.Background(), "explicit-parent")
	defer explicit.End()
	restoreActive := tracing.InjectContextIntoGoroutine(activeCtx)
	defer restoreActive()

	cmd := goredis.NewStringCmd(explicitCtx, "get", "private-key")
	if err := hook.ProcessHook(func(context.Context, goredis.Cmder) error { return nil })(explicitCtx, cmd); err != nil {
		t.Fatalf("ProcessHook: %v", err)
	}

	child := exportedRedisSpan(t, exporter, "get")
	if got := child.Parent.SpanID(); got != explicit.SpanContext().SpanID() {
		t.Fatalf("Redis span parent = %s, want explicit span %s", got, explicit.SpanContext().SpanID())
	}
	if got := child.Parent.SpanID(); got == active.SpanContext().SpanID() {
		t.Fatalf("Redis span unexpectedly inherited active goroutine span %s", got)
	}
}

func TestSafeTracingHookDoesNotExportCommandsConnectionDetailsOrBackendErrors(t *testing.T) {
	tracingtest.EnabledGlobal(t)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
	})

	const (
		keyCanary      = "redis-key-canary:user@example.com"
		valueCanary    = "redis-value-canary:access-token"
		passwordCanary = "redis-password-canary"
		hostCanary     = "redis-host-canary.internal:6379"
		errorCanary    = "redis backend error key=" + keyCanary + " value=" + valueCanary + " password=" + passwordCanary
	)
	backendErr := errors.New(errorCanary)
	hook := newSafeTracingHook(provider)
	parentCtx, parent := provider.Tracer("redis-privacy-test").Start(context.Background(), "parent")
	parentContext := parent.SpanContext()

	cmd := goredis.NewStatusCmd(parentCtx, "set", keyCanary, valueCanary)
	processSawChild := false
	processErr := hook.ProcessHook(func(ctx context.Context, got goredis.Cmder) error {
		if got != cmd {
			t.Fatalf("process command changed: got %p want %p", got, cmd)
		}
		child := trace.SpanFromContext(ctx).SpanContext()
		processSawChild = child.IsValid() && child.TraceID() == parentContext.TraceID() && child.SpanID() != parentContext.SpanID()
		return backendErr
	})(parentCtx, cmd)
	if processErr != backendErr {
		t.Fatalf("ProcessHook error = %v, want original backend error", processErr)
	}
	if !processSawChild {
		t.Fatal("process hook did not propagate its child span through context")
	}

	cmds := []goredis.Cmder{
		goredis.NewStatusCmd(parentCtx, "set", keyCanary, valueCanary),
		goredis.NewStringCmd(parentCtx, "get", keyCanary),
	}
	pipelineSawChild := false
	pipelineErr := hook.ProcessPipelineHook(func(ctx context.Context, got []goredis.Cmder) error {
		if len(got) != len(cmds) {
			t.Fatalf("pipeline command count = %d, want %d", len(got), len(cmds))
		}
		child := trace.SpanFromContext(ctx).SpanContext()
		pipelineSawChild = child.IsValid() && child.TraceID() == parentContext.TraceID() && child.SpanID() != parentContext.SpanID()
		return backendErr
	})(parentCtx, cmds)
	if pipelineErr != backendErr {
		t.Fatalf("ProcessPipelineHook error = %v, want original backend error", pipelineErr)
	}
	if !pipelineSawChild {
		t.Fatal("pipeline hook did not propagate its child span through context")
	}

	dialSawChild := false
	dialErr := hook.DialHook(func(ctx context.Context, network, addr string) (net.Conn, error) {
		if network != "tcp" || addr != hostCanary {
			t.Fatalf("dial target changed: %s %s", network, addr)
		}
		child := trace.SpanFromContext(ctx).SpanContext()
		dialSawChild = child.IsValid() && child.TraceID() == parentContext.TraceID() && child.SpanID() != parentContext.SpanID()
		return nil, backendErr
	})
	if _, err := dialErr(parentCtx, "tcp", hostCanary); err != backendErr {
		t.Fatalf("DialHook error = %v, want original backend error", err)
	}
	if !dialSawChild {
		t.Fatal("dial hook did not propagate its child span through context")
	}
	parent.End()

	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 4 {
		t.Fatalf("exported span count = %d, want parent plus three Redis spans", len(spans))
	}
	secrets := []string{keyCanary, valueCanary, passwordCanary, hostCanary, errorCanary}
	found := map[string]bool{}
	commandOperation := ""
	for _, span := range spans {
		found[span.Name] = true
		assertRedisSpanContainsNoCanaries(t, span, secrets)
		for _, attr := range span.Attributes {
			if span.Name == "set" && string(attr.Key) == "db.operation.name" {
				commandOperation = attr.Value.AsString()
			}
			switch string(attr.Key) {
			case "db.query.text", "db.statement", "server.address", "server.port", "network.peer.address", "network.peer.port":
				t.Fatalf("span %q exported forbidden Redis attribute %q", span.Name, attr.Key)
			}
		}
		if span.Name != "parent" {
			if span.Status.Description != "redis operation failed" {
				t.Fatalf("span %q status = %q, want generic failure", span.Name, span.Status.Description)
			}
			if len(span.Events) != 0 {
				t.Fatalf("span %q exported error events: %+v", span.Name, span.Events)
			}
		}
	}
	for _, name := range []string{"set", "redis.pipeline", "redis.dial"} {
		if !found[name] {
			t.Fatalf("missing operation span %q", name)
		}
	}
	if commandOperation != "set" {
		t.Fatalf("Redis command operation = %q, want safe verb set", commandOperation)
	}
}

func assertRedisSpanContainsNoCanaries(t *testing.T, span tracetest.SpanStub, canaries []string) {
	t.Helper()
	check := func(surface, value string) {
		t.Helper()
		for _, canary := range canaries {
			if strings.Contains(value, canary) {
				t.Fatalf("span %q leaked %q through %s", span.Name, canary, surface)
			}
		}
	}

	check("name", span.Name)
	check("status", span.Status.Description)
	for _, attr := range span.Attributes {
		check("attribute "+string(attr.Key), attr.Value.Emit())
	}
	for _, event := range span.Events {
		check("event name", event.Name)
		for _, attr := range event.Attributes {
			check("event attribute "+string(attr.Key), attr.Value.Emit())
		}
	}
}

type tracedIORedisTransport struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func (t *tracedIORedisTransport) Exchange(_ context.Context, commands [][]string) SlingshotIORedisExchange {
	replies := make([]SlingshotIORedisReply, len(commands))
	for index, command := range commands {
		if len(command) > 0 && command[0] == "get" {
			replies[index].Error = errors.New("redis failure contains private-key-canary")
			continue
		}
		replies[index].Value = "OK"
	}
	return SlingshotIORedisExchange{Replies: replies}
}

func (t *tracedIORedisTransport) Closed() <-chan struct{} { return t.closed }

func (t *tracedIORedisTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func TestIORedisCompatibilitySpansAreParentedAndPrivacySafe(t *testing.T) {
	tracer, exporter := recordingRedisTracer(t)
	restoreGlobal := tracing.SetGlobal(tracer)
	t.Cleanup(restoreGlobal)

	transport := &tracedIORedisTransport{closed: make(chan struct{})}
	client, err := NewSlingshotIORedisCompatClientReady(
		context.Background(),
		SlingshotIORedisTransportFactoryFunc(func(context.Context) (SlingshotIORedisTransport, error) {
			return transport, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewSlingshotIORedisCompatClientReady: %v", err)
	}
	defer client.Disconnect()

	parentCtx, parent := tracer.StartSpan(context.Background(), "ioredis-parent", tracing.SpanKindInternal)
	const (
		keyCanary   = "private-key-canary"
		valueCanary = "private-value-canary"
	)
	if _, err := client.SubmitContext(parentCtx, "SET", keyCanary, valueCanary).Wait(context.Background()); err != nil {
		t.Fatalf("SubmitContext SET: %v", err)
	}
	result, err := client.SubmitPipelineContext(
		parentCtx,
		[]string{"GET", keyCanary},
		[]string{"SET", keyCanary, valueCanary},
	).Wait(context.Background())
	if err != nil {
		t.Fatalf("SubmitPipelineContext: %v", err)
	}
	if len(result.Replies) != 2 || result.Replies[0].Error == nil {
		t.Fatalf("pipeline replies = %+v, want first Redis reply error", result.Replies)
	}
	parent.End()

	setSpan := exportedRedisSpan(t, exporter, "set")
	pipelineSpan := exportedRedisSpan(t, exporter, "redis.pipeline")
	for _, span := range []tracetest.SpanStub{setSpan, pipelineSpan} {
		if got := span.Parent.SpanID().String(); got != parent.SpanID() {
			t.Fatalf("span %q parent = %s, want %s", span.Name, got, parent.SpanID())
		}
		assertRedisSpanContainsNoCanaries(t, span, []string{keyCanary, valueCanary})
	}
	if setSpan.Status.Code == codes.Error {
		t.Fatalf("successful command status = %v, want non-error", setSpan.Status)
	}
	if pipelineSpan.Status.Code != codes.Error || pipelineSpan.Status.Description != "redis operation failed" {
		t.Fatalf("pipeline status = %+v, want generic Redis failure", pipelineSpan.Status)
	}
	if len(pipelineSpan.Events) != 0 {
		t.Fatalf("pipeline exported backend error event: %+v", pipelineSpan.Events)
	}
	batchSize := int64(0)
	for _, attr := range pipelineSpan.Attributes {
		if string(attr.Key) == "db.operation.batch.size" {
			batchSize = attr.Value.AsInt64()
		}
	}
	if batchSize != 2 {
		t.Fatalf("pipeline batch size = %d, want 2", batchSize)
	}
}
