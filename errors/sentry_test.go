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

package errors

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	sentrylib "github.com/getsentry/sentry-go"

	"github.com/gofynd/fit-go/redact"
)

// mockTransport captures events sent to Sentry for test assertions.
type mockTransport struct {
	mu     sync.Mutex
	events []*sentrylib.Event
}

func (t *mockTransport) Configure(options sentrylib.ClientOptions) {}
func (t *mockTransport) SendEvent(event *sentrylib.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}
func (t *mockTransport) Flush(timeout time.Duration) bool          { return true }
func (t *mockTransport) Close()                                    {}
func (t *mockTransport) FlushWithContext(ctx context.Context) bool { return true }
func (t *mockTransport) SendEventWithContext(ctx context.Context, event *sentrylib.Event) {
	t.SendEvent(event)
}
func (t *mockTransport) Events() []*sentrylib.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	copied := make([]*sentrylib.Event, len(t.events))
	copy(copied, t.events)
	return copied
}

// newTestSentry creates a fresh reporter independent of the package global.
func newTestSentry() *sentrySdk {
	return &sentrySdk{}
}

// ---------------------------------------------------------------------------
// TestSentryInit_NoDSN: graceful degradation when DSN is empty
// ---------------------------------------------------------------------------

func TestSentryInit_NoDSN(t *testing.T) {
	s := newTestSentry()
	err := s.InitWithConfig(SentryConfig{DSN: ""})
	if err != nil {
		t.Fatalf("InitWithConfig with empty DSN should not error, got: %v", err)
	}
	if s.IsInitialized() {
		t.Error("Sentry should NOT be initialized when DSN is empty")
	}

	// CaptureError should not panic on uninitialized instance.
	s.CaptureError(fmt.Errorf("test error"))
	s.CaptureMessage("test message")
	s.Flush()
}

func TestSentryInit_CanRetryAfterMissingDSN(t *testing.T) {
	s := newTestSentry()
	if err := s.InitWithConfig(SentryConfig{}); err != nil {
		t.Fatalf("empty init: %v", err)
	}
	transport := &mockTransport{}
	if err := s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	}); err != nil {
		t.Fatalf("retry init: %v", err)
	}
	if !s.IsInitialized() {
		t.Fatal("Sentry did not initialize after configuration became available")
	}
}

func TestSentryInit_CanRetryAfterInitializationFailure(t *testing.T) {
	s := newTestSentry()
	if err := s.InitWithConfig(SentryConfig{DSN: "://invalid"}); err == nil {
		t.Fatal("invalid DSN initialization unexpectedly succeeded")
	}
	if s.IsInitialized() {
		t.Fatal("Sentry marked initialized after SDK initialization failed")
	}
	if err := s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: &mockTransport{},
	}); err != nil {
		t.Fatalf("retry init: %v", err)
	}
	if !s.IsInitialized() {
		t.Fatal("Sentry did not initialize after a failed attempt was corrected")
	}
}

// ---------------------------------------------------------------------------
// TestSentryInit_WithDSN: real init with mock transport
// ---------------------------------------------------------------------------

func TestSentryInit_WithDSN(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	err := s.InitWithConfig(SentryConfig{
		DSN:         "https://examplePublicKey@o0.ingest.sentry.io/0",
		Environment: "test",
		Debug:       true,
		SampleRate:  1.0,
		Transport:   transport,
		Tags:        map[string]string{"service": "fit-test"},
	})
	if err != nil {
		t.Fatalf("InitWithConfig error: %v", err)
	}
	if !s.IsInitialized() {
		t.Error("Sentry should be initialized after valid config")
	}
}

// ---------------------------------------------------------------------------
// TestCaptureError_NOP: FitError with NOP flag should be skipped
// ---------------------------------------------------------------------------

func TestCaptureError_NOP(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	// Create a FitError and mark it as NOP.
	fe := New(fmt.Errorf("test"), 9999).Ignore()
	s.CaptureError(fe)

	// Flush and check: no events should be sent.
	s.Flush()
	if len(transport.Events()) != 0 {
		t.Errorf("Expected 0 events for NOP error, got %d", len(transport.Events()))
	}
}

// ---------------------------------------------------------------------------
// TestCaptureError_Regular: non-NOP error should be captured
// ---------------------------------------------------------------------------

func TestCaptureError_Regular(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	s.CaptureError(fmt.Errorf("real error"))
	s.Flush()

	events := transport.Events()
	if len(events) == 0 {
		t.Fatal("Expected at least 1 event for a regular error")
	}
	// The event should contain our error message.
	found := false
	for _, e := range events {
		if e.Message == "real error" || (len(e.Exception) > 0 && e.Exception[0].Value == "real error") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Captured event does not contain the expected error message")
	}
}

func TestSentryBeforeSendSanitizesPIIAndSecrets(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	var customHookSawSanitized bool
	if err := s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
		BeforeSend: func(event *sentrylib.Event, _ *sentrylib.EventHint) *sentrylib.Event {
			serialized := fmt.Sprintf("%#v", event)
			customHookSawSanitized = !strings.Contains(serialized, "private@example.com") &&
				!strings.Contains(serialized, "secret-token")
			return event
		},
	}); err != nil {
		t.Fatalf("InitWithConfig: %v", err)
	}
	s.SetUser("user-1", "private@example.com", "private-user")
	s.SetExtra("authorization_token", "secret-token")
	s.CaptureError(fmt.Errorf("request failed for private@example.com with Bearer secret-token"))
	s.Flush()

	events := transport.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	serialized := fmt.Sprintf("%#v", events[0])
	if strings.Contains(serialized, "private@example.com") || strings.Contains(serialized, "secret-token") {
		t.Fatalf("Sentry event leaked PII or secret: %s", serialized)
	}
	if !events[0].User.IsEmpty() {
		t.Fatalf("Sentry user was not removed: %#v", events[0].User)
	}
	if !customHookSawSanitized {
		t.Fatal("custom BeforeSend ran before fit-go sanitization")
	}
}

func TestSentrySanitizesAfterCallerHook(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	if err := s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
		BeforeSend: func(event *sentrylib.Event, _ *sentrylib.EventHint) *sentrylib.Event {
			event.Extra["authorization_token"] = "hook-secret-token"
			event.Message = "private@example.com"
			event.Logger = "private@example.com"
			event.Logs = []sentrylib.Log{{Body: "hook-secret-token"}}
			event.Metrics = []sentrylib.Metric{{Name: "private@example.com"}}
			return event
		},
	}); err != nil {
		t.Fatalf("InitWithConfig: %v", err)
	}
	s.CaptureMessage("safe")
	s.Flush()

	events := transport.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	serialized := fmt.Sprintf("%#v", events[0])
	if strings.Contains(serialized, "hook-secret-token") || strings.Contains(serialized, "private@example.com") {
		t.Fatalf("post-hook sanitizer leaked caller data: %s", serialized)
	}
	if len(events[0].Logs) != 0 || len(events[0].Metrics) != 0 {
		t.Fatalf("post-hook sanitizer retained unsupported signals: %#v", events[0])
	}
}

func TestSentryTransactionUsesMandatorySanitizer(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	if err := s.InitWithConfig(SentryConfig{
		DSN:              "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport:        transport,
		TracesSampleRate: 1,
		BeforeSendTransaction: func(event *sentrylib.Event, _ *sentrylib.EventHint) *sentrylib.Event {
			event.Extra["password"] = "transaction-secret"
			return event
		},
	}); err != nil {
		t.Fatalf("InitWithConfig: %v", err)
	}
	sentrylib.CurrentHub().CaptureEvent(&sentrylib.Event{
		Type:        "transaction",
		Transaction: "checkout private@example.com",
		Extra:       map[string]interface{}{"api_key": "original-secret"},
	})
	s.Flush()

	events := transport.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	serialized := fmt.Sprintf("%#v", events[0])
	for _, secret := range []string{"transaction-secret", "original-secret", "private@example.com"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("transaction sanitizer leaked %q: %s", secret, serialized)
		}
	}
}

func TestSanitizeSentryValueHandlesTypedValuesAndCycles(t *testing.T) {
	type credentials struct {
		Name     string `json:"name"`
		Password string `json:"password"`
		Nested   *credentials
	}
	value := &credentials{Name: "private@example.com", Password: "typed-secret"}
	value.Nested = value
	sanitized := sanitizeSentryValue("metadata", value)
	serialized := fmt.Sprintf("%#v", sanitized)
	if strings.Contains(serialized, "private@example.com") || strings.Contains(serialized, "typed-secret") {
		t.Fatalf("typed value sanitizer leaked data: %s", serialized)
	}
	if !strings.Contains(serialized, redact.Mask) {
		t.Fatalf("typed value sanitizer did not filter sensitive/cyclic data: %s", serialized)
	}
}

func TestWithSentryContextIsolatesConcurrentRequestScopes(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	if err := s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	}); err != nil {
		t.Fatalf("InitWithConfig: %v", err)
	}

	ctxOne := WithSentryContext(context.Background(), SentryContext{CorrelationID: "request-one"})
	ctxTwo := WithSentryContext(context.Background(), SentryContext{CorrelationID: "request-two"})
	s.CaptureErrorWithContext(ctxOne, fmt.Errorf("first"))
	s.CaptureErrorWithContext(ctxTwo, fmt.Errorf("second"))
	s.Flush()

	events := transport.Events()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].Tags["correlation_id"] != "request-one" || events[1].Tags["correlation_id"] != "request-two" {
		t.Fatalf("request scopes leaked: %#v, %#v", events[0].Tags, events[1].Tags)
	}
}

func TestSanitizeSentryEventCoversNestedRuntimeData(t *testing.T) {
	event := &sentrylib.Event{
		Fingerprint: []string{"private@example.com"},
		Transaction: "checkout for private@example.com",
		Exception: []sentrylib.Exception{{
			Value: "Bearer secret-token",
			Stacktrace: &sentrylib.Stacktrace{Frames: []sentrylib.Frame{{
				ContextLine: `token := "secret-token"`,
				Vars:        map[string]interface{}{"authorization": "Bearer secret-token"},
			}}},
		}},
		Threads: []sentrylib.Thread{{
			Name: "private@example.com",
			Stacktrace: &sentrylib.Stacktrace{Frames: []sentrylib.Frame{{
				Vars: map[string]interface{}{"email": "private@example.com"},
			}}},
		}},
		Spans: []*sentrylib.Span{{
			Description: "request for private@example.com",
			Data:        map[string]interface{}{"payload": "secret-token"},
			Tags:        map[string]string{"api_key": "secret-token"},
		}},
		Attachments: []*sentrylib.Attachment{{Filename: "raw-request.txt"}},
	}

	sanitized := sanitizeSentryEvent(event)
	serialized := fmt.Sprintf("%#v", sanitized)
	if strings.Contains(serialized, "private@example.com") || strings.Contains(serialized, "secret-token") {
		t.Fatalf("nested Sentry data leaked PII or secrets: %s", serialized)
	}
	if len(sanitized.Attachments) != 0 {
		t.Fatalf("opaque attachments were retained: %#v", sanitized.Attachments)
	}
}

// ---------------------------------------------------------------------------
// TestCaptureErrorWithContext
// ---------------------------------------------------------------------------

func TestCaptureErrorWithContext(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	ctx := context.Background()
	s.CaptureErrorWithContext(ctx, fmt.Errorf("context error"))
	s.Flush()

	if len(transport.Events()) == 0 {
		t.Error("Expected event from CaptureErrorWithContext")
	}
}

func TestCaptureErrorWithNilContext(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	if err := s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	}); err != nil {
		t.Fatalf("InitWithConfig: %v", err)
	}

	s.CaptureErrorWithContext(nil, fmt.Errorf("nil context error"))
	s.Flush()
	if len(transport.Events()) != 1 {
		t.Fatalf("events = %d, want 1", len(transport.Events()))
	}
}

// ---------------------------------------------------------------------------
// TestCaptureErrorWithContext_NOP
// ---------------------------------------------------------------------------

func TestCaptureErrorWithContext_NOP(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	fe := New(fmt.Errorf("test"), 8888).Ignore()
	s.CaptureErrorWithContext(context.Background(), fe)
	s.Flush()

	if len(transport.Events()) != 0 {
		t.Errorf("Expected 0 events for NOP error via CaptureErrorWithContext, got %d", len(transport.Events()))
	}
}

// ---------------------------------------------------------------------------
// TestCaptureMessage
// ---------------------------------------------------------------------------

func TestCaptureMessage(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	s.CaptureMessage("hello sentry")
	s.Flush()

	events := transport.Events()
	if len(events) == 0 {
		t.Fatal("Expected at least 1 event from CaptureMessage")
	}
	if events[0].Message != "hello sentry" {
		t.Errorf("Message = %q, want 'hello sentry'", events[0].Message)
	}
}

// ---------------------------------------------------------------------------
// TestCaptureMessageWithLevel
// ---------------------------------------------------------------------------

func TestCaptureMessageWithLevel(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	s.CaptureMessageWithLevel("warning message", SentryLevelWarning)
	s.Flush()

	events := transport.Events()
	if len(events) == 0 {
		t.Fatal("Expected at least 1 event from CaptureMessageWithLevel")
	}
}

// ---------------------------------------------------------------------------
// TestSetUser / SetTag / SetExtra
// ---------------------------------------------------------------------------

func TestSetUserTagExtra(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	// These should not panic.
	s.SetUser("user-123", "test@example.com", "testuser")
	s.SetTag("version", "1.0")
	s.SetExtra("request_id", "req-abc")
}

// ---------------------------------------------------------------------------
// TestFlushWithTimeout
// ---------------------------------------------------------------------------

func TestFlushWithTimeout(t *testing.T) {
	s := newTestSentry()
	// Not initialized - should return true (nothing to flush).
	if !s.FlushWithTimeout(time.Second) {
		t.Error("FlushWithTimeout on uninitialized should return true")
	}

	transport := &mockTransport{}
	s2 := newTestSentry()
	_ = s2.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})
	if !s2.FlushWithTimeout(time.Second) {
		t.Error("FlushWithTimeout on initialized should return true with mock transport")
	}
}

// ---------------------------------------------------------------------------
// TestCaptureError_Nil
// ---------------------------------------------------------------------------

func TestCaptureError_Nil(t *testing.T) {
	s := newTestSentry()
	// Should not panic on nil error.
	s.CaptureError(nil)
	s.CaptureErrorWithContext(context.Background(), nil)
}

// ---------------------------------------------------------------------------
// TestToSentryLevel
// ---------------------------------------------------------------------------

func TestToSentryLevel(t *testing.T) {
	tests := []struct {
		input    SentryLevel
		expected sentrylib.Level
	}{
		{SentryLevelDebug, sentrylib.LevelDebug},
		{SentryLevelInfo, sentrylib.LevelInfo},
		{SentryLevelWarning, sentrylib.LevelWarning},
		{SentryLevelError, sentrylib.LevelError},
		{SentryLevelFatal, sentrylib.LevelFatal},
		{SentryLevel("unknown"), sentrylib.LevelError},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			got := toSentryLevel(tt.input)
			if got != tt.expected {
				t.Errorf("toSentryLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestAddBreadcrumb
// ---------------------------------------------------------------------------

func TestAddBreadcrumb(t *testing.T) {
	transport := &mockTransport{}
	s := newTestSentry()
	_ = s.InitWithConfig(SentryConfig{
		DSN:       "https://examplePublicKey@o0.ingest.sentry.io/0",
		Transport: transport,
	})

	// Should not panic.
	s.AddBreadcrumb("http", "GET /api/users", map[string]interface{}{"status": 200})
}

// ---------------------------------------------------------------------------
// TestAddBreadcrumb_NotInitialized
// ---------------------------------------------------------------------------

func TestAddBreadcrumb_NotInitialized(t *testing.T) {
	s := newTestSentry()
	// Should not panic when not initialized.
	s.AddBreadcrumb("http", "GET /api/users", nil)
}
