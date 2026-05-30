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
	"sync"
	"testing"
	"time"

	sentrylib "github.com/getsentry/sentry-go"
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

// newTestSentry creates a fresh sentrySdk (bypassing the sync.Once of the global).
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
