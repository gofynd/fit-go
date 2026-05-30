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

// Package errors provides Sentry error reporting integration for the fit.go framework.
//
// This file provides a real Sentry SDK integration using github.com/getsentry/sentry-go.
//
// Environment variables:
// - SENTRY_DSN: Sentry Data Source Name (required for reporting)
// - SENTRY_ENVIRONMENT: Environment name (production, staging, etc.)
// - SENTRY_RELEASE: Release version
// - SENTRY_DEBUG: Enable debug logging (true/false)
// - SENTRY_SAMPLE_RATE: Error sample rate (0.0-1.0)
// - SENTRY_TRACES_SAMPLE_RATE: Tracing sample rate (0.0-1.0)
package errors

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	sentrylib "github.com/getsentry/sentry-go"
)

// SentryConfig holds Sentry SDK configuration.
type SentryConfig struct {
	DSN              string            `json:"dsn"`
	Environment      string            `json:"environment"`
	Release          string            `json:"release,omitempty"`
	Debug            bool              `json:"debug,omitempty"`
	SampleRate       float64           `json:"sample_rate,omitempty"`
	TracesSampleRate float64           `json:"traces_sample_rate,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
	ServerName       string            `json:"server_name,omitempty"`
	// Transport allows overriding the Sentry transport (useful for testing).
	Transport sentrylib.Transport `json:"-"`
}

// SentryReporter is the interface that a real Sentry integration must satisfy.
type SentryReporter interface {
	// Init initializes the Sentry SDK. It should read configuration from
	// environment variables (SENTRY_DSN, SENTRY_ENVIRONMENT) and be safe
	// to call multiple times (subsequent calls are no-ops).
	Init() error

	// InitWithConfig initializes Sentry with explicit configuration.
	InitWithConfig(cfg SentryConfig) error

	// IsInitialized returns true if Sentry has been initialized.
	IsInitialized() bool

	// CaptureError reports an error to Sentry. If the error is a *FitError
	// with NOP set to true, the report is silently skipped.
	CaptureError(err error)

	// CaptureErrorWithContext reports an error with context for tracing.
	CaptureErrorWithContext(ctx context.Context, err error)

	// CaptureMessage reports a message to Sentry.
	CaptureMessage(message string)

	// CaptureMessageWithLevel reports a message with a specific severity level.
	CaptureMessageWithLevel(message string, level SentryLevel)

	// AddBreadcrumb adds a breadcrumb to the current scope.
	AddBreadcrumb(category, message string, data map[string]interface{})

	// SetUser sets user information for error context.
	SetUser(id, email, username string)

	// SetTag sets a tag on the current scope.
	SetTag(key, value string)

	// SetExtra sets extra data on the current scope.
	SetExtra(key string, value interface{})

	// Flush blocks until buffered events are sent or the timeout expires.
	Flush()

	// FlushWithTimeout flushes with a specific timeout.
	FlushWithTimeout(timeout time.Duration) bool
}

// SentryLevel represents Sentry severity levels.
type SentryLevel string

const (
	SentryLevelDebug   SentryLevel = "debug"
	SentryLevelInfo    SentryLevel = "info"
	SentryLevelWarning SentryLevel = "warning"
	SentryLevelError   SentryLevel = "error"
	SentryLevelFatal   SentryLevel = "fatal"
)

// sentrySdk wraps the real sentry-go SDK.
type sentrySdk struct {
	once        sync.Once
	initialized bool
	config      SentryConfig
}

func (s *sentrySdk) Init() error {
	return s.InitWithConfig(SentryConfig{
		DSN:              os.Getenv("SENTRY_DSN"),
		Environment:      os.Getenv("SENTRY_ENVIRONMENT"),
		Release:          os.Getenv("SENTRY_RELEASE"),
		Debug:            envBoolSentry("SENTRY_DEBUG", false),
		SampleRate:       envFloatSentry("SENTRY_SAMPLE_RATE", 1.0),
		TracesSampleRate: envFloatSentry("SENTRY_TRACES_SAMPLE_RATE", 0.0),
		ServerName:       os.Getenv("K8S_POD_NAME"),
	})
}

func (s *sentrySdk) InitWithConfig(cfg SentryConfig) error {
	var initErr error
	s.once.Do(func() {
		s.config = cfg
		if cfg.DSN == "" {
			log.Println("fit/errors/sentry: SENTRY_DSN not set, error reporting disabled (graceful degradation)")
			return
		}

		opts := sentrylib.ClientOptions{
			Dsn:              cfg.DSN,
			Environment:      cfg.Environment,
			Release:          cfg.Release,
			Debug:            cfg.Debug,
			SampleRate:       cfg.SampleRate,
			TracesSampleRate: cfg.TracesSampleRate,
			ServerName:       cfg.ServerName,
		}

		if cfg.Transport != nil {
			opts.Transport = cfg.Transport
		}

		if err := sentrylib.Init(opts); err != nil {
			log.Printf("fit/errors/sentry: failed to initialize Sentry SDK: %v", err)
			initErr = err
			return
		}

		// Apply initial tags if any.
		if len(cfg.Tags) > 0 {
			sentrylib.ConfigureScope(func(scope *sentrylib.Scope) {
				for k, v := range cfg.Tags {
					scope.SetTag(k, v)
				}
			})
		}

		s.initialized = true
		if cfg.Debug {
			log.Printf("fit/errors/sentry: initialized (DSN=%s...)", truncateDSN(cfg.DSN))
		}
	})
	return initErr
}

func (s *sentrySdk) IsInitialized() bool {
	return s.initialized
}

func (s *sentrySdk) CaptureError(err error) {
	if err == nil {
		return
	}
	// Skip NOP errors: FitErrors flagged as no-operation should not be reported.
	if fe, ok := IsFitError(err); ok && fe.NOP {
		return
	}
	if !s.initialized {
		if s.config.Debug {
			log.Printf("fit/errors/sentry: [not initialized] would report error: %v", err)
		}
		return
	}
	sentrylib.CaptureException(err)
}

func (s *sentrySdk) CaptureErrorWithContext(ctx context.Context, err error) {
	if err == nil {
		return
	}
	// Skip NOP errors.
	if fe, ok := IsFitError(err); ok && fe.NOP {
		return
	}
	if !s.initialized {
		if s.config.Debug {
			log.Printf("fit/errors/sentry: [not initialized] would report error: %v", err)
		}
		return
	}
	hub := sentrylib.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentrylib.CurrentHub().Clone()
	}
	hub.CaptureException(err)
}

func (s *sentrySdk) CaptureMessage(message string) {
	if !s.initialized {
		if s.config.Debug {
			log.Printf("fit/errors/sentry: [not initialized] would report message: %s", message)
		}
		return
	}
	sentrylib.CaptureMessage(message)
}

func (s *sentrySdk) CaptureMessageWithLevel(message string, level SentryLevel) {
	if !s.initialized {
		if s.config.Debug {
			log.Printf("fit/errors/sentry: [not initialized] would report %s message: %s", level, message)
		}
		return
	}
	sentrylib.WithScope(func(scope *sentrylib.Scope) {
		scope.SetLevel(toSentryLevel(level))
		sentrylib.CaptureMessage(message)
	})
}

func (s *sentrySdk) AddBreadcrumb(category, message string, data map[string]interface{}) {
	if !s.initialized {
		return
	}
	sentrylib.AddBreadcrumb(&sentrylib.Breadcrumb{
		Category: category,
		Message:  message,
		Data:     data,
		Level:    sentrylib.LevelInfo,
	})
}

func (s *sentrySdk) SetUser(id, email, username string) {
	if !s.initialized {
		return
	}
	sentrylib.ConfigureScope(func(scope *sentrylib.Scope) {
		scope.SetUser(sentrylib.User{
			ID:       id,
			Email:    email,
			Username: username,
		})
	})
}

func (s *sentrySdk) SetTag(key, value string) {
	if !s.initialized {
		return
	}
	sentrylib.ConfigureScope(func(scope *sentrylib.Scope) {
		scope.SetTag(key, value)
	})
}

func (s *sentrySdk) SetExtra(key string, value interface{}) {
	if !s.initialized {
		return
	}
	sentrylib.ConfigureScope(func(scope *sentrylib.Scope) {
		scope.SetExtra(key, value)
	})
}

func (s *sentrySdk) Flush() {
	if !s.initialized {
		return
	}
	sentrylib.Flush(2 * time.Second)
}

func (s *sentrySdk) FlushWithTimeout(timeout time.Duration) bool {
	if !s.initialized {
		return true
	}
	return sentrylib.Flush(timeout)
}

// toSentryLevel converts our SentryLevel to the sentry-go library level.
func toSentryLevel(level SentryLevel) sentrylib.Level {
	switch level {
	case SentryLevelDebug:
		return sentrylib.LevelDebug
	case SentryLevelInfo:
		return sentrylib.LevelInfo
	case SentryLevelWarning:
		return sentrylib.LevelWarning
	case SentryLevelError:
		return sentrylib.LevelError
	case SentryLevelFatal:
		return sentrylib.LevelFatal
	default:
		return sentrylib.LevelError
	}
}

// Sentry is the package-level reporter, backed by the real sentry-go SDK.
var Sentry SentryReporter = &sentrySdk{}

// SetSentryReporter replaces the default Sentry reporter with a custom implementation.
// This is typically called once at startup with a real Sentry SDK wrapper.
func SetSentryReporter(reporter SentryReporter) {
	Sentry = reporter
}

// InitSentry initialises the active Sentry reporter. It is safe to call early
// in program startup; if SENTRY_DSN is not set the call is a no-op.
func InitSentry() error {
	return Sentry.Init()
}

// InitSentryWithConfig initializes Sentry with explicit configuration.
func InitSentryWithConfig(cfg SentryConfig) error {
	return Sentry.InitWithConfig(cfg)
}

// IsSentryInitialized returns true if Sentry has been initialized.
func IsSentryInitialized() bool {
	return Sentry.IsInitialized()
}

// CaptureError reports an error via the active Sentry reporter. Errors with
// the NOP flag set are silently skipped.
func CaptureError(err error) {
	Sentry.CaptureError(err)
}

// CaptureErrorWithContext reports an error with context for tracing.
func CaptureErrorWithContext(ctx context.Context, err error) {
	Sentry.CaptureErrorWithContext(ctx, err)
}

// CaptureMessage reports a message to Sentry.
func CaptureMessage(message string) {
	Sentry.CaptureMessage(message)
}

// FlushSentry blocks until buffered events are sent.
func FlushSentry() {
	Sentry.Flush()
}

// FlushSentryWithTimeout flushes with a specific timeout.
func FlushSentryWithTimeout(timeout time.Duration) bool {
	return Sentry.FlushWithTimeout(timeout)
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func truncateDSN(dsn string) string {
	if len(dsn) <= 20 {
		return dsn
	}
	return dsn[:20]
}

func envBoolSentry(key string, defaultVal bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return defaultVal
	}
	return b
}

func envFloatSentry(key string, defaultVal float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultVal
	}
	return f
}
