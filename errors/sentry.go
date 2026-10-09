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
	"fmt"
	"log"
	"maps"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	sentrylib "github.com/getsentry/sentry-go"

	"github.com/gofynd/fit-go/redact"
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

// SentryAdvancedConfig contains opt-in hooks added after the original
// SentryConfig contract.
// Deprecated: use SentryHooksConfig.
type SentryAdvancedConfig struct {
	SentryConfig
	// BeforeSend runs after fit-go's mandatory sanitizer. It may further enrich
	// or drop the already-sanitized event.
	BeforeSend func(event *sentrylib.Event, hint *sentrylib.EventHint) *sentrylib.Event `json:"-"`
	// BeforeSendTransaction runs between fit-go's mandatory sanitization passes.
	BeforeSendTransaction func(event *sentrylib.Event, hint *sentrylib.EventHint) *sentrylib.Event `json:"-"`
}

// SentryContext contains request-scoped error-reporting metadata. It is
// attached to a cloned hub and cannot leak through the process-global scope.
type SentryContext struct {
	CorrelationID string
	Tags          map[string]string
	Extra         map[string]interface{}
	Breadcrumbs   []*sentrylib.Breadcrumb
}

// SentryReporter is the interface that a real Sentry integration must satisfy.
type SentryReporter interface {
	// Init initializes the Sentry SDK. It should read configuration from
	// environment variables (SENTRY_DSN, SENTRY_ENVIRONMENT). The legacy
	// initializer is first-call-wins: repeated calls are safe and idempotent,
	// but an initial missing DSN or initialization failure is not retried.
	// Retryable startup is available through InitSentryWithHooks.
	Init() error

	// InitWithConfig initializes Sentry with explicit configuration. It retains
	// the same first-call-wins contract as Init.
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

	// SetUser sets user information on the global startup scope. Mandatory export
	// sanitization removes user fields; use an opaque correlation tag instead.
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
	mu          sync.RWMutex
	initialized bool
	config      SentryAdvancedConfig
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
	// Preserve the original public constructor contract: its first attempt owns
	// initialization for the lifetime of this reporter, including an empty DSN
	// or a failed SDK initialization. Retryable startup is intentionally exposed
	// only by InitWithAdvancedConfig.
	s.once.Do(func() {
		initErr = s.initWithAdvancedConfig(SentryAdvancedConfig{SentryConfig: cfg})
	})
	return initErr
}

func (s *sentrySdk) InitWithAdvancedConfig(cfg SentryAdvancedConfig) error {
	return s.initWithAdvancedConfig(cfg)
}

func (s *sentrySdk) initWithAdvancedConfig(cfg SentryAdvancedConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.initialized {
		return nil
	}
	s.config = cfg
	if cfg.DSN == "" {
		log.Println("fit/errors/sentry: SENTRY_DSN not set, error reporting disabled (graceful degradation)")
		return nil
	}

	opts := sentrylib.ClientOptions{
		Dsn:              cfg.DSN,
		Environment:      cfg.Environment,
		Release:          cfg.Release,
		Debug:            cfg.Debug,
		SampleRate:       cfg.SampleRate,
		TracesSampleRate: cfg.TracesSampleRate,
		EnableTracing:    cfg.TracesSampleRate > 0,
		// fit-go owns structured logs and metrics through OpenTelemetry. Keep the
		// Sentry wrapper limited to errors and explicitly requested transactions.
		EnableLogs:     false,
		DisableMetrics: true,
		ServerName:     cfg.ServerName,
		BeforeSend: func(event *sentrylib.Event, hint *sentrylib.EventHint) *sentrylib.Event {
			event = sanitizeSentryEvent(event)
			if event != nil && cfg.BeforeSend != nil {
				event = cfg.BeforeSend(event, hint)
			}
			return sanitizeSentryEvent(event)
		},
		BeforeSendTransaction: func(event *sentrylib.Event, hint *sentrylib.EventHint) *sentrylib.Event {
			event = sanitizeSentryEvent(event)
			if event != nil && cfg.BeforeSendTransaction != nil {
				event = cfg.BeforeSendTransaction(event, hint)
			}
			return sanitizeSentryEvent(event)
		},
	}

	if cfg.Transport != nil {
		opts.Transport = cfg.Transport
	}

	if err := sentrylib.Init(opts); err != nil {
		// SDK initialization errors can echo DSN/transport details. Keep the
		// process log secret-safe; callers still receive the original error.
		log.Printf("fit/errors/sentry: failed to initialize Sentry SDK")
		return err
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
		log.Printf("fit/errors/sentry: initialized (debug)")
	}
	return nil
}

func (s *sentrySdk) IsInitialized() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialized
}

func (s *sentrySdk) state() (initialized, debug bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialized, s.config.Debug
}

func (s *sentrySdk) CaptureError(err error) {
	if err == nil {
		return
	}
	// Skip NOP errors: FitErrors flagged as no-operation should not be reported.
	if fe, ok := IsFitError(err); ok && fe.NOP {
		return
	}
	if initialized, debug := s.state(); !initialized {
		if debug {
			log.Printf("fit/errors/sentry: [not initialized] would report error: %s", redact.Text(err.Error()))
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
	if initialized, debug := s.state(); !initialized {
		if debug {
			log.Printf("fit/errors/sentry: [not initialized] would report error: %s", redact.Text(err.Error()))
		}
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	hub := sentrylib.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentrylib.CurrentHub().Clone()
	}
	hub.CaptureException(err)
}

func (s *sentrySdk) CaptureMessage(message string) {
	if initialized, debug := s.state(); !initialized {
		if debug {
			log.Printf("fit/errors/sentry: [not initialized] would report message: %s", redact.Text(message))
		}
		return
	}
	sentrylib.CaptureMessage(message)
}

func (s *sentrySdk) CaptureMessageWithLevel(message string, level SentryLevel) {
	if initialized, debug := s.state(); !initialized {
		if debug {
			log.Printf("fit/errors/sentry: [not initialized] would report %s message: %s", level, redact.Text(message))
		}
		return
	}
	sentrylib.WithScope(func(scope *sentrylib.Scope) {
		scope.SetLevel(toSentryLevel(level))
		sentrylib.CaptureMessage(message)
	})
}

func (s *sentrySdk) AddBreadcrumb(category, message string, data map[string]interface{}) {
	if initialized, _ := s.state(); !initialized {
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
	if initialized, _ := s.state(); !initialized {
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
	if initialized, _ := s.state(); !initialized {
		return
	}
	sentrylib.ConfigureScope(func(scope *sentrylib.Scope) {
		scope.SetTag(key, value)
	})
}

func (s *sentrySdk) SetExtra(key string, value interface{}) {
	if initialized, _ := s.state(); !initialized {
		return
	}
	sentrylib.ConfigureScope(func(scope *sentrylib.Scope) {
		scope.SetExtra(key, value)
	})
}

func (s *sentrySdk) Flush() {
	if initialized, _ := s.state(); !initialized {
		return
	}
	sentrylib.Flush(2 * time.Second)
}

func (s *sentrySdk) FlushWithTimeout(timeout time.Duration) bool {
	if initialized, _ := s.state(); !initialized {
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

// WithSentryContext returns a context containing an isolated Sentry hub. The
// original context and process-global scope are not mutated.
func WithSentryContext(ctx context.Context, values SentryContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	hub := sentrylib.GetHubFromContext(ctx)
	if hub == nil {
		hub = sentrylib.CurrentHub()
	}
	hub = hub.Clone()
	hub.ConfigureScope(func(scope *sentrylib.Scope) {
		if values.CorrelationID != "" {
			scope.SetTag("correlation_id", values.CorrelationID)
		}
		scope.SetTags(values.Tags)
		scope.SetExtras(values.Extra)
		for _, breadcrumb := range values.Breadcrumbs {
			if breadcrumb != nil {
				scope.AddBreadcrumb(breadcrumb, 100)
			}
		}
	})
	return sentrylib.SetHubOnContext(ctx, hub)
}

// InitSentry initialises the active Sentry reporter. Its first call owns the
// legacy reporter lifetime; if SENTRY_DSN is not set the call is a permanent
// no-op for that reporter. Use InitSentryWithHooks when startup must remain
// retryable until configuration becomes available.
func InitSentry() error {
	return Sentry.Init()
}

// InitSentryWithConfig initializes Sentry with explicit configuration. Its
// first call owns the legacy reporter lifetime, including an empty DSN or SDK
// initialization failure. Use InitSentryWithHooks for retryable startup.
func InitSentryWithConfig(cfg SentryConfig) error {
	return Sentry.InitWithConfig(cfg)
}

// InitSentryWithAdvancedConfig initializes Sentry with post-main opt-in hooks.
// It returns an error when a custom reporter does not implement the advanced
// initialization contract instead of silently dropping controls.
// Deprecated: use InitSentryWithHooks.
func InitSentryWithAdvancedConfig(cfg SentryAdvancedConfig) error {
	reporter, ok := Sentry.(interface {
		InitWithAdvancedConfig(SentryAdvancedConfig) error
	})
	if !ok {
		return fmt.Errorf("errors: configured Sentry reporter does not support advanced initialization")
	}
	return reporter.InitWithAdvancedConfig(cfg)
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

func sanitizeSentryEvent(event *sentrylib.Event) *sentrylib.Event {
	if event == nil {
		return nil
	}
	event = detachSentryEvent(event)
	// The SDK freezes dynamic sampling context before BeforeSend and keeps it
	// outside the JSON event. Redacting Transaction alone does not redact the
	// envelope header. Its public getter returns a copy, with no direct setter.
	// Omit private metadata only for unsafe contexts; never mutate shared SDK
	// maps or use unsafe reflection. The event and its trace IDs remain intact.
	for key, value := range event.GetDynamicSamplingContext() {
		if redact.Text(key) != key || sanitizeSentryValue(key, value) != value {
			event = sentryEventWithoutPrivateMetadata(event)
			break
		}
	}
	event.Message = redact.Text(event.Message)
	for i := range event.Exception {
		event.Exception[i].Value = redact.Text(event.Exception[i].Value)
		sanitizeSentryStacktrace(event.Exception[i].Stacktrace)
		if event.Exception[i].Mechanism != nil {
			event.Exception[i].Mechanism.Type = redact.Text(event.Exception[i].Mechanism.Type)
			event.Exception[i].Mechanism.Description = redact.Text(event.Exception[i].Mechanism.Description)
			event.Exception[i].Mechanism.HelpLink = redact.Text(event.Exception[i].Mechanism.HelpLink)
			event.Exception[i].Mechanism.Source = redact.Text(event.Exception[i].Mechanism.Source)
			sanitizeSentryMap(event.Exception[i].Mechanism.Data)
		}
	}
	for _, breadcrumb := range event.Breadcrumbs {
		if breadcrumb == nil {
			continue
		}
		breadcrumb.Message = redact.Text(breadcrumb.Message)
		breadcrumb.Category = redact.Text(breadcrumb.Category)
		sanitizeSentryMap(breadcrumb.Data)
	}
	sanitizeSentryMap(event.Extra)
	for _, contextValues := range event.Contexts {
		sanitizeSentryMap(contextValues)
	}
	for key, value := range event.Tags {
		if sensitiveSentryKey(key) {
			event.Tags[key] = redact.Mask
		} else {
			event.Tags[key] = redact.Text(value)
		}
	}
	for i := range event.Fingerprint {
		event.Fingerprint[i] = redact.Text(event.Fingerprint[i])
	}
	event.Transaction = redact.Text(event.Transaction)
	event.Logger = redact.Text(event.Logger)
	for i := range event.Threads {
		event.Threads[i].ID = redact.Text(event.Threads[i].ID)
		event.Threads[i].Name = redact.Text(event.Threads[i].Name)
		sanitizeSentryStacktrace(event.Threads[i].Stacktrace)
	}
	for _, span := range event.Spans {
		if span == nil {
			continue
		}
		span.Name = redact.Text(span.Name)
		span.Description = redact.Text(span.Description)
		sanitizeSentryMap(span.Data)
		sanitizeSentryMap(span.Extra)
		for key, value := range span.Tags {
			if sensitiveSentryKey(key) {
				span.Tags[key] = redact.Mask
			} else {
				span.Tags[key] = redact.Text(value)
			}
		}
	}
	// Attachments are opaque bytes and cannot be safely inspected here.
	event.Attachments = nil
	// fit-go deliberately does not use Sentry as a logs or metrics backend. A
	// caller hook must not be able to smuggle those signal payloads into an
	// otherwise sanitized error or transaction event.
	event.Logs = nil
	event.Metrics = nil
	// User fields are PII by definition. Service code can attach an opaque,
	// allowlisted identifier as a tag when correlation is required.
	event.User = sentrylib.User{}
	if event.Request != nil {
		event.Request.URL = redact.Text(event.Request.URL)
		event.Request.QueryString = redact.Mask
		event.Request.Data = redact.Mask
		event.Request.Cookies = redact.Mask
		for key, value := range event.Request.Headers {
			if sensitiveSentryKey(key) {
				event.Request.Headers[key] = redact.Mask
			} else {
				event.Request.Headers[key] = redact.HeaderValue(key, redact.Text(value))
			}
		}
		for key, value := range event.Request.Env {
			if sensitiveSentryKey(key) {
				event.Request.Env[key] = redact.Mask
			} else {
				event.Request.Env[key] = redact.Text(value)
			}
		}
	}
	return rebuildSanitizedSentryEvent(event)
}

// rebuildSanitizedSentryEvent discards SDK serialization caches, which can have
// been populated before capture or by a caller hook. MakeSerializationSafe on
// the old event cannot clear a cached user after User is emptied, and MarshalJSON
// otherwise prefers those stale bytes over our sanitized public fields.
func rebuildSanitizedSentryEvent(event *sentrylib.Event) *sentrylib.Event {
	sampling := event.GetDynamicSamplingContext()
	rebuilt := sentryEventWithoutPrivateMetadata(event)
	if len(sampling) != 0 {
		// An empty scope with no client or processors installs only the vetted DSC.
		// ApplyToEvent also installs its propagation trace context; temporarily
		// detach ours so that operation cannot overwrite or mutate the event's
		// existing correlation IDs or add a trace context where none existed.
		contexts := rebuilt.Contexts
		rebuilt.Contexts = nil
		scope := sentrylib.NewScope()
		scope.SetPropagationContext(sentrylib.PropagationContext{
			DynamicSamplingContext: sentrylib.DynamicSamplingContext{Entries: sampling, Frozen: true},
		})
		scope.ApplyToEvent(rebuilt, nil, nil)
		rebuilt.Contexts = contexts
	}
	rebuilt.MakeSerializationSafe()
	return rebuilt
}

// sentryEventWithoutPrivateMetadata preserves every public SDK event field
// while discarding frozen sampling metadata and cached JSON. Safe sampling is
// restored separately by rebuildSanitizedSentryEvent; unsafe sampling is omitted.
// Keep this projection covered by the SDK-field regression test on SDK updates.
func sentryEventWithoutPrivateMetadata(e *sentrylib.Event) *sentrylib.Event {
	return &sentrylib.Event{
		Breadcrumbs: e.Breadcrumbs, Contexts: e.Contexts, Dist: e.Dist,
		Environment: e.Environment, EventID: e.EventID, Extra: e.Extra,
		Fingerprint: e.Fingerprint, Level: e.Level, Message: e.Message,
		Platform: e.Platform, Release: e.Release, Sdk: e.Sdk,
		ServerName: e.ServerName, Threads: e.Threads, Tags: e.Tags,
		Timestamp: e.Timestamp, Transaction: e.Transaction, User: e.User,
		Logger: e.Logger, Modules: e.Modules, Request: e.Request,
		Exception: e.Exception, DebugMeta: e.DebugMeta, Attachments: e.Attachments,
		Type: e.Type, StartTime: e.StartTime, Spans: e.Spans,
		TransactionInfo: e.TransactionInfo, CheckIn: e.CheckIn,
		MonitorConfig: e.MonitorConfig, Logs: e.Logs, Metrics: e.Metrics,
	}
}

func cloneSentryBreadcrumb(original *sentrylib.Breadcrumb) *sentrylib.Breadcrumb {
	if original == nil {
		return nil
	}
	cloned := *original
	if original.Data != nil {
		cloned.Data = make(map[string]interface{}, len(original.Data))
		for key, value := range original.Data {
			cloned.Data[key] = value
		}
	}
	return &cloned
}

// detachSentryEvent copies every public object this sanitizer modifies. The SDK
// clones scope maps, but event-provided maps, frame slices and span pointers can
// still be shared by otherwise independent captures. Nested variable values are
// copied by the bounded reflected walk; no original graph is changed here.
// Event has no locks. Its private caches/DSC survive this shallow copy only until
// the existing cache-free public projection at the end of sanitization.
func detachSentryEvent(original *sentrylib.Event) *sentrylib.Event {
	cloned := *original
	cloned.Breadcrumbs = slices.Clone(original.Breadcrumbs)
	for index, breadcrumb := range original.Breadcrumbs {
		cloned.Breadcrumbs[index] = cloneSentryBreadcrumb(breadcrumb)
	}
	cloned.Extra = maps.Clone(original.Extra)
	cloned.Contexts = maps.Clone(original.Contexts)
	for key, values := range original.Contexts {
		cloned.Contexts[key] = maps.Clone(values)
	}
	cloned.Tags = maps.Clone(original.Tags)
	cloned.Fingerprint = slices.Clone(original.Fingerprint)
	cloned.Exception = slices.Clone(original.Exception)
	for index := range cloned.Exception {
		exception := &cloned.Exception[index]
		exception.Stacktrace = cloneSentryStacktrace(exception.Stacktrace)
		if exception.Mechanism != nil {
			mechanism := *exception.Mechanism
			mechanism.Data = maps.Clone(mechanism.Data)
			exception.Mechanism = &mechanism
		}
	}
	cloned.Threads = slices.Clone(original.Threads)
	for index := range cloned.Threads {
		cloned.Threads[index].Stacktrace = cloneSentryStacktrace(cloned.Threads[index].Stacktrace)
	}
	if original.Request != nil {
		request := *original.Request
		request.Headers = maps.Clone(request.Headers)
		request.Env = maps.Clone(request.Env)
		cloned.Request = &request
	}
	cloned.Spans = slices.Clone(original.Spans)
	for index, span := range original.Spans {
		cloned.Spans[index] = cloneSentrySpanSnapshot(span)
	}
	return &cloned
}

func cloneSentryStacktrace(original *sentrylib.Stacktrace) *sentrylib.Stacktrace {
	if original == nil {
		return nil
	}
	cloned := *original
	cloned.Frames = slices.Clone(original.Frames)
	for index := range cloned.Frames {
		frame := &cloned.Frames[index]
		frame.Vars = maps.Clone(frame.Vars)
		frame.PreContext = slices.Clone(frame.PreContext)
		frame.PostContext = slices.Clone(frame.PostContext)
	}
	return &cloned
}

// BeforeSend receives spans as diagnostic export payloads, not live span
// lifecycles. Project their public fields without copying SDK mutex/Once state
// or starting synthetic spans. Private Context/parent/recorder state is not
// reproduced; the snapshot retains every public wire and sampling field.
func cloneSentrySpanSnapshot(original *sentrylib.Span) *sentrylib.Span {
	if original == nil {
		return nil
	}
	return &sentrylib.Span{
		TraceID: original.TraceID, SpanID: original.SpanID, ParentSpanID: original.ParentSpanID,
		Name: original.Name, Op: original.Op, Description: original.Description, Status: original.Status,
		Tags: maps.Clone(original.Tags), StartTime: original.StartTime, EndTime: original.EndTime,
		Extra: maps.Clone(original.Extra), Data: maps.Clone(original.Data), Sampled: original.Sampled,
		Source: original.Source, Origin: original.Origin,
	}
}

func sanitizeSentryStacktrace(stacktrace *sentrylib.Stacktrace) {
	if stacktrace == nil {
		return
	}
	for i := range stacktrace.Frames {
		frame := &stacktrace.Frames[i]
		frame.Function = redact.Text(frame.Function)
		frame.Symbol = redact.Text(frame.Symbol)
		frame.Module = redact.Text(frame.Module)
		frame.Filename = redact.Text(frame.Filename)
		frame.AbsPath = redact.Text(frame.AbsPath)
		frame.Package = redact.Text(frame.Package)
		frame.ContextLine = redact.Text(frame.ContextLine)
		for j := range frame.PreContext {
			frame.PreContext[j] = redact.Text(frame.PreContext[j])
		}
		for j := range frame.PostContext {
			frame.PostContext[j] = redact.Text(frame.PostContext[j])
		}
		sanitizeSentryMap(frame.Vars)
	}
}

func sanitizeSentryMap(values map[string]interface{}) {
	state := newSentryValueTraversal()
	for key, value := range values {
		values[key] = sanitizeSentryValueDepth(key, value, state, 0)
	}
}

func sanitizeSentryValue(key string, value interface{}) interface{} {
	return sanitizeSentryValueDepth(key, value, newSentryValueTraversal(), 0)
}

const (
	maxSentryValueDepth = 12
	maxSentryCollection = 100
	maxSentryValueNodes = 8192
)

type sentryVisit struct {
	typ reflect.Type
	ptr uintptr
	len int // Overlapping acyclic subslices may share a backing pointer.
}

type sentryValueTraversal struct {
	active map[sentryVisit]struct{}
	nodes  int
}

func newSentryValueTraversal() *sentryValueTraversal {
	return &sentryValueTraversal{active: make(map[sentryVisit]struct{})}
}

// Shared acyclic values must not be mistaken for cycles, but expanding a DAG
// can grow exponentially. Bound the complete walk and mask exhausted branches;
// this is diagnostic truncation, not a restriction on application payloads.
func (s *sentryValueTraversal) takeNode() bool {
	if s.nodes >= maxSentryValueNodes {
		return false
	}
	s.nodes++
	return true
}

func sanitizeSentryValueDepth(key string, value interface{}, state *sentryValueTraversal, depth int) interface{} {
	if !state.takeNode() {
		return redact.Mask
	}
	if sensitiveSentryKey(key) {
		return redact.Mask
	}
	if value == nil {
		return nil
	}
	if depth >= maxSentryValueDepth {
		return redact.Mask
	}
	switch typed := value.(type) {
	case sentrylib.TraceID:
		return typed.String()
	case sentrylib.SpanID:
		return typed.String()
	case string:
		// A caller hook is followed by a second sanitization pass. Preserve only
		// correctly sized hex identifiers under the SDK's correlation keys; do
		// not exempt arbitrary byte arrays, strings, or Stringer implementations.
		if validSentryCorrelationID(key, typed) {
			return typed
		}
		return redact.Text(typed)
	case error:
		return redact.Text(typed.Error())
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return sanitizeReflectedSentryValue(reflect.ValueOf(value), state, depth)
	}
}

func validSentryCorrelationID(key, value string) bool {
	length := 0
	switch key {
	case "trace_id":
		length = 32
	case "span_id", "parent_span_id":
		length = 16
	default:
		return false
	}
	if len(value) != length {
		return false
	}
	for _, digit := range value {
		if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f' || digit >= 'A' && digit <= 'F') {
			return false
		}
	}
	return true
}

func sanitizeReflectedSentryValue(value reflect.Value, state *sentryValueTraversal, depth int) interface{} {
	if !value.IsValid() {
		return nil
	}
	if depth >= maxSentryValueDepth {
		return redact.Mask
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.String:
		return redact.Text(value.String())
	case reflect.Bool:
		return value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return value.Uint()
	case reflect.Float32, reflect.Float64:
		return value.Float()
	case reflect.Ptr:
		if value.IsNil() {
			return nil
		}
		if seenSentryReference(value, state.active) {
			return redact.Mask
		}
		defer delete(state.active, sentryReference(value))
		if !state.takeNode() {
			return redact.Mask
		}
		return sanitizeReflectedSentryValue(value.Elem(), state, depth+1)
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		if value.Type().Key().Kind() != reflect.String || seenSentryReference(value, state.active) {
			return redact.Mask
		}
		defer delete(state.active, sentryReference(value))
		result := make(map[string]interface{})
		iter := value.MapRange()
		for len(result) < maxSentryCollection && iter.Next() {
			nestedKey := iter.Key().String()
			result[nestedKey] = sanitizeSentryValueDepth(nestedKey, iter.Value().Interface(), state, depth+1)
		}
		return result
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return redact.Mask
		}
		if value.Kind() == reflect.Slice {
			if value.IsNil() {
				return nil
			}
			if seenSentryReference(value, state.active) {
				return redact.Mask
			}
			defer delete(state.active, sentryReference(value))
		}
		length := value.Len()
		if length > maxSentryCollection {
			length = maxSentryCollection
		}
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			result[i] = sanitizeSentryValueDepth("", value.Index(i).Interface(), state, depth+1)
		}
		return result
	case reflect.Struct:
		result := make(map[string]interface{})
		typ := value.Type()
		for i := 0; i < value.NumField() && len(result) < maxSentryCollection; i++ {
			field := typ.Field(i)
			if field.PkgPath != "" {
				continue
			}
			name := field.Name
			jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
			if jsonName == "-" {
				continue
			}
			if jsonName != "" {
				name = jsonName
			}
			if sensitiveSentryKey(field.Name) || sensitiveSentryKey(name) {
				result[name] = redact.Mask
			} else {
				result[name] = sanitizeSentryValueDepth(name, value.Field(i).Interface(), state, depth+1)
			}
		}
		return result
	default:
		return redact.Mask
	}
}

func seenSentryReference(value reflect.Value, visited map[sentryVisit]struct{}) bool {
	visit := sentryReference(value)
	if visit.ptr == 0 {
		return false
	}
	if _, exists := visited[visit]; exists {
		return true
	}
	visited[visit] = struct{}{}
	return false
}

func sentryReference(value reflect.Value) sentryVisit {
	visit := sentryVisit{typ: value.Type(), ptr: value.Pointer()}
	if value.Kind() == reflect.Slice {
		visit.len = value.Len()
	}
	return visit
}

// sensitiveSentryCredentialAlias recognizes short credential names as complete
// label components, not substrings: "paymentCVV" is sensitive, while "shipping"
// and "span_id" are not. Scan forward once with at most two preceding components
// so arbitrary user-supplied keys cannot introduce an unbounded look-back.
func sensitiveSentryCredentialAlias(key string) bool {
	var previous, beforePrevious string
	componentSensitive := func(component string) bool {
		for _, alias := range [...]string{
			"pwd", "pass", "passphrase", "pin", "otp", "cvv", "cvc", "pan",
			"cardnumber", "creditcardnumber", "primaryaccountnumber", "verificationcode", "onetimepassword",
		} {
			if strings.EqualFold(component, alias) {
				return true
			}
		}
		if strings.EqualFold(component, "number") &&
			(strings.EqualFold(previous, "card") ||
				strings.EqualFold(previous, "account") && strings.EqualFold(beforePrevious, "primary")) {
			return true
		}
		if strings.EqualFold(component, "code") && strings.EqualFold(previous, "verification") {
			return true
		}
		beforePrevious, previous = previous, component
		return false
	}
	word := func(b byte) bool {
		return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
	}
	upper := func(b byte) bool { return b >= 'A' && b <= 'Z' }
	lower := func(b byte) bool { return b >= 'a' && b <= 'z' }
	start := -1
	for index := 0; index < len(key); index++ {
		if !word(key[index]) {
			if start >= 0 && componentSensitive(key[start:index]) {
				return true
			}
			start = -1
			continue
		}
		if start < 0 {
			start = index
			continue
		}
		// Split lowerUpper and ACRONYMWord while retaining an all-caps alias.
		if upper(key[index]) && (lower(key[index-1]) ||
			upper(key[index-1]) && index+1 < len(key) && lower(key[index+1])) {
			if componentSensitive(key[start:index]) {
				return true
			}
			start = index
		}
	}
	return start >= 0 && componentSensitive(key[start:])
}

func sensitiveSentryKey(key string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(strings.TrimSpace(key)))
	if normalized == "raw" || normalized == "query" || normalized == "querystring" ||
		strings.HasSuffix(normalized, "body") || strings.HasSuffix(normalized, "payload") ||
		strings.HasSuffix(normalized, "response") {
		return true
	}
	for _, fragment := range []string{
		"password", "passwd", "secret", "token", "authorization", "cookie",
		"apikey", "email", "phone", "recipient", "username",
	} {
		if strings.Contains(normalized, fragment) {
			return true
		}
	}
	return sensitiveSentryCredentialAlias(key)
}
