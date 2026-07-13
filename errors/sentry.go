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
	"reflect"
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
	// environment variables (SENTRY_DSN, SENTRY_ENVIRONMENT) and be safe
	// to call multiple times. Successful initialization is idempotent; a
	// missing DSN or failed initialization remains retryable.
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
	mu          sync.RWMutex
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
			event.Request.Headers[key] = redact.HeaderValue(key, redact.Text(value))
		}
		for key, value := range event.Request.Env {
			if sensitiveSentryKey(key) {
				event.Request.Env[key] = redact.Mask
			} else {
				event.Request.Env[key] = redact.Text(value)
			}
		}
	}
	return event
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
	visited := make(map[sentryVisit]struct{})
	for key, value := range values {
		values[key] = sanitizeSentryValueDepth(key, value, visited, 0)
	}
}

func sanitizeSentryValue(key string, value interface{}) interface{} {
	return sanitizeSentryValueDepth(key, value, make(map[sentryVisit]struct{}), 0)
}

const (
	maxSentryValueDepth = 12
	maxSentryCollection = 100
)

type sentryVisit struct {
	typ reflect.Type
	ptr uintptr
}

func sanitizeSentryValueDepth(key string, value interface{}, visited map[sentryVisit]struct{}, depth int) interface{} {
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
	case string:
		return redact.Text(typed)
	case error:
		return redact.Text(typed.Error())
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return sanitizeReflectedSentryValue(reflect.ValueOf(value), visited, depth)
	}
}

func sanitizeReflectedSentryValue(value reflect.Value, visited map[sentryVisit]struct{}, depth int) interface{} {
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
		if seenSentryReference(value, visited) {
			return redact.Mask
		}
		return sanitizeReflectedSentryValue(value.Elem(), visited, depth+1)
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		if seenSentryReference(value, visited) || value.Type().Key().Kind() != reflect.String {
			return redact.Mask
		}
		result := make(map[string]interface{})
		iter := value.MapRange()
		for len(result) < maxSentryCollection && iter.Next() {
			nestedKey := iter.Key().String()
			result[nestedKey] = sanitizeSentryValueDepth(nestedKey, iter.Value().Interface(), visited, depth+1)
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
			if seenSentryReference(value, visited) {
				return redact.Mask
			}
		}
		length := value.Len()
		if length > maxSentryCollection {
			length = maxSentryCollection
		}
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			result[i] = sanitizeSentryValueDepth("", value.Index(i).Interface(), visited, depth+1)
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
			result[name] = sanitizeSentryValueDepth(name, value.Field(i).Interface(), visited, depth+1)
		}
		return result
	default:
		return redact.Mask
	}
}

func seenSentryReference(value reflect.Value, visited map[sentryVisit]struct{}) bool {
	visit := sentryVisit{typ: value.Type(), ptr: value.Pointer()}
	if visit.ptr == 0 {
		return false
	}
	if _, exists := visited[visit]; exists {
		return true
	}
	visited[visit] = struct{}{}
	return false
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
	return false
}
