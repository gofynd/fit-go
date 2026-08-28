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

// Package logging provides structured JSON logging for the fit.go framework.
// Structured JSON logger
// adapted to use Go's log/slog with timezone-aware timestamps, colorized
// development output, and OpenTelemetry trace context propagation.
package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"

	"go.opentelemetry.io/otel/sdk"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/gofynd/fit-go/internal/goroutinectx"
	"github.com/gofynd/fit-go/redact"
)

// implicitTraceEnabled gates the goroutine-local trace fallback in (*Logger).log,
// so the runtime.Stack-based goroutine-id lookup only runs when tracing is on.
// tracing.New keeps it in sync with the tracer's enabled state. Default false.
var implicitTraceEnabled atomic.Bool

// SetImplicitTraceEnabled toggles the implicit trace-in-logs fallback. Called by
// tracing.New.
func SetImplicitTraceEnabled(b bool) { implicitTraceEnabled.Store(b) }

// Context keys for trace propagation. These match the OpenTelemetry context
// keys that the tracing module injects.
type ctxKey int

const (
	ctxKeyTraceID ctxKey = iota
	ctxKeySpanID
	ctxKeyTraceFlags
)

// ContextWithTrace returns a context carrying trace_id and span_id.
// The tracing module calls this to propagate IDs into the logger.
func ContextWithTrace(ctx context.Context, traceID, spanID string) context.Context {
	return ContextWithTraceFlags(ctx, traceID, spanID, 0)
}

// ContextWithTraceFlags returns a context carrying trace_id, span_id, and the
// W3C trace flags used by the active span. Keeping the flags beside the
// compatibility IDs lets the TraceClue log envelope preserve sampled versus
// unsampled inbound requests even when the caller only has a fit-go context.
func ContextWithTraceFlags(ctx context.Context, traceID, spanID string, traceFlags byte) context.Context {
	ctx = context.WithValue(ctx, ctxKeyTraceID, traceID)
	ctx = context.WithValue(ctx, ctxKeySpanID, spanID)
	ctx = context.WithValue(ctx, ctxKeyTraceFlags, traceFlags)
	return ctx
}

// Level represents log severity. Levels are ordered so that a logger set to
// a given level will emit that level and all higher-severity levels.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

var levelNames = map[Level]string{
	LevelDebug: "debug",
	LevelInfo:  "info",
	LevelWarn:  "warn",
	LevelError: "error",
	LevelFatal: "fatal",
}

var levelFromString = map[string]Level{
	"debug": LevelDebug,
	"info":  LevelInfo,
	"warn":  LevelWarn,
	"error": LevelError,
	"fatal": LevelFatal,
}

// Schema controls the serialized log envelope.
type Schema string

// TraceClueBodyTruncation selects the installed TraceClue generation's body
// behavior. TraceClue 3.1.3 truncates only when LOG_LEVEL=debug; pyfit 1.10
// truncates at non-debug levels; TraceClue 3.0.5 and some older integrations
// truncate at every level.
type TraceClueBodyTruncation string

const (
	// SchemaPlatform is the explicit fit-go platform JSON contract. It remains
	// available for callers that do not consume the legacy TraceClue envelope.
	SchemaPlatform Schema = "platform"
	// SchemaTraceClue emits the stdout JSON envelope used by deployed TraceClue.
	SchemaTraceClue Schema = "traceclue"

	TraceClueTruncateDebugOnly TraceClueBodyTruncation = "debug-only"
	TraceClueTruncateNonDebug  TraceClueBodyTruncation = "non-debug"
	TraceClueTruncateAlways    TraceClueBodyTruncation = "always"
	TraceClueTruncateNever     TraceClueBodyTruncation = "never"
)

func (l Level) String() string {
	if s, ok := levelNames[l]; ok {
		return s
	}
	return "info"
}

// ParseLevel converts a string level name to a Level. Defaults to LevelInfo
// for unrecognized values.
func ParseLevel(s string) Level {
	if l, ok := levelFromString[strings.ToLower(strings.TrimSpace(s))]; ok {
		return l
	}
	return LevelInfo
}

// HealthCheckPaths are the default URL paths that should be filtered from
// request logging to reduce noise. Callers (e.g. the server middleware) can
// check against these before emitting a log line.
var HealthCheckPaths = []string{"/_healthz", "/_readyz"}

// IsHealthCheckPath reports whether the given URL path is a health check
// endpoint that should typically be excluded from request logging.
func IsHealthCheckPath(path string) bool {
	for _, hp := range HealthCheckPaths {
		if path == hp {
			return true
		}
	}
	return false
}

// Options configures a Logger instance. Mirrors the Winston logger
// configuration tracing/index.ts.
type Options struct {
	// Level is the minimum log level (debug, info, warn, error, fatal).
	// Falls back to the LOG_LEVEL env var, then defaults to "info".
	Level string

	// Timezone is an IANA timezone name for formatting timestamps.
	// Falls back to the LOG_TIMEZONE env var, then defaults to "UTC".
	Timezone string

	// Env is the deployment environment (e.g. "production", "development").
	// Falls back to NODE_ENV env var, then defaults to "development".
	// In non-production environments, colorized output is written to stderr.
	Env string

	// OmitEnvironmentResource removes deployment.environment from the
	// TraceClue resource. It is an opt-in compatibility switch for pinned
	// TraceClue versions which never emitted that resource attribute. The
	// default remains false so existing fit-go callers keep their current
	// resource schema.
	OmitEnvironmentResource bool

	// Service is an optional service name included in every log entry.
	Service string

	// Schema selects the JSON envelope. Empty falls back to FIT_LOG_SCHEMA and
	// then SchemaTraceClue, the global compatibility default. Set SchemaPlatform
	// explicitly when the platform envelope is required.
	Schema Schema

	// ResourceAttributes extends the TraceClue-compatible resource object.
	// Explicit values override its service/host/SDK defaults.
	ResourceAttributes map[string]interface{}

	// TraceClueBodyLimit and TraceClueMetaLimit default to the deployed values
	// 500 and 1000. BodyTruncation defaults to 3.1.3's debug-only behavior and
	// can be set to always for TraceClue 3.0.5/2.1.x compatibility.
	TraceClueBodyLimit      int
	TraceClueMetaLimit      int
	TraceClueBodyTruncation TraceClueBodyTruncation
	// When RestrictAttributesTo is non-empty, unlisted attributes are JSON
	// encoded into `_meta`, matching TraceClue's structured attribute mode.
	TraceClueRestrictAttributesTo  []string
	TraceClueDiscardAttributesFrom []string

	// Output overrides the default writer. Normally stdout for production,
	// stderr for development. Primarily useful for testing.
	Output io.Writer
}

// entry is a single structured log record serialized as JSON.
type entry struct {
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Timestamp string                 `json:"timestamp"`
	Service   string                 `json:"service,omitempty"`
	TraceID   string                 `json:"trace_id,omitempty"`
	SpanID    string                 `json:"span_id,omitempty"`
	Caller    string                 `json:"caller,omitempty"`
	Extra     map[string]interface{} `json:"extra,omitempty"`
}

// traceClueEntry mirrors the deployed Winston OpenTelemetry formatter envelope.
// Empty trace identifiers are retained because legacy always serialized them.
type traceClueEntry struct {
	Body           interface{}            `json:"body"`
	SeverityNumber *int                   `json:"severity_number,omitempty"`
	SeverityText   string                 `json:"severity_text"`
	Attributes     map[string]interface{} `json:"attributes"`
	Timestamp      string                 `json:"timestamp"`
	TraceID        string                 `json:"trace_id"`
	SpanID         string                 `json:"span_id"`
	TraceFlags     byte                   `json:"trace_flags"`
	Resource       map[string]interface{} `json:"resource"`
}

// Logger is a structured, thread-safe JSON logger. It is the Go equivalent
// of the Winston logger created tracing/index.ts.
type Logger struct {
	mu                      *sync.Mutex // shared across clones (see clone); guards out + level
	level                   Level
	loc                     *time.Location
	env                     string
	service                 string
	out                     io.Writer
	colorize                bool
	fields                  map[string]interface{} // persistent fields added via WithFields
	traceID                 string                 // bound trace context (via WithContext)
	spanID                  string
	traceFlags              byte
	schema                  Schema
	resource                map[string]interface{}
	traceClueBodyLimit      int
	traceClueMetaLimit      int
	traceClueBodyTruncation TraceClueBodyTruncation
	traceClueRestrict       map[string]struct{}
	traceClueDiscard        map[string]struct{}
}

// New creates a Logger from the given Options. It loads the timezone,
// resolves env-var fallbacks, and selects the appropriate output stream and
// format (colorized for development, plain JSON for production).
func New(opts Options) (*Logger, error) {
	// Resolve env-var fallbacks.
	if opts.Level == "" {
		if v := os.Getenv("LOG_LEVEL"); v != "" {
			opts.Level = v
		} else {
			opts.Level = "info"
		}
	}
	if opts.Timezone == "" {
		if v := os.Getenv("LOG_TIMEZONE"); v != "" {
			opts.Timezone = v
		} else {
			opts.Timezone = "UTC"
		}
	}
	if opts.Env == "" {
		if v := os.Getenv("NODE_ENV"); v != "" {
			opts.Env = v
		} else {
			opts.Env = "development"
		}
	}
	if opts.Schema == "" {
		opts.Schema = Schema(strings.ToLower(strings.TrimSpace(os.Getenv("FIT_LOG_SCHEMA"))))
		if opts.Schema == "" {
			opts.Schema = SchemaTraceClue
		}
	}
	if opts.Schema != SchemaPlatform && opts.Schema != SchemaTraceClue {
		return nil, fmt.Errorf("logging: unsupported schema %q", opts.Schema)
	}
	if opts.TraceClueBodyLimit <= 0 {
		opts.TraceClueBodyLimit = 500
	}
	if opts.TraceClueMetaLimit <= 0 {
		opts.TraceClueMetaLimit = 1000
	}
	if opts.TraceClueBodyTruncation == "" {
		opts.TraceClueBodyTruncation = TraceClueBodyTruncation(strings.ToLower(strings.TrimSpace(os.Getenv("FIT_TRACECLUE_BODY_TRUNCATION"))))
		if opts.TraceClueBodyTruncation == "" {
			opts.TraceClueBodyTruncation = TraceClueTruncateDebugOnly
		}
	}
	switch opts.TraceClueBodyTruncation {
	case TraceClueTruncateDebugOnly, TraceClueTruncateNonDebug, TraceClueTruncateAlways, TraceClueTruncateNever:
	default:
		return nil, fmt.Errorf("logging: unsupported TraceClue body truncation mode %q", opts.TraceClueBodyTruncation)
	}

	loc, err := time.LoadLocation(opts.Timezone)
	if err != nil {
		return nil, fmt.Errorf("logging: invalid timezone %q: %w", opts.Timezone, err)
	}

	isProd := strings.ToLower(strings.TrimSpace(opts.Env)) == "production"

	w := opts.Output
	if w == nil {
		if isProd {
			w = os.Stdout
		} else {
			w = os.Stderr
		}
	}

	logger := &Logger{
		mu:                      &sync.Mutex{},
		level:                   ParseLevel(opts.Level),
		loc:                     loc,
		env:                     opts.Env,
		service:                 opts.Service,
		out:                     w,
		colorize:                !isProd,
		schema:                  opts.Schema,
		traceClueBodyLimit:      opts.TraceClueBodyLimit,
		traceClueMetaLimit:      opts.TraceClueMetaLimit,
		traceClueBodyTruncation: opts.TraceClueBodyTruncation,
	}
	if opts.Schema == SchemaTraceClue {
		logger.resource = traceClueResource(opts)
		logger.traceClueRestrict = stringSet(opts.TraceClueRestrictAttributesTo)
		logger.traceClueDiscard = stringSet(opts.TraceClueDiscardAttributesFrom)
	}
	return logger, nil
}

func traceClueResource(opts Options) map[string]interface{} {
	service := strings.TrimSpace(opts.Service)
	if service == "" {
		service = strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	}
	if service == "" {
		if resourceService, ok := opts.ResourceAttributes["service.name"].(string); ok {
			service = strings.TrimSpace(resourceService)
		}
	}
	if service == "" {
		service = envString("SERVICE_NAME", "unknown_service")
	}
	instanceID, _ := os.Hostname()
	resource := map[string]interface{}{
		"service.instance.id":    instanceID,
		"telemetry.sdk.language": "go",
		"telemetry.sdk.name":     "opentelemetry",
		"telemetry.sdk.version":  sdk.Version(),
		"pathname":               "",
	}
	if !opts.OmitEnvironmentResource {
		resource["deployment.environment"] = opts.Env
	}
	for k, v := range opts.ResourceAttributes {
		resource[k] = v
	}
	// Keep log and trace resource precedence aligned: explicit Service,
	// OTEL_SERVICE_NAME, explicit resource service.name, SERVICE_NAME fallback.
	resource["service.name"] = service
	return resource
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func stringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

// clone returns a shallow copy of the logger with a deep-copied fields map so
// that derived loggers are independent. The write mutex is SHARED (pointer) with
// the parent because clones share the same underlying out writer — an independent
// mutex per clone would let a logger and its derived child race on that writer.
func (l *Logger) clone() *Logger {
	// Snapshot the parent under the shared lock: level is mutable via SetLevel, so
	// an unlocked read here would race with a concurrent SetLevel.
	l.mu.Lock()
	defer l.mu.Unlock()
	c := &Logger{
		mu:                      l.mu,
		level:                   l.level,
		loc:                     l.loc,
		env:                     l.env,
		service:                 l.service,
		out:                     l.out,
		colorize:                l.colorize,
		traceID:                 l.traceID,
		spanID:                  l.spanID,
		traceFlags:              l.traceFlags,
		schema:                  l.schema,
		traceClueBodyLimit:      l.traceClueBodyLimit,
		traceClueMetaLimit:      l.traceClueMetaLimit,
		traceClueBodyTruncation: l.traceClueBodyTruncation,
		traceClueRestrict:       l.traceClueRestrict,
		traceClueDiscard:        l.traceClueDiscard,
	}
	if len(l.fields) > 0 {
		c.fields = make(map[string]interface{}, len(l.fields))
		for k, v := range l.fields {
			c.fields[k] = v
		}
	}
	if len(l.resource) > 0 {
		c.resource = make(map[string]interface{}, len(l.resource))
		for k, v := range l.resource {
			c.resource[k] = v
		}
	}
	return c
}

// WithContext returns a child Logger that extracts trace_id and span_id from
// the context. This mirrors the OpenTelemetry log format enrichment that
// performs via the opentelemetryLogFormat Winston format.
func (l *Logger) WithContext(ctx context.Context) *Logger {
	c := l.clone()
	if ctx == nil {
		return c
	}
	// Native OTel context is authoritative. A native instrumentation layer may
	// have created a child after fit-go seeded compatibility keys in its parent.
	if sc := oteltrace.SpanContextFromContext(ctx); sc.IsValid() {
		c.traceID = sc.TraceID().String()
		c.spanID = sc.SpanID().String()
		c.traceFlags = byte(sc.TraceFlags())
		return c
	}
	if v, ok := ctx.Value(ctxKeyTraceID).(string); ok && v != "" {
		c.traceID = v
		if flags, ok := ctx.Value(ctxKeyTraceFlags).(byte); ok {
			c.traceFlags = flags
		} else {
			c.traceFlags = 0
		}
	}
	if v, ok := ctx.Value(ctxKeySpanID).(string); ok && v != "" {
		c.spanID = v
	}
	return c
}

// levelEnabled reports whether a log at lvl would be emitted (>= threshold).
func (l *Logger) levelEnabled(lvl Level) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return lvl >= l.level
}

// WithFields returns a child Logger that includes the given key-value pairs
// in every subsequent log entry. Fields are merged with any existing
// persistent fields; new values overwrite old ones on key collision.
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	c := l.clone()
	if c.fields == nil {
		c.fields = make(map[string]interface{}, len(fields))
	}
	for k, v := range fields {
		c.fields[k] = v
	}
	return c
}

// WithService returns a child Logger with the given service name.
func (l *Logger) WithService(name string) *Logger {
	c := l.clone()
	c.service = name
	if c.schema == SchemaTraceClue && c.resource != nil {
		c.resource["service.name"] = name
	}
	return c
}

// SetLevel changes the minimum log level at runtime. Thread-safe.
func (l *Logger) SetLevel(level string) {
	l.mu.Lock()
	l.level = ParseLevel(level)
	l.mu.Unlock()
}

// ---------- Log methods ----------
// Each method accepts a message and optional key-value pairs (alternating
// string keys and arbitrary values), matching the slog-style variadic API
// used in fit.go's Init function.

// Debug logs at debug level.
func (l *Logger) Debug(msg string, kvs ...interface{}) {
	l.log(LevelDebug, msg, kvs)
}

// Info logs at info level.
func (l *Logger) Info(msg string, kvs ...interface{}) {
	l.log(LevelInfo, msg, kvs)
}

// Warn logs at warn level.
func (l *Logger) Warn(msg string, kvs ...interface{}) {
	l.log(LevelWarn, msg, kvs)
}

// Error logs at error level.
func (l *Logger) Error(msg string, kvs ...interface{}) {
	l.log(LevelError, msg, kvs)
}

// DebugObject logs a Winston-style object message. In TraceClue schema mode it
// reproduces the deployed object-message mapping; use the string methods for
// normal Go logging.
func (l *Logger) DebugObject(message map[string]interface{}) {
	l.logObject(LevelDebug, message)
}

// InfoObject logs a Winston-style object message.
func (l *Logger) InfoObject(message map[string]interface{}) {
	l.logObject(LevelInfo, message)
}

// WarnObject logs a Winston-style object message.
func (l *Logger) WarnObject(message map[string]interface{}) {
	l.logObject(LevelWarn, message)
}

// ErrorObject logs a Winston-style object message.
func (l *Logger) ErrorObject(message map[string]interface{}) {
	l.logObject(LevelError, message)
}

// Fatal logs at fatal level and then calls os.Exit(1).
func (l *Logger) Fatal(msg string, kvs ...interface{}) {
	l.log(LevelFatal, msg, kvs)
	os.Exit(1)
}

// ---------- Internal ----------

// ANSI colour codes used only in non-production (development) mode.
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
)

var levelColor = map[Level]string{
	LevelDebug: colorBlue,
	LevelInfo:  colorGreen,
	LevelWarn:  colorYellow,
	LevelError: colorRed,
	LevelFatal: colorPurple,
}

func (l *Logger) log(lvl Level, msg string, kvs []interface{}) {
	l.mu.Lock()
	threshold := l.level
	l.mu.Unlock()
	if lvl < threshold {
		return
	}

	extra := make(map[string]interface{}, len(l.fields)+len(kvs)/2)
	for k, v := range l.fields {
		extra[k] = v
	}
	// Parse alternating key-value pairs. Mismatched trailing values get a
	// synthetic key to avoid silent data loss.
	for i := 0; i < len(kvs); i += 2 {
		key, ok := kvs[i].(string)
		if !ok {
			key = fmt.Sprintf("!BADKEY(%v)", kvs[i])
		}
		if i+1 < len(kvs) {
			extra[key] = formatValue(kvs[i+1])
		} else {
			extra[key] = "!MISSING_VALUE"
		}
	}
	l.writeLog(lvl, threshold, msg, extra, msg, extra)
}

func (l *Logger) logObject(lvl Level, message map[string]interface{}) {
	l.mu.Lock()
	threshold := l.level
	l.mu.Unlock()
	if lvl < threshold {
		return
	}

	body, mappedAttributes := traceClueObjectMessage(message)
	traceAttributes := make(map[string]interface{}, len(l.fields)+len(mappedAttributes))
	for key, value := range l.fields {
		traceAttributes[key] = formatValue(value)
	}
	for key, value := range mappedAttributes {
		traceAttributes[key] = formatValue(value)
	}

	platformMessage := ""
	if text, ok := body.(string); ok {
		platformMessage = text
	}
	platformExtra := make(map[string]interface{}, len(l.fields)+len(message))
	for key, value := range l.fields {
		platformExtra[key] = formatValue(value)
	}
	for key, value := range message {
		if key != "message" {
			platformExtra[key] = formatValue(value)
		}
	}
	l.writeLog(lvl, threshold, platformMessage, platformExtra, body, traceAttributes)
}

// traceClueObjectMessage mirrors winston-opentelemetry-format 0.0.4. Winston
// supplies the object's message property as the formatter body. If it is absent,
// the whole object becomes the body; the formatter then moves non-array objects
// into attributes and emits an empty body. Other top-level properties are not
// retained when a message property exists.
func traceClueObjectMessage(message map[string]interface{}) (interface{}, map[string]interface{}) {
	body, hasMessage := message["message"]
	if !hasMessage {
		body = message
	}
	if object, ok := body.(map[string]interface{}); ok {
		attributes := make(map[string]interface{}, len(object))
		for key, value := range object {
			attributes[key] = value
		}
		return "", attributes
	}
	if err, ok := body.(error); ok {
		// JavaScript Error properties are non-enumerable, so Object.assign emits
		// no attributes. Do not introduce the raw error text in compatibility mode.
		_ = err
		return "", map[string]interface{}{}
	}
	return body, map[string]interface{}{}
}

// writeLog builds the selected envelope and writes it atomically.
func (l *Logger) writeLog(
	lvl Level,
	threshold Level,
	msg string,
	platformExtra map[string]interface{},
	traceBody interface{},
	traceAttributes map[string]interface{},
) {
	now := time.Now().In(l.loc)
	e := entry{
		Level:     lvl.String(),
		Message:   msg,
		Timestamp: now.Format(time.RFC3339Nano),
		Service:   l.service,
		TraceID:   l.traceID,
		SpanID:    l.spanID,
	}
	traceFlags := l.traceFlags

	// Implicit trace propagation uses the goroutine-local active context. A native
	// active span overrides older bound compatibility IDs; fit-go-only IDs are a
	// fallback when no native span is present. The runtime.Stack lookup remains
	// gated off when tracing is disabled.
	if implicitTraceEnabled.Load() {
		if gctx := goroutinectx.Active(); gctx != nil {
			if sc := oteltrace.SpanContextFromContext(gctx); sc.IsValid() {
				e.TraceID = sc.TraceID().String()
				e.SpanID = sc.SpanID().String()
				traceFlags = byte(sc.TraceFlags())
			} else if e.TraceID == "" {
				if v, ok := gctx.Value(ctxKeyTraceID).(string); ok && v != "" {
					e.TraceID = v
				}
				if v, ok := gctx.Value(ctxKeySpanID).(string); ok && v != "" {
					e.SpanID = v
				}
			}
		}
	}

	if len(platformExtra) > 0 {
		e.Extra = platformExtra
	}

	// Add caller info for error and fatal levels to aid debugging.
	if lvl >= LevelError {
		if _, file, line, ok := runtime.Caller(3); ok {
			e.Caller = fmt.Sprintf("%s:%d", file, line)
		}
	}

	var record interface{} = e
	if l.schema == SchemaTraceClue {
		body, attributes := l.traceClueBodyAndAttributes(traceBody, traceAttributes, threshold)
		record = traceClueEntry{
			Body:           body,
			SeverityNumber: traceClueSeverityNumber(lvl),
			SeverityText:   lvl.String(),
			Attributes:     attributes,
			Timestamp:      traceClueTimestamp(now),
			TraceID:        e.TraceID,
			SpanID:         e.SpanID,
			TraceFlags:     traceFlags,
			Resource:       l.resource,
		}
	}

	buf, err := json.Marshal(record)
	if err != nil {
		// Fallback: write a plain-text line if JSON marshalling fails.
		buf = []byte(fmt.Sprintf(`{"level":"%s","message":"%s","error":"json marshal failed"}`, lvl, msg))
	}

	if l.colorize {
		color := levelColor[lvl]
		buf = []byte(color + string(buf) + colorReset)
	}

	buf = append(buf, '\n')

	l.mu.Lock()
	_, _ = l.out.Write(buf)
	l.mu.Unlock()
}

func (l *Logger) traceClueBodyAndAttributes(body interface{}, raw map[string]interface{}, configuredLevel Level) (interface{}, map[string]interface{}) {
	attributes := make(map[string]interface{}, len(raw)+2)
	for key, value := range raw {
		attributes[key] = value
	}
	truncate := l.traceClueBodyTruncation == TraceClueTruncateAlways ||
		(l.traceClueBodyTruncation == TraceClueTruncateDebugOnly && configuredLevel == LevelDebug) ||
		(l.traceClueBodyTruncation == TraceClueTruncateNonDebug && configuredLevel != LevelDebug)
	if truncate {
		switch value := body.(type) {
		case string:
			if shortened, originalLength, changed := truncateCharacters(value, l.traceClueBodyLimit); changed {
				body = shortened
				attributes["_body_original_length"] = originalLength
				attributes["_body_too_large"] = true
			}
		default:
			sequence := reflect.ValueOf(value)
			if sequence.IsValid() && (sequence.Kind() == reflect.Slice || sequence.Kind() == reflect.Array) && sequence.Len() > l.traceClueBodyLimit {
				length := sequence.Len()
				if sequence.Kind() == reflect.Slice {
					body = sequence.Slice(0, l.traceClueBodyLimit).Interface()
				} else {
					shortened := reflect.MakeSlice(reflect.SliceOf(sequence.Type().Elem()), l.traceClueBodyLimit, l.traceClueBodyLimit)
					for index := 0; index < l.traceClueBodyLimit; index++ {
						shortened.Index(index).Set(sequence.Index(index))
					}
					body = shortened.Interface()
				}
				attributes["_body_original_length"] = length
				attributes["_body_too_large"] = true
			}
		}
	}
	return body, l.traceClueAttributes(attributes)
}

func (l *Logger) traceClueAttributes(raw map[string]interface{}) map[string]interface{} {
	attributes := make(map[string]interface{}, len(raw))
	structured := len(l.traceClueRestrict) > 0
	meta := make(map[string]interface{})
	for key, value := range raw {
		if _, discard := l.traceClueDiscard[key]; discard {
			continue
		}
		_, restricted := l.traceClueRestrict[key]
		if !structured || restricted || traceClueSizeAttribute(key) {
			attributes[key] = value
		} else {
			meta[key] = value
		}
	}
	if len(meta) == 0 {
		return attributes
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return attributes
	}
	metaText := string(encoded)
	if shortened, originalLength, changed := truncateCharacters(metaText, l.traceClueMetaLimit); changed {
		metaText = shortened
		attributes["_meta_original_length"] = originalLength
		attributes["_meta_too_large"] = true
	}
	attributes["_meta"] = metaText
	return attributes
}

func traceClueSizeAttribute(key string) bool {
	switch key {
	case "_body_too_large", "_body_original_length", "_meta_too_large", "_meta_original_length":
		return true
	default:
		return false
	}
}

func truncateCharacters(value string, limit int) (string, int, bool) {
	// JavaScript String.length and slice, which TraceClue uses, operate on
	// UTF-16 code units rather than Unicode code points. Decode converts a
	// boundary-split surrogate to RuneError, the valid-Unicode representation
	// downstream Go JSON decoders observe for that legacy edge case.
	characters := utf16.Encode([]rune(value))
	if limit <= 0 || len(characters) <= limit {
		return value, len(characters), false
	}
	return string(utf16.Decode(characters[:limit])), len(characters), true
}

func traceClueSeverityNumber(level Level) *int {
	// Deployed TraceClue maps "warning" rather than Winston's "warn", and has
	// no "fatal" entry. JSON.stringify therefore omits severity_number for those
	// two levels; preserve that exact wire behavior in compatibility mode.
	var severity int
	switch level {
	case LevelDebug:
		severity = 5
	case LevelInfo:
		severity = 9
	case LevelError:
		severity = 17
	default:
		return nil
	}
	return &severity
}

func traceClueTimestamp(now time.Time) string {
	// TraceClue shifts the instant into LOG_TIMEZONE and serializes that wall time
	// with a trailing Z. Reproduce it only in compatibility mode.
	return now.Format("2006-01-02T15:04:05.000Z")
}

// formatValue converts a value to a JSON-friendly representation.
// error types are converted to their string form so they serialize properly.
func formatValue(v interface{}) interface{} {
	switch val := v.(type) {
	case error:
		return redact.ErrorMessage(val)
	case fmt.Stringer:
		return val.String()
	default:
		return v
	}
}
