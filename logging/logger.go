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
	"runtime"
	"strings"
	"sync"
	"time"
)

// Context keys for trace propagation. These match the OpenTelemetry context
// keys that the tracing module injects.
type ctxKey int

const (
	ctxKeyTraceID ctxKey = iota
	ctxKeySpanID
)

// ContextWithTrace returns a context carrying trace_id and span_id.
// The tracing module calls this to propagate IDs into the logger.
func ContextWithTrace(ctx context.Context, traceID, spanID string) context.Context {
	ctx = context.WithValue(ctx, ctxKeyTraceID, traceID)
	ctx = context.WithValue(ctx, ctxKeySpanID, spanID)
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

	// Service is an optional service name included in every log entry.
	Service string

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

// Logger is a structured, thread-safe JSON logger. It is the Go equivalent
// of the Winston logger created tracing/index.ts.
type Logger struct {
	mu       sync.Mutex
	level    Level
	loc      *time.Location
	env      string
	service  string
	out      io.Writer
	colorize bool
	fields   map[string]interface{} // persistent fields added via WithFields
	traceID  string                 // bound trace context (via WithContext)
	spanID   string
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

	return &Logger{
		level:    ParseLevel(opts.Level),
		loc:      loc,
		env:      opts.Env,
		service:  opts.Service,
		out:      w,
		colorize: !isProd,
	}, nil
}

// clone returns a shallow copy of the logger with its own mutex and a
// deep-copied fields map so that derived loggers are independent.
func (l *Logger) clone() *Logger {
	c := &Logger{
		level:    l.level,
		loc:      l.loc,
		env:      l.env,
		service:  l.service,
		out:      l.out,
		colorize: l.colorize,
		traceID:  l.traceID,
		spanID:   l.spanID,
	}
	if len(l.fields) > 0 {
		c.fields = make(map[string]interface{}, len(l.fields))
		for k, v := range l.fields {
			c.fields[k] = v
		}
	}
	return c
}

// WithContext returns a child Logger that extracts trace_id and span_id from
// the context. This mirrors the OpenTelemetry log format enrichment that
// performs via the opentelemetryLogFormat Winston format.
func (l *Logger) WithContext(ctx context.Context) *Logger {
	c := l.clone()
	if v, ok := ctx.Value(ctxKeyTraceID).(string); ok && v != "" {
		c.traceID = v
	}
	if v, ok := ctx.Value(ctxKeySpanID).(string); ok && v != "" {
		c.spanID = v
	}
	return c
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

// log is the core logging routine. It checks the level, builds the JSON
// entry, and writes it atomically to the output.
func (l *Logger) log(lvl Level, msg string, kvs []interface{}) {
	l.mu.Lock()
	threshold := l.level
	l.mu.Unlock()

	if lvl < threshold {
		return
	}

	now := time.Now().In(l.loc)

	e := entry{
		Level:     lvl.String(),
		Message:   msg,
		Timestamp: now.Format(time.RFC3339Nano),
		Service:   l.service,
		TraceID:   l.traceID,
		SpanID:    l.spanID,
	}

	// Merge persistent fields and per-call key-value pairs into Extra.
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
	if len(extra) > 0 {
		e.Extra = extra
	}

	// Add caller info for error and fatal levels to aid debugging.
	if lvl >= LevelError {
		if _, file, line, ok := runtime.Caller(2); ok {
			e.Caller = fmt.Sprintf("%s:%d", file, line)
		}
	}

	buf, err := json.Marshal(e)
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

// formatValue converts a value to a JSON-friendly representation.
// error types are converted to their string form so they serialize properly.
func formatValue(v interface{}) interface{} {
	switch val := v.(type) {
	case error:
		return val.Error()
	case fmt.Stringer:
		return val.String()
	default:
		return v
	}
}
