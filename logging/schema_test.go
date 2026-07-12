// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func decodeLog(t *testing.T, buf *bytes.Buffer) map[string]interface{} {
	t.Helper()
	var record map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("decode log %q: %v", buf.String(), err)
	}
	return record
}

func TestTraceClueSchemaGoldenAndNativeChildCorrelation(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	defer provider.Shutdown(context.Background())

	stale := ContextWithTrace(context.Background(), "00000000000000000000000000000001", "0000000000000001")
	ctx, child := provider.Tracer("native").Start(stale, "child")
	defer child.End()

	var buf bytes.Buffer
	logger, err := New(Options{
		Level:   "debug",
		Env:     "production",
		Service: "communications",
		Schema:  SchemaTraceClue,
		Output:  &buf,
		ResourceAttributes: map[string]interface{}{
			"service.instance.id": "pod-1",
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.WithContext(ctx).Info("sent", "provider", "whatsapp")
	record := decodeLog(t, &buf)

	keys := make([]string, 0, len(record))
	for key := range record {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	wantKeys := []string{"attributes", "body", "resource", "severity_number", "severity_text", "span_id", "timestamp", "trace_flags", "trace_id"}
	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("TraceClue keys = %v, want %v", keys, wantKeys)
	}
	if record["body"] != "sent" || record["severity_text"] != "info" || record["severity_number"] != float64(9) {
		t.Fatalf("body/severity = %v/%v/%v", record["body"], record["severity_text"], record["severity_number"])
	}
	sc := child.SpanContext()
	if record["trace_id"] != sc.TraceID().String() || record["span_id"] != sc.SpanID().String() {
		t.Fatalf("log IDs = %v/%v, want active native child %s/%s", record["trace_id"], record["span_id"], sc.TraceID(), sc.SpanID())
	}
	if record["trace_flags"] != float64(1) {
		t.Fatalf("trace_flags = %v, want sampled flag 1", record["trace_flags"])
	}
	attrs := record["attributes"].(map[string]interface{})
	if attrs["provider"] != "whatsapp" {
		t.Fatalf("attributes.provider = %v", attrs["provider"])
	}
	resource := record["resource"].(map[string]interface{})
	for key, want := range map[string]interface{}{
		"service.name":           "communications",
		"service.instance.id":    "pod-1",
		"deployment.environment": "production",
		"telemetry.sdk.language": "go",
		"telemetry.sdk.name":     "opentelemetry",
		"pathname":               "",
	} {
		if resource[key] != want {
			t.Errorf("resource[%q] = %v, want %v", key, resource[key], want)
		}
	}
	if resource["telemetry.sdk.version"] == "" {
		t.Error("resource missing telemetry.sdk.version")
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", record["timestamp"].(string)); err != nil {
		t.Errorf("timestamp %q is not TraceClue-compatible: %v", record["timestamp"], err)
	}
}

func TestTriggerHappyObjectMessageGolden(t *testing.T) {
	tests := []struct {
		name    string
		level   Level
		message map[string]interface{}
		want    string
	}{
		{
			name:  "Kafka success keeps array message as body",
			level: LevelInfo,
			message: map[string]interface{}{
				"topic": "jobs",
				"message": []interface{}{
					map[string]interface{}{"topicName": "jobs", "partition": 0},
				},
				"info": "Kafka succeeded",
				"_id":  "job-1",
			},
			want: `{"attributes":{},"body":[{"partition":0,"topicName":"jobs"}],"severity_text":"info"}`,
		},
		{
			name:  "Kafka failure without message becomes attributes",
			level: LevelError,
			message: map[string]interface{}{
				"topic": "jobs",
				"info":  "Kafka message produce failed",
				"error": errors.New("password=hunter2"),
				"_id":   "job-2",
			},
			want: `{"attributes":{"_id":"job-2","error":"password=[REDACTED]","info":"Kafka message produce failed","topic":"jobs"},"body":"","severity_text":"error"}`,
		},
		{
			name:  "API request string message drops siblings",
			level: LevelInfo,
			message: map[string]interface{}{
				"message": "Api request succeeded",
				"jobData": map[string]interface{}{"url": "/private"},
				"_id":     "job-3",
				"response": map[string]interface{}{
					"ok": true,
				},
			},
			want: `{"attributes":{},"body":"Api request succeeded","severity_text":"info"}`,
		},
		{
			name:  "object message becomes attributes and drops siblings",
			level: LevelInfo,
			message: map[string]interface{}{
				"message": map[string]interface{}{"topic": "jobs", "info": "nested"},
				"_id":     "dropped",
			},
			want: `{"attributes":{"info":"nested","topic":"jobs"},"body":"","severity_text":"info"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger, err := New(Options{
				Level:                   "debug",
				Env:                     "production",
				Schema:                  SchemaTraceClue,
				TraceClueBodyTruncation: TraceClueTruncateAlways,
				Output:                  &buf,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if tt.level == LevelError {
				logger.ErrorObject(tt.message)
			} else {
				logger.InfoObject(tt.message)
			}
			record := decodeLog(t, &buf)
			golden, err := json.Marshal(map[string]interface{}{
				"body":          record["body"],
				"attributes":    record["attributes"],
				"severity_text": record["severity_text"],
			})
			if err != nil {
				t.Fatalf("marshal golden projection: %v", err)
			}
			if got := string(golden); got != tt.want {
				t.Fatalf("object mapping = %s\nwant           = %s", got, tt.want)
			}
		})
	}
}

func TestTraceClueResourceServiceNamePrecedence(t *testing.T) {
	tests := []struct {
		name     string
		service  string
		otel     string
		resource string
		legacy   string
		want     string
	}{
		{name: "explicit option", service: "explicit", otel: "otel", resource: "resource", legacy: "legacy", want: "explicit"},
		{name: "OTEL_SERVICE_NAME", otel: "otel", resource: "resource", legacy: "legacy", want: "otel"},
		{name: "resource service", resource: "resource", legacy: "legacy", want: "resource"},
		{name: "SERVICE_NAME fallback", legacy: "legacy", want: "legacy"},
		{name: "unknown fallback", want: "unknown_service"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", tt.otel)
			t.Setenv("SERVICE_NAME", tt.legacy)
			attrs := map[string]interface{}{}
			if tt.resource != "" {
				attrs["service.name"] = tt.resource
			}
			resource := traceClueResource(Options{Service: tt.service, ResourceAttributes: attrs})
			if got := resource["service.name"]; got != tt.want {
				t.Fatalf("service.name = %v, want %q", got, tt.want)
			}
		})
	}
}

func TestTraceClueSchemaPreservesLegacyWarnSeverityQuirk(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(Options{Level: "debug", Env: "production", Schema: SchemaTraceClue, Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Warn("warning")
	record := decodeLog(t, &buf)
	if _, ok := record["severity_number"]; ok {
		t.Fatal("severity_number must be omitted for Winston 'warn', matching deployed TraceClue")
	}
	if record["severity_text"] != "warn" {
		t.Fatalf("severity_text = %v, want warn", record["severity_text"])
	}
}

func TestPlatformSchemaRemainsDefault(t *testing.T) {
	t.Setenv("FIT_LOG_SCHEMA", "")
	var buf bytes.Buffer
	logger, err := New(Options{Env: "production", Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("platform")
	record := decodeLog(t, &buf)
	if record["level"] != "info" || record["message"] != "platform" {
		t.Fatalf("default platform record = %v", record)
	}
	if _, ok := record["body"]; ok {
		t.Fatal("default logger unexpectedly switched to TraceClue schema")
	}
}

func TestTraceClueSchemaCanBeSelectedFromEnvironment(t *testing.T) {
	t.Setenv("FIT_LOG_SCHEMA", "traceclue")
	var buf bytes.Buffer
	logger, err := New(Options{Env: "production", Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("env-selected")
	if got := decodeLog(t, &buf)["body"]; got != "env-selected" {
		t.Fatalf("body = %v", got)
	}
}

func TestInvalidLogSchemaFailsInitialization(t *testing.T) {
	if _, err := New(Options{Schema: Schema("unknown")}); err == nil {
		t.Fatal("unsupported log schema did not return an initialization error")
	}
}

func TestTraceClueBodyTruncationProfiles(t *testing.T) {
	longBody := strings.Repeat("x", 501)
	tests := []struct {
		name       string
		level      string
		mode       TraceClueBodyTruncation
		wantLength int
		wantFlag   bool
	}{
		{name: "3.1.3 non-debug keeps body", level: "info", wantLength: 501},
		{name: "3.1.3 debug truncates", level: "debug", wantLength: 500, wantFlag: true},
		{name: "3.0.5 always truncates", level: "info", mode: TraceClueTruncateAlways, wantLength: 500, wantFlag: true},
		{name: "explicitly disabled", level: "debug", mode: TraceClueTruncateNever, wantLength: 501},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger, err := New(Options{
				Level: tt.level, Env: "production", Schema: SchemaTraceClue,
				TraceClueBodyTruncation: tt.mode, Output: &buf,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			logger.Info(longBody)
			record := decodeLog(t, &buf)
			if got := len([]rune(record["body"].(string))); got != tt.wantLength {
				t.Fatalf("body length = %d, want %d", got, tt.wantLength)
			}
			attrs := record["attributes"].(map[string]interface{})
			if got := attrs["_body_too_large"] == true; got != tt.wantFlag {
				t.Fatalf("_body_too_large present = %v, want %v", got, tt.wantFlag)
			}
			if tt.wantFlag && attrs["_body_original_length"] != float64(501) {
				t.Fatalf("_body_original_length = %v", attrs["_body_original_length"])
			}
		})
	}
}

func TestTraceClueTruncationUsesJavaScriptUTF16Units(t *testing.T) {
	value := strings.Repeat("😀", 251)
	shortened, originalLength, changed := truncateCharacters(value, 500)
	if !changed {
		t.Fatal("UTF-16-sized body was not truncated")
	}
	if originalLength != 502 {
		t.Fatalf("original UTF-16 length = %d, want 502", originalLength)
	}
	if got := len(utf16.Encode([]rune(shortened))); got != 500 {
		t.Fatalf("shortened UTF-16 length = %d, want 500", got)
	}
}

func TestLoggerRedactsErrorValues(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(Options{Level: "info", Env: "production", Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Error("failed", "error", errors.New("password=hunter2 user@example.com"))
	output := buf.String()
	for _, forbidden := range []string{"hunter2", "user@example.com"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("log leaked %q: %s", forbidden, output)
		}
	}
}

func TestTraceClueStructuredMetadataLimitsAndDiscard(t *testing.T) {
	var buf bytes.Buffer
	logger, err := New(Options{
		Level: "info", Env: "production", Schema: SchemaTraceClue, Output: &buf,
		TraceClueMetaLimit:             20,
		TraceClueRestrictAttributesTo:  []string{"provider"},
		TraceClueDiscardAttributesFrom: []string{"password"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("sent", "provider", "whatsapp", "details", strings.Repeat("x", 50), "password", "secret")
	attrs := decodeLog(t, &buf)["attributes"].(map[string]interface{})
	if attrs["provider"] != "whatsapp" || attrs["password"] != nil {
		t.Fatalf("structured attributes = %v", attrs)
	}
	if len([]rune(attrs["_meta"].(string))) != 20 || attrs["_meta_too_large"] != true {
		t.Fatalf("limited _meta attributes = %v", attrs)
	}
	if attrs["_meta_original_length"].(float64) <= 20 {
		t.Fatalf("_meta_original_length = %v", attrs["_meta_original_length"])
	}
}
