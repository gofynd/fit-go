package errors

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	sentrylib "github.com/getsentry/sentry-go"
	"github.com/gofynd/fit-go/redact"
)

func TestSentrySDKPreservesGoCredentialFieldNames(t *testing.T) {
	previous := sentrylib.CurrentHub().Client()
	defer sentrylib.CurrentHub().BindClient(previous)
	transport := &mockTransport{}
	reporter := newTestSentry()
	if err := reporter.InitWithConfig(SentryConfig{DSN: "https://public@example.com/1", Transport: transport, SampleRate: 1}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sentrylib.CurrentHub().Client().Close)
	credentials := struct {
		Pwd     string `json:"value"`
		CVV     int    `json:"verification"`
		State   string `json:"state"`
		Omitted string `json:"-"`
	}{"structured-alias-sensitive-marker", 123, "safe", "omitted-secret-marker"}
	event := sentrylib.NewEvent()
	event.Extra = map[string]any{"metadata": credentials}
	event.Exception = []sentrylib.Exception{{Stacktrace: &sentrylib.Stacktrace{Frames: []sentrylib.Frame{{Vars: event.Extra}}}}}
	if sentrylib.NewHub(sentrylib.CurrentHub().Client(), sentrylib.NewScope()).CaptureEvent(event) == nil {
		t.Fatal("no export")
	}
	exported := transport.Events()[0]
	for _, surface := range []map[string]any{exported.Extra, exported.Exception[0].Stacktrace.Frames[0].Vars} {
		values := surface["metadata"].(map[string]any)
		if values["value"] != redact.Mask || values["verification"] != redact.Mask || values["state"] != "safe" {
			t.Fatalf("Go-name credentials escaped SDK export: %#v", values)
		}
		if _, exists := values["Omitted"]; exists {
			t.Fatal("json:- field was exported")
		}
	}
	encoded, err := json.Marshal(exported)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Extra map[string]map[string]any `json:"extra"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if wire.Extra["metadata"]["value"] != redact.Mask || wire.Extra["metadata"]["verification"] != redact.Mask {
		t.Fatal("serialized SDK caches retained structured secrets")
	}
}

func TestSentrySharedAcyclicReferencesAndGenuineCycles(t *testing.T) {
	shared := map[string]any{"offset": 42, "state": "safe"}
	got := sanitizeSentryValue("metadata", []any{shared, shared}).([]any)
	if !reflect.DeepEqual(got[0], got[1]) || got[0] == redact.Mask {
		t.Fatalf("shared acyclic diagnostics lost: %#v", got)
	}
	values := make([]any, 3)
	values[0] = "safe"
	values[1] = values[:1]
	values[2] = values[:0]
	if !reflect.DeepEqual(sanitizeSentryValue("metadata", values), []any{"safe", []any{"safe"}, []any{}}) {
		t.Fatal("overlapping acyclic subslices mistaken for cycles")
	}
	cycle := map[string]any{}
	cycle["self"] = cycle
	if sanitizeSentryValue("metadata", cycle).(map[string]any)["self"] != redact.Mask {
		t.Fatal("map cycle was not stopped")
	}
	slice := make([]any, 1)
	slice[0] = slice
	if sanitizeSentryValue("metadata", slice).([]any)[0] != redact.Mask {
		t.Fatal("slice cycle was not stopped")
	}
	var pointer any
	pointer = &pointer
	if sanitizeSentryValue("metadata", pointer) != redact.Mask {
		t.Fatal("pointer cycle was not stopped")
	}
	type node struct{ Next *node }
	root := &node{}
	root.Next = root
	if _, err := json.Marshal(sanitizeSentryValue("metadata", root)); err != nil {
		t.Fatal("struct pointer cycle survives", err)
	}
}

func sharedSentryOwnershipFixture() *sentrylib.Event {
	values := map[string]any{"pwd": "ownership-sensitive-marker", "offset": 42, "safe": "stable"}
	stack := &sentrylib.Stacktrace{Frames: []sentrylib.Frame{{Function: "stable", Vars: values, PreContext: []string{"password=ownership-sensitive-marker"}, PostContext: []string{"stable"}, Lineno: 42, InApp: true}}}
	event := sentrylib.NewEvent()
	event.Contexts["operations"] = sentrylib.Context(values)
	event.Extra = values
	event.Tags = map[string]string{"pwd": "ownership-sensitive-marker", "state": "stable"}
	event.Fingerprint = []string{"password=ownership-sensitive-marker", "stable"}
	event.Breadcrumbs = []*sentrylib.Breadcrumb{{Message: "password=ownership-sensitive-marker", Category: "stable", Data: values}, nil}
	event.Exception = []sentrylib.Exception{{Type: "stable", Value: "password=ownership-sensitive-marker", Stacktrace: stack, Mechanism: &sentrylib.Mechanism{Type: "stable", Data: values}}}
	event.Threads = []sentrylib.Thread{{Name: "stable", Stacktrace: stack}}
	event.Request = &sentrylib.Request{URL: "http://example.com/x", Data: "ownership-sensitive-marker", Headers: map[string]string{"pwd": "ownership-sensitive-marker", "Content-Type": "application/json"}, Env: map[string]string{"pwd": "ownership-sensitive-marker", "MODE": "worker"}}
	event.Spans = []*sentrylib.Span{{
		TraceID: sentrylib.TraceID{1, 2, 3}, SpanID: sentrylib.SpanID{4, 5, 6}, ParentSpanID: sentrylib.SpanID{7, 8, 9},
		Name: "stable", Op: "task", Description: "password=ownership-sensitive-marker", Status: sentrylib.SpanStatusOK,
		StartTime: time.Unix(1, 0), EndTime: time.Unix(2, 0), Tags: event.Tags, Data: values, Extra: values,
		Sampled: sentrylib.SampledTrue, Source: sentrylib.SourceCustom, Origin: sentrylib.SpanOriginManual,
	}, nil}
	return event
}

func assertSentryOwnershipFixtureSafe(t *testing.T, event *sentrylib.Event) {
	t.Helper()
	for _, surface := range []map[string]any{event.Extra, event.Contexts["operations"], event.Breadcrumbs[0].Data, event.Exception[0].Mechanism.Data, event.Exception[0].Stacktrace.Frames[0].Vars, event.Threads[0].Stacktrace.Frames[0].Vars, event.Spans[0].Data, event.Spans[0].Extra} {
		if surface["pwd"] != redact.Mask || surface["safe"] != "stable" {
			t.Fatalf("secret leaked or safe metadata changed: %#v", surface)
		}
	}
	if event.Tags["pwd"] != redact.Mask || event.Spans[0].Tags["pwd"] != redact.Mask || event.Request.Headers["pwd"] != redact.Mask || event.Request.Env["pwd"] != redact.Mask {
		t.Fatal("structured string credential leaked")
	}
	if event.Request.Headers["Content-Type"] != "application/json" || event.Request.Env["MODE"] != "worker" || event.Exception[0].Stacktrace.Frames[0].Lineno != 42 || !event.Exception[0].Stacktrace.Frames[0].InApp {
		t.Fatal("safe metadata changed")
	}
	span := event.Spans[0]
	if span.TraceID != (sentrylib.TraceID{1, 2, 3}) || span.SpanID != (sentrylib.SpanID{4, 5, 6}) || span.ParentSpanID != (sentrylib.SpanID{7, 8, 9}) || span.Op != "task" || span.Status != sentrylib.SpanStatusOK || span.StartTime != time.Unix(1, 0) || span.EndTime != time.Unix(2, 0) || span.Sampled != sentrylib.SampledTrue || span.Source != sentrylib.SourceCustom || span.Origin != sentrylib.SpanOriginManual {
		t.Fatal("native SDK span metadata changed")
	}
}

func TestSentrySanitizerPreservesAllCallerOwnedSurfaces(t *testing.T) {
	original := sharedSentryOwnershipFixture()
	want := sharedSentryOwnershipFixture()
	sanitized := sanitizeSentryEvent(original)
	assertSentryOwnershipFixtureSafe(t, sanitized)
	if !reflect.DeepEqual(original, want) {
		// NewEvent gives a variable timestamp; compare mutable diagnostic fields.
		want.Timestamp = original.Timestamp
		if !reflect.DeepEqual(original, want) {
			t.Fatal("caller-owned event surfaces changed")
		}
	}
	if sanitized == original || sanitized.Spans[0] == original.Spans[0] || sanitized.Request == original.Request || sanitized.Exception[0].Stacktrace == original.Exception[0].Stacktrace {
		t.Fatal("modified event pointers remain caller-owned")
	}
}

func TestSentrySDKConcurrentSharedDiagnosticSurfaces(t *testing.T) {
	previous := sentrylib.CurrentHub().Client()
	defer sentrylib.CurrentHub().BindClient(previous)
	transport := &mockTransport{}
	reporter := newTestSentry()
	shared := sharedSentryOwnershipFixture()
	want := sharedSentryOwnershipFixture()
	want.Timestamp = shared.Timestamp
	if err := reporter.InitWithAdvancedConfig(SentryAdvancedConfig{
		SentryConfig: SentryConfig{DSN: "https://public@example.com/1", Transport: transport, SampleRate: 1},
		BeforeSend: func(event *sentrylib.Event, _ *sentrylib.EventHint) *sentrylib.Event {
			// The SDK's own contextify integration mutates supplied frames
			// before BeforeSend. Introduce shared diagnostic frame objects
			// after SDK preparation to test FIT's mandatory second pass,
			// without claiming ownership of upstream processor behavior.
			event.Extra, event.Tags, event.Request = shared.Extra, shared.Tags, shared.Request
			event.Fingerprint, event.Exception, event.Threads, event.Spans = shared.Fingerprint, shared.Exception, shared.Threads, shared.Spans
			event.Breadcrumbs = shared.Breadcrumbs
			return event
		},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sentrylib.CurrentHub().Client().Close)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			hub := sentrylib.NewHub(sentrylib.CurrentHub().Client(), sentrylib.NewScope())
			for range 10 {
				// Each SDK event has an independent root container; application
				// diagnostic values and objects may legitimately be reused.
				event := sentrylib.NewEvent()
				event.Contexts["operations"] = shared.Contexts["operations"]
				if hub.CaptureEvent(event) == nil {
					t.Error("no export")
				}
			}
		})
	}
	workers.Wait()
	if !reflect.DeepEqual(shared, want) {
		t.Fatal("SDK export mutated shared diagnostic inputs")
	}
	if len(transport.Events()) != 20 {
		t.Fatal("SDK export count differs")
	}
	for _, exported := range transport.Events() {
		assertSentryOwnershipFixtureSafe(t, exported)
	}
}

func TestSentryConcurrentImmutableEventBoundary(t *testing.T) {
	original := sharedSentryOwnershipFixture()
	want := sharedSentryOwnershipFixture()
	want.Timestamp = original.Timestamp
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for range 10 {
				assertSentryOwnershipFixtureSafe(t, sanitizeSentryEvent(original))
			}
		})
	}
	workers.Wait()
	if !reflect.DeepEqual(original, want) {
		t.Fatal("concurrent sanitizer changed immutable event input")
	}
}

func TestSentrySpanSnapshotCoversPublicSDKFields(t *testing.T) {
	exported := map[string]bool{
		"TraceID": true, "SpanID": true, "ParentSpanID": true, "Name": true, "Op": true,
		"Description": true, "Status": true, "Tags": true, "StartTime": true, "EndTime": true,
		"Extra": true, "Data": true, "Sampled": true, "Source": true, "Origin": true,
	}
	original := sharedSentryOwnershipFixture().Spans[0]
	snapshot := cloneSentrySpanSnapshot(original)
	typeOf := reflect.TypeOf(original).Elem()
	for index := range typeOf.NumField() {
		field := typeOf.Field(index)
		if field.PkgPath != "" {
			continue
		}
		if !exported[field.Name] {
			t.Fatalf("new SDK public Span field %s requires projection review", field.Name)
		}
		if !reflect.DeepEqual(reflect.ValueOf(original).Elem().Field(index).Interface(), reflect.ValueOf(snapshot).Elem().Field(index).Interface()) {
			t.Fatalf("SDK public Span field %s was changed", field.Name)
		}
	}
	if snapshot == original || snapshot.GetTransaction() != nil {
		t.Fatal("export snapshot retained a live SDK span lifecycle")
	}
	snapshot.SetTag("state", "changed")
	snapshot.SetData("state", "changed")
	if original.Tags["state"] != "stable" || original.Data["state"] != nil {
		t.Fatal("snapshot maps alias caller-owned span maps")
	}
}

func TestSentryAcyclicDAGHasTotalTraversalBudget(t *testing.T) {
	var graph any = map[string]any{"state": "safe"}
	for range 8 {
		parents := make([]any, maxSentryCollection)
		for index := range parents {
			parents[index] = graph
		}
		graph = parents
	}
	state := newSentryValueTraversal()
	sanitized := sanitizeSentryValueDepth("metadata", graph, state, 0)
	if state.nodes != maxSentryValueNodes || len(state.active) != 0 {
		t.Fatalf("total budget/path cleanup differs: nodes=%d active=%d", state.nodes, len(state.active))
	}
	if _, err := json.Marshal(sanitized); err != nil {
		t.Fatal("bounded DAG is not exportable", err)
	}
	if sanitizeSentryValueDepth("pwd", "secret", state, 0) != redact.Mask {
		t.Fatal("budget exhaustion returned a sensitive value")
	}
}
