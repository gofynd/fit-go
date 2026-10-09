// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// The fixture is checked against MatcherRegistry in the installed FeatureHub
// JavaScript SDK 1.4.0, not derived from the Go matcher under test. Exercise the
// rollout boundary so failed conditions must retain the feature's default.
func TestFeatureDateOperatorFixtureMatchesPinnedJavaScriptSDK(t *testing.T) {
	data, err := os.ReadFile("testdata/date_strategy_fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SDKVersion string `json:"sdkVersion"`
		Cases      []struct {
			Name        string
			Type        string
			Conditional string
			Supplied    string
			Values      []interface{}
			Matched     bool
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SDKVersion != "1.4.0" || len(fixture.Cases) == 0 {
		t.Fatal("missing pinned SDK date fixture")
	}
	for _, test := range fixture.Cases {
		t.Run(test.Name, func(t *testing.T) {
			flag := &featureState{ID: "date-id", Key: "date-rollout", Type: featureTypeBoolean, Value: false,
				Strategies: []rolloutStrategy{{Value: true, Attributes: []strategyAttribute{{
					Conditional: test.Conditional, FieldName: "date", Type: test.Type, Values: test.Values,
				}}}},
			}
			value, matched := applyStrategies(flag, map[string][]string{"date": {test.Supplied}}, time.Now())
			if matched != test.Matched || (matched && value != true) || (!matched && value != nil) {
				t.Fatalf("rollout = %v, matched %v; SDK expects %v", value, matched, test.Matched)
			}
			client := &Client{clientEvaluated: true, features: map[string]*featureState{flag.Key: flag}}
			if got := client.NewContext().Attribute("date", test.Supplied).IsEnabled(flag.Key); got != test.Matched {
				t.Fatalf("public context flag = %v; SDK expects %v", got, test.Matched)
			}
		})
	}
}

func TestFeatureDateStrategiesUseUTCDate(t *testing.T) {
	for _, test := range []struct {
		name, supplied, date string
	}{
		{"positive offset crosses previous day", "2026-10-09T00:30:00+05:30", "2026-10-08"},
		{"negative offset crosses next day", "2026-10-08T23:30:00-05:00", "2026-10-09"},
		{"UTC timestamp", "2026-10-09T00:30:00Z", "2026-10-09"},
		{"date only", "2026-10-09", "2026-10-09"},
	} {
		t.Run(test.name, func(t *testing.T) {
			feature := &featureState{ID: "date-id", Key: "date-rollout", Type: featureTypeBoolean, Value: false,
				Strategies: []rolloutStrategy{{Value: true, Attributes: []strategyAttribute{{
					Conditional: "EQUALS", FieldName: "date", Type: "DATE", Values: []interface{}{test.date},
				}}}},
			}
			value, matched := applyStrategies(feature, map[string][]string{"date": {test.supplied}}, time.Now())
			if !matched || value != true {
				t.Fatalf("DATE rollout = %v, matched %v; want UTC date %s", value, matched, test.date)
			}
		})
	}
}

func TestFeatureSSEEventAccumulatorBoundaries(t *testing.T) {
	var event featureSSEEvent
	for _, line := range []string{"event: features", ": heartbeat", "data: [", "data", "data: ]"} {
		if err := event.appendLine(line); err != nil {
			t.Fatal(err)
		}
	}
	if event.name != "features" || event.data.String() != "[\n\n]" {
		t.Fatalf("event = %q, %q", event.name, event.data.String())
	}
	if event.bytes != len("event: features\ndata: [\ndata\ndata: ]\n") {
		t.Fatalf("event byte count = %d", event.bytes)
	}
	event.reset()
	if !event.empty() || event.bytes != 0 || event.data.Len() != 0 {
		t.Fatal("event state or buffer was retained after reset")
	}
	// Bound empty data fields, overwritten metadata, and unrecognized fields,
	// without constructing millions of lines just to reach the fixed ceiling.
	for _, line := range []string{"data:", "event: ack", "ignored: value", "id", "data: private-value"} {
		event.reset()
		event.bytes = maxSSEEventSize - len(line) - 1
		if err := event.appendLine(line); err != nil || event.bytes != maxSSEEventSize {
			t.Fatalf("exact limit for %q = %d, %v", line, event.bytes, err)
		}
		if err := event.appendLine(line); !errors.Is(err, errFeatureSSEEventTooLarge) {
			t.Fatalf("over limit for %q = %v", line, err)
		}
		if err := event.appendLine(": heartbeat"); err != nil {
			t.Fatalf("comment accumulated event state: %v", err)
		}
	}
}

func TestFeatureSSEAggregateLimitBothParsers(t *testing.T) {
	padding := strings.Repeat(" ", 1<<20)
	var wire strings.Builder
	wire.WriteString("event: features\ndata: [\n")
	for range 17 {
		wire.WriteString("data: ")
		wire.WriteString(padding)
		wire.WriteByte('\n')
	}
	wire.WriteString("data: {\"key\":\"flag\",\"type\":\"BOOLEAN\",\"value\":true}]\n")
	oversized := wire.String()
	for _, ending := range []string{"", "\n"} {
		t.Run("stream/ending="+ending, func(t *testing.T) {
			client := newEventBoundaryClient(oversized+ending, io.EOF)
			permanent, err := client.consumeStream(context.Background(), 1)
			if permanent || !errors.Is(err, errFeatureSSEEventTooLarge) {
				t.Fatalf("oversized stream = permanent %v, %v", permanent, err)
			}
			if client.Ready() || len(client.features) != 0 || client.featureEvents.Load() != 0 {
				t.Fatal("oversized event published feature state")
			}
		})
		t.Run("snapshot/ending="+ending, func(t *testing.T) {
			client := newEventBoundaryClient(oversized+ending, io.EOF)
			features, permanent, err := client.consumeEvaluationSnapshot(context.Background(), nil)
			if permanent || features != nil || !errors.Is(err, errFeatureSSEEventTooLarge) {
				t.Fatalf("oversized snapshot = %v, permanent %v, %v", features, permanent, err)
			}
		})
	}
}

func TestFeatureSSELimitResetsAtBlankLine(t *testing.T) {
	padding := strings.Repeat(" ", 256<<10)
	ack := "event: ack\ndata: " + padding + "\n\n"
	// The complete stream exceeds the event cap, but no individual event does.
	wire := strings.Repeat(ack, 65) + "event: features\ndata: [\ndata: {\"key\":\"flag\",\"type\":\"BOOLEAN\",\"value\":true}\ndata: ]\n\n"
	for _, snapshot := range []bool{false, true} {
		client := newEventBoundaryClient(wire, io.EOF)
		if snapshot {
			features, _, err := client.consumeEvaluationSnapshot(context.Background(), nil)
			if err != nil || features["flag"] == nil || features["flag"].Value != true {
				t.Fatalf("bounded snapshot after ignored events = %v, %v", features, err)
			}
		} else {
			if _, err := client.consumeStream(context.Background(), 1); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if !client.IsEnabled("flag") {
				t.Fatal("bounded feature event was not applied after prior events")
			}
		}
	}
}
