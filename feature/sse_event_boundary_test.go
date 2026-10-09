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

package feature

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

// EventSource dispatches only on a blank line, never merely because the
// connection closes. A final line ending completes that line, not its event.
var eventBoundaryFrames = []struct {
	name, separator, suffix string
	complete                bool
}{
	{name: "incomplete-none", separator: "\n"},
	{name: "incomplete-LF", separator: "\n", suffix: "\n"},
	{name: "incomplete-CR", separator: "\r", suffix: "\r"},
	{name: "incomplete-CRLF", separator: "\r\n", suffix: "\r\n"},
	{name: "complete-LF", separator: "\n", suffix: "\n\n", complete: true},
	{name: "complete-CR", separator: "\r", suffix: "\r\r", complete: true},
	{name: "complete-CRLF", separator: "\r\n", suffix: "\r\n\r\n", complete: true},
}

type eventBoundaryErrorReader struct{ err error }

func (r eventBoundaryErrorReader) Read([]byte) (int, error) { return 0, r.err }

func newEventBoundaryClient(wire string, terminalError error) *Client {
	client := &Client{
		url: "http://synthetic-feature-boundary.invalid", apiKey: "synthetic-key",
		ctx: context.Background(), features: make(map[string]*featureState),
		readySignal: make(chan struct{}), terminalFailure: make(chan struct{}),
	}
	client.contextRevision.Store(1)
	client.httpClient = &http.Client{Transport: featureRoundTripFunc(func(*http.Request) (*http.Response, error) {
		body := io.MultiReader(strings.NewReader(wire), eventBoundaryErrorReader{terminalError})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(body)}, nil
	})}
	return client
}

func TestFeatureSSEEventBoundaryStream(t *testing.T) {
	readFailure := errors.New("synthetic SSE read failure")
	for _, ending := range []struct {
		name string
		err  error
	}{
		{"clean-EOF", io.EOF}, {"read-error", readFailure},
		{"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded},
	} {
		for _, event := range []struct {
			name, payload string
		}{
			{"features", `[{"key":"flag","version":2,"type":"BOOLEAN","value":true}]`},
			{"feature", `{"key":"flag","version":2,"type":"BOOLEAN","value":true}`},
			{"delete_feature", `{"key":"flag","version":2}`},
		} {
			for _, frame := range eventBoundaryFrames {
				t.Run(ending.name+"/"+event.name+"/"+frame.name, func(t *testing.T) {
					// Incremental and delete events follow a real completed snapshot.
					prefix := ""
					if event.name != "features" {
						prefix = "event: features\ndata: [{\"key\":\"flag\",\"version\":1,\"type\":\"BOOLEAN\",\"value\":false}]\n\n"
					}
					wire := prefix + "event: " + event.name + frame.separator + "data: " + event.payload + frame.suffix
					client := newEventBoundaryClient(wire, ending.err)
					permanent, err := client.consumeStream(context.Background(), 1)
					if permanent || !errors.Is(err, ending.err) {
						t.Fatalf("stream outcome = permanent %t, %v; want false, %v", permanent, err, ending.err)
					}
					wantEvents := uint64(0)
					var wantValue interface{}
					if prefix != "" {
						wantEvents, wantValue = 1, false
					}
					if frame.complete {
						wantEvents++
						wantValue = true
						if event.name == "delete_feature" {
							wantValue = nil
						}
					}
					wantReady := prefix != "" || frame.complete
					if client.Ready() != wantReady || client.GetValue("flag") != wantValue || client.featureEvents.Load() != wantEvents {
						t.Fatalf("ready=%t value=%v events=%d; want ready=%t value=%v events=%d", client.Ready(), client.GetValue("flag"), client.featureEvents.Load(), wantReady, wantValue, wantEvents)
					}
				})
			}
		}
	}
}

func TestFeatureSSEEventBoundaryRetainsEarlierCompleteEvent(t *testing.T) {
	for _, frame := range eventBoundaryFrames {
		if frame.complete {
			continue
		}
		t.Run(frame.name, func(t *testing.T) {
			prefix := "event: features\ndata: [{\"key\":\"retained\",\"type\":\"BOOLEAN\",\"value\":true}]\n\n"
			wire := prefix + "event: features" + frame.separator + `data: [{"key":"discarded","type":"BOOLEAN","value":true}]` + frame.suffix
			client := newEventBoundaryClient(wire, io.EOF)
			if _, err := client.consumeStream(context.Background(), 1); !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if !client.Ready() || !client.IsEnabled("retained") || client.GetValue("discarded") != nil || client.featureEvents.Load() != 1 {
				t.Fatal("unfinished replacement changed the earlier complete snapshot")
			}
		})
	}
}

func TestFeatureSSEEventBoundarySnapshot(t *testing.T) {
	readFailure := errors.New("synthetic snapshot read failure")
	for _, ending := range []struct {
		name string
		err  error
	}{{"clean-EOF", io.EOF}, {"read-error", readFailure}, {"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded}} {
		for _, frame := range eventBoundaryFrames {
			t.Run(ending.name+"/"+frame.name, func(t *testing.T) {
				wire := "event: features" + frame.separator + `data: [{"key":"flag","type":"BOOLEAN","value":true}]` + frame.suffix
				client := newEventBoundaryClient(wire, ending.err)
				features, permanent, err := client.consumeEvaluationSnapshot(context.Background(), nil)
				if permanent {
					t.Fatal("stream boundary produced a permanent failure")
				}
				if frame.complete {
					if err != nil || features["flag"] == nil || features["flag"].Value != true {
						t.Fatalf("completed snapshot = %v, %v", features, err)
					}
				} else if features != nil || !errors.Is(err, ending.err) {
					t.Fatalf("unfinished snapshot = %v, %v; want nil, %v", features, err, ending.err)
				}
				if client.Ready() || len(client.features) != 0 || client.featureEvents.Load() != 0 {
					t.Fatal("isolated snapshot modified parent streaming state")
				}
			})
		}
	}
}
