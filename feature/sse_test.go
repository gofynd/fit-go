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

package feature

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFeatureSSELineFramingAcrossReadBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, wire string
		want       []string
	}{
		{"mixed", "\xef\xbb\xbfevent: features\r\ndata: [\rdata: {}\ndata: ]\r\n\r\n", []string{"event: features", "data: [", "data: {}", "data: ]", ""}},
		{"CR blanks", "first\r\rsecond\r", []string{"first", "", "second"}},
		{"LF blanks", "first\n\nsecond\n", []string{"first", "", "second"}},
		{"one leading BOM", "\xef\xbb\xbf\xef\xbb\xbffirst\n\xef\xbb\xbfsecond\n", []string{"\xef\xbb\xbffirst", "\xef\xbb\xbfsecond"}},
		{"BOM only", "\xef\xbb\xbf", nil},
		{"partial BOM EOF", "\xef\xbb", []string{"\xef\xbb"}},
		{"unterminated line", "last", []string{"last"}},
		{"empty", "", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Force every possible boundary, including inside the BOM and CRLF.
			for boundary := 0; boundary <= len(test.wire); boundary++ {
				reader := io.MultiReader(strings.NewReader(test.wire[:boundary]), strings.NewReader(test.wire[boundary:]))
				scanner := newFeatureSSEScanner(reader)
				var lines []string
				for scanner.Scan() {
					lines = append(lines, scanner.Text())
				}
				if err := scanner.Err(); err != nil || !reflect.DeepEqual(lines, test.want) {
					t.Fatalf("boundary %d lines = %#v, %v; want %#v", boundary, lines, err, test.want)
				}
			}
		})
	}
}

func TestFeatureSSEScannerPreservesLineLimit(t *testing.T) {
	for _, delimiter := range []string{"\n", "\r", "\r\n"} {
		scanner := newFeatureSSEScanner(strings.NewReader(strings.Repeat("x", maxSSEEventSize-1) + delimiter))
		if !scanner.Scan() || len(scanner.Bytes()) != maxSSEEventSize-1 || scanner.Err() != nil {
			t.Fatalf("maximum bounded line with %q rejected: %v", delimiter, scanner.Err())
		}
		scanner = newFeatureSSEScanner(strings.NewReader(strings.Repeat("x", maxSSEEventSize+1) + delimiter))
		if scanner.Scan() || scanner.Err() == nil {
			t.Fatalf("oversized line with %q accepted", delimiter)
		}
	}
}

func TestFeatureSSEFramingReadinessAndIsolatedSnapshot(t *testing.T) {
	for _, test := range []struct{ name, prefix, separator string }{
		{"LF", "", "\n"},
		{"CRLF", "", "\r\n"},
		{"CR", "", "\r"},
		{"BOM LF", "\xef\xbb\xbf", "\n"},
		{"BOM CRLF", "\xef\xbb\xbf", "\r\n"},
		{"BOM CR", "\xef\xbb\xbf", "\r"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			snapshotClosed := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				snapshot := strings.Contains(r.Header.Get("x-featurehub"), "segment=snapshot")
				w.Header().Set("Content-Type", "text/event-stream")
				wire := test.prefix + strings.Join([]string{
					": comment", "id: ignored", "retry: 100", "event: features", "data: [",
					fmt.Sprintf(`data: {"key":"flag","type":"BOOLEAN","value":%t}`, snapshot), "data: ]", "", "",
				}, test.separator)
				// Flush each byte, so BOM and CRLF can arrive in partial chunks.
				for index := range wire {
					if _, err := io.WriteString(w, wire[index:index+1]); err != nil {
						return
					}
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
				if snapshot {
					snapshotClosed <- struct{}{}
				}
			}))
			defer server.Close()
			client, err := InitWithOptions(Options{Enabled: true, URL: server.URL, APIKey: "synthetic-test-key", RequireInitialState: true, InitTimeout: time.Second})
			if err != nil {
				t.Fatalf("stream readiness: %v", err)
			}
			defer client.Stop()
			if !client.Ready() || client.IsEnabled("flag") {
				t.Fatal("parent stream state not published")
			}
			evaluation := client.NewContext().Attribute("segment", "snapshot")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := evaluation.Build(ctx); err != nil {
				t.Fatalf("request snapshot: %v", err)
			}
			if !evaluation.IsEnabled("flag") || client.IsEnabled("flag") || !client.Ready() {
				t.Fatal("snapshot did not remain isolated from the ready parent stream")
			}
			select {
			case <-snapshotClosed:
			case <-ctx.Done():
				t.Fatal("completed snapshot did not close its request")
			}
			if requests.Load() != 2 {
				t.Fatalf("requests = %d; want one stream and one owned snapshot", requests.Load())
			}
		})
	}
}
