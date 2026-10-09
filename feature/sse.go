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
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

var errFeatureSSEEventTooLarge = errors.New("FeatureHub SSE event exceeds the 16 MiB size limit")

// featureSSEEvent bounds the complete event, not just each scanner line. Count
// fields even when they replace earlier metadata or are unrecognized, so a
// malformed event cannot bypass the limit by spreading data across lines. SSE
// comments are not event fields and do not accumulate state.
type featureSSEEvent struct {
	name      string
	data      strings.Builder
	bytes     int
	dataLines int
}

func (e *featureSSEEvent) appendLine(line string) error {
	if strings.HasPrefix(line, ":") {
		return nil
	}
	// Include a normalized line terminator; subtraction avoids integer overflow
	// and also bounds the newline inserted between empty data fields.
	if len(line) >= maxSSEEventSize-e.bytes {
		return errFeatureSSEEventTooLarge
	}
	e.bytes += len(line) + 1
	field, value, found := strings.Cut(line, ":")
	if found {
		value = strings.TrimPrefix(value, " ")
	}
	switch field {
	case "event":
		e.name = value
	case "data":
		if e.dataLines > 0 {
			e.data.WriteByte('\n')
		}
		e.data.WriteString(value)
		e.dataLines++
	}
	return nil
}

func (e *featureSSEEvent) empty() bool { return e.name == "" && e.dataLines == 0 }

// Reset releases the previous event's buffer instead of retaining the largest
// event for the entire lifetime of a streaming client.
func (e *featureSSEEvent) reset() { *e = featureSSEEvent{} }

// SSE permits LF, CRLF and lone CR, and ignores one UTF-8 BOM at stream start.
// Consume CR immediately so a live stream ending its event with CR does not
// need another byte to publish readiness. If its LF arrives in a later read,
// discard that LF without creating an extra empty line.
func newFeatureSSEScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), maxSSEEventSize)
	leading := true
	skipLF := false
	bom := []byte{0xef, 0xbb, 0xbf}
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if len(data) == 0 {
			return 0, nil, nil
		}
		if leading {
			if len(data) < len(bom) && bytes.HasPrefix(bom, data) && !atEOF {
				return 0, nil, nil
			}
			leading = false
			if bytes.HasPrefix(data, bom) {
				advance = len(bom)
				data = data[len(bom):]
			}
		}
		if skipLF && len(data) > 0 {
			skipLF = false
			if data[0] == '\n' {
				advance++
				data = data[1:]
			}
		}
		if len(data) == 0 {
			return advance, nil, nil
		}
		for index, value := range data {
			switch value {
			case '\n':
				return advance + index + 1, data[:index], nil
			case '\r':
				skipLF = true
				return advance + index + 1, data[:index], nil
			}
		}
		if atEOF {
			return advance + len(data), data, nil
		}
		return advance, nil, nil
	})
	return scanner
}
