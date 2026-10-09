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
	"io"
)

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
