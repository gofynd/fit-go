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

package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
)

// ---------------------------------------------------------------------------
// General string utilities for the fit.go framework.
// ---------------------------------------------------------------------------

// TruncateString truncates s to maxLen characters, appending suffix if
// truncation occurs. If maxLen is 0 or negative, s is returned unchanged.
func TruncateString(s string, maxLen int, suffix string) string {
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	if len(suffix) >= maxLen {
		return suffix[:maxLen]
	}
	return s[:maxLen-len(suffix)] + suffix
}

// TrimAndLower trims whitespace and converts a string to lowercase.
func TrimAndLower(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// IsEmpty returns true if the string is empty or contains only whitespace.
func IsEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

// Coalesce returns the first non-empty string from the provided arguments.
// If all are empty, it returns the empty string.
func Coalesce(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// MaskString masks all but the first and last visible characters of a string.
// Useful for logging sensitive values (e.g. tokens, keys).
//
//	MaskString("secret-token-123", '*') => "s**************3"
func MaskString(s string, mask rune) string {
	runes := []rune(s)
	if len(runes) <= 2 {
		return strings.Repeat(string(mask), len(runes))
	}
	var b strings.Builder
	b.Grow(len(runes))
	b.WriteRune(runes[0])
	for i := 1; i < len(runes)-1; i++ {
		b.WriteRune(mask)
	}
	b.WriteRune(runes[len(runes)-1])
	return b.String()
}

// SnakeCase converts a camelCase or PascalCase string to snake_case.
//
//	SnakeCase("ServiceName") => "service_name"
//	SnakeCase("getHTTPResponse") => "get_http_response"
func SnakeCase(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 4)

	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := rune(s[i-1])
				if !unicode.IsUpper(prev) || (i+1 < len(s) && unicode.IsLower(rune(s[i+1]))) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// CamelCase converts a snake_case or kebab-case string to camelCase.
//
//	CamelCase("service_name") => "serviceName"
//	CamelCase("get-http-response") => "getHttpResponse"
func CamelCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	if len(parts) == 0 {
		return ""
	}

	var b strings.Builder
	b.Grow(len(s))

	for i, part := range parts {
		if part == "" {
			continue
		}
		if i == 0 {
			b.WriteString(strings.ToLower(part))
		} else {
			runes := []rune(strings.ToLower(part))
			runes[0] = unicode.ToUpper(runes[0])
			b.WriteString(string(runes))
		}
	}
	return b.String()
}

// PascalCase converts a snake_case or kebab-case string to PascalCase.
//
//	PascalCase("service_name") => "ServiceName"
func PascalCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool {
		return r == '_' || r == '-' || r == ' '
	})
	if len(parts) == 0 {
		return ""
	}

	var b strings.Builder
	b.Grow(len(s))

	for _, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(strings.ToLower(part))
		runes[0] = unicode.ToUpper(runes[0])
		b.WriteString(string(runes))
	}
	return b.String()
}

// Slugify converts a string to a URL-friendly slug by lowering case, replacing
// spaces and non-alphanumeric characters with hyphens, and collapsing
// consecutive hyphens.
//
//	Slugify("Hello World! #2024") => "hello-world-2024"
func Slugify(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	prevHyphen := false

	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
			prevHyphen = false
		default:
			if !prevHyphen && b.Len() > 0 {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
	}

	result := b.String()
	return strings.TrimRight(result, "-")
}

// RandomHex generates a random hex string of the given byte length.
// The returned string is 2*n characters long.
func RandomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("utils: failed to generate random bytes: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// ContainsAny returns true if s contains any of the substrings.
func ContainsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// SplitAndTrim splits a string by the separator and trims whitespace from each
// part. Empty parts after trimming are omitted.
func SplitAndTrim(s, sep string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// EllipsisMiddle truncates a string in the middle with an ellipsis if it
// exceeds maxLen. Useful for logging long identifiers.
//
//	EllipsisMiddle("abcdefghijklmnop", 10) => "abcd...nop"
func EllipsisMiddle(s string, maxLen int) string {
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	if maxLen < 5 {
		return s[:maxLen]
	}
	ellipsis := "..."
	half := (maxLen - len(ellipsis)) / 2
	remainder := maxLen - len(ellipsis) - half
	return s[:half] + ellipsis + s[len(s)-remainder:]
}

// PadLeft pads a string on the left with the given padding character until it
// reaches the desired length. If s is already at or above length, it is
// returned unchanged.
func PadLeft(s string, length int, pad rune) string {
	if len(s) >= length {
		return s
	}
	return strings.Repeat(string(pad), length-len(s)) + s
}

// PadRight pads a string on the right with the given padding character until
// it reaches the desired length.
func PadRight(s string, length int, pad rune) string {
	if len(s) >= length {
		return s
	}
	return s + strings.Repeat(string(pad), length-len(s))
}
