// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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
	"regexp"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Sanitize tests
// ---------------------------------------------------------------------------

func TestSanitizeString(t *testing.T) {
	t.Run("removes invisible characters", func(t *testing.T) {
		input := "hello\u200Bworld" // zero-width space
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "\u200B") {
			t.Error("Should remove zero-width space")
		}
	})

	t.Run("removes control characters", func(t *testing.T) {
		input := "hello\x00\x01\x02world"
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "\x00") || strings.Contains(result, "\x01") {
			t.Error("Should remove control characters")
		}
	})

	t.Run("removes HTML tags", func(t *testing.T) {
		input := "<script>alert('xss')</script>hello"
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "<script>") {
			t.Error("Should remove script tags")
		}
	})

	t.Run("removes SQL injection patterns", func(t *testing.T) {
		input := "test'; DROP TABLE users;--"
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "--") {
			t.Error("Should remove SQL injection patterns (-- comments)")
		}
	})

	t.Run("removes NoSQL injection patterns", func(t *testing.T) {
		input := `{"$ne": null, "$gt": 0}`
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "$ne") || strings.Contains(result, "$gt") {
			t.Error("Should remove NoSQL injection patterns")
		}
	})

	t.Run("removes command injection patterns", func(t *testing.T) {
		input := "; rm -rf /"
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "; rm") {
			t.Error("Should remove command injection patterns")
		}
	})

	t.Run("removes path traversal patterns", func(t *testing.T) {
		input := "../../../etc/passwd"
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, "../") {
			t.Error("Should remove path traversal patterns")
		}
	})

	t.Run("escapes HTML characters", func(t *testing.T) {
		input := "test & test"
		result := SanitizeString(input, SanitizeOptions{})
		if !strings.Contains(result, "&amp;") {
			t.Error("Should escape & to &amp;")
		}
	})

	t.Run("enforces max length", func(t *testing.T) {
		input := "this is a very long string that should be truncated"
		result := SanitizeString(input, SanitizeOptions{MaxLength: 10})
		if len(result) > 10 {
			t.Errorf("Expected max length 10, got %d", len(result))
		}
	})

	t.Run("applies character whitelist", func(t *testing.T) {
		input := "abc123!@#"
		result := SanitizeString(input, SanitizeOptions{
			AllowedCharacters: regexp.MustCompile(`[a-z0-9]`),
		})
		if strings.Contains(result, "!") || strings.Contains(result, "@") {
			t.Error("Should filter characters not in whitelist")
		}
	})

	t.Run("applies custom patterns", func(t *testing.T) {
		input := "hello BADWORD world"
		result := SanitizeString(input, SanitizeOptions{
			CustomPatterns: []*regexp.Regexp{regexp.MustCompile(`BADWORD`)},
		})
		if strings.Contains(result, "BADWORD") {
			t.Error("Should remove custom pattern")
		}
	})

	t.Run("respects disabled options", func(t *testing.T) {
		input := "<b>bold</b>"
		disabled := false
		result := SanitizeString(input, SanitizeOptions{
			SanitizeHTML: &disabled,
		})
		if len(result) == 0 && len(input) > 0 {
			t.Error("Result should not be empty when input is not empty")
		}
	})

	t.Run("NFKC normalization", func(t *testing.T) {
		enabled := true
		input := "\uFF21\uFF22\uFF23" // fullwidth ABC
		result := SanitizeString(input, SanitizeOptions{
			NormalizeUnicode: &enabled,
		})
		if result != "ABC" {
			t.Errorf("NFKC should normalize fullwidth to ASCII, got %q", result)
		}
	})
}

func TestDetectThreats(t *testing.T) {
	tests := []struct {
		name string
		input string
		expected func(ThreatReport) bool
	}{
		{"invisible chars", "test\u200Btest", func(r ThreatReport) bool { return r.InvisibleChars }},
		{"HTML content", "<script>alert(1)</script>", func(r ThreatReport) bool { return r.HTMLContent }},
		{"SQL injection", "test' OR 1=1--", func(r ThreatReport) bool { return r.SQLInjection }},
		{"NoSQL injection", `{"$ne": null}`, func(r ThreatReport) bool { return r.NoSQLInjection }},
		{"command injection", "; rm -rf /", func(r ThreatReport) bool { return r.CommandInjection }},
		{"LDAP injection", "*(objectClass=*)", func(r ThreatReport) bool { return r.LDAPInjection }},
		{"XXE injection", "<!DOCTYPE foo [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]>", func(r ThreatReport) bool { return r.XXEInjection }},
		{"path traversal", "../../etc/passwd", func(r ThreatReport) bool { return r.PathTraversal }},
		{"control chars", "test\x01test", func(r ThreatReport) bool { return r.ControlChars }},
		{"null bytes", "test\x00test", func(r ThreatReport) bool { return r.NullBytes }},
		{"XSS events", "<div onclick=\"alert(1)\">", func(r ThreatReport) bool { return r.XSSEvents }},
		{"CSS injection", "expression(alert(1))", func(r ThreatReport) bool { return r.CSSInjection }},
		{"template injection", "{{constructor.constructor('return this')()}}", func(r ThreatReport) bool { return r.TemplateInjection }},
		{"SSI injection", "<!--#exec cmd=\"ls\" -->", func(r ThreatReport) bool { return r.SSIInjection }},
		{"clean input", "Hello, World!", func(r ThreatReport) bool { return !r.HasThreats() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := DetectThreats(tt.input)
			if !tt.expected(report) {
				t.Errorf("DetectThreats(%q) failed expected check", tt.input)
			}
		})
	}
}

func TestThreatReport_HasThreats(t *testing.T) {
	t.Run("no threats", func(t *testing.T) {
		r := ThreatReport{}
		if r.HasThreats() {
			t.Error("Empty report should not have threats")
		}
	})
	t.Run("with threats", func(t *testing.T) {
		r := ThreatReport{SQLInjection: true}
		if !r.HasThreats() {
			t.Error("Report with SQL injection should have threats")
		}
	})
}

func TestContainsHTML(t *testing.T) {
	tests := []struct {
		input string
		expected bool
	}{
		{"<div>test</div>", true},
		{"&amp;", true},
		{"&nbsp;", true},
		{"plain text", false},
		{"test &amp test", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ContainsHTML(tt.input); got != tt.expected {
				t.Errorf("ContainsHTML(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestValidateCharacters(t *testing.T) {
	pattern := regexp.MustCompile(`^[a-z]+$`)
	tests := []struct {
		input string
		expected bool
	}{
		{"abc", true},
		{"ABC", false},
		{"abc123", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ValidateCharacters(tt.input, pattern); got != tt.expected {
				t.Errorf("ValidateCharacters(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestBoolPtr(t *testing.T) {
	truePtr := BoolPtr(true)
	falsePtr := BoolPtr(false)
	if *truePtr != true {
		t.Error("BoolPtr(true) should return pointer to true")
	}
	if *falsePtr != false {
		t.Error("BoolPtr(false) should return pointer to false")
	}
}

func TestIsPrintable(t *testing.T) {
	tests := []struct {
		input string
		expected bool
	}{
		{"Hello, World!", true},
		{"abc123", true},
		{"test\x00", false},
		{"test\x01", false},
		{"", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := IsPrintable(tt.input); got != tt.expected {
				t.Errorf("IsPrintable(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// String utilities tests
// ---------------------------------------------------------------------------

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input, suffix, expected string
		maxLen int
	}{
		{"hello world", "...", "he...", 5},
		{"hello", "...", "hello", 10},
		{"hello", "...", "...", 3},
	}
	for _, tt := range tests {
		if got := TruncateString(tt.input, tt.maxLen, tt.suffix); got != tt.expected {
			t.Errorf("TruncateString(%q, %d, %q) = %q, want %q", tt.input, tt.maxLen, tt.suffix, got, tt.expected)
		}
	}
}

func TestTrimAndLower(t *testing.T) {
	if got := TrimAndLower(" HELLO "); got != "hello" {
		t.Errorf("got %q", got)
	}
}

func TestIsEmpty(t *testing.T) {
	if !IsEmpty("") || !IsEmpty(" ") || IsEmpty("hello") {
		t.Error("IsEmpty mismatch")
	}
}

func TestCoalesce(t *testing.T) {
	if got := Coalesce("", "hello", "world"); got != "hello" {
		t.Errorf("Coalesce = %q, want hello", got)
	}
}

func TestSnakeCase(t *testing.T) {
	if got := SnakeCase("ServiceName"); got != "service_name" {
		t.Errorf("SnakeCase = %q", got)
	}
}

func TestCamelCase(t *testing.T) {
	if got := CamelCase("service_name"); got != "serviceName" {
		t.Errorf("CamelCase = %q", got)
	}
}

func TestPascalCase(t *testing.T) {
	if got := PascalCase("service_name"); got != "ServiceName" {
		t.Errorf("PascalCase = %q", got)
	}
}

func TestSlugify(t *testing.T) {
	if got := Slugify("Hello World! #2024"); got != "hello-world-2024" {
		t.Errorf("Slugify = %q", got)
	}
}

func TestRandomHex(t *testing.T) {
	hex, err := RandomHex(16)
	if err != nil {
		t.Fatal(err)
	}
	if len(hex) != 32 {
		t.Errorf("len = %d, want 32", len(hex))
	}
}

func TestContainsAny(t *testing.T) {
	if !ContainsAny("hello world", "world") {
		t.Error("should contain world")
	}
}

func TestSplitAndTrim(t *testing.T) {
	got := SplitAndTrim("a, b, c", ",")
	if len(got) != 3 || got[0] != "a" {
		t.Errorf("SplitAndTrim = %v", got)
	}
}

func TestPadLeft(t *testing.T) {
	if got := PadLeft("123", 5, '0'); got != "00123" {
		t.Errorf("PadLeft = %q", got)
	}
}

func TestPadRight(t *testing.T) {
	if got := PadRight("123", 5, '0'); got != "12300" {
		t.Errorf("PadRight = %q", got)
	}
}

func TestMaskString(t *testing.T) {
	if got := MaskString("secret-token-123", '*'); got != "s**************3" {
		t.Errorf("MaskString = %q", got)
	}
}

func TestEllipsisMiddle(t *testing.T) {
	if got := EllipsisMiddle("abcdefghijklmnop", 10); got != "abc...mnop" {
		t.Errorf("EllipsisMiddle = %q", got)
	}
}
