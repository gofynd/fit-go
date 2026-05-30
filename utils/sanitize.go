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
	"regexp"
	"strings"
	"unicode"
)

// ---------------------------------------------------------------------------
// Threat detection patterns - port utilities/sanitize/patterns.ts
// ---------------------------------------------------------------------------

// securityPatterns holds compiled regular expressions for detecting security
// threats in input strings.
var securityPatterns = struct {
	InvisibleChars      *regexp.Regexp
	HTMLTags            *regexp.Regexp
	HTMLEntities        *regexp.Regexp
	ScriptTags          *regexp.Regexp
	SQLInjection        *regexp.Regexp
	NoSQLInjection      *regexp.Regexp
	CommandInjection    *regexp.Regexp
	LDAPInjection       *regexp.Regexp
	XXEInjection        *regexp.Regexp
	PathTraversal       *regexp.Regexp
	XSSEvents           *regexp.Regexp
	JavaScriptExecution *regexp.Regexp
	ControlChars        *regexp.Regexp
	NullBytes           *regexp.Regexp
	DangerousProtocols  *regexp.Regexp
	XSSVectors          *regexp.Regexp
	CSSInjection        *regexp.Regexp
	TemplateInjection   *regexp.Regexp
	SSIInjection        *regexp.Regexp
}{
	InvisibleChars:      regexp.MustCompile("[\u200B\u200C\u200D\u2063\uFEFF\u3164\uFFA0\u115F\u1160\u061C\u180E\u2000-\u200F\u2028-\u202F\u205F-\u206F]"),
	HTMLTags:            regexp.MustCompile(`<[^>]*>`),
	HTMLEntities:        regexp.MustCompile(`(?i)&(?:#(?:\d+|x[0-9a-fA-F]+)|[a-zA-Z0-9]+);`),
	ScriptTags:          regexp.MustCompile(`(?is)<\s*script[^>]*>.*?<\s*/\s*script\s*>`),
	SQLInjection:        regexp.MustCompile(`(?i)(\b(?:SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC|UNION|TRUNCATE|GRANT|REVOKE)\b\s+\w+|--|\'\s*OR\s+|\'\s*UNION\s+|\'\s*;|\|\||\bOR\b\s+\d+\s*=\s*\d+|\bUNION\b\s+(?:ALL\s+)?SELECT|\bINTO\b\s+(?:OUTFILE|DUMPFILE))`),
	NoSQLInjection:      regexp.MustCompile(`(?i)(\$ne|\$gt|\$lt|\$gte|\$lte|\$in|\$nin|\$and|\$or|\$not|\$where|\$regex|\$options|\$elemMatch|\$size|\$exists|\$type|\$mod|\$all)`),
	CommandInjection:    regexp.MustCompile(`(?i)(;\s*(?:rm|cat|ls|pwd|whoami|ps|mv|cp|chmod|chown|curl|wget|netcat|nc)\b|\|\s*(?:rm|cat|ls|pwd|whoami|ps|mv|cp|chmod|chown|curl|wget|netcat|nc)\b|&&\s*(?:rm|cat|ls|pwd|whoami|ps|mv|cp|chmod|chown|curl|wget|netcat|nc)\b|\$\(|` + "`" + `.*` + "`" + `|%0a|%0d)`),
	LDAPInjection:       regexp.MustCompile(`(?i)(\*\)|(\|\()|(\&\()|(\!\()|(\(\|)|(\(\&)|(\(\!)|(objectClass=)|(cn=)|(uid=)|(ou=)|(dc=))`),
	XXEInjection:        regexp.MustCompile(`(?i)(<!ENTITY|<!DOCTYPE|SYSTEM|PUBLIC|CDATA|\[CDATA\[)`),
	PathTraversal:       regexp.MustCompile(`(?i)(\.\.\/|\.\.\\|%2e%2e%2f|%2e%2e%5c|%252e%252e%252f|%252e%252e%255c)`),
	XSSEvents:           regexp.MustCompile(`(?i)\bon\w+\s*=`),
	JavaScriptExecution: regexp.MustCompile(`(?i)(javascript:|data:text/html|vbscript:|expression\(|eval\(|setTimeout\(|setInterval\()`),
	ControlChars:        regexp.MustCompile(`[\x00-\x1f\x7f]`),
	NullBytes:           regexp.MustCompile("\x00"),
	DangerousProtocols:  regexp.MustCompile(`(?i)(javascript:|data:|vbscript:|file:|ftp:|gopher:|ldap:|mailto:|news:|telnet:)`),
	XSSVectors:          regexp.MustCompile(`(?i)(<\s*script[^>]*>|<\s*/\s*script\s*>|<\s*iframe[^>]*>|<\s*object[^>]*>|<\s*embed[^>]*>|<\s*link[^>]*>|<\s*meta[^>]*>|<\s*style[^>]*>)`),
	CSSInjection:        regexp.MustCompile(`(?i)(expression\s*\(|javascript\s*:|@import|url\s*\(|behavior\s*:|binding\s*:)`),
	TemplateInjection:   regexp.MustCompile(`(\{\{|\}\}|\$\{|\}|<%|%>|\[\[|\]\])`),
	SSIInjection:        regexp.MustCompile(`(?i)(<!--\s*#\s*(?:include|exec|echo|config|set)\s*)`),
}

// ---------------------------------------------------------------------------
// Types - port utilities/sanitize/types.ts
// ---------------------------------------------------------------------------

// ThreatReport contains boolean flags indicating which security threats were
// detected in an input string. Port ThreatReport interface.
type ThreatReport struct {
	InvisibleChars      bool `json:"invisible_chars,omitempty"`
	HTMLContent         bool `json:"html_content,omitempty"`
	SQLInjection        bool `json:"sql_injection,omitempty"`
	NoSQLInjection      bool `json:"nosql_injection,omitempty"`
	CommandInjection    bool `json:"command_injection,omitempty"`
	LDAPInjection       bool `json:"ldap_injection,omitempty"`
	XXEInjection        bool `json:"xxe_injection,omitempty"`
	PathTraversal       bool `json:"path_traversal,omitempty"`
	ControlChars        bool `json:"control_chars,omitempty"`
	NullBytes           bool `json:"null_bytes,omitempty"`
	XSSEvents           bool `json:"xss_events,omitempty"`
	CSSInjection        bool `json:"css_injection,omitempty"`
	TemplateInjection   bool `json:"template_injection,omitempty"`
	SSIInjection        bool `json:"ssi_injection,omitempty"`
	MaxLengthExceeded   bool `json:"max_length_exceeded,omitempty"`
	ForbiddenCharacters bool `json:"forbidden_characters,omitempty"`
}

// HasThreats returns true if any threat flag is set.
func (t ThreatReport) HasThreats() bool {
	return t.InvisibleChars || t.HTMLContent || t.SQLInjection ||
		t.NoSQLInjection || t.CommandInjection || t.LDAPInjection ||
		t.XXEInjection || t.PathTraversal || t.ControlChars ||
		t.NullBytes || t.XSSEvents || t.CSSInjection ||
		t.TemplateInjection || t.SSIInjection
}

// SanitizeOptions configures the string sanitization behavior.
// By default, all protections are enabled (
// options default to enabled unless explicitly set to false).
type SanitizeOptions struct {
	// Injection protection (all enabled by default).
	SQLInjection     *bool
	NoSQLInjection   *bool
	CommandInjection *bool
	LDAPInjection    *bool
	XXEInjection     *bool
	PathTraversal    *bool

	// Character handling (all enabled by default).
	RemoveInvisibleChars *bool
	RemoveControlChars   *bool
	RemoveNullBytes      *bool
	NormalizeUnicode     *bool

	// HTML handling (enabled by default).
	SanitizeHTML *bool

	// Validation.
	MaxLength         int
	AllowedCharacters *regexp.Regexp
	CustomPatterns    []*regexp.Regexp
}

// optEnabled returns true if the option pointer is nil (default enabled) or
// explicitly set to true.
func optEnabled(opt *bool) bool {
	return opt == nil || *opt
}

// ---------------------------------------------------------------------------
// Sanitization functions - port utilities/sanitize/index.ts
// ---------------------------------------------------------------------------

// SanitizeString sanitizes a general string by removing HTML content, invisible
// characters, injection patterns, and other security threats. It does NOT
// expect HTML in the input - all HTML is stripped.
func SanitizeString(input string, opts SanitizeOptions) string {
	sanitized := input

	// 1. Remove invisible characters.
	if optEnabled(opts.RemoveInvisibleChars) {
		sanitized = securityPatterns.InvisibleChars.ReplaceAllString(sanitized, "")
	}

	// 2. Remove control characters and null bytes.
	if optEnabled(opts.RemoveControlChars) {
		sanitized = securityPatterns.ControlChars.ReplaceAllString(sanitized, "")
	}
	if optEnabled(opts.RemoveNullBytes) {
		sanitized = securityPatterns.NullBytes.ReplaceAllString(sanitized, "")
	}

	// 3. Handle HTML content.
	if optEnabled(opts.SanitizeHTML) {
		// Remove script tags completely (including content).
		sanitized = securityPatterns.ScriptTags.ReplaceAllString(sanitized, "")
		// Remove all other HTML tags but keep their content.
		sanitized = securityPatterns.HTMLTags.ReplaceAllString(sanitized, "")
		// Remove HTML entities.
		sanitized = securityPatterns.HTMLEntities.ReplaceAllString(sanitized, "")
	}

	// 4. Remove injection patterns.
	if optEnabled(opts.SQLInjection) {
		sanitized = securityPatterns.SQLInjection.ReplaceAllString(sanitized, "")
	}
	if optEnabled(opts.NoSQLInjection) {
		sanitized = securityPatterns.NoSQLInjection.ReplaceAllString(sanitized, "")
	}
	if optEnabled(opts.CommandInjection) {
		sanitized = securityPatterns.CommandInjection.ReplaceAllString(sanitized, "")
	}
	if optEnabled(opts.LDAPInjection) {
		sanitized = securityPatterns.LDAPInjection.ReplaceAllString(sanitized, "")
	}
	if optEnabled(opts.XXEInjection) {
		sanitized = securityPatterns.XXEInjection.ReplaceAllString(sanitized, "")
	}
	if optEnabled(opts.PathTraversal) {
		sanitized = securityPatterns.PathTraversal.ReplaceAllString(sanitized, "")
	}

	// 4b. Remove additional XSS/CSS/template/SSI vectors (always enabled when
	// HTML sanitization is on.
	if optEnabled(opts.SanitizeHTML) {
		sanitized = securityPatterns.XSSEvents.ReplaceAllString(sanitized, "")
		sanitized = securityPatterns.XSSVectors.ReplaceAllString(sanitized, "")
		sanitized = securityPatterns.JavaScriptExecution.ReplaceAllString(sanitized, "")
		sanitized = securityPatterns.DangerousProtocols.ReplaceAllString(sanitized, "")
		sanitized = securityPatterns.CSSInjection.ReplaceAllString(sanitized, "")
		sanitized = securityPatterns.TemplateInjection.ReplaceAllString(sanitized, "")
		sanitized = securityPatterns.SSIInjection.ReplaceAllString(sanitized, "")
	}

	// 5. Apply custom patterns.
	for _, pattern := range opts.CustomPatterns {
		sanitized = pattern.ReplaceAllString(sanitized, "")
	}

	// 6. Length validation.
	if opts.MaxLength > 0 && len(sanitized) > opts.MaxLength {
		sanitized = sanitized[:opts.MaxLength]
	}

	// 7. Character whitelist validation.
	if opts.AllowedCharacters != nil {
		var filtered strings.Builder
		for _, ch := range sanitized {
			if opts.AllowedCharacters.MatchString(string(ch)) {
				filtered.WriteRune(ch)
			}
		}
		sanitized = filtered.String()
	}

	// 8. Trim whitespace and remove remaining low-control characters.
	sanitized = strings.TrimSpace(sanitized)
	sanitized = stripLowChars(sanitized)

	// 9. Escape HTML characters for safety (< > & " ').
	sanitized = escapeHTML(sanitized)

	// 10. Unicode normalization.
	if opts.NormalizeUnicode != nil && *opts.NormalizeUnicode {
		// NFKC normalization using standard library.
		// Go strings are already valid UTF-8, but we normalize composed forms.
		sanitized = normalizeNFKC(sanitized)
	}

	return sanitized
}

// DetectThreats analyzes an input string for security threats without modifying
// it. Returns a ThreatReport with boolean flags for each detected threat type.
func DetectThreats(input string) ThreatReport {
	report := ThreatReport{}

	if securityPatterns.InvisibleChars.MatchString(input) {
		report.InvisibleChars = true
	}
	if securityPatterns.HTMLTags.MatchString(input) || securityPatterns.HTMLEntities.MatchString(input) {
		report.HTMLContent = true
	}
	if securityPatterns.SQLInjection.MatchString(input) {
		report.SQLInjection = true
	}
	if securityPatterns.NoSQLInjection.MatchString(input) {
		report.NoSQLInjection = true
	}
	if securityPatterns.CommandInjection.MatchString(input) {
		report.CommandInjection = true
	}
	if securityPatterns.LDAPInjection.MatchString(input) {
		report.LDAPInjection = true
	}
	if securityPatterns.XXEInjection.MatchString(input) {
		report.XXEInjection = true
	}
	if securityPatterns.PathTraversal.MatchString(input) {
		report.PathTraversal = true
	}
	if securityPatterns.ControlChars.MatchString(input) {
		report.ControlChars = true
	}
	if securityPatterns.NullBytes.MatchString(input) {
		report.NullBytes = true
	}
	if securityPatterns.XSSEvents.MatchString(input) {
		report.XSSEvents = true
	}
	if securityPatterns.CSSInjection.MatchString(input) {
		report.CSSInjection = true
	}
	if securityPatterns.TemplateInjection.MatchString(input) {
		report.TemplateInjection = true
	}
	if securityPatterns.SSIInjection.MatchString(input) {
		report.SSIInjection = true
	}

	return report
}

// ContainsHTML returns true if the input contains HTML tags or entities.
func ContainsHTML(input string) bool {
	return securityPatterns.HTMLTags.MatchString(input) ||
		securityPatterns.HTMLEntities.MatchString(input)
}

// ValidateCharacters returns true if the input fully matches the given compiled
// pattern.
func ValidateCharacters(input string, pattern *regexp.Regexp) bool {
	return pattern.MatchString(input)
}

// ValidateCharactersString returns true if the input fully matches the given
// pattern string. Returns false if the pattern fails to compile.
func ValidateCharactersString(input string, pattern string) bool {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return re.MatchString(input)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// escapeHTML converts < > & " ' to their HTML entity equivalents.
// This is the Go equivalent of validator.escape() in the TypeScript version.
func escapeHTML(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '&':
			b.WriteString("&amp;")
		case '<':
			b.WriteString("&lt;")
		case '>':
			b.WriteString("&gt;")
		case '"':
			b.WriteString("&quot;")
		case '\'':
			b.WriteString("&#x27;")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// stripLowChars removes control characters (0x00-0x1F, 0x7F-0x9F) except
// for tab (\t), newline (\n), and carriage return (\r), matching the behavior
// of validator.stripLow(str, true).
func stripLowChars(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\t' || r == '\n' || r == '\r' {
			b.WriteRune(r)
			continue
		}
		if (r >= 0x00 && r <= 0x1F) || (r >= 0x7F && r <= 0x9F) {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// normalizeNFKC performs NFKC Unicode normalization. This is a simplified
// implementation using the standard library. For full NFKC support the
// golang.org/x/text/unicode/norm package would be used, but since we are
// restricted to stdlib only, we apply compatibility decomposition for common
// cases.
func normalizeNFKC(s string) string {
	// Go's standard library does not include NFKC normalization.
	// We handle the most common compatibility mappings used in security
	// bypass attacks: fullwidth characters -> ASCII equivalents.
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		// Fullwidth ASCII variants (U+FF01 to U+FF5E) -> ASCII (U+0021 to U+007E).
		if r >= 0xFF01 && r <= 0xFF5E {
			b.WriteRune(r - 0xFF01 + 0x0021)
			continue
		}
		// Halfwidth Katakana and other common compat forms are left as-is
		// since they are not typically used in injection attacks.
		b.WriteRune(r)
	}
	return b.String()
}

// BoolPtr returns a pointer to a bool value. Useful for setting SanitizeOptions
// fields that default to enabled.
func BoolPtr(b bool) *bool {
	return &b
}

// isLoggableContentType checks if a content type is in the whitelist.
func isLoggableContentType(ct string) bool {
	for _, allowed := range whitelistedContentTypes {
		if strings.Contains(ct, allowed) {
			return true
		}
	}
	return false
}

// IsPrintable returns true if all runes in the string are printable.
func IsPrintable(s string) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}
