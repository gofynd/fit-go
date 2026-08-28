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

// Package redact provides small, allocation-light helpers for keeping PII and
// secrets out of logs: safe URLs (no query/userinfo), allowlist-redacted query
// parameters, and sensitive-header masking. These are the primitives the server
// request logger and the HTTP clients use, and they are exported so service code
// can apply the same policy at any log call-site.
//
// Policy (secure-by-default):
//   - URLs are logged scheme://host/path — never the query string or userinfo.
//   - Query parameter KEYS are always kept (they reveal request shape without a
//     value, e.g. "email" tells you a search-by-email happened); VALUES are kept
//     only for an allowlist of operational keys and masked otherwise.
//   - Sensitive header VALUES (auth, cookies, api keys) are always masked.
package redact

import (
	"context"
	"errors"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

// Mask is the placeholder substituted for a redacted value.
const Mask = "[REDACTED]"

var (
	textURLPattern = regexp.MustCompile(
		`(?i)\b(?:https?|mongodb(?:\+srv)?|postgres(?:ql)?|redis(?:s)?|mysql)://[^\s"'<>]+`,
	)
	textRelativeQueryPattern = regexp.MustCompile(`(^|[\s(=:])(/[^\s"'<>?]*)\?[^\s"'<>]+`)
	textSensitivePathPattern = regexp.MustCompile(
		`(?i)(/(?:api[_-]?key|access[_-]?token|auth[_-]?token|client[_-]?secret|credential|` +
			`one[_-]?time[_-]?password|otp|password|passwd|refresh[_-]?token|reset[_-]?(?:password|token)|` +
			`secret|session|token|verification[_-]?code)/)([^/\s"'<>]+)`,
	)
	textUserInfoPattern = regexp.MustCompile(`(^|[\s(=])[^\s:/@]+:[^\s/@]+@([A-Za-z0-9[(])`)
	textEmailPattern    = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	textPhonePattern    = regexp.MustCompile(`\+?[0-9][0-9 ()\-.]{7,}[0-9]`)
	textBearerPattern   = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+\-/=]+`)
	textBasicPattern    = regexp.MustCompile(`(?i)\bBasic\s+[A-Za-z0-9+/=]+`)
	textSecretPattern   = regexp.MustCompile(
		`(?i)\b(api[_-]?key|access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|` +
			`credential|password|passwd|refresh[_-]?token|secret|session[_-]?token|token|user(?:name)?)` +
			`\s*[:=]\s*("[^"]*"|'[^']*'|[^&\s,;}]+)`,
	)
	textSecretJSONPattern = regexp.MustCompile(
		`(?i)("(?:api[_-]?key|access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|` +
			`credential|password|passwd|refresh[_-]?token|secret|session[_-]?token|token|user(?:name)?)"` +
			`\s*:\s*")[^"]*(")`,
	)
	textSensitiveHeaderPattern = regexp.MustCompile(
		`(?i)\b(authorization|proxy[_-]?authorization|cookie|set[_-]?cookie|authentication|api[_-]?key|` +
			`x[-_][a-z0-9_-]*(?:api[_-]?key|auth[_-]?token|access[_-]?token|csrf[_-]?token|xsrf[_-]?token|` +
			`credential|password|secret|session[_-]?token|signature))\s*:\s*[^\r\n,}]+`,
	)
	textJWTLikePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9_-]{8,})?$`)
)

var sensitivePathKeys = map[string]bool{
	"api-key":           true,
	"access-token":      true,
	"auth-token":        true,
	"client-secret":     true,
	"credential":        true,
	"credentials":       true,
	"email":             true,
	"one-time-password": true,
	"otp":               true,
	"passwd":            true,
	"password":          true,
	"password-reset":    true,
	"phone":             true,
	"refresh-token":     true,
	"reset-password":    true,
	"reset-token":       true,
	"secret":            true,
	"session":           true,
	"session-token":     true,
	"token":             true,
	"verification-code": true,
}

// DefaultQueryAllowlist is the set of query keys whose VALUES are safe to log —
// pagination / sorting / projection controls that never carry PII. Any key not in
// this set has its value masked. Deliberately excludes free-text search keys
// (e.g. "q", "search", "email", "phone") which routinely carry PII.
var DefaultQueryAllowlist = map[string]bool{
	"limit": true, "page": true, "page_size": true, "pageSize": true,
	"per_page": true, "perPage": true, "size": true, "offset": true,
	"cursor": true, "sort": true, "order": true, "order_by": true,
	"orderBy": true, "dir": true, "direction": true, "fields": true,
}

// sensitiveHeaders are header names whose values must never be logged verbatim.
// Compared case-insensitively.
var sensitiveHeaders = map[string]bool{
	"authorization":           true,
	"proxy-authorization":     true,
	"cookie":                  true,
	"set-cookie":              true,
	"x-api-key":               true,
	"x-auth-token":            true,
	"x-access-token":          true,
	"x-csrf-token":            true,
	"x-xsrf-token":            true,
	"api-key":                 true,
	"authentication":          true,
	"x-amz-security-token":    true,
	"x-application-data":      true,
	"x-client-cert":           true,
	"x-forwarded-client-cert": true,
	"x-goog-api-key":          true,
	"x-session-token":         true,
	"x-signature":             true,
	"x-user-data":             true,
}

// SafeURL renders a URL for logging without query, fragment, or userinfo. Static
// HTTP path context is retained, while credential-bearing path values are
// masked. Database DSN paths are always masked because they identify a database
// or tenant rather than an HTTP route. Nil-safe.
func SafeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	var b strings.Builder
	if u.Scheme != "" {
		b.WriteString(u.Scheme)
		b.WriteString("://")
	}
	b.WriteString(u.Host) // Host excludes userinfo.
	path := u.EscapedPath()
	if isDatabaseScheme(u.Scheme) && path != "" && path != "/" {
		path = "/" + Mask
	} else {
		path = safePath(path)
	}
	if path == "" {
		b.WriteByte('/')
	} else {
		b.WriteString(path)
	}
	return b.String()
}

func isDatabaseScheme(scheme string) bool {
	switch strings.ToLower(scheme) {
	case "mongodb", "mongodb+srv", "mysql", "postgres", "postgresql", "redis", "rediss":
		return true
	default:
		return false
	}
}

func safePath(path string) string {
	if path == "" {
		return "/"
	}
	segments := strings.Split(path, "/")
	redactNext := false
	for i, raw := range segments {
		if raw == "" {
			continue
		}
		decoded, err := url.PathUnescape(raw)
		if err != nil {
			segments[i] = Mask
			redactNext = false
			continue
		}
		if redactNext || sensitivePathValue(decoded) {
			segments[i] = Mask
			redactNext = false
			continue
		}
		redactNext = sensitivePathKeys[normalizePathKey(decoded)]
	}
	return strings.Join(segments, "/")
}

func normalizePathKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	return value
}

func sensitivePathValue(value string) bool {
	return textEmailPattern.MatchString(value) ||
		textPhonePattern.MatchString(value) ||
		textBearerPattern.MatchString(value) ||
		textBasicPattern.MatchString(value) ||
		textSecretPattern.MatchString(value) ||
		textJWTLikePattern.MatchString(value)
}

// QueryMap redacts parsed query values into a key->value map suitable for
// structured logging: every key is kept, values are kept only for keys in allow
// (nil allow = DefaultQueryAllowlist) and masked otherwise. Multi-valued keys are
// joined with ",". Returns nil for an empty query so callers can skip the field.
func QueryMap(values url.Values, allow map[string]bool) map[string]string {
	if len(values) == 0 {
		return nil
	}
	if allow == nil {
		allow = DefaultQueryAllowlist
	}
	out := make(map[string]string, len(values))
	for k, vs := range values {
		if allow[k] {
			out[k] = strings.Join(vs, ",")
		} else {
			out[k] = Mask
		}
	}
	return out
}

// Query redacts a raw query string ("a=1&b=x") into a stable, sorted, redacted
// string form ("a=1&b=[REDACTED]") for callers that log a single string field.
// Empty in, empty out.
func Query(rawQuery string, allow map[string]bool) string {
	if rawQuery == "" {
		return ""
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		// Unparseable — don't risk logging raw PII; mask the whole thing.
		return Mask
	}
	m := QueryMap(values, allow)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys) // stable output (ParseQuery/map order is nondeterministic)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m[k])
	}
	return b.String()
}

// IsSensitiveHeader reports whether a header name carries a secret/credential
// that must not be logged (case-insensitive).
func IsSensitiveHeader(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	if sensitiveHeaders[name] {
		return true
	}
	for _, suffix := range []string{
		"-api-key", "-authorization", "-cookie", "-credential", "-password",
		"-secret", "-signature", "-token",
	} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// HeaderValue returns value, or Mask when name is a sensitive header.
func HeaderValue(name, value string) string {
	if IsSensitiveHeader(name) {
		return Mask
	}
	return value
}

// Text redacts common credentials and PII from arbitrary diagnostic text while
// retaining non-sensitive operational context. It is suitable for log/error
// fields; normal payload logging still requires a structured allowlist.
func Text(value string) string {
	value = textURLPattern.ReplaceAllStringFunc(value, func(raw string) string {
		candidate, trailing := trimURLPunctuation(raw)
		parsed, err := url.Parse(candidate)
		if err != nil || parsed.Host == "" {
			return "[REDACTED_URL]" + trailing
		}
		return SafeURL(parsed) + trailing
	})
	value = textRelativeQueryPattern.ReplaceAllStringFunc(value, redactRelativeQuery)
	value = textSensitivePathPattern.ReplaceAllString(value, "$1"+Mask)
	value = textUserInfoPattern.ReplaceAllString(value, "$1"+Mask+"@$2")
	value = textSensitiveHeaderPattern.ReplaceAllStringFunc(value, func(raw string) string {
		idx := strings.IndexByte(raw, ':')
		if idx < 0 {
			return Mask
		}
		return raw[:idx+1] + " " + Mask
	})
	value = textBearerPattern.ReplaceAllString(value, "Bearer "+Mask)
	value = textBasicPattern.ReplaceAllString(value, "Basic "+Mask)
	value = textSecretJSONPattern.ReplaceAllString(value, "$1"+Mask+"$2")
	value = textSecretPattern.ReplaceAllString(value, "$1="+Mask)
	value = textEmailPattern.ReplaceAllString(value, "[REDACTED_EMAIL]")
	value = textPhonePattern.ReplaceAllString(value, "[REDACTED_PHONE]")
	return value
}

func trimURLPunctuation(value string) (string, string) {
	end := len(value)
	for end > 0 && strings.ContainsRune(".,;!?)]}", rune(value[end-1])) {
		end--
	}
	return value[:end], value[end:]
}

func redactRelativeQuery(raw string) string {
	prefix := ""
	candidate := raw
	if candidate != "" && candidate[0] != '/' {
		prefix, candidate = candidate[:1], candidate[1:]
	}
	candidate, trailing := trimURLPunctuation(candidate)
	parsed, err := url.Parse(candidate)
	if err != nil {
		return prefix + "[REDACTED_URL]" + trailing
	}
	return prefix + safePath(parsed.EscapedPath()) + trailing
}

// ErrorMessage classifies an error for telemetry without serializing the raw
// provider/driver text. Go transport errors routinely embed a complete URL,
// including userinfo and query parameters. Generic backend errors can likewise
// contain SQL, Redis values, credentials, or response bodies. Callers should
// return the original error to preserve API behavior, but use this string for
// logs, span status, and error-reporting breadcrumbs.
func ErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "operation canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "operation timed out"
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		op := strings.ToUpper(strings.TrimSpace(urlErr.Op))
		if op == "" {
			op = "HTTP"
		}
		safeURL := ""
		if parsed, parseErr := url.Parse(urlErr.URL); parseErr == nil {
			safeURL = SafeURL(parsed)
		}
		classification := transportErrorClass(urlErr.Err)
		if safeURL == "" {
			return op + ": " + classification
		}
		return op + " " + safeURL + ": " + classification
	}
	message := strings.TrimSpace(Text(err.Error()))
	if message == "" {
		return "operation failed"
	}
	return message
}

func transportErrorClass(err error) string {
	if err == nil {
		return "request failed"
	}
	if errors.Is(err, context.Canceled) {
		return "operation canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "operation timed out"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "network timeout"
		}
		return "network error"
	}
	return "operation failed"
}
