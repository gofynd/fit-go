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
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
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
			`one[_-]?time[_-]?password|otp|pass|password|passwd|pwd|refresh[_-]?token|reset[_-]?(?:password|token)|` +
			`secret|session|token|verification[_-]?code)/)([^/\s"'<>]+)`,
	)
	textUserInfoPattern       = regexp.MustCompile(`(^|[\s(=])[^\s:/@]+:[^\s/@]+@([A-Za-z0-9[(])`)
	textEmailPattern          = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	textPhonePattern          = regexp.MustCompile(`\+?[0-9][0-9 ()\-.]{7,}[0-9]`)
	textDottedPhonePattern    = regexp.MustCompile(`^(?:\+?(?:[0-9]{1,3}\.)?[0-9]{3}\.[0-9]{3}\.[0-9]{4}|(?:\+[0-9]{1,3}\.)?[6-9][0-9]{4}\.[0-9]{5})$`)
	textFormattedPhonePattern = regexp.MustCompile(
		`^(?:\+?[0-9]{3}[ -][0-9]{3}[ -][0-9]{4}|\+[0-9]{1,3}[ -][0-9]{5}[ -][0-9]{5}|` +
			`[0-9][ -][0-9]{3}[ -][0-9]{3}[ -][0-9]{4}|\+[0-9]{1,3}[ -][0-9]{3}[ -][0-9]{3}[ -][0-9]{4}|` +
			`\+?[0-9]{1,3}[ -]?\([0-9]{3}\)[ -]?[0-9]{3}[ -][0-9]{4})$`,
	)
	// textParenAreaPhonePattern matches a NANP number whose area code opens
	// with "(" — the phone candidate regex starts at the first digit, so the
	// opening parenthesis is re-attached before this check.
	textParenAreaPhonePattern = regexp.MustCompile(`^\([0-9]{3}\)[ -]?[0-9]{3}[ .-]?[0-9]{4}$`)
	textDatePrefixPattern     = regexp.MustCompile(`^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}(?:[ T]\d{1,2})?$`)
	textEpochMillisPattern    = regexp.MustCompile(`\b[0-9]{13}\b`)
	textEpochSecondsPattern   = regexp.MustCompile(
		`(?i)\b(?:epoch(?:_seconds)?|timestamp|created_at|updated_at)\s*[:=]\s*[0-9]{10}\b`,
	)
	textUUIDPattern   = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	textBearerPattern = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+\-/=]+`)
	textBasicPattern  = regexp.MustCompile(`(?i)\bBasic\s+[A-Za-z0-9+/=]+`)
	textSecretPattern = regexp.MustCompile(
		`(?i)\b(api[_-]?key|access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|` +
			`credential|cvv|one[_-]?time[_-]?password|otp|(?:[a-z0-9]+[_-])*(?:pass|passphrase|password|passwd|pwd)|pin|refresh[_-]?token|` +
			`secret|session[_-]?token|token|user(?:name)?|verification[_-]?code)` +
			`\s*([:=])\s*("[^"]*"|'[^']*'|[^&\s,;}]+)`,
	)
	textLooseNumericSecretPattern = regexp.MustCompile(`(?i)\b(cvv|otp|pin)\s+([0-9]{3,8})\b`)
	textSecretJSONPattern         = regexp.MustCompile(
		`(?i)("(?:api[_-]?key|access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|` +
			`credential|cvv|one[_-]?time[_-]?password|otp|(?:[a-z0-9]+[_-])*(?:pass|passphrase|password|passwd|pwd)|pin|refresh[_-]?token|` +
			`secret|session[_-]?token|token|user(?:name)?|verification[_-]?code)"\s*:\s*)` +
			`(?:"(?:\\.|[^"\\])*"|'[^']*'|[-+]?\d+(?:\.\d+)?|true|false|null)`,
	)
	textSensitiveHeaderPattern = regexp.MustCompile(
		`(?i)\b(authorization|proxy[_-]?authorization|cookie|set[_-]?cookie|authentication|api[_-]?key|` +
			`x[-_][a-z0-9_-]*(?:api[_-]?key|auth[_-]?token|access[_-]?token|csrf[_-]?token|xsrf[_-]?token|` +
			`credential|password|secret|session[_-]?token|signature))\s*:\s*[^\r\n,}]+`,
	)
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
	"pass":              true,
	"passwd":            true,
	"password":          true,
	"password-reset":    true,
	"phone":             true,
	"pwd":               true,
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
	previousKey := ""
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
		if redactNext || sensitivePathValue(decoded) && !isTimestampPathValue(previousKey, decoded) {
			segments[i] = Mask
			redactNext = false
			previousKey = ""
			continue
		}
		previousKey = normalizePathKey(decoded)
		redactNext = sensitivePathKeys[previousKey]
	}
	return strings.Join(segments, "/")
}

func isTimestampPathValue(previousKey, value string) bool {
	switch previousKey {
	case "event", "events", "timestamp", "timestamps":
		return isPlausibleEpochMillis(value)
	default:
		return false
	}
}

func normalizePathKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	return value
}

func sensitivePathValue(value string) bool {
	return textEmailPattern.MatchString(value) ||
		containsSensitivePhone(value) ||
		textBearerPattern.MatchString(value) ||
		textBasicPattern.MatchString(value) ||
		textSecretPattern.MatchString(value) ||
		containsValidJWT(value) ||
		containsPaymentCard(value)
}

func containsSensitivePhone(value string) bool {
	return redactPhoneCandidates(value) != value
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
	value = textSecretJSONPattern.ReplaceAllString(value, "$1\""+Mask+"\"")
	value = redactTextSecrets(value)
	value = textLooseNumericSecretPattern.ReplaceAllString(value, "$1 "+Mask)
	value = redactJWTs(value)
	value = textEmailPattern.ReplaceAllString(value, "[REDACTED_EMAIL]")
	value = redactPaymentCards(value)
	value = redactPhoneCandidates(value)
	return value
}

// redactTextSecrets masks key/value secrets. PWD is deliberately treated as a
// password alias: a logger cannot reliably tell a working directory such as
// /app from a credential such as /hunter2, and leaking the latter is worse than
// losing working-directory detail. The only "pass" value kept is an explicit
// numeric retry counter ("retry pass: 3").
func redactTextSecrets(value string) string {
	matches := textSecretPattern.FindAllStringSubmatchIndex(value, -1)
	if len(matches) == 0 {
		return value
	}
	var output strings.Builder
	output.Grow(len(value))
	last := 0
	for _, match := range matches {
		if len(match) != 8 || match[2] < 0 || match[4] < 0 || match[6] < 0 {
			continue
		}
		output.WriteString(value[last:match[0]])
		key := value[match[2]:match[3]]
		delimiter := value[match[4]:match[5]]
		secret := value[match[6]:match[7]]
		if delimiter == ":" && strings.EqualFold(key, "pass") && isUnsignedDecimal(secret) &&
			hasImmediateWordBefore(value, match[0], "retry") {
			output.WriteString(value[match[0]:match[1]])
		} else {
			output.WriteString(key)
			output.WriteString(delimiter)
			output.WriteString(Mask)
		}
		last = match[1]
	}
	output.WriteString(value[last:])
	return output.String()
}

// hasImmediateWordBefore reports whether word (ASCII case-insensitive) is the
// whole word immediately before start, separated only by whitespace. The
// whitespace look-back is bounded so adversarial runs stay linear.
func hasImmediateWordBefore(value string, start int, word string) bool {
	end := start
	for end > 0 && start-end < 64 && isASCIISpace(value[end-1]) {
		end--
	}
	wordStart := end - len(word)
	if wordStart < 0 || !strings.EqualFold(value[wordStart:end], word) {
		return false
	}
	return wordStart == 0 || !isASCIIWord(value[wordStart-1])
}

func isUnsignedDecimal(value string) bool {
	value = strings.Trim(value, `"'`)
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		if !isASCIIDigit(value[index]) {
			return false
		}
	}
	return true
}

func containsValidJWT(value string) bool {
	return redactJWTs(value) != value
}

func validJWT(candidate string) bool {
	parts := strings.Split(candidate, ".")
	if len(parts) != 3 {
		return false
	}
	var header map[string]interface{}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(headerBytes, &header) != nil {
		return false
	}
	if _, ok := header["alg"]; !ok {
		return false
	}
	var claims map[string]interface{}
	claimsBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	return err == nil && json.Unmarshal(claimsBytes, &claims) == nil
}

func validJWE(candidate string) bool {
	parts := strings.Split(candidate, ".")
	if len(parts) != 5 {
		return false
	}
	var header map[string]interface{}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(headerBytes, &header) != nil || header["alg"] == nil || header["enc"] == nil {
		return false
	}
	// Direct-encryption JWEs may have an empty encrypted-key segment. The IV,
	// ciphertext, and authentication tag are always present and base64url.
	for index := 1; index < len(parts); index++ {
		if parts[index] == "" {
			if index == 1 {
				continue
			}
			return false
		}
		if _, err := base64.RawURLEncoding.DecodeString(parts[index]); err != nil {
			return false
		}
	}
	return true
}

func redactJWTs(value string) string {
	var output strings.Builder
	output.Grow(len(value))
	last := 0
	for start := 0; start < len(value); {
		if !isJWTTokenByte(value[start]) {
			start++
			continue
		}
		end := start + 1
		for end < len(value) && isJWTTokenByte(value[end]) {
			end++
		}
		token := value[start:end]
		replacements := jwtSpans(token)
		if len(replacements) != 0 {
			output.WriteString(value[last:start])
			tokenLast := 0
			for _, span := range replacements {
				output.WriteString(token[tokenLast:span[0]])
				output.WriteString(Mask)
				tokenLast = span[1]
			}
			output.WriteString(token[tokenLast:])
			last = end
		}
		start = end
	}
	if last == 0 {
		return value
	}
	output.WriteString(value[last:])
	return output.String()
}

func isJWTTokenByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' || value == '-' || value == '_' || value == '.'
}

func jwtSpans(token string) [][]int {
	type segment struct{ start, end int }
	segments := make([]segment, 0, 6)
	segmentStart := 0
	for index := 0; index <= len(token); index++ {
		if index == len(token) || token[index] == '.' {
			segments = append(segments, segment{start: segmentStart, end: index})
			segmentStart = index + 1
		}
	}
	spans := make([][]int, 0, 1)
	for index := 0; index < len(segments); {
		matched := false
		for _, width := range []int{5, 3} {
			if index+width > len(segments) {
				continue
			}
			start := segments[index].start
			end := segments[index+width-1].end
			candidate := token[start:end]
			if width == 5 && validJWE(candidate) || width == 3 && validJWT(candidate) {
				spans = append(spans, []int{start, end})
				index += width
				matched = true
				break
			}
		}
		if !matched {
			index++
		}
	}
	return spans
}

func containsPaymentCard(value string) bool {
	return redactPaymentCards(value) != value
}

func redactPaymentCards(value string) string {
	var output strings.Builder
	last := 0
	for start := 0; start < len(value); start++ {
		if !isASCIIDigit(value[start]) || start > 0 && isASCIIDigit(value[start-1]) {
			continue
		}
		labelled := hasPaymentCardLabelBefore(value, start)
		if !validPaymentCardLeftBoundary(value, start, labelled) {
			continue
		}
		end, ok := paymentCardEnd(value, start, labelled)
		if !ok {
			continue
		}
		if last == 0 {
			output.Grow(len(value))
		}
		output.WriteString(value[last:start])
		output.WriteString("[REDACTED_CARD]")
		last = end
		start = end - 1
	}
	if last == 0 {
		return value
	}
	output.WriteString(value[last:])
	return output.String()
}

func validPaymentCardLeftBoundary(value string, start int, labelled bool) bool {
	if start == 0 {
		return true
	}
	previous := value[start-1]
	if isASCIIDigit(previous) {
		return false
	}
	if !isASCIIWord(previous) && previous != '.' && previous != '-' {
		return true
	}
	// A word character, '.', or '-' immediately before the digits is a valid
	// boundary only when it completes a payment-card label (my_card4111...,
	// card.4111..., pan-4111...). This keeps ref-4111..., versions, and dotted
	// numeric fragments out of the card scanner.
	return labelled
}

func paymentCardEnd(value string, start int, labelled bool) (int, bool) {
	// An unseparated PAN is one complete 13-19 digit token. Never test a
	// Luhn-valid prefix of a longer identifier.
	end := start
	for end < len(value) && isASCIIDigit(value[end]) {
		end++
	}
	if end-start == 13 && isPlausibleEpochMillis(value[start:end]) &&
		(hasTimestampLabelBefore(value, start) || hasTimestampPathBefore(value, start)) {
		return 0, false
	}
	if end-start >= 13 && end-start <= 19 && hasOperationalNumericLabelBefore(value, start) && !labelled {
		return 0, false
	}
	if digits := end - start; digits >= 13 && digits <= 19 && validPaymentCardRightBoundary(value, end) {
		var candidate [19]byte
		copy(candidate[:], value[start:end])
		if !allSameDigit(candidate[:digits]) && validLuhn(candidate[:digits]) {
			return end, true
		}
	}

	// Separated PANs use one consistent delimiter and an established grouping.
	// This deliberately excludes arbitrary IP/version/list fragments that happen
	// to contain a Luhn-valid 13-19 digit window.
	for _, groups := range [...][5]uint8{
		{4, 4, 4, 4, 3}, // 19 digit
		{4, 4, 4, 4},    // 16 digit
		{4, 6, 5},       // 15 digit (Amex)
		{4, 4, 4, 1},    // 13 digit
	} {
		var candidate [19]byte
		if end, digitCount, ok := groupedPaymentCard(value, start, groups, &candidate); ok &&
			!allSameDigit(candidate[:digitCount]) && validLuhn(candidate[:digitCount]) && validPaymentCardRightBoundary(value, end) {
			return end, true
		}
	}
	if end, ok := flexibleGroupedPaymentCardEnd(value, start); ok {
		return end, true
	}
	// Arbitrary groupings are only reinterpreted as a PAN behind an explicit
	// card label; unlabelled separator-joined numeric lists are operational.
	if labelled {
		return labelledPaymentCardEnd(value, start)
	}
	return 0, false
}

// flexibleGroupedPaymentCardEnd matches established PAN layouts whose groups
// are joined by one or two separator characters, mixed separators allowed
// (4111  1111 1111-1111, 41111111 11111111, 3056 930902 5904). It is bounded by
// the 19-digit maximum and therefore constant-time per start position.
func flexibleGroupedPaymentCardEnd(value string, start int) (int, bool) {
	for _, groups := range [...][5]uint8{
		{4, 4, 4, 4, 3}, // 19 digit
		{4, 4, 4, 4},    // 16 digit
		{8, 8},          // 16 digit, 8-8
		{4, 6, 5},       // 15 digit (Amex)
		{4, 6, 4},       // 14 digit (Diners)
	} {
		var digits [19]byte
		digitCount := 0
		position := start
		ok := true
		for groupIndex, groupSize := range groups {
			if groupSize == 0 {
				break
			}
			if groupIndex > 0 {
				separators := 0
				for position < len(value) && separators < 2 && isCardSeparator(value[position]) {
					position++
					separators++
				}
				if separators == 0 {
					ok = false
					break
				}
			}
			for count := uint8(0); count < groupSize; count++ {
				if position >= len(value) || !isASCIIDigit(value[position]) {
					ok = false
					break
				}
				digits[digitCount] = value[position]
				digitCount++
				position++
			}
			if !ok {
				break
			}
		}
		if ok && validPaymentCardRightBoundary(value, position) &&
			!allSameDigit(digits[:digitCount]) && validLuhn(digits[:digitCount]) {
			return position, true
		}
	}
	return 0, false
}

func labelledPaymentCardEnd(value string, start int) (int, bool) {
	// Do not reinterpret an address merely because it follows a card-like
	// label. This also prevents a following numeric field from extending an IP
	// into a Luhn-valid window.
	// The scan is bounded: an IPv4/IPv6 literal (with brackets) never exceeds
	// maxAddressTokenLength bytes, so longer runs cannot be an address and are
	// not re-scanned per start position.
	if looksLikeIPAddressAt(value, start) {
		return 0, false
	}

	var digits [19]byte
	digitCount := 0
	position := start
	lastDigitEnd := start
	for position < len(value) {
		if isASCIIDigit(value[position]) {
			if digitCount == len(digits) {
				// Never redact a valid prefix of a longer numeric token.
				return 0, false
			}
			digits[digitCount] = value[position]
			digitCount++
			position++
			lastDigitEnd = position
			continue
		}
		if !isCardSeparator(value[position]) {
			break
		}
		separatorStart := position
		for position < len(value) && isCardSeparator(value[position]) {
			position++
		}
		if position >= len(value) || !isASCIIDigit(value[position]) {
			break
		}
		// A decimal/punctuation suffix after a complete PAN is not part of the
		// card. Dotted card layouts continue when the following group has four
		// or more digits.
		if value[separatorStart] == '.' && digitCount >= 13 && validLuhn(digits[:digitCount]) {
			nextEnd := position
			for nextEnd < len(value) && isASCIIDigit(value[nextEnd]) {
				nextEnd++
			}
			if nextEnd-position < 4 {
				return lastDigitEnd, !allSameDigit(digits[:digitCount])
			}
		}
	}
	if digitCount < 13 || digitCount > 19 || allSameDigit(digits[:digitCount]) || !validLuhn(digits[:digitCount]) {
		return 0, false
	}
	return lastDigitEnd, true
}

const maxAddressTokenLength = 64

func isIPAddressTokenByte(value byte) bool {
	return isASCIIDigit(value) || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F' ||
		value == '.' || value == ':' || value == '(' || value == ')' || value == '[' || value == ']'
}

func looksLikeIPAddressAt(value string, start int) bool {
	tokenEnd := start
	hasAddressSeparator := false
	for tokenEnd < len(value) && isIPAddressTokenByte(value[tokenEnd]) {
		if tokenEnd-start >= maxAddressTokenLength {
			return false
		}
		if value[tokenEnd] == '.' || value[tokenEnd] == ':' {
			hasAddressSeparator = true
		}
		tokenEnd++
	}
	if !hasAddressSeparator {
		return false
	}
	return net.ParseIP(strings.Trim(value[start:tokenEnd], "()[]{}")) != nil
}

func groupedPaymentCard(value string, start int, groups [5]uint8, digits *[19]byte) (int, int, bool) {
	digitCount := 0
	position := start
	separator := byte(0)
	for groupIndex, groupSize := range groups {
		if groupSize == 0 {
			break
		}
		if groupIndex > 0 {
			if position >= len(value) || !isCardSeparator(value[position]) {
				return 0, 0, false
			}
			if separator == 0 {
				separator = value[position]
			} else if value[position] != separator {
				return 0, 0, false
			}
			position++
		}
		for count := uint8(0); count < groupSize; count++ {
			if position >= len(value) || !isASCIIDigit(value[position]) {
				return 0, 0, false
			}
			digits[digitCount] = value[position]
			digitCount++
			position++
		}
	}
	return position, digitCount, true
}

func validPaymentCardRightBoundary(value string, end int) bool {
	return end == len(value) || !isASCIIDigit(value[end]) && value[end] != '.' && value[end] != '-'
}

// paymentCardLabelTails are the normalized endings every payment-card label
// must have: each exact label below ends with one of them, and the delimited or
// camel-case suffix rule only accepts labels ending in card or pan.
var paymentCardLabelTails = [...]string{
	"card", "cards", "cardno", "cardnumber", "cardpan", "cardholder", "pan", "panno", "pannumber",
}

// maxCardLabelTail is the longest entry in paymentCardLabelTails.
const maxCardLabelTail = len("cardnumber")

// mayHavePaymentCardLabelBefore is a constant-time pre-filter for
// hasPaymentCardLabelBefore. It skips the same value/label delimiters as
// labelBeforeNumericValue, normalizes at most maxCardLabelTail label bytes, and
// rejects unless they end with a payment-card label tail. Without it every digit
// in a long alphanumeric word paid for a 64-byte look-back plus 14 comparisons.
func mayHavePaymentCardLabelBefore(value string, start int) bool {
	position := start
	for position > 0 && start-position < 64 && isASCIISpace(value[position-1]) {
		position--
	}
	for position > 0 && start-position < 64 && (value[position-1] == '"' || value[position-1] == '\'') {
		position--
	}
	if position > 0 && (value[position-1] == ':' || value[position-1] == '=') {
		position--
		for position > 0 && start-position < 64 &&
			(isASCIISpace(value[position-1]) || value[position-1] == '"' || value[position-1] == '\'') {
			position--
		}
	}
	var tail [maxCardLabelTail]byte // reversed, lower-case alphanumerics
	count := 0
	for scanned := 0; position > 0 && count < len(tail) && scanned < 3*maxCardLabelTail; scanned++ {
		ch := value[position-1]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		if ch >= 'a' && ch <= 'z' || isASCIIDigit(ch) {
			// Every tail ends in d, s, o, n, or r: reject ordinary words at once.
			if count == 0 && ch != 'd' && ch != 's' && ch != 'o' && ch != 'n' && ch != 'r' {
				return false
			}
			tail[count] = ch
			count++
		} else if ch != '_' && ch != '-' && ch != '.' && ch != ' ' {
			break
		}
		position--
	}
	for _, want := range paymentCardLabelTails {
		if len(want) > count {
			continue
		}
		matched := true
		for index := 0; index < len(want); index++ {
			if tail[index] != want[len(want)-1-index] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func hasPaymentCardLabelBefore(value string, start int) bool {
	if !mayHavePaymentCardLabelBefore(value, start) {
		return false
	}
	label := labelBeforeNumericValue(value, start)
	for _, candidate := range [...]string{
		"card", "cards", "cardno", "cardnumber", "cardpan", "creditcard", "debitcard",
		"giftcard", "mycard", "paymentcard", "pan", "panno", "pannumber", "cardholder",
	} {
		if normalizedNumericLabelEqual(label, candidate) {
			return true
		}
	}
	return hasDelimitedOrCamelLabelSuffix(label, "card") || hasDelimitedOrCamelLabelSuffix(label, "pan")
}

func hasTimestampLabelBefore(value string, start int) bool {
	label := labelBeforeNumericValue(value, start)
	for _, candidate := range [...]string{
		"epoch", "epochms", "epochmillis", "epochmilliseconds", "eventtime", "eventts",
		"sentat", "time", "timestamp", "timestampms", "timestampmillis", "timestampmilliseconds",
		"createdat", "updatedat",
	} {
		if normalizedNumericLabelEqual(label, candidate) {
			return true
		}
	}
	// Token-suffix forms such as request_ts, start_timestamp, and requestTs.
	return hasDelimitedOrCamelLabelSuffix(label, "ts") || hasDelimitedOrCamelLabelSuffix(label, "timestamp")
}

func hasTimestampPathBefore(value string, start int) bool {
	prefixStart := start
	for prefixStart > 0 && start-prefixStart < 24 && value[prefixStart-1] != '/' &&
		!isASCIISpace(value[prefixStart-1]) {
		prefixStart--
	}
	if prefixStart == 0 || value[prefixStart-1] != '/' {
		return false
	}
	segmentEnd := prefixStart - 1
	segmentStart := segmentEnd
	for segmentStart > 0 && segmentEnd-segmentStart < 64 && value[segmentStart-1] != '/' &&
		!isASCIISpace(value[segmentStart-1]) {
		segmentStart--
	}
	return isTimestampPathValue(normalizePathKey(value[segmentStart:segmentEnd]), value[start:min(start+13, len(value))])
}

func isPlausibleEpochMillis(value string) bool {
	if len(value) != 13 {
		return false
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false
	}
	// 2000-01-01 through 2100-01-01. Context is still required before this
	// window suppresses card matching.
	return millis >= 946684800000 && millis < 4102444800000
}

func isASCIISpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func labelBeforeNumericValue(value string, start int) string {
	if start <= 0 {
		return ""
	}
	position := start
	for position > 0 && isASCIISpace(value[position-1]) {
		position--
	}
	for position > 0 && (value[position-1] == '"' || value[position-1] == '\'') {
		position--
	}
	if position > 0 && (value[position-1] == ':' || value[position-1] == '=') {
		position--
		for position > 0 && (isASCIISpace(value[position-1]) || value[position-1] == '"' || value[position-1] == '\'') {
			position--
		}
	}
	end := position
	for position > 0 && end-position < 64 {
		ch := value[position-1]
		if isASCIIWord(ch) || ch == '-' || ch == '.' || ch == ' ' {
			position--
			continue
		}
		break
	}
	return strings.Trim(value[position:end], " \t\r\n\"'")
}

func normalizeNumericLabel(value string) string {
	var normalized strings.Builder
	normalized.Grow(len(value))
	for index := 0; index < len(value); index++ {
		ch := value[index]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		if ch >= 'a' && ch <= 'z' || isASCIIDigit(ch) {
			normalized.WriteByte(ch)
		}
	}
	return normalized.String()
}

func normalizedNumericLabelEqual(value, want string) bool {
	wantIndex := 0
	for index := 0; index < len(value); index++ {
		ch := value[index]
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		if !(ch >= 'a' && ch <= 'z') && !isASCIIDigit(ch) {
			continue
		}
		if wantIndex >= len(want) || ch != want[wantIndex] {
			return false
		}
		wantIndex++
	}
	return wantIndex == len(want)
}

func hasDelimitedOrCamelLabelSuffix(label, suffix string) bool {
	label = strings.TrimRight(label, "_-. ")
	if len(label) < len(suffix) || !strings.EqualFold(label[len(label)-len(suffix):], suffix) {
		return false
	}
	if len(label) == len(suffix) {
		return true
	}
	boundary := len(label) - len(suffix)
	previous := label[boundary-1]
	if previous == '_' || previous == '-' || previous == '.' || previous == ' ' {
		return true
	}
	// CamelCase is a boundary only when the preceding character is lower-case
	// or numeric. This keeps PAID, VALID, UID, and HOTEL from accidentally
	// becoming id/tel labels.
	return label[boundary] >= 'A' && label[boundary] <= 'Z' &&
		(previous >= 'a' && previous <= 'z' || isASCIIDigit(previous))
}

func allSameDigit(digits []byte) bool {
	if len(digits) == 0 {
		return true
	}
	for _, digit := range digits[1:] {
		if digit != digits[0] {
			return false
		}
	}
	return true
}

func isASCIIDigit(value byte) bool { return value >= '0' && value <= '9' }

func isASCIIWord(value byte) bool {
	return isASCIIDigit(value) || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value == '_'
}

func isCardSeparator(value byte) bool {
	return value == ' ' || value == '-' || value == '.'
}

func validLuhn(digits []byte) bool {
	sum := 0
	double := false
	for index := len(digits) - 1; index >= 0; index-- {
		value := int(digits[index] - '0')
		if double {
			value *= 2
			if value > 9 {
				value -= 9
			}
		}
		sum += value
		double = !double
	}
	return sum%10 == 0
}

func redactPhoneCandidates(value string) string {
	value = redactBareIndianMobiles(value)
	matches := textPhonePattern.FindAllStringIndex(value, -1)
	if len(matches) == 0 {
		return value
	}
	protected := append(textUUIDPattern.FindAllStringIndex(value, -1), textEpochMillisPattern.FindAllStringIndex(value, -1)...)
	protected = append(protected, textEpochSecondsPattern.FindAllStringIndex(value, -1)...)
	sort.Slice(protected, func(i, j int) bool {
		if protected[i][0] != protected[j][0] {
			return protected[i][0] < protected[j][0]
		}
		return protected[i][1] < protected[j][1]
	})
	var output strings.Builder
	output.Grow(len(value))
	last := 0
	protectedIndex := 0
	for _, match := range matches {
		candidate := value[match[0]:match[1]]
		for protectedIndex < len(protected) && protected[protectedIndex][1] <= match[0] {
			protectedIndex++
		}
		if protectedIndex < len(protected) && match[0] < protected[protectedIndex][1] && protected[protectedIndex][0] < match[1] {
			output.WriteString(value[last:match[0]])
			output.WriteString(candidate)
		} else if match[0] > last && value[match[0]-1] == '(' && match[1]-match[0] <= 16 &&
			textParenAreaPhonePattern.MatchString(value[match[0]-1:match[1]]) {
			// "(415) 555-1234": redact the opening parenthesis with the number.
			output.WriteString(value[last : match[0]-1])
			output.WriteString("[REDACTED_PHONE]")
		} else {
			output.WriteString(value[last:match[0]])
			phoneLabel := hasPhoneLabelBefore(value, match[0])
			operationalLabel := hasOperationalNumericLabelBefore(value, match[0])
			if argumentEnd, ok := operationalCallArgumentEnd(value, match[0], match[1], phoneLabel); ok {
				// Preserve a function-like operational argument such as
				// f(1234567890), then continue scanning anything the regex
				// joined after its closing parenthesis.
				output.WriteString(value[match[0]:argumentEnd])
				if argumentEnd < match[1] {
					output.WriteString(redactPhoneCandidate(value[argumentEnd:match[1]], false, false))
				}
			} else {
				output.WriteString(redactPhoneCandidate(candidate, phoneLabel, operationalLabel))
			}
		}
		last = match[1]
	}
	output.WriteString(value[last:])
	return output.String()
}

// maxCallArgumentLength bounds the function-call argument inspection. Every
// supported phone layout is shorter, so longer arguments are never phone-shaped.
const maxCallArgumentLength = 32

// operationalCallArgumentEnd reports whether the phone-regex match at
// [start,end) is the argument of a word-prefixed call such as f(1234567890)
// whose argument is NOT phone-shaped. It returns the end of the preserved
// argument (including its closing parenthesis when that lies inside the match).
func operationalCallArgumentEnd(value string, start, end int, phoneLabel bool) (int, bool) {
	if phoneLabel || start < 2 || value[start-1] != '(' || !isASCIIWord(value[start-2]) {
		return 0, false
	}
	argumentEnd := end
	limit := min(len(value), start+maxCallArgumentLength+1)
	if closing := strings.IndexByte(value[start:limit], ')'); closing >= 0 && start+closing < end {
		argumentEnd = start + closing
	}
	argument := value[start:argumentEnd]
	if isPhoneShapedArgument(argument) {
		return 0, false
	}
	if argumentEnd < end {
		argumentEnd++ // keep the closing parenthesis with the argument
	}
	return argumentEnd, true
}

// isPhoneShapedArgument reports whether a parenthesised argument has a phone
// layout: an Indian mobile (compact or 5-5), a +CC form, a formatted/dotted
// NANP form, or a UK local grouping.
func isPhoneShapedArgument(argument string) bool {
	argument = strings.TrimSpace(argument)
	if argument == "" || len(argument) > maxCallArgumentLength {
		return false
	}
	return redactPhoneCandidate(argument, false, false) != argument
}

func redactPhoneCandidate(value string, phoneLabel, operationalLabel bool) string {
	candidate := strings.TrimSpace(value)
	if net.ParseIP(candidate) != nil || textDatePrefixPattern.MatchString(candidate) {
		return value
	}
	digits := 0
	for i := 0; i < len(candidate); i++ {
		if candidate[i] >= '0' && candidate[i] <= '9' {
			digits++
		}
	}
	if digits > maxPhoneDigits {
		// The candidate regex joins whitespace-separated numbers, so one
		// candidate can hold an ID followed by a phone. Evaluate its parts.
		return redactMergedPhoneCandidate(value, phoneLabel, operationalLabel)
	}
	if strings.Contains(candidate, ".") && !textDottedPhonePattern.MatchString(candidate) {
		return value
	}
	if digits < 8 {
		return value
	}
	compact := phoneDigits(candidate)
	explicitCountryCode := strings.HasPrefix(candidate, "+") || strings.HasPrefix(candidate, "00")
	if operationalLabel && !phoneLabel && !explicitCountryCode {
		return value
	}
	if phoneLabel && validPhonePunctuation(candidate) ||
		explicitCountryCode && plausibleInternationalPhone(candidate, compact) ||
		textDottedPhonePattern.MatchString(candidate) || textFormattedPhonePattern.MatchString(candidate) ||
		textParenAreaPhonePattern.MatchString(candidate) || plausibleIndianGroupedPhone(candidate, compact) || plausibleUKLocalPhone(candidate, compact) ||
		len(candidate) == 10 && len(compact) == 10 && compact[0] >= '6' && compact[0] <= '9' {
		return preservePhoneWhitespace(value)
	}
	return value
}

const (
	maxPhoneDigits = 15
	// A phone layout spans at most five whitespace-separated groups
	// (+CC area exchange subscriber and similar); see plausibleInternationalPhone.
	maxPhoneSpanTokens = 5
	maxPhoneSpanBytes  = 40
)

// redactMergedPhoneCandidate splits an over-long phone candidate on whitespace
// and redacts each maximal run of up to maxPhoneSpanTokens tokens that is
// phone-shaped on its own. The first run inherits the candidate's label
// context, so an ID label before the first token still keeps a non-+CC value;
// later runs are judged on shape alone. Each token starts at most
// maxPhoneSpanTokens bounded checks, so the pass is linear in the candidate.
func redactMergedPhoneCandidate(value string, phoneLabel, operationalLabel bool) string {
	type token struct{ start, end, digits int }
	tokens := make([]token, 0, 8)
	for index := 0; index < len(value); {
		if isASCIISpace(value[index]) {
			index++
			continue
		}
		start, digits := index, 0
		for index < len(value) && !isASCIISpace(value[index]) {
			if isASCIIDigit(value[index]) {
				digits++
			}
			index++
		}
		tokens = append(tokens, token{start: start, end: index, digits: digits})
	}
	if len(tokens) < 2 {
		return value // one oversized token cannot be a phone
	}
	var output strings.Builder
	output.Grow(len(value))
	last := 0
	for first := 0; first < len(tokens); {
		labelled := first == 0 && phoneLabel
		operational := first == 0 && operationalLabel
		// Prefer the longest run with a real phone layout; a phone label's
		// looser punctuation rule is only a fallback so that
		// "phone: 98765 43210 98765 43211" yields two phones, not one
		// over-long run that leaves a fragment behind.
		matchedEnd, lenientEnd := -1, -1
		digits := 0
		for next := first; next < len(tokens) && next-first < maxPhoneSpanTokens; next++ {
			digits += tokens[next].digits
			if digits > maxPhoneDigits || tokens[next].end-tokens[first].start > maxPhoneSpanBytes {
				break
			}
			if digits < 8 {
				continue
			}
			span := value[tokens[first].start:tokens[next].end]
			if redactPhoneCandidate(span, false, operational && !labelled) != span {
				matchedEnd = next
			} else if labelled && redactPhoneCandidate(span, true, false) != span {
				lenientEnd = next
			}
		}
		if matchedEnd < 0 {
			matchedEnd = lenientEnd
		}
		if matchedEnd < 0 {
			first++
			continue
		}
		output.WriteString(value[last:tokens[first].start])
		output.WriteString("[REDACTED_PHONE]")
		last = tokens[matchedEnd].end
		first = matchedEnd + 1
	}
	if last == 0 {
		return value
	}
	output.WriteString(value[last:])
	return output.String()
}

func redactBareIndianMobiles(value string) string {
	var output strings.Builder
	last := 0
	for start := 0; start+10 <= len(value); start++ {
		if value[start] < '6' || value[start] > '9' {
			continue
		}
		end := start + 10
		allDigits := true
		for index := start + 1; index < end; index++ {
			if !isASCIIDigit(value[index]) {
				allDigits = false
				break
			}
		}
		if !allDigits || start > 0 && isASCIIWord(value[start-1]) || end < len(value) && isASCIIWord(value[end]) {
			continue
		}
		// A 6-9-leading 10-digit value is phone-shaped, so the function-call
		// exception (f(...)) never applies here: Ravi(9876543210) is redacted.
		phoneLabel := hasPhoneLabelBefore(value, start)
		if hasOperationalNumericLabelBefore(value, start) && !phoneLabel {
			continue
		}
		if last == 0 {
			output.Grow(len(value))
		}
		output.WriteString(value[last:start])
		output.WriteString("[REDACTED_PHONE]")
		last = end
		start = end - 1
	}
	if last == 0 {
		return value
	}
	output.WriteString(value[last:])
	return output.String()
}

func phoneDigits(value string) string {
	var digits strings.Builder
	digits.Grow(len(value))
	for index := 0; index < len(value); index++ {
		if isASCIIDigit(value[index]) {
			digits.WriteByte(value[index])
		}
	}
	return digits.String()
}

func validPhonePunctuation(value string) bool {
	for index := 0; index < len(value); index++ {
		ch := value[index]
		if !isASCIIDigit(ch) && ch != '+' && ch != ' ' && ch != '\t' && ch != '(' && ch != ')' && ch != '-' && ch != '.' {
			return false
		}
	}
	return true
}

func plausibleInternationalPhone(value, digits string) bool {
	if len(digits) < 10 || len(digits) > 15 || !validPhonePunctuation(value) {
		return false
	}
	if strings.HasPrefix(value, "00") {
		return len(digits) >= 12
	}
	if !strings.HasPrefix(value, "+") {
		return false
	}
	// Reject arithmetic/version-like runs made exclusively from one-digit
	// groups, while retaining common country/area/subscriber layouts.
	groups := phoneDigitGroups(strings.TrimPrefix(value, "+"))
	if len(groups) > 5 {
		return false
	}
	return len(groups) == 1 || len(groups) >= 2 && len(groups[0]) <= 3
}

func plausibleIndianGroupedPhone(value, digits string) bool {
	trimmed := strings.TrimSpace(value)
	if len(digits) != 10 || digits[0] < '6' || digits[0] > '9' {
		return false
	}
	groups := phoneDigitGroups(trimmed)
	return len(groups) == 1 || len(groups) == 2 && len(groups[0]) == 5 && len(groups[1]) == 5
}

func plausibleUKLocalPhone(value, digits string) bool {
	if len(digits) != 11 || !strings.HasPrefix(digits, "0") {
		return false
	}
	groups := phoneDigitGroups(value)
	return len(groups) == 3 && len(groups[0]) >= 3 && len(groups[0]) <= 5
}

func phoneDigitGroups(value string) []string {
	groups := make([]string, 0, 4)
	start := -1
	for index := 0; index <= len(value); index++ {
		if index < len(value) && isASCIIDigit(value[index]) {
			if start < 0 {
				start = index
			}
			continue
		}
		if start >= 0 {
			groups = append(groups, value[start:index])
			start = -1
		}
	}
	return groups
}

func preservePhoneWhitespace(value string) string {
	left := len(value) - len(strings.TrimLeft(value, " \t\r\n"))
	right := len(strings.TrimRight(value, " \t\r\n"))
	return value[:left] + "[REDACTED_PHONE]" + value[right:]
}

func hasPhoneLabelBefore(value string, start int) bool {
	label := labelBeforeNumericValue(value, start)
	switch normalizeNumericLabel(label) {
	case "contactphone", "mobile", "mobilenumber", "msisdn", "phone", "phonenumber", "phones",
		"tel", "telephone", "telephonenumber", "whatsapp", "whatsappnumber":
		return true
	default:
		// Suffix matching uses the delimiter/camel-boundary rule, so all-caps
		// words such as HOTEL are not tel labels.
		for _, suffix := range [...]string{"phone", "mobile", "tel", "whatsapp", "msisdn"} {
			if hasDelimitedOrCamelLabelSuffix(label, suffix) {
				return true
			}
		}
		return false
	}
}

func hasOperationalNumericLabelBefore(value string, start int) bool {
	label := labelBeforeNumericValue(value, start)
	switch normalizeNumericLabel(label) {
	case "amount", "build", "bytes", "count", "duration", "durationms", "id", "identifier", "index",
		"invoiceid", "latency", "latencyms", "offset", "orderid", "partition", "quantity", "qty", "seq",
		"sequence", "size", "version":
		return true
	default:
		return hasDelimitedOrCamelLabelSuffix(label, "id")
	}
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
