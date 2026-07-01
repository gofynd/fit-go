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
	"net/url"
	"sort"
	"strings"
)

// Mask is the placeholder substituted for a redacted value.
const Mask = "[REDACTED]"

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
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"set-cookie":          true,
	"x-api-key":           true,
	"x-auth-token":        true,
	"x-access-token":      true,
	"x-csrf-token":        true,
	"x-xsrf-token":        true,
	"api-key":             true,
	"authentication":      true,
}

// SafeURL renders a URL for logging as scheme://host/path, dropping the query
// string and any userinfo (user:password@). Nil-safe.
func SafeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	var b strings.Builder
	if u.Scheme != "" {
		b.WriteString(u.Scheme)
		b.WriteString("://")
	}
	b.WriteString(u.Host) // Host excludes userinfo
	if u.Path == "" {
		b.WriteByte('/')
	} else {
		b.WriteString(u.Path)
	}
	return b.String()
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
	return sensitiveHeaders[strings.ToLower(strings.TrimSpace(name))]
}

// HeaderValue returns value, or Mask when name is a sensitive header.
func HeaderValue(name, value string) string {
	if IsSensitiveHeader(name) {
		return Mask
	}
	return value
}
