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

// Package international ports the address form/display parsing helpers from the
// Node `fit/international` module, so services relying on country-specific address
// layouts can migrate to Go unchanged. The two functions are the byte-compatible
// equivalents of `addressFormParser` and `addressDisplayParser`.
package international

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AddressField is a single address field descriptor in a form template. It is a
// free-form object (at minimum a "slug", typically also "display_name" and
// country-specific attributes); arbitrary keys are preserved through parsing.
type AddressField = map[string]any

// AddressFormParser expands a form template into rows of field objects.
//
// template holds `{slug}` placeholders; input is the list of field objects (each
// with a "slug"). Each `{slug}` is substituted by its field object, the template
// is then split into rows on "_" (each row a list of the field objects it
// contains), and empty rows are dropped. This mirrors Node
// `international.addressFormParser` — the returned rows carry the full field
// objects, so intermediate JSON key ordering (which differs between Go and Node)
// does not affect the result.
//
// Returns an error if a brace-delimited segment is not valid JSON (the Node
// version throws in the same case).
func AddressFormParser(template string, input []AddressField) ([][]AddressField, error) {
	// 1. Replace each {slug} with the JSON encoding of its field object.
	for _, d := range input {
		slug, _ := d["slug"].(string)
		if slug == "" {
			continue
		}
		b, err := json.Marshal(d)
		if err != nil {
			return nil, fmt.Errorf("international: marshalling field %q: %w", slug, err)
		}
		template = strings.ReplaceAll(template, "{"+slug+"}", string(b))
	}

	// 2. Walk the template: a `{`-delimited (brace-balanced) segment is a field
	//    object; `_` starts a new row; any other char is layout and ignored. The
	//    structural bytes {, }, _ are ASCII and never occur inside multi-byte
	//    UTF-8 sequences, so byte iteration is safe for arbitrary field content.
	rows := [][]AddressField{{}}
	current := 0
	for i := 0; i < len(template); i++ {
		switch template[i] {
		case '{':
			depth := 1
			closing := i
			for j := i + 1; j < len(template); j++ {
				if template[j] == '{' {
					depth++
				} else if template[j] == '}' {
					depth--
				}
				if depth == 0 {
					closing = j
					break
				}
			}
			var obj AddressField
			if err := json.Unmarshal([]byte(template[i:closing+1]), &obj); err != nil {
				return nil, fmt.Errorf("international: parsing field segment: %w", err)
			}
			rows[current] = append(rows[current], obj)
			i = closing
		case '_':
			current++
			rows = append(rows, []AddressField{})
		}
	}

	// 3. Drop empty rows (Node: output.filter(arr => arr.length > 0)).
	out := make([][]AddressField, 0, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			out = append(out, row)
		}
	}
	return out, nil
}

// AddressDisplayParser fills a display template's `{key}` placeholders with the
// corresponding values from input and splits the result into lines on "_". Only
// the FIRST occurrence of each `{key}` is replaced (matching Node
// `international.addressDisplayParser`). Values are coerced to their default
// string form.
func AddressDisplayParser(template string, input map[string]any) []string {
	result := template
	for key, val := range input {
		result = strings.Replace(result, "{"+key+"}", fmt.Sprintf("%v", val), 1)
	}
	return strings.Split(result, "_")
}
