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
// layouts can migrate to Go unchanged. The two functions mirror
// `addressFormParser` and `addressDisplayParser` for JSON-shaped inputs; native
// Go integer values additionally keep their exact decimal precision.
package international

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
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
			closing := -1
			inString := false
			escaped := false
			for j := i + 1; j < len(template); j++ {
				if inString {
					if escaped {
						escaped = false
						continue
					}
					if template[j] == '\\' {
						escaped = true
						continue
					}
					if template[j] == '"' {
						inString = false
					}
					continue
				}
				if template[j] == '"' {
					inString = true
				} else if template[j] == '{' {
					depth++
				} else if template[j] == '}' {
					depth--
				}
				if depth == 0 {
					closing = j
					break
				}
			}
			if closing < 0 {
				return nil, errors.New("international: parsing field segment: unmatched opening brace")
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
// `international.addressDisplayParser`). Values use JavaScript String coercion:
// null becomes "null", arrays join with commas, objects become
// "[object Object]", and booleans/floating-point/JSON numbers use their
// JavaScript textual form. Native Go integer kinds retain exact decimal
// precision for compatibility with existing Go callers.
func AddressDisplayParser(template string, input map[string]any) []string {
	result := template
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result = strings.Replace(result, "{"+key+"}", javascriptString(input[key]), 1)
	}
	return strings.Split(result, "_")
}

func javascriptString(value any) string {
	return javascriptReflectString(reflect.ValueOf(value), false)
}

func javascriptReflectString(value reflect.Value, arrayElement bool) string {
	if !value.IsValid() {
		if arrayElement {
			return ""
		}
		return "null"
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			if arrayElement {
				return ""
			}
			return "null"
		}
		value = value.Elem()
	}

	if value.CanInterface() {
		if number, ok := value.Interface().(json.Number); ok {
			parsed, err := strconv.ParseFloat(string(number), 64)
			if err != nil {
				return "NaN"
			}
			return javascriptFloatString(parsed, 64)
		}
	}

	switch value.Kind() {
	case reflect.String:
		return value.String()
	case reflect.Bool:
		return strconv.FormatBool(value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Go integer callers already had exact decimal formatting. Preserve that
		// precision instead of routing values above 2^53 through float64.
		return strconv.FormatInt(value.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return javascriptFloatString(value.Float(), value.Type().Bits())
	case reflect.Slice:
		if value.IsNil() {
			if arrayElement {
				return ""
			}
			return "null"
		}
		return javascriptArrayString(value)
	case reflect.Array:
		return javascriptArrayString(value)
	case reflect.Map:
		if value.IsNil() {
			if arrayElement {
				return ""
			}
			return "null"
		}
		return "[object Object]"
	case reflect.Struct:
		return "[object Object]"
	default:
		return "[object Object]"
	}
}

func javascriptArrayString(value reflect.Value) string {
	parts := make([]string, value.Len())
	for index := 0; index < value.Len(); index++ {
		parts[index] = javascriptReflectString(value.Index(index), true)
	}
	return strings.Join(parts, ",")
}

func javascriptFloatString(value float64, bits int) string {
	switch {
	case math.IsNaN(value):
		return "NaN"
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	case value == 0:
		// JavaScript String(-0) is "0".
		return "0"
	}

	// encoding/json uses the same finite-number exponent thresholds as
	// ECMAScript's number-to-string operation and normalizes exponent padding.
	var encoded []byte
	if bits == 32 {
		encoded, _ = json.Marshal(float32(value))
	} else {
		encoded, _ = json.Marshal(value)
	}
	return string(encoded)
}
