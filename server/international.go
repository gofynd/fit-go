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

// International address parsing for the fit.go framework.
//
// templated address form parsing and display formatting for international
// address handling across Fynd Commerce.
//
// Address form templates use {slug} placeholders that are replaced with JSON
// objects. Underscore characters (_) act as row separators, producing a nested
// array structure suitable for rendering multi-row address forms.
//
// Display templates use {key} placeholders that are replaced with actual values,
// with underscores acting as line separators.
package server

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AddressFormParser parses a templated address form string and returns a
// nested array structure. Each top-level element represents a row of fields,
// and each field is a map of properties (slug, display_name, etc.).
//
// Template format: "{slug1}{slug2}_{slug3}_{slug4}{slug5}"
// - {slug} placeholders are replaced with the matching input object
// - Underscores (_) separate rows
// - Multiple fields in the same row are adjacent without separators
//
// Parameters:
// - template: the address form template string with {slug} placeholders
// - input: slice of field definitions, each must have a "slug" key
//
// Returns a nested slice where each element is a row of field definition maps.
//
// Example:
//
//	template := "{firstName}{lastName}_{city}_{pincode}"
//	input := []map[string]interface{}{
//	 {"slug": "firstName", "display_name": "First Name", "required": true},
//	 {"slug": "lastName", "display_name": "Last Name", "required": true},
//	 {"slug": "city", "display_name": "City", "required": true},
//	 {"slug": "pincode", "display_name": "Pincode", "required": true},
//	}
//	result := AddressFormParser(template, input)
//	// result[0] = [{slug: firstName...}, {slug: lastName...}]
//	// result[1] = [{slug: city...}]
//	// result[2] = [{slug: pincode...}]
func AddressFormParser(template string, input []map[string]interface{}) [][]map[string]interface{} {
	// Phase 1: Replace {slug} placeholders with their JSON-serialized objects.
	for _, field := range input {
		slug, ok := field["slug"].(string)
		if !ok {
			continue
		}
		placeholder := "{" + slug + "}"
		serialized, err := json.Marshal(field)
		if err != nil {
			continue
		}
		template = strings.ReplaceAll(template, placeholder, string(serialized))
	}

	// Phase 2: Parse the resulting string into a nested structure.
	// Walk through the template character by character, extracting JSON objects
	// and splitting on underscore row separators.
	output := [][]map[string]interface{}{
		{},
	}
	currentRow := 0

	i := 0
	for i < len(template) {
		ch := template[i]

		if ch == '{' {
			// Find the matching closing brace by counting brace depth.
			braceCount := 1
			closingIdx := i
			for j := i + 1; j < len(template); j++ {
				if template[j] == '{' {
					braceCount++
				} else if template[j] == '}' {
					braceCount--
				}
				if braceCount == 0 {
					closingIdx = j
					break
				}
			}

			objStr := template[i : closingIdx+1]
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(objStr), &obj); err == nil {
				output[currentRow] = append(output[currentRow], obj)
			}
			i = closingIdx + 1
		} else if ch == '_' {
			// Underscore marks a new row.
			currentRow++
			output = append(output, []map[string]interface{}{})
			i++
		} else {
			i++
		}
	}

	// Filter out empty rows (matching JS .filter(arr => arr.length > 0)).
	var result [][]map[string]interface{}
	for _, row := range output {
		if len(row) > 0 {
			result = append(result, row)
		}
	}

	return result
}

// AddressDisplayParser parses a display template and returns lines of
// formatted address text. Placeholders {key} are replaced with corresponding
// values from the input map, and underscores split the result into lines.
//
// Parameters:
// - template: the display template string, e.g. "{name}_{address1}_{city}, {state} {pincode}"
// - input: key-value map of field values to substitute
//
// Returns a slice of strings, one per display line.
//
// Example:
//
//	template := "{name}_{address1}_{city}, {state} {pincode}"
//	input := map[string]interface{}{
//	 "name": "John Doe",
//	 "address1": "123 Main St",
//	 "city": "Mumbai",
//	 "state": "Maharashtra",
//	 "pincode": "400001",
//	}
//	result := AddressDisplayParser(template, input)
//	// result = ["John Doe", "123 Main St", "Mumbai, Maharashtra 400001"]
func AddressDisplayParser(template string, input map[string]interface{}) []string {
	result := template

	for key, value := range input {
		placeholder := "{" + key + "}"
		strValue := formatDisplayValue(value)
		result = strings.ReplaceAll(result, placeholder, strValue)
	}

	return strings.Split(result, "_")
}

// formatDisplayValue converts an interface{} value to its string representation
// for display purposes.
func formatDisplayValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return formatFloat(val)
	case json.Number:
		return val.String()
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		// For complex types, JSON-encode them.
		b, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// formatFloat formats a float64 to a clean string representation.
// Integers are rendered without a decimal point; floats have trailing
// zeros removed.
func formatFloat(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}
	s := fmt.Sprintf("%f", f)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}
