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

package server

import (
	"encoding/json"
	"fmt"
	"strings"
)

// AddressFormParser expands a template into rows of address fields.
// Deprecated: use international.AddressFormParser, which reports malformed
// templates instead of silently discarding them.
func AddressFormParser(template string, input []map[string]interface{}) [][]map[string]interface{} {
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

	output := [][]map[string]interface{}{
		{},
	}
	currentRow := 0

	i := 0
	for i < len(template) {
		ch := template[i]

		if ch == '{' {
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
			currentRow++
			output = append(output, []map[string]interface{}{})
			i++
		} else {
			i++
		}
	}

	var result [][]map[string]interface{}
	for _, row := range output {
		if len(row) > 0 {
			result = append(result, row)
		}
	}

	return result
}

// AddressDisplayParser expands a display template and splits it into lines.
// Deprecated: use international.AddressDisplayParser.
func AddressDisplayParser(template string, input map[string]interface{}) []string {
	result := template

	for key, value := range input {
		placeholder := "{" + key + "}"
		strValue := formatDisplayValue(value)
		result = strings.ReplaceAll(result, placeholder, strValue)
	}

	return strings.Split(result, "_")
}

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
		b, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

func formatFloat(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}
	s := fmt.Sprintf("%f", f)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}
