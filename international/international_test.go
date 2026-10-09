// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package international

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestAddressFormParser(t *testing.T) {
	input := []AddressField{
		{"slug": "name", "display_name": "Name"},
		{"slug": "city", "display_name": "City"},
		{"slug": "state", "display_name": "State"},
	}
	// Row 1: name; Row 2: city + state (space between is layout, ignored).
	out, err := AddressFormParser("{name}_{city} {state}", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d: %#v", len(out), out)
	}
	if len(out[0]) != 1 || out[0][0]["slug"] != "name" {
		t.Errorf("row0 = %#v, want [name]", out[0])
	}
	if len(out[1]) != 2 || out[1][0]["slug"] != "city" || out[1][1]["slug"] != "state" {
		t.Errorf("row1 = %#v, want [city, state]", out[1])
	}
	// Full field object is preserved (not just the slug).
	if out[1][0]["display_name"] != "City" {
		t.Errorf("field object not preserved: %#v", out[1][0])
	}
}

func TestAddressFormParser_DropsEmptyRows(t *testing.T) {
	// Trailing/leading "_" and a "_" with no fields must not yield empty rows.
	out, err := AddressFormParser("_{name}__", []AddressField{{"slug": "name"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || out[0][0]["slug"] != "name" {
		t.Fatalf("expected 1 non-empty row, got %#v", out)
	}
}

func TestAddressFormParser_InvalidSegment(t *testing.T) {
	// A brace segment with no matching field -> not valid JSON -> error (Node throws).
	if _, err := AddressFormParser("{unknown}", nil); err == nil {
		t.Fatal("expected an error for an unresolved placeholder segment")
	}
}

func TestAddressFormParserIgnoresBracesInsideJSONString(t *testing.T) {
	input := []AddressField{{
		"slug":         "line1",
		"display_name": `Apartment {west} and \"east\"`,
	}}
	got, err := AddressFormParser("{line1}", input)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0]) != 1 || got[0][0]["display_name"] != input[0]["display_name"] {
		t.Fatalf("AddressFormParser() = %#v, want original field", got)
	}
}

func TestAddressFormParserRejectsUnmatchedOpeningBrace(t *testing.T) {
	if _, err := AddressFormParser(`{"slug":"broken"`, nil); err == nil {
		t.Fatal("AddressFormParser accepted unmatched opening brace")
	}
}

func TestAddressDisplayParser(t *testing.T) {
	got := AddressDisplayParser("{name}_{line1}_{city}", map[string]any{
		"name":  "John Doe",
		"line1": "1 Main St",
		"city":  "NYC",
	})
	want := []string{"John Doe", "1 Main St", "NYC"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestAddressDisplayParser_FirstOccurrenceOnly(t *testing.T) {
	// Node replaces only the first occurrence of each {key}.
	got := AddressDisplayParser("{x} {x}", map[string]any{"x": "A"})
	if !reflect.DeepEqual(got, []string{"A {x}"}) {
		t.Fatalf("got %#v, want [\"A {x}\"]", got)
	}
}

func TestAddressDisplayParser_CoercesValues(t *testing.T) {
	got := AddressDisplayParser("{zip}", map[string]any{"zip": 560001})
	if !reflect.DeepEqual(got, []string{"560001"}) {
		t.Fatalf("got %#v, want [\"560001\"]", got)
	}
}

func TestAddressDisplayParserUsesJavaScriptStringCoercion(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "null", value: nil, want: "null"},
		{name: "array", value: []any{"a", "b"}, want: "a,b"},
		{name: "nested array", value: []any{1, nil, []any{2, 3}, map[string]any{"x": 1}}, want: "1,,2,3,[object Object]"},
		{name: "object", value: map[string]any{"a": 1}, want: "[object Object]"},
		{name: "boolean true", value: true, want: "true"},
		{name: "boolean false", value: false, want: "false"},
		{name: "large signed integer remains exact", value: int64(9223372036854775807), want: "9223372036854775807"},
		{name: "large unsigned integer remains exact", value: uint64(18446744073709551615), want: "18446744073709551615"},
		{name: "JSON number uses JavaScript precision", value: json.Number("9007199254740993"), want: "9007199254740992"},
		{name: "negative zero", value: math.Copysign(0, -1), want: "0"},
		{name: "small exponent", value: 1e-7, want: "1e-7"},
		{name: "small decimal threshold", value: 1e-6, want: "0.000001"},
		{name: "large decimal threshold", value: 1e20, want: "100000000000000000000"},
		{name: "large exponent", value: 1e21, want: "1e+21"},
		{name: "positive infinity", value: math.Inf(1), want: "Infinity"},
		{name: "negative infinity", value: math.Inf(-1), want: "-Infinity"},
		{name: "not a number", value: math.NaN(), want: "NaN"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := AddressDisplayParser("{value}", map[string]any{"value": test.value})
			if !reflect.DeepEqual(got, []string{test.want}) {
				t.Fatalf("AddressDisplayParser() = %#v; want %#v", got, []string{test.want})
			}
		})
	}
}
