// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestApplySchemaCoercesTypesAndAtomicallyAppliesDefaults(t *testing.T) {
	minimum := 1.0
	maximum := 10.0
	minimumLength := 2
	config := New()
	config.Set("PORT", "5")
	config.Set("ENABLED", "yes")
	config.Set("TIMEOUT", "250ms")
	config.Set("TAGS", `["one","two"]`)
	config.Set("METADATA", `{"region":"in"}`)
	schema, err := NewSchema(
		SchemaField{Key: "PORT", Kind: IntKind, Required: true, Min: &minimum, Max: &maximum},
		SchemaField{Key: "ENABLED", Kind: BoolKind},
		SchemaField{Key: "TIMEOUT", Kind: DurationKind},
		SchemaField{Key: "TAGS", Kind: StringSliceKind, MinLength: &minimumLength},
		SchemaField{Key: "METADATA", Kind: JSONKind},
		SchemaField{Key: "MODE", Kind: StringKind, HasDefault: true, Default: "safe", AllowedValues: []string{"safe", "fast"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := config.ApplySchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	assertResolved(t, resolved, "PORT", 5)
	assertResolved(t, resolved, "ENABLED", true)
	assertResolved(t, resolved, "TIMEOUT", 250*time.Millisecond)
	assertResolved(t, resolved, "TAGS", []string{"one", "two"})
	assertResolved(t, resolved, "MODE", "safe")
	if value := config.GetString("MODE", ""); value != "safe" {
		t.Fatalf("schema default was not installed: %q", value)
	}
}

func TestApplySchemaDoesNotInstallDefaultsOnValidationFailure(t *testing.T) {
	config := New()
	config.Set("PORT", "invalid")
	schema, err := NewSchema(
		SchemaField{Key: "PORT", Kind: IntKind, Required: true},
		SchemaField{Key: "MODE", HasDefault: true, Default: "safe"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.ApplySchema(schema); err == nil {
		t.Fatal("ApplySchema accepted invalid input")
	}
	if config.Has("MODE") {
		t.Fatal("ApplySchema installed a default after validation failed")
	}
}

func TestApplySchemaValidatesPatternAllowedRangeLengthAndCustomFormat(t *testing.T) {
	minimum := 3.0
	maximumLength := 4
	config := New()
	config.Set("CODE", "ab")
	config.Set("COUNT", "2")
	config.Set("MODE", "danger")
	config.Set("CUSTOM", "bad")
	schema, err := NewSchema(
		SchemaField{Key: "CODE", Pattern: `^[A-Z]+$`, MaxLength: &maximumLength},
		SchemaField{Key: "COUNT", Kind: IntKind, Min: &minimum},
		SchemaField{Key: "MODE", AllowedValues: []string{"safe"}},
		SchemaField{
			Key: "CUSTOM", Parser: func(raw string) (any, error) {
				if raw != "ok" {
					return nil, errors.New("not ok")
				}
				return 99, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = config.ApplySchema(schema)
	validation, ok := err.(*ValidationError)
	if !ok || len(validation.Errors) != 4 {
		t.Fatalf("ApplySchema error = %#v", err)
	}
}

func TestApplySchemaRedactsSensitiveValidationDetails(t *testing.T) {
	config := New()
	secret := "super-secret-value"
	config.Set("TOKEN", secret)
	schema, err := NewSchema(SchemaField{
		Key: "TOKEN", Sensitive: true,
		Validate: func(value any) error { return errors.New("rejected " + value.(string)) },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = config.ApplySchema(schema)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("sensitive validation error leaked value: %v", err)
	}
}

func TestSchemaCrossFieldValidationAndDefensiveCopies(t *testing.T) {
	config := New()
	config.Set("MIN", "5")
	config.Set("MAX", "4")
	schema, err := NewSchema(
		SchemaField{Key: "MIN", Kind: IntKind},
		SchemaField{Key: "MAX", Kind: IntKind},
	)
	if err != nil {
		t.Fatal(err)
	}
	schema = schema.WithValidationMessage("MIN must not exceed MAX", func(resolved *Resolved) error {
		min, _ := resolved.Value("MIN")
		max, _ := resolved.Value("MAX")
		if min.(int) > max.(int) {
			return errors.New("MIN must not exceed MAX")
		}
		return nil
	})
	if _, err := config.ApplySchema(schema); err == nil || !strings.Contains(err.Error(), "MIN must not exceed MAX") {
		t.Fatalf("cross-field validation error = %v", err)
	}

	config.Set("MAX", "6")
	resolved, err := config.ApplySchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	values := resolved.Values()
	values["MIN"] = 100
	value, _ := resolved.Value("MIN")
	if value != 5 {
		t.Fatal("Values exposed mutable resolved state")
	}
}

func TestSchemaCrossFieldValidationRedactsReturnedError(t *testing.T) {
	config := New()
	secret := "cross-field-secret"
	config.Set("TOKEN", secret)
	schema, err := NewSchema(SchemaField{Key: "TOKEN", Sensitive: true})
	if err != nil {
		t.Fatal(err)
	}
	schema = schema.WithValidation(func(resolved *Resolved) error {
		value, _ := resolved.Value("TOKEN")
		return errors.New("rejected " + value.(string))
	})
	_, err = config.ApplySchema(schema)
	if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "cross-field validation failed") {
		t.Fatalf("cross-field validation error = %v", err)
	}
}

func TestNewSchemaRejectsInvalidDefinitionsAndDefaults(t *testing.T) {
	for name, fields := range map[string][]SchemaField{
		"duplicate": {{Key: "ONE"}, {Key: "ONE"}},
		"pattern":   {{Key: "ONE", Pattern: "["}},
		"kind":      {{Key: "ONE", Kind: "unknown"}},
		"default":   {{Key: "ONE", Kind: IntKind, HasDefault: true, Default: "not-int"}},
		"default constraint": {{
			Key: "ONE", HasDefault: true, Default: "unsafe", AllowedValues: []string{"safe"},
		}},
		"empty required default": {{Key: "ONE", Required: true, HasDefault: true, Default: ""}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewSchema(fields...); err == nil {
				t.Fatalf("NewSchema accepted invalid %s definition", name)
			}
		})
	}
}

func TestSchemaDefinitionIsIndependentOfCallerMutation(t *testing.T) {
	minimum := 1.0
	allowed := []string{"safe"}
	defaultJSON := map[string]any{"mode": "safe"}
	schema, err := NewSchema(
		SchemaField{Key: "COUNT", Kind: IntKind, Min: &minimum},
		SchemaField{Key: "MODE", HasDefault: true, Default: "safe", AllowedValues: allowed},
		SchemaField{Key: "JSON", Kind: JSONKind, HasDefault: true, Default: defaultJSON},
	)
	if err != nil {
		t.Fatal(err)
	}
	minimum = 100
	allowed[0] = "changed"
	defaultJSON["mode"] = "changed"

	config := New()
	config.Set("COUNT", "5")
	resolved, err := config.ApplySchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	assertResolved(t, resolved, "MODE", "safe")
	assertResolved(t, resolved, "JSON", map[string]any{"mode": "safe"})
}

func TestSchemaRejectsNonFiniteNumbers(t *testing.T) {
	config := New()
	config.Set("FLOAT", "NaN")
	config.Set("CUSTOM", "anything")
	schema, err := NewSchema(
		SchemaField{Key: "FLOAT", Kind: FloatKind},
		SchemaField{Key: "CUSTOM", Parser: func(string) (any, error) { return math.Inf(1), nil }},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = config.ApplySchema(schema)
	validation, ok := err.(*ValidationError)
	if !ok || len(validation.Errors) != 2 {
		t.Fatalf("ApplySchema error = %#v", err)
	}
}

func TestSchemaOwnsMutableCustomParserValues(t *testing.T) {
	captured := map[string][]byte{"token": []byte("original")}
	config := New()
	config.Set("CUSTOM", "value")
	schema, err := NewSchema(SchemaField{
		Key: "CUSTOM",
		Parser: func(string) (any, error) {
			return captured, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := config.ApplySchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	captured["token"][0] = 'X'
	first, ok := resolved.Value("CUSTOM")
	if !ok || string(first.(map[string][]byte)["token"]) != "original" {
		t.Fatalf("resolved custom value = %#v", first)
	}
	first.(map[string][]byte)["token"][0] = 'Y'
	second, _ := resolved.Value("CUSTOM")
	if string(second.(map[string][]byte)["token"]) != "original" {
		t.Fatalf("Value exposed mutable custom state: %#v", second)
	}
}

func TestSchemaRejectsDurationMillisecondOverflow(t *testing.T) {
	config := New()
	config.Set("TIMEOUT", "9223372036854775807")
	schema, err := NewSchema(SchemaField{Key: "TIMEOUT", Kind: DurationKind})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.ApplySchema(schema); err == nil {
		t.Fatal("ApplySchema accepted overflowing millisecond duration")
	}
}

func TestApplySchemaRejectsConcurrentConfigChange(t *testing.T) {
	config := New()
	config.Set("VALUE", "before")
	parserStarted := make(chan struct{})
	parserContinue := make(chan struct{})
	var once sync.Once
	schema, err := NewSchema(
		SchemaField{
			Key: "VALUE",
			Parser: func(raw string) (any, error) {
				once.Do(func() { close(parserStarted) })
				<-parserContinue
				return raw, nil
			},
		},
		SchemaField{Key: "DEFAULT", HasDefault: true, Default: "safe"},
	)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := config.ApplySchema(schema)
		result <- err
	}()
	<-parserStarted
	config.Set("VALUE", "after")
	close(parserContinue)
	if err := <-result; err == nil || !strings.Contains(err.Error(), "changed during schema validation") {
		t.Fatalf("ApplySchema error = %v", err)
	}
	if config.Has("DEFAULT") {
		t.Fatal("ApplySchema installed defaults after a concurrent change")
	}
}

func assertResolved(t *testing.T, resolved *Resolved, key string, expected any) {
	t.Helper()
	actual, exists := resolved.Value(key)
	if !exists || !reflect.DeepEqual(actual, expected) {
		t.Fatalf("resolved %s = %#v, %v; want %#v", key, actual, exists, expected)
	}
}
