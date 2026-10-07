// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ValueKind selects strict schema coercion.
type ValueKind string

const (
	StringKind      ValueKind = "string"
	IntKind         ValueKind = "int"
	Int64Kind       ValueKind = "int64"
	FloatKind       ValueKind = "float"
	BoolKind        ValueKind = "bool"
	DurationKind    ValueKind = "duration"
	StringSliceKind ValueKind = "string_slice"
	JSONKind        ValueKind = "json"
)

// Parser implements an application-specific Convict/Pydantic-style format.
type Parser func(string) (any, error)

// ValueCloner returns an independent copy of a parsed value. Supply this for
// custom types that contain unexported mutable state and therefore cannot be
// cloned safely through reflection.
type ValueCloner func(any) (any, error)

// ValueValidator validates one parsed value.
type ValueValidator func(any) error

// SchemaField defines one config key. Defaults are applied only when the key is
// absent; an explicitly empty environment value remains empty and can fail a
// required/type constraint.
type SchemaField struct {
	Key           string
	Kind          ValueKind
	Required      bool
	Default       any
	HasDefault    bool
	AllowedValues []string
	Pattern       string
	Min           *float64
	Max           *float64
	MinLength     *int
	MaxLength     *int
	Parser        Parser
	Clone         ValueCloner
	Validate      ValueValidator
	Description   string
	Sensitive     bool

	compiledPattern *regexp.Regexp
	defaultRaw      string
}

// Schema validates and resolves a fixed collection of config fields.
type Schema struct {
	fields            []SchemaField
	validate          func(*Resolved) error
	validationMessage string
}

// NewSchema validates schema definitions before any application config is read.
func NewSchema(fields ...SchemaField) (*Schema, error) {
	if len(fields) == 0 {
		return nil, errors.New("config: schema has no fields")
	}
	seen := make(map[string]struct{}, len(fields))
	compiled := make([]SchemaField, len(fields))
	for index, field := range fields {
		field.AllowedValues = append([]string(nil), field.AllowedValues...)
		field.Min = cloneFloatPointer(field.Min)
		field.Max = cloneFloatPointer(field.Max)
		field.MinLength = cloneIntPointer(field.MinLength)
		field.MaxLength = cloneIntPointer(field.MaxLength)
		field.Key = strings.TrimSpace(field.Key)
		if field.Key == "" {
			return nil, errors.New("config: schema field has empty key")
		}
		if _, duplicate := seen[field.Key]; duplicate {
			return nil, fmt.Errorf("config: duplicate schema field %q", field.Key)
		}
		seen[field.Key] = struct{}{}
		if field.Kind == "" {
			field.Kind = StringKind
		}
		if field.Parser == nil && !validKind(field.Kind) {
			return nil, fmt.Errorf("config: field %q has unsupported kind %q", field.Key, field.Kind)
		}
		if field.Pattern != "" {
			pattern, err := regexp.Compile(field.Pattern)
			if err != nil {
				return nil, fmt.Errorf("config: field %q has invalid pattern: %w", field.Key, err)
			}
			field.compiledPattern = pattern
		}
		if field.Min != nil && field.Max != nil && *field.Min > *field.Max {
			return nil, fmt.Errorf("config: field %q min exceeds max", field.Key)
		}
		if (field.MinLength != nil && *field.MinLength < 0) ||
			(field.MaxLength != nil && *field.MaxLength < 0) {
			return nil, fmt.Errorf("config: field %q has negative length constraint", field.Key)
		}
		if field.MinLength != nil && field.MaxLength != nil && *field.MinLength > *field.MaxLength {
			return nil, fmt.Errorf("config: field %q minimum length exceeds maximum length", field.Key)
		}
		if field.HasDefault {
			raw, err := stringifyDefault(field.Default)
			if err != nil {
				return nil, fmt.Errorf("config: field %q default: %w", field.Key, err)
			}
			parsed, err := parseSchemaValue(field, raw)
			if err != nil {
				return nil, fmt.Errorf("config: field %q default: %w", field.Key, err)
			}
			parsed, err = cloneSchemaValue(field, parsed)
			if err != nil {
				return nil, fmt.Errorf("config: field %q default cannot be owned safely: %w", field.Key, err)
			}
			if violation := validateSchemaValue(field, raw, parsed); violation != "" {
				return nil, fmt.Errorf("config: field %q default %s", field.Key, violation)
			}
			if field.Required && strings.TrimSpace(raw) == "" {
				return nil, fmt.Errorf("config: field %q required default must not be empty", field.Key)
			}
			field.defaultRaw = raw
		}
		compiled[index] = field
	}
	return &Schema{fields: compiled}, nil
}

// WithValidation adds an immutable cross-field validator.
func (schema *Schema) WithValidation(validate func(*Resolved) error) *Schema {
	return schema.WithValidationMessage("cross-field validation failed", validate)
}

// WithValidationMessage adds an immutable cross-field validator with a static,
// privacy-reviewed error message. The validator's own error is never exposed.
func (schema *Schema) WithValidationMessage(message string, validate func(*Resolved) error) *Schema {
	if schema == nil {
		return nil
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "cross-field validation failed"
	}
	copy := &Schema{
		fields:            append([]SchemaField(nil), schema.fields...),
		validate:          validate,
		validationMessage: message,
	}
	return copy
}

// Resolved contains strictly parsed, schema-owned values.
type Resolved struct {
	values  map[string]any
	raw     map[string]string
	cloners map[string]ValueCloner
}

// Value returns one parsed value.
func (resolved *Resolved) Value(key string) (any, bool) {
	if resolved == nil {
		return nil, false
	}
	value, exists := resolved.values[key]
	if !exists {
		return nil, false
	}
	copy, err := cloneValue(resolved.cloners[key], value)
	return copy, err == nil
}

// Raw returns the resolved raw value after defaults.
func (resolved *Resolved) Raw(key string) (string, bool) {
	if resolved == nil {
		return "", false
	}
	value, exists := resolved.raw[key]
	return value, exists
}

// Values returns a copy of every parsed value.
func (resolved *Resolved) Values() map[string]any {
	if resolved == nil {
		return nil
	}
	values := make(map[string]any, len(resolved.values))
	for key, value := range resolved.values {
		copy, err := cloneValue(resolved.cloners[key], value)
		if err != nil {
			return nil
		}
		values[key] = copy
	}
	return values
}

// ApplySchema validates a snapshot, then atomically installs schema defaults
// for keys that remain absent. Existing process/config values are never changed.
func (config *Config) ApplySchema(schema *Schema) (*Resolved, error) {
	if config == nil {
		return nil, errors.New("config: nil Config")
	}
	if schema == nil {
		return nil, errors.New("config: nil Schema")
	}
	config.mu.RLock()
	snapshot := make(map[string]string, len(config.values))
	for key, value := range config.values {
		snapshot[key] = value
	}
	config.mu.RUnlock()

	defaults := make(map[string]string)
	resolved := &Resolved{
		values:  make(map[string]any),
		raw:     make(map[string]string),
		cloners: make(map[string]ValueCloner),
	}
	var violations []string
	for _, field := range schema.fields {
		raw, exists := snapshot[field.Key]
		if !exists && field.HasDefault {
			raw = field.defaultRaw
			exists = true
			defaults[field.Key] = raw
		}
		if !exists {
			if field.Required {
				violations = append(violations, schemaMessage(field, "is required"))
			}
			continue
		}
		if field.Required && strings.TrimSpace(raw) == "" {
			violations = append(violations, schemaMessage(field, "must not be empty"))
			continue
		}
		parsed, err := parseSchemaValue(field, raw)
		if err != nil {
			violations = append(violations, schemaMessage(field, "has invalid "+string(field.Kind)+" value"))
			continue
		}
		parsed, err = cloneSchemaValue(field, parsed)
		if err != nil {
			violations = append(violations, schemaMessage(field, "returned a value that cannot be owned safely"))
			continue
		}
		if violation := validateSchemaValue(field, raw, parsed); violation != "" {
			violations = append(violations, schemaMessage(field, violation))
			continue
		}
		resolved.raw[field.Key] = raw
		resolved.values[field.Key] = parsed
		resolved.cloners[field.Key] = field.Clone
	}
	if len(violations) > 0 {
		return nil, &ValidationError{Errors: violations}
	}
	if schema.validate != nil {
		if err := schema.validate(resolved); err != nil {
			return nil, &ValidationError{Errors: []string{schema.validationMessage}}
		}
	}

	config.mu.Lock()
	for _, field := range schema.fields {
		previous, previouslyExists := snapshot[field.Key]
		current, currentlyExists := config.values[field.Key]
		if previouslyExists != currentlyExists || (previouslyExists && previous != current) {
			config.mu.Unlock()
			return nil, fmt.Errorf("config: key %q changed during schema validation", field.Key)
		}
	}
	for key, value := range defaults {
		if _, exists := config.values[key]; !exists {
			config.values[key] = value
		}
	}
	config.mu.Unlock()
	return resolved, nil
}

func cloneFloatPointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func parseSchemaValue(field SchemaField, raw string) (any, error) {
	if field.Parser != nil {
		return field.Parser(raw)
	}
	switch field.Kind {
	case StringKind:
		return raw, nil
	case IntKind:
		return strconv.Atoi(raw)
	case Int64Kind:
		return strconv.ParseInt(raw, 10, 64)
	case FloatKind:
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, errors.New("invalid finite float")
		}
		return value, nil
	case BoolKind:
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "true", "1", "yes", "on":
			return true, nil
		case "false", "0", "no", "off":
			return false, nil
		default:
			return nil, errors.New("invalid boolean")
		}
	case DurationKind:
		if duration, err := time.ParseDuration(raw); err == nil {
			return duration, nil
		}
		milliseconds, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
		const (
			maxInt64 = int64(^uint64(0) >> 1)
			minInt64 = -maxInt64 - 1
			scale    = int64(time.Millisecond)
		)
		if milliseconds > maxInt64/scale || milliseconds < minInt64/scale {
			return nil, errors.New("duration milliseconds overflow time.Duration")
		}
		return time.Duration(milliseconds * scale), nil
	case StringSliceKind:
		var values []string
		if strings.HasPrefix(strings.TrimSpace(raw), "[") {
			if err := json.Unmarshal([]byte(raw), &values); err != nil {
				return nil, err
			}
			return values, nil
		}
		for _, value := range strings.Split(raw, ",") {
			if value = strings.TrimSpace(value); value != "" {
				values = append(values, value)
			}
		}
		return values, nil
	case JSONKind:
		var value any
		if err := json.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported kind %q", field.Kind)
	}
}

func validateSchemaValue(field SchemaField, raw string, parsed any) string {
	if len(field.AllowedValues) > 0 {
		allowed := false
		for _, value := range field.AllowedValues {
			if strings.EqualFold(raw, value) {
				allowed = true
				break
			}
		}
		if !allowed {
			return "is not an allowed value"
		}
	}
	if field.compiledPattern != nil && !field.compiledPattern.MatchString(raw) {
		return "does not match the required pattern"
	}
	length := valueLength(parsed)
	if field.MinLength != nil && length >= 0 && length < *field.MinLength {
		return fmt.Sprintf("length must be at least %d", *field.MinLength)
	}
	if field.MaxLength != nil && length >= 0 && length > *field.MaxLength {
		return fmt.Sprintf("length must be at most %d", *field.MaxLength)
	}
	if number, numeric := numericValue(parsed); numeric {
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return "must be finite"
		}
		if field.Min != nil && number < *field.Min {
			return fmt.Sprintf("must be at least %v", *field.Min)
		}
		if field.Max != nil && number > *field.Max {
			return fmt.Sprintf("must be at most %v", *field.Max)
		}
	}
	if field.Validate != nil {
		copy, cloneErr := cloneSchemaValue(field, parsed)
		if cloneErr != nil {
			return "could not be copied for custom validation"
		}
		if err := field.Validate(copy); err != nil {
			if field.Sensitive {
				return "failed custom validation"
			}
			return "failed custom validation: " + err.Error()
		}
	}
	return ""
}

func validKind(kind ValueKind) bool {
	switch kind {
	case StringKind, IntKind, Int64Kind, FloatKind, BoolKind, DurationKind, StringSliceKind, JSONKind:
		return true
	default:
		return false
	}
}

func stringifyDefault(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	case time.Duration:
		return typed.String(), nil
	case fmt.Stringer:
		return typed.String(), nil
	case nil:
		return "", nil
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
}

func numericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case time.Duration:
		return float64(typed), true
	default:
		return 0, false
	}
}

func valueLength(value any) int {
	switch typed := value.(type) {
	case string:
		return len([]rune(typed))
	case []string:
		return len(typed)
	default:
		reflected := reflect.ValueOf(value)
		if reflected.IsValid() && (reflected.Kind() == reflect.Array || reflected.Kind() == reflect.Map || reflected.Kind() == reflect.Slice) {
			return reflected.Len()
		}
		return -1
	}
}

func schemaMessage(field SchemaField, message string) string {
	result := fmt.Sprintf("key %q %s", field.Key, message)
	if field.Description != "" {
		result += " (" + field.Description + ")"
	}
	return result
}

func cloneSchemaValue(field SchemaField, value any) (any, error) {
	return cloneValue(field.Clone, value)
}

func cloneValue(cloner ValueCloner, value any) (any, error) {
	if cloner != nil {
		return cloner(value)
	}
	cloned, err := cloneReflectValue(reflect.ValueOf(value), make(map[cloneVisit]reflect.Value))
	if err != nil {
		return nil, err
	}
	if !cloned.IsValid() {
		return nil, nil
	}
	return cloned.Interface(), nil
}

type cloneVisit struct {
	typeOf  reflect.Type
	kind    reflect.Kind
	pointer uintptr
	length  int
}

var timeType = reflect.TypeOf(time.Time{})

func cloneReflectValue(value reflect.Value, visited map[cloneVisit]reflect.Value) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		cloned, err := cloneReflectValue(value.Elem(), visited)
		if err != nil {
			return reflect.Value{}, err
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		visit := cloneVisit{typeOf: value.Type(), kind: value.Kind(), pointer: value.Pointer()}
		if previous, exists := visited[visit]; exists {
			return previous, nil
		}
		result := reflect.New(value.Type().Elem())
		visited[visit] = result
		cloned, err := cloneReflectValue(value.Elem(), visited)
		if err != nil {
			return reflect.Value{}, err
		}
		result.Elem().Set(cloned)
		return result, nil
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		visit := cloneVisit{typeOf: value.Type(), kind: value.Kind(), pointer: uintptr(value.UnsafePointer()), length: value.Len()}
		if previous, exists := visited[visit]; exists {
			return previous, nil
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		visited[visit] = result
		iterator := value.MapRange()
		for iterator.Next() {
			key, err := cloneReflectValue(iterator.Key(), visited)
			if err != nil {
				return reflect.Value{}, err
			}
			entry, err := cloneReflectValue(iterator.Value(), visited)
			if err != nil {
				return reflect.Value{}, err
			}
			result.SetMapIndex(key, entry)
		}
		return result, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		visit := cloneVisit{typeOf: value.Type(), kind: value.Kind(), pointer: value.Pointer(), length: value.Len()}
		if previous, exists := visited[visit]; exists {
			return previous, nil
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		visited[visit] = result
		for index := 0; index < value.Len(); index++ {
			cloned, err := cloneReflectValue(value.Index(index), visited)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(index).Set(cloned)
		}
		return result, nil
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for index := 0; index < value.Len(); index++ {
			cloned, err := cloneReflectValue(value.Index(index), visited)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(index).Set(cloned)
		}
		return result, nil
	case reflect.Struct:
		if value.Type() == timeType {
			return value, nil
		}
		result := reflect.New(value.Type()).Elem()
		for index := 0; index < value.NumField(); index++ {
			field := value.Type().Field(index)
			if field.PkgPath != "" {
				return reflect.Value{}, fmt.Errorf("type %s has unexported field %s; provide SchemaField.Clone", value.Type(), field.Name)
			}
			cloned, err := cloneReflectValue(value.Field(index), visited)
			if err != nil {
				return reflect.Value{}, err
			}
			result.Field(index).Set(cloned)
		}
		return result, nil
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		return value, nil
	default:
		return reflect.Value{}, fmt.Errorf("type %s cannot be cloned safely; provide SchemaField.Clone", value.Type())
	}
}
