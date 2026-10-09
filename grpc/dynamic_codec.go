// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package grpc

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// protobufMessageToMap exposes the stable, language-neutral part of FIT.js'
// proto-loader handler contract: proto field names, default scalar/list/map
// values, string int64/enums, byte slices, and oneof discriminator fields.
func protobufMessageToMap(message proto.Message) (map[string]interface{}, error) {
	if message == nil {
		return nil, nil
	}
	value, err := dynamicMessageValue(message.ProtoReflect())
	if err != nil {
		return nil, err
	}
	result, ok := value.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("grpc: top-level request %s is not an object", message.ProtoReflect().Descriptor().FullName())
	}
	return result, nil
}

func dynamicMessageValue(message protoreflect.Message) (interface{}, error) {
	if isJSONWellKnownType(message.Descriptor().FullName()) {
		encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message.Interface())
		if err != nil {
			return nil, err
		}
		var decoded interface{}
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	}

	result := make(map[string]interface{})
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if oneof := field.ContainingOneof(); oneof != nil && message.WhichOneof(oneof) != field {
			continue
		}
		if field.HasPresence() && !message.Has(field) {
			if field.Message() != nil {
				result[string(field.Name())] = nil
			}
			continue
		}
		value, err := dynamicFieldValue(field, message.Get(field))
		if err != nil {
			return nil, fmt.Errorf("grpc: decode field %s: %w", field.FullName(), err)
		}
		result[string(field.Name())] = value
	}
	oneofs := message.Descriptor().Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		if selected := message.WhichOneof(oneof); selected != nil {
			result[string(oneof.Name())] = string(selected.Name())
		}
	}
	return result, nil
}

func dynamicFieldValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (interface{}, error) {
	if field.IsList() {
		list := value.List()
		result := make([]interface{}, list.Len())
		for i := 0; i < list.Len(); i++ {
			item, err := dynamicSingularValue(field, list.Get(i))
			if err != nil {
				return nil, err
			}
			result[i] = item
		}
		return result, nil
	}
	if field.IsMap() {
		result := make(map[string]interface{})
		var mapErr error
		value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
			decoded, err := dynamicSingularValue(field.MapValue(), item)
			if err != nil {
				mapErr = err
				return false
			}
			result[dynamicMapKey(key, field.MapKey())] = decoded
			return true
		})
		return result, mapErr
	}
	return dynamicSingularValue(field, value)
}

func dynamicSingularValue(field protoreflect.FieldDescriptor, value protoreflect.Value) (interface{}, error) {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return dynamicMessageValue(value.Message())
	case protoreflect.EnumKind:
		if enumValue := field.Enum().Values().ByNumber(value.Enum()); enumValue != nil {
			return string(enumValue.Name()), nil
		}
		return strconv.FormatInt(int64(value.Enum()), 10), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(value.Int(), 10), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(value.Uint(), 10), nil
	case protoreflect.BytesKind:
		return append([]byte(nil), value.Bytes()...), nil
	default:
		return value.Interface(), nil
	}
}

func dynamicMapKey(key protoreflect.MapKey, descriptor protoreflect.FieldDescriptor) string {
	switch descriptor.Kind() {
	case protoreflect.BoolKind:
		return strconv.FormatBool(key.Bool())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(key.Int(), 10)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(key.Uint(), 10)
	default:
		return key.String()
	}
}

// mapToProtobufMessage accepts FIT-compatible maps while deliberately ignoring
// unknown response keys, matching grpc-js/protobufjs serialization behavior.
func mapToProtobufMessage(value map[string]interface{}, message proto.Message) error {
	if value == nil {
		value = map[string]interface{}{}
	}
	return populateDynamicMessage(message.ProtoReflect(), value)
}

func populateDynamicMessage(message protoreflect.Message, values map[string]interface{}) error {
	if isJSONWellKnownType(message.Descriptor().FullName()) {
		encoded, err := json.Marshal(values)
		if err != nil {
			return err
		}
		return (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(encoded, message.Interface())
	}
	fields := message.Descriptor().Fields()
	for key, raw := range values {
		field := fields.ByName(protoreflect.Name(key))
		if field == nil {
			for i := 0; i < fields.Len(); i++ {
				if fields.Get(i).JSONName() == key {
					field = fields.Get(i)
					break
				}
			}
		}
		if field == nil || raw == nil {
			continue
		}
		if err := setDynamicField(message, field, raw); err != nil {
			return fmt.Errorf("grpc: encode field %s: %w", field.FullName(), err)
		}
	}
	return nil
}

func setDynamicField(message protoreflect.Message, field protoreflect.FieldDescriptor, raw interface{}) error {
	if field.IsList() {
		items, ok := dynamicSlice(raw)
		if !ok {
			return fmt.Errorf("expected array, got %T", raw)
		}
		list := message.Mutable(field).List()
		for _, item := range items {
			value, err := encodeDynamicValue(field, item, list.NewElement())
			if err != nil {
				return err
			}
			list.Append(value)
		}
		return nil
	}
	if field.IsMap() {
		items, ok := dynamicStringMap(raw)
		if !ok {
			return fmt.Errorf("expected object, got %T", raw)
		}
		mapping := message.Mutable(field).Map()
		for key, item := range items {
			mapKey, err := encodeDynamicMapKey(field.MapKey(), key)
			if err != nil {
				return err
			}
			value, err := encodeDynamicValue(field.MapValue(), item, mapping.NewValue())
			if err != nil {
				return err
			}
			mapping.Set(mapKey, value)
		}
		return nil
	}
	value, err := encodeDynamicValue(field, raw, message.NewField(field))
	if err != nil {
		return err
	}
	message.Set(field, value)
	return nil
}

func dynamicSlice(raw interface{}) ([]interface{}, bool) {
	value := reflect.ValueOf(raw)
	if !value.IsValid() || (value.Kind() != reflect.Slice && value.Kind() != reflect.Array) {
		return nil, false
	}
	items := make([]interface{}, value.Len())
	for i := range items {
		items[i] = value.Index(i).Interface()
	}
	return items, true
}

func dynamicStringMap(raw interface{}) (map[string]interface{}, bool) {
	value := reflect.ValueOf(raw)
	if !value.IsValid() || value.Kind() != reflect.Map || value.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	items := make(map[string]interface{}, value.Len())
	iter := value.MapRange()
	for iter.Next() {
		items[iter.Key().String()] = iter.Value().Interface()
	}
	return items, true
}

func encodeDynamicValue(field protoreflect.FieldDescriptor, raw interface{}, target protoreflect.Value) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		// Struct, Value, and ListValue use the JSON representation accepted by
		// protojson. Value may be a scalar and ListValue must be an array, so
		// treating every message as a string-keyed object loses valid FIT values.
		if isJSONWellKnownType(field.Message().FullName()) {
			encoded, err := json.Marshal(raw)
			if err != nil {
				return protoreflect.Value{}, err
			}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(encoded, target.Message().Interface()); err != nil {
				return protoreflect.Value{}, err
			}
			return protoreflect.ValueOfMessage(target.Message()), nil
		}
		object, ok := dynamicStringMap(raw)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected object, got %T", raw)
		}
		if err := populateDynamicMessage(target.Message(), object); err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfMessage(target.Message()), nil
	case protoreflect.StringKind:
		value, ok := raw.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string, got %T", raw)
		}
		return protoreflect.ValueOfString(value), nil
	case protoreflect.BytesKind:
		switch value := raw.(type) {
		case []byte:
			return protoreflect.ValueOfBytes(append([]byte(nil), value...)), nil
		case string:
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return protoreflect.Value{}, err
			}
			return protoreflect.ValueOfBytes(decoded), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected bytes or base64 string, got %T", raw)
		}
	case protoreflect.BoolKind:
		value, ok := raw.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected boolean, got %T", raw)
		}
		return protoreflect.ValueOfBool(value), nil
	case protoreflect.EnumKind:
		switch value := raw.(type) {
		case string:
			if descriptor := field.Enum().Values().ByName(protoreflect.Name(value)); descriptor != nil {
				return protoreflect.ValueOfEnum(descriptor.Number()), nil
			}
			if number, err := strconv.ParseInt(value, 10, 32); err == nil {
				return protoreflect.ValueOfEnum(protoreflect.EnumNumber(number)), nil
			}
		default:
			number, err := dynamicInt64(raw)
			if err == nil && number >= math.MinInt32 && number <= math.MaxInt32 {
				return protoreflect.ValueOfEnum(protoreflect.EnumNumber(number)), nil
			}
		}
		return protoreflect.Value{}, fmt.Errorf("unknown enum value %v", raw)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		value, err := dynamicInt64(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		if value < math.MinInt32 || value > math.MaxInt32 {
			return protoreflect.Value{}, fmt.Errorf("integer %d overflows int32", value)
		}
		return protoreflect.ValueOfInt32(int32(value)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		value, err := dynamicInt64(raw)
		return protoreflect.ValueOfInt64(value), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		value, err := dynamicUint64(raw)
		if err != nil {
			return protoreflect.Value{}, err
		}
		if value > math.MaxUint32 {
			return protoreflect.Value{}, fmt.Errorf("integer %d overflows uint32", value)
		}
		return protoreflect.ValueOfUint32(uint32(value)), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		value, err := dynamicUint64(raw)
		return protoreflect.ValueOfUint64(value), err
	case protoreflect.FloatKind:
		value, err := dynamicFloat64(raw)
		return protoreflect.ValueOfFloat32(float32(value)), err
	case protoreflect.DoubleKind:
		value, err := dynamicFloat64(raw)
		return protoreflect.ValueOfFloat64(value), err
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported protobuf kind %s", field.Kind())
	}
}

func encodeDynamicMapKey(field protoreflect.FieldDescriptor, raw string) (protoreflect.MapKey, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(raw).MapKey(), nil
	case protoreflect.BoolKind:
		value, err := strconv.ParseBool(raw)
		return protoreflect.ValueOfBool(value).MapKey(), err
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		if value < math.MinInt32 || value > math.MaxInt32 {
			return protoreflect.MapKey{}, fmt.Errorf("map key %d overflows int32", value)
		}
		return protoreflect.ValueOfInt32(int32(value)).MapKey(), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		value, err := strconv.ParseInt(raw, 10, 64)
		return protoreflect.ValueOfInt64(value).MapKey(), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		if value > math.MaxUint32 {
			return protoreflect.MapKey{}, fmt.Errorf("map key %d overflows uint32", value)
		}
		return protoreflect.ValueOfUint32(uint32(value)).MapKey(), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		value, err := strconv.ParseUint(raw, 10, 64)
		return protoreflect.ValueOfUint64(value).MapKey(), err
	default:
		return protoreflect.MapKey{}, fmt.Errorf("unsupported map key kind %s", field.Kind())
	}
}

func dynamicInt64(raw interface{}) (int64, error) {
	switch value := raw.(type) {
	case string:
		return strconv.ParseInt(value, 10, 64)
	case json.Number:
		return strconv.ParseInt(string(value), 10, 64)
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value {
			return 0, fmt.Errorf("expected integer, got %v", value)
		}
		// JSON numbers above 2^53 cannot carry an exact integer. FIT-compatible
		// 64-bit values should use their string representation.
		if math.Abs(value) > 1<<53-1 {
			return 0, fmt.Errorf("integer %v exceeds exact float64 range; use a string", value)
		}
		return int64(value), nil
	case float32:
		return dynamicInt64(float64(value))
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint:
		if uint64(value) > math.MaxInt64 {
			return 0, fmt.Errorf("integer %d overflows int64", value)
		}
		return int64(value), nil
	case uint64:
		if value > math.MaxInt64 {
			return 0, fmt.Errorf("integer %d overflows int64", value)
		}
		return int64(value), nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", raw)
	}
}

func dynamicUint64(raw interface{}) (uint64, error) {
	switch value := raw.(type) {
	case string:
		return strconv.ParseUint(value, 10, 64)
	case json.Number:
		return strconv.ParseUint(string(value), 10, 64)
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value < 0 {
			return 0, fmt.Errorf("expected unsigned integer, got %v", value)
		}
		if value > 1<<53-1 {
			return 0, fmt.Errorf("integer %v exceeds exact float64 range; use a string", value)
		}
		return uint64(value), nil
	case float32:
		return dynamicUint64(float64(value))
	case int8:
		if value < 0 {
			return 0, fmt.Errorf("expected unsigned integer, got %d", value)
		}
		return uint64(value), nil
	case int16:
		if value < 0 {
			return 0, fmt.Errorf("expected unsigned integer, got %d", value)
		}
		return uint64(value), nil
	case int:
		if value < 0 {
			return 0, fmt.Errorf("expected unsigned integer, got %d", value)
		}
		return uint64(value), nil
	case int32:
		if value < 0 {
			return 0, fmt.Errorf("expected unsigned integer, got %d", value)
		}
		return uint64(value), nil
	case int64:
		if value < 0 {
			return 0, fmt.Errorf("expected unsigned integer, got %d", value)
		}
		return uint64(value), nil
	case uint8:
		return uint64(value), nil
	case uint16:
		return uint64(value), nil
	case uint:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case uint64:
		return value, nil
	default:
		return 0, fmt.Errorf("expected unsigned integer, got %T", raw)
	}
}

func dynamicFloat64(raw interface{}) (float64, error) {
	switch value := raw.(type) {
	case json.Number:
		return strconv.ParseFloat(string(value), 64)
	case float32:
		return float64(value), nil
	case float64:
		return value, nil
	case int8:
		return float64(value), nil
	case int16:
		return float64(value), nil
	case int:
		return float64(value), nil
	case int32:
		return float64(value), nil
	case int64:
		return float64(value), nil
	case uint8:
		return float64(value), nil
	case uint16:
		return float64(value), nil
	case uint:
		return float64(value), nil
	case uint32:
		return float64(value), nil
	case uint64:
		return float64(value), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", raw)
	}
}

func isJSONWellKnownType(name protoreflect.FullName) bool {
	switch name {
	case "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue":
		return true
	default:
		return false
	}
}
