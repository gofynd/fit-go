// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package grpc

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/dynamicpb"
)

func dynamicContractMethod(t *testing.T) (*Server, string) {
	t.Helper()
	root := "testdata"
	serverType := "contract"
	protoPath := filepath.Join(root, serverType, "contract.proto")
	srv, err := Init(Config{
		ServerType: serverType,
		Port:       "0",
		FileName:   "contract",
		ProtoDir:   root,
		Logger:     slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil)),
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	service, err := compileDynamicService(srv.cfg, protoPath)
	if err != nil {
		t.Fatalf("compileDynamicService: %v", err)
	}
	srv.dynamicService = service
	return srv, protoPath
}

func TestDynamicCodecPreservesFITHandlerContract(t *testing.T) {
	srv, _ := dynamicContractMethod(t)
	method := srv.dynamicService.Methods().ByName("Execute")
	message := dynamicpb.NewMessage(method.Input())
	input := map[string]interface{}{
		"snake_name":  "example",
		"large":       "9223372036854775806",
		"raw":         []byte{0, 1, 2, 255},
		"state":       "STATE_READY",
		"ids":         []string{"7", "8"},
		"counts":      map[string]int64{"one": 1, "two": 2},
		"text":        "selected",
		"created_at":  map[string]interface{}{"seconds": "1700000000", "nanos": float64(123)},
		"ttl":         map[string]interface{}{"seconds": "12", "nanos": float64(5)},
		"payload":     map[string]interface{}{"type_url": "type.example/Test", "value": []byte{3, 4}},
		"metadata":    map[string]interface{}{"enabled": true, "count": float64(2)},
		"any_value":   "ready",
		"values":      []interface{}{"one", float64(2), true, nil},
		"int32_keys":  map[string]string{"-1": "negative"},
		"uint32_keys": map[string]string{"4294967295": "max"},
	}
	if err := mapToProtobufMessage(input, message); err != nil {
		t.Fatalf("mapToProtobufMessage: %v", err)
	}
	decoded, err := protobufMessageToMap(message)
	if err != nil {
		t.Fatalf("protobufMessageToMap: %v", err)
	}
	if decoded["large"] != input["large"] || decoded["state"] != "STATE_READY" || decoded["choice"] != "text" {
		t.Fatalf("scalar/oneof contract changed: %#v", decoded)
	}
	if !reflect.DeepEqual(decoded["raw"], input["raw"]) || !reflect.DeepEqual(decoded["ids"], []interface{}{"7", "8"}) {
		t.Fatalf("bytes/list contract changed: %#v", decoded)
	}
	if !reflect.DeepEqual(decoded["counts"], map[string]interface{}{"one": "1", "two": "2"}) {
		t.Fatalf("map contract changed: %#v", decoded["counts"])
	}
	if !reflect.DeepEqual(decoded["metadata"], input["metadata"]) {
		t.Fatalf("Struct contract changed: %#v", decoded["metadata"])
	}
	if decoded["any_value"] != "ready" || !reflect.DeepEqual(decoded["values"], []interface{}{"one", float64(2), true, nil}) {
		t.Fatalf("JSON well-known type contract changed: %#v", decoded)
	}
	if !reflect.DeepEqual(decoded["int32_keys"], map[string]interface{}{"-1": "negative"}) ||
		!reflect.DeepEqual(decoded["uint32_keys"], map[string]interface{}{"4294967295": "max"}) {
		t.Fatalf("integer map-key contract changed: %#v", decoded)
	}
	if _, present := decoded["nickname"]; present {
		t.Fatalf("absent optional field unexpectedly materialized: %#v", decoded)
	}
}

func TestDynamicCodecIgnoresUnknownResponseFields(t *testing.T) {
	srv, _ := dynamicContractMethod(t)
	method := srv.dynamicService.Methods().ByName("Execute")
	message := dynamicpb.NewMessage(method.Output())
	if err := mapToProtobufMessage(map[string]interface{}{
		"snake_name": "known",
		"future_key": "ignored",
	}, message); err != nil {
		t.Fatalf("unknown response key should be ignored: %v", err)
	}
	decoded, err := protobufMessageToMap(message)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["snake_name"] != "known" {
		t.Fatalf("known response field lost: %#v", decoded)
	}
}

func TestDynamicCodecRejectsLossyIntegerConversions(t *testing.T) {
	srv, _ := dynamicContractMethod(t)
	method := srv.dynamicService.Methods().ByName("Execute")

	for name, value := range map[string]interface{}{
		"int32 overflow": float64(2147483648),
		"fractional":     1.5,
	} {
		t.Run(name, func(t *testing.T) {
			message := dynamicpb.NewMessage(method.Output())
			if err := mapToProtobufMessage(map[string]interface{}{"number": value}, message); err == nil {
				t.Fatalf("lossy integer conversion unexpectedly accepted: %v", value)
			}
		})
	}
	if _, err := dynamicInt64(float64(1 << 53)); err == nil {
		t.Fatal("inexact float64 int64 unexpectedly accepted")
	}
	if _, err := dynamicUint64(-1); err == nil {
		t.Fatal("negative unsigned integer unexpectedly accepted")
	}
}

func TestDynamicCodecRejectsOverflowingMapKeys(t *testing.T) {
	srv, _ := dynamicContractMethod(t)
	method := srv.dynamicService.Methods().ByName("Execute")
	for name, value := range map[string]interface{}{
		"int32 overflow":  map[string]string{"2147483648": "bad"},
		"uint32 overflow": map[string]string{"4294967296": "bad"},
	} {
		t.Run(name, func(t *testing.T) {
			message := dynamicpb.NewMessage(method.Output())
			field := "int32_keys"
			if strings.HasPrefix(name, "uint32") {
				field = "uint32_keys"
			}
			if err := mapToProtobufMessage(map[string]interface{}{field: value}, message); err == nil {
				t.Fatalf("overflowing map key unexpectedly accepted: %#v", value)
			}
		})
	}
}

func TestDynamicCodecAcceptsTypedNestedMaps(t *testing.T) {
	srv, _ := dynamicContractMethod(t)
	method := srv.dynamicService.Methods().ByName("Execute")
	message := dynamicpb.NewMessage(method.Output())
	if err := mapToProtobufMessage(map[string]interface{}{
		"created_at": map[string]int64{"seconds": 1700000000, "nanos": 123},
	}, message); err != nil {
		t.Fatalf("typed nested map: %v", err)
	}
}

func TestDynamicCodecAcceptsAllGoIntegerWidthsForFloatFields(t *testing.T) {
	values := []struct {
		input    interface{}
		expected float64
	}{
		{input: int8(-1), expected: -1},
		{input: int16(-2), expected: -2},
		{input: uint8(3), expected: 3},
		{input: uint16(4), expected: 4},
	}
	for _, test := range values {
		got, err := dynamicFloat64(test.input)
		if err != nil {
			t.Errorf("dynamicFloat64(%T) returned error: %v", test.input, err)
			continue
		}
		if got != test.expected {
			t.Errorf("dynamicFloat64(%T) = %v, want %v", test.input, got, test.expected)
		}
	}
}

func TestInitDefersDynamicProtoCompilation(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "generated")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "generated.proto"), []byte(`syntax = "proto3"; import "missing.proto";`), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, err := Init(Config{ServerType: "generated", Port: "0", FileName: "generated", ProtoDir: root})
	if err != nil {
		t.Fatalf("generated-only initialization should not compile runtime protos: %v", err)
	}
	err = srv.AddServiceDefinitions(ServiceImplementation{})
	if err == nil || !strings.Contains(err.Error(), "compile proto") {
		t.Fatalf("dynamic registration did not surface compile failure: %v", err)
	}
}

func TestDynamicStatusErrorProtectsInternalMessages(t *testing.T) {
	for _, code := range []codes.Code{codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable} {
		err := dynamicStatusError(status.Error(code, "database password=secret"))
		if status.Code(err) != code {
			t.Fatalf("code = %v, want %v", status.Code(err), code)
		}
		if strings.Contains(status.Convert(err).Message(), "password") || strings.Contains(status.Convert(err).Message(), "secret") {
			t.Fatalf("internal status leaked for %v: %v", code, err)
		}
	}
	public := dynamicStatusError(status.Error(codes.InvalidArgument, "invalid request"))
	if status.Convert(public).Message() != "invalid request" {
		t.Fatalf("public status changed: %v", public)
	}
}
