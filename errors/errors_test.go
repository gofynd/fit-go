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

package errors

import (
	"errors"
	"net/http"
	"testing"
)

func TestNew(t *testing.T) {
	// Setup a test registry
	reg := &ErrorRegistry{
		messageCodes: map[int]int{1: 1},
	}
	reg.Init("TST", map[string]int{"TEST_ERROR": 1}, nil, nil)

	// Temporarily replace DefaultRegistry
	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("original error"), 22)
	if err == nil {
		t.Fatal("New() returned nil")
	}

	if err.IntCode != 22 {
		t.Errorf("IntCode = %d; want 22", err.IntCode)
	}

	if err.Code != "TST0022" {
		t.Errorf("Code = %q; want %q", err.Code, "TST0022")
	}

	if err.ErrorMessage != "original error" {
		t.Errorf("ErrorMessage = %q; want %q", err.ErrorMessage, "original error")
	}

	if err.HTTPStatusCode != http.StatusInternalServerError {
		t.Errorf("HTTPStatusCode = %d; want %d", err.HTTPStatusCode, http.StatusInternalServerError)
	}

	if err.NOP != false {
		t.Error("NOP should default to false")
	}

	if err.ISOLangCode != DefaultLanguageCode {
		t.Errorf("ISOLangCode = %q; want %q", err.ISOLangCode, DefaultLanguageCode)
	}
}

func TestNew_NilError(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(nil, 42)
	if err.ErrorMessage != "" {
		t.Errorf("ErrorMessage for nil error = %q; want empty", err.ErrorMessage)
	}
}

func TestFitError_Error(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("AVS", map[string]int{"ORDER_ERROR": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("failed to get order"), 22)
	expected := "AVS0022: failed to get order"

	if got := err.Error(); got != expected {
		t.Errorf("Error() = %q; want %q", got, expected)
	}
}

func TestFitError_SetStatus(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("test"), 1)

	// Valid status
	result := err.SetStatus(http.StatusBadRequest)
	if result != err {
		t.Error("SetStatus should return receiver for chaining")
	}
	if err.HTTPStatusCode != http.StatusBadRequest {
		t.Errorf("HTTPStatusCode = %d; want %d", err.HTTPStatusCode, http.StatusBadRequest)
	}

	// Invalid status (too low) - should be ignored
	err.SetStatus(100)
	if err.HTTPStatusCode != http.StatusBadRequest {
		t.Error("SetStatus should ignore status < 200")
	}

	// Invalid status (too high) - should be ignored
	err.SetStatus(700)
	if err.HTTPStatusCode != http.StatusBadRequest {
		t.Error("SetStatus should ignore status > 599")
	}
}

func TestFitError_SetMeta(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("test"), 1)

	meta := map[string]interface{}{
		"user_id":    123,
		"request_id": "abc-123",
	}

	result := err.SetMeta(meta)
	if result != err {
		t.Error("SetMeta should return receiver for chaining")
	}
	if err.Meta["user_id"] != 123 {
		t.Errorf("Meta[user_id] = %v; want 123", err.Meta["user_id"])
	}
	if err.Meta["request_id"] != "abc-123" {
		t.Errorf("Meta[request_id] = %v; want abc-123", err.Meta["request_id"])
	}
}

func TestFitError_Ignore(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("test"), 1)
	if err.NOP {
		t.Error("NOP should start as false")
	}

	result := err.Ignore()
	if result != err {
		t.Error("Ignore should return receiver for chaining")
	}
	if !err.NOP {
		t.Error("NOP should be true after Ignore()")
	}
}

func TestFitError_SetLang(t *testing.T) {
	messages := map[string]map[int]string{
		"EN": {1: "English message"},
		"ES": {1: "Mensaje en espanol"},
	}
	messageCodes := map[int]int{100: 1}

	reg := &ErrorRegistry{}
	reg.Init("TST", map[string]int{"TEST": 100}, messages, messageCodes)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("test"), 100)

	// Default language
	if err.ISOLangCode != "EN" {
		t.Errorf("Default ISOLangCode = %q; want EN", err.ISOLangCode)
	}

	// Valid language switch
	result := err.SetLang("ES")
	if result != err {
		t.Error("SetLang should return receiver for chaining")
	}
	if err.ISOLangCode != "ES" {
		t.Errorf("ISOLangCode = %q; want ES", err.ISOLangCode)
	}

	// Invalid language (not in messages) - should be ignored
	err.SetLang("FR")
	if err.ISOLangCode != "ES" {
		t.Error("SetLang should ignore unknown language codes")
	}

	// Empty language - should be ignored
	err.SetLang("")
	if err.ISOLangCode != "ES" {
		t.Error("SetLang should ignore empty string")
	}
}

func TestFitError_GetStrCode(t *testing.T) {
	tests := []struct {
		serviceCode string
		intCode     int
		expected    string
	}{
		{"AVS", 1, "AVS0001"},
		{"AVS", 22, "AVS0022"},
		{"AVS", 999, "AVS0999"},
		{"AVS", 9999, "AVS9999"},
		{"AVS", 10000, "AVS10000"}, // over 4 digits
		{"XYZ", 0, "XYZ0000"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			reg := &ErrorRegistry{}
			reg.Init(tt.serviceCode, map[string]int{"TEST": 1}, nil, nil)

			err := NewWithRegistry(errors.New("test"), tt.intCode, reg)
			if got := err.GetStrCode(); got != tt.expected {
				t.Errorf("GetStrCode() = %q; want %q", got, tt.expected)
			}
		})
	}
}

func TestFitError_MessageResolution(t *testing.T) {
	messages := map[string]map[int]string{
		"EN": {
			1: "Request failed",
			2: "Invalid input",
		},
	}
	messageCodes := map[int]int{
		100: 1, // intCode 100 -> message ID 1
		200: 2, // intCode 200 -> message ID 2
	}

	reg := &ErrorRegistry{}
	reg.Init("TST", map[string]int{"REQ_FAIL": 100, "INVALID": 200}, messages, messageCodes)

	// Test message resolution
	err := NewWithRegistry(errors.New("original"), 100, reg)
	expected := "TST0100: Request failed"
	if got := err.Error(); got != expected {
		t.Errorf("Error() = %q; want %q", got, expected)
	}

	// Test different code
	err2 := NewWithRegistry(errors.New("original"), 200, reg)
	expected2 := "TST0200: Invalid input"
	if got := err2.Error(); got != expected2 {
		t.Errorf("Error() = %q; want %q", got, expected2)
	}

	// Test fallback to ErrorMessage when no message mapping
	err3 := NewWithRegistry(errors.New("fallback message"), 999, reg)
	expected3 := "TST0999: fallback message"
	if got := err3.Error(); got != expected3 {
		t.Errorf("Error() = %q; want %q", got, expected3)
	}

	// Test fallback to DefaultErrorMessage when no mapping and no ErrorMessage
	err4 := NewWithRegistry(nil, 999, reg)
	expected4 := "TST0999: " + DefaultErrorMessage
	if got := err4.Error(); got != expected4 {
		t.Errorf("Error() = %q; want %q", got, expected4)
	}
}

func TestErrorRegistry_Init(t *testing.T) {
	// Test with empty serviceNameCode
	reg := &ErrorRegistry{}
	err := reg.Init("", map[string]int{"TEST": 1}, nil, nil)
	if err == nil {
		t.Error("Init should fail with empty serviceNameCode")
	}

	// Test with empty errorCodes
	reg = &ErrorRegistry{}
	err = reg.Init("TST", nil, nil, nil)
	if err == nil {
		t.Error("Init should fail with nil errorCodes")
	}

	reg = &ErrorRegistry{}
	err = reg.Init("TST", map[string]int{}, nil, nil)
	if err == nil {
		t.Error("Init should fail with empty errorCodes")
	}

	// Test with messages missing default language
	messages := map[string]map[int]string{
		"ES": {1: "Spanish only"},
	}
	reg = &ErrorRegistry{}
	err = reg.Init("TST", map[string]int{"TEST": 1}, messages, nil)
	if err == nil {
		t.Error("Init should fail when messages lacks default language")
	}

	// Test successful init
	messages = map[string]map[int]string{
		"EN": {1: "English"},
		"ES": {1: "Spanish"},
	}
	reg = &ErrorRegistry{}
	err = reg.Init("TST", map[string]int{"TEST": 1}, messages, nil)
	if err != nil {
		t.Errorf("Init should succeed; got error: %v", err)
	}
}

func TestErrorRegistry_InitServiceCodeAndReset(t *testing.T) {
	reg := &ErrorRegistry{}
	if err := reg.InitServiceCode("ONE"); err != nil {
		t.Fatalf("InitServiceCode: %v", err)
	}
	if got := NewWithRegistry(nil, 7, reg).Code; got != "ONE0007" {
		t.Fatalf("code = %q, want ONE0007", got)
	}

	reg.Reset()
	if got := NewWithRegistry(nil, 7, reg).Code; got != "0007" {
		t.Fatalf("code after Reset = %q, want 0007", got)
	}
	if err := reg.InitServiceCode("TWO"); err != nil {
		t.Fatalf("second InitServiceCode: %v", err)
	}
	if got := NewWithRegistry(nil, 7, reg).Code; got != "TWO0007" {
		t.Fatalf("code after reinit = %q, want TWO0007", got)
	}
}

func TestIsFitError(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	fitErr := New(errors.New("test"), 1)

	// Test with FitError
	fe, ok := IsFitError(fitErr)
	if !ok {
		t.Error("IsFitError should return true for *FitError")
	}
	if fe != fitErr {
		t.Error("IsFitError should return the same FitError pointer")
	}

	// Test with regular error
	regularErr := errors.New("not a fit error")
	fe, ok = IsFitError(regularErr)
	if ok {
		t.Error("IsFitError should return false for regular error")
	}
	if fe != nil {
		t.Error("IsFitError should return nil for regular error")
	}

	// Test with nil
	fe, ok = IsFitError(nil)
	if ok {
		t.Error("IsFitError should return false for nil")
	}
	if fe != nil {
		t.Error("IsFitError should return nil for nil input")
	}
}

func TestFitError_Chaining(t *testing.T) {
	reg := &ErrorRegistry{}
	reg.Init("TST", map[string]int{"TEST": 1}, map[string]map[int]string{
		"EN": {1: "Test"},
	}, map[int]int{1: 1})

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	// Test chaining all setters
	err := New(errors.New("test"), 1).
		SetStatus(http.StatusBadRequest).
		SetMeta(map[string]interface{}{"key": "value"}).
		Ignore()

	if err.HTTPStatusCode != http.StatusBadRequest {
		t.Error("Chained SetStatus failed")
	}
	if err.Meta["key"] != "value" {
		t.Error("Chained SetMeta failed")
	}
	if !err.NOP {
		t.Error("Chained Ignore failed")
	}
}

func TestFitError_Unwrap(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	err := New(errors.New("test"), 1)
	if err.Unwrap() != nil {
		t.Error("Unwrap should return nil")
	}
}

func TestFitError_HTTPStatusCodes(t *testing.T) {
	reg := &ErrorRegistry{messageCodes: map[int]int{1: 1}}
	reg.Init("TST", map[string]int{"TEST": 1}, nil, nil)

	oldDefault := DefaultRegistry
	DefaultRegistry = reg
	defer func() { DefaultRegistry = oldDefault }()

	tests := []struct {
		name       string
		statusCode int
		expected   int
	}{
		{"OK", http.StatusOK, http.StatusOK},
		{"Created", http.StatusCreated, http.StatusCreated},
		{"BadRequest", http.StatusBadRequest, http.StatusBadRequest},
		{"Unauthorized", http.StatusUnauthorized, http.StatusUnauthorized},
		{"Forbidden", http.StatusForbidden, http.StatusForbidden},
		{"NotFound", http.StatusNotFound, http.StatusNotFound},
		{"InternalServerError", http.StatusInternalServerError, http.StatusInternalServerError},
		{"ServiceUnavailable", http.StatusServiceUnavailable, http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := New(errors.New("test"), 1).SetStatus(tt.statusCode)
			if err.HTTPStatusCode != tt.expected {
				t.Errorf("HTTPStatusCode = %d; want %d", err.HTTPStatusCode, tt.expected)
			}
		})
	}
}

// TestConcurrentAccess verifies thread-safety of the ErrorRegistry.
func TestConcurrentAccess(t *testing.T) {
	reg := &ErrorRegistry{}
	reg.Init("TST", map[string]int{"TEST": 1}, map[string]map[int]string{
		"EN": {1: "Test"},
	}, map[int]int{1: 1})

	done := make(chan bool)

	// Multiple goroutines creating errors
	for i := 0; i < 10; i++ {
		go func(n int) {
			for j := 0; j < 100; j++ {
				err := NewWithRegistry(errors.New("test"), n, reg)
				_ = err.Error()
				_ = err.GetStrCode()
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	// If we get here without a race condition, the test passes
}
