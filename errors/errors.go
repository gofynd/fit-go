// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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

// Package errors provides structured error handling for fit.go services.
//
// This is a port of the Err class (modules/err/index.ts). It provides
// typed errors with integer codes, string codes (e.g. "AVS0022"), HTTP status
// codes, multilingual message resolution, and a NOP flag to suppress Sentry
// reporting for expected/ignorable errors.
package errors

import (
	"fmt"
	"net/http"
	"sync"
)

// Default constants matching the JS implementation.
const (
	DefaultLanguageCode = "EN"
	DefaultErrorMessage = "Uncaught Exception"
)

// ErrorRegistry holds the service-level configuration that maps integer error
// codes to formatted string codes and human-readable messages.
type ErrorRegistry struct {
	mu              sync.RWMutex
	serviceNameCode string
	// messages maps ISO language code -> message ID -> message string.
	// Example: {"EN": {1: "Uncaught Exception", 2: "Invalid request"}}
	messages map[string]map[int]string
	// messageCodes maps intCode -> message ID.
	// Example: {1: 1, 650: 2, 651: 2}
	messageCodes map[int]int
}

// DefaultRegistry is the package-level registry used by New and other helpers.
// Services call DefaultRegistry.Init(...) at startup.
var DefaultRegistry = &ErrorRegistry{
	messageCodes: copyIntMap(builtinMessageCodes),
}

// Init configures the error registry. serviceNameCode is a short prefix such
// as "AVS" for Avis. errorCodes is a map of error name -> int code (used for
// validation only). messages maps language codes to message-ID -> string
// mappings. messageCodes maps intCode -> message ID for message resolution.
//
// Init must be called before constructing any FitError that relies on message
// resolution.
func (r *ErrorRegistry) Init(
	serviceNameCode string,
	errorCodes map[string]int,
	messages map[string]map[int]string,
	messageCodes map[int]int,
) error {
	if serviceNameCode == "" {
		return fmt.Errorf("fit/errors: serviceNameCode must not be empty")
	}
	if len(errorCodes) == 0 {
		return fmt.Errorf("fit/errors: errorCodes must not be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.serviceNameCode = serviceNameCode

	if len(messages) > 0 {
		if _, ok := messages[DefaultLanguageCode]; !ok {
			return fmt.Errorf(
				"fit/errors: default language messages (%s) are unavailable in messages map",
				DefaultLanguageCode,
			)
		}
		r.messages = messages
	}

	// Merge caller-supplied messageCodes on top of the built-in codes.
	if r.messageCodes == nil {
		r.messageCodes = copyIntMap(builtinMessageCodes)
	}
	for k, v := range messageCodes {
		r.messageCodes[k] = v
	}

	return nil
}

// FitError is a structured error carrying a machine-readable code, an HTTP
// status code, optional metadata, and support for multilingual messages.
type FitError struct {
	// IntCode is the unique integer identifier for this error case.
	IntCode int

	// Code is the formatted string code, e.g. "AVS0022".
	Code string

	// Meta carries optional debugging context. It is logged and included in
	// Sentry reports when the NOP flag is not set.
	Meta map[string]interface{}

	// HTTPStatusCode is the HTTP status code to send to the client.
	HTTPStatusCode int

	// NOP (no-operation): when true, this error will not be reported to Sentry.
	NOP bool

	// ISOLangCode controls which language is used for message resolution.
	ISOLangCode string

	// ErrorMessage is the raw/original error message from the wrapped error.
	ErrorMessage string

	// registry is the ErrorRegistry used for code formatting and message
	// resolution. It defaults to DefaultRegistry.
	registry *ErrorRegistry
}

// New creates a FitError from an existing error and an integer error code.
// If err is nil, the ErrorMessage is left empty.
func New(err error, intCode int) *FitError {
	fe := &FitError{
		IntCode:        intCode,
		Meta:           make(map[string]interface{}),
		NOP:            false,
		HTTPStatusCode: http.StatusInternalServerError,
		ISOLangCode:    DefaultLanguageCode,
		registry:       DefaultRegistry,
	}

	if err != nil {
		fe.ErrorMessage = err.Error()
	}

	fe.Code = fe.GetStrCode()
	return fe
}

// NewWithRegistry creates a FitError bound to a specific ErrorRegistry instead
// of the package-level DefaultRegistry.
func NewWithRegistry(err error, intCode int, registry *ErrorRegistry) *FitError {
	fe := New(err, intCode)
	fe.registry = registry
	fe.Code = fe.GetStrCode()
	return fe
}

// Error implements the error interface. It returns a formatted message like
// "AVS0022: Failed to get order details. Please try again."
func (e *FitError) Error() string {
	return e.GetMessage()
}

// Unwrap returns nil. FitError does not wrap another error value but carries
// the original message in ErrorMessage.
func (e *FitError) Unwrap() error {
	return nil
}

// SetStatus sets the HTTP status code. Values outside 200-599 are ignored.
// Returns the receiver for chaining.
func (e *FitError) SetStatus(code int) *FitError {
	if code < 200 || code > 599 {
		return e
	}
	e.HTTPStatusCode = code
	return e
}

// SetMeta replaces the metadata map. Returns the receiver for chaining.
func (e *FitError) SetMeta(meta map[string]interface{}) *FitError {
	e.Meta = meta
	return e
}

// SetLang sets the ISO language code used for message resolution. If the
// language is empty or not present in the registry's messages map, the call
// is a no-op. Returns the receiver for chaining.
func (e *FitError) SetLang(lang string) *FitError {
	if lang == "" {
		return e
	}

	e.registry.mu.RLock()
	defer e.registry.mu.RUnlock()

	if e.registry.messages == nil {
		return e
	}
	if _, ok := e.registry.messages[lang]; !ok {
		return e
	}
	e.ISOLangCode = lang
	return e
}

// Ignore marks this error as a no-op so it will not be reported to Sentry.
// Returns the receiver for chaining.
func (e *FitError) Ignore() *FitError {
	e.NOP = true
	return e
}

// GetStrCode formats the integer code into a string code like "AVS0022".
func (e *FitError) GetStrCode() string {
	e.registry.mu.RLock()
	defer e.registry.mu.RUnlock()

	return fmt.Sprintf("%s%04d", e.registry.serviceNameCode, e.IntCode)
}

// GetMessage returns the full error string: code + resolved message.
func (e *FitError) GetMessage() string {
	return fmt.Sprintf("%s: %s", e.Code, e.messageByCode())
}

// messageByCode resolves the human-readable message for this error's intCode
// and language. Falls back to ErrorMessage, then DefaultErrorMessage.
func (e *FitError) messageByCode() string {
	e.registry.mu.RLock()
	defer e.registry.mu.RUnlock()

	if e.registry.messages != nil && e.registry.messageCodes != nil {
		if msgID, ok := e.registry.messageCodes[e.IntCode]; ok {
			if langMsgs, ok := e.registry.messages[e.ISOLangCode]; ok {
				if msg, ok := langMsgs[msgID]; ok && msg != "" {
					return msg
				}
			}
		}
	}

	if e.ErrorMessage != "" {
		return e.ErrorMessage
	}
	return DefaultErrorMessage
}

// IsFitError checks whether an error is a *FitError. If it is, the FitError
// pointer is returned along with true; otherwise nil, false.
func IsFitError(err error) (*FitError, bool) {
	if fe, ok := err.(*FitError); ok {
		return fe, true
	}
	return nil, false
}

// copyIntMap returns a shallow copy of a map[int]int.
func copyIntMap(m map[int]int) map[int]int {
	if m == nil {
		return nil
	}
	out := make(map[int]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
