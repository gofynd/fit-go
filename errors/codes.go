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

package errors

// Built-in error codes originating from the fit framework itself.
// The numbering convention mirrors (modules/err/err-codes.ts):
//
// - Generic exceptions: 1 - 99
// - FIT source-code specifics: 600 - 999
const (
	// UNCAUGHT_EXCEPTION is the catch-all code for unhandled errors.
	UNCAUGHT_EXCEPTION = 1

	// APPLICATION_HEADER_JSON_PARSE_FAILURE indicates that the
	// x-application-data header could not be parsed as JSON.
	APPLICATION_HEADER_JSON_PARSE_FAILURE = 650

	// USER_HEADER_JSON_PARSE_FAILURE indicates that the x-user-data header
	// could not be parsed as JSON.
	USER_HEADER_JSON_PARSE_FAILURE = 651
)

// builtinMessageCodes maps built-in intCodes to message IDs. Services may
// extend this via ErrorRegistry.Init.
var builtinMessageCodes = map[int]int{
	UNCAUGHT_EXCEPTION: 1,
	APPLICATION_HEADER_JSON_PARSE_FAILURE: 2,
	USER_HEADER_JSON_PARSE_FAILURE: 2,
}

// builtinMessagesEN provides default English messages keyed by message ID.
var builtinMessagesEN = map[int]string{
	1: "Uncaught Exception",
	2: "Invalid request",
}

// BuiltinMessages returns the default message map (EN only) that ships with
// the framework. Services can merge this with their own messages before
// passing them to ErrorRegistry.Init.
func BuiltinMessages() map[string]map[int]string {
	// Return a copy so callers cannot mutate the package-level map.
	en := make(map[int]string, len(builtinMessagesEN))
	for k, v := range builtinMessagesEN {
		en[k] = v
	}
	return map[string]map[int]string{
		DefaultLanguageCode: en,
	}
}

// BuiltinMessageCodes returns a copy of the default intCode -> message-ID
// mapping that ships with the framework.
func BuiltinMessageCodes() map[int]int {
	return copyIntMap(builtinMessageCodes)
}
