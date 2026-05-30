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

package server

import (
	"crypto/rand"
	"io"
	"os"
	"strings"
)

// osGetenv wraps os.Getenv for use in middleware.go's envGet.
func osGetenv(key string) string {
	return os.Getenv(key)
}

// cryptoRandReaderInstance provides crypto/rand.Reader.
var cryptoRandReaderInstance io.Reader = rand.Reader

// envGetBool reads an env var and returns true if its lowercased, trimmed value
// is "true".
func envGetBool(key string) bool {
	return strings.TrimSpace(strings.ToLower(os.Getenv(key))) == "true"
}
