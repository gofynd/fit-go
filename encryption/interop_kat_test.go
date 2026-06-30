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

package encryption

import "testing"

// Cross-language known-answer test for the G2 variable-length-nonce change.
//
// WHY THIS EXISTS: TestRoundTrip only proves fit-go can decrypt what fit-go
// encrypted — it would still pass even if fit-go were byte-incompatible with the
// rest of the platform. G2's whole point is interoperability: fit-go must read
// PII that Node `fit/encryption` and `pyfit` already wrote (and write PII they
// can read) using the production nonce length, which is NOT 12 bytes (9 in the
// wild). This test pins that with fixed vectors.
//
// The vectors below were produced by Python's `cryptography` library — the exact
// primitive `pyfit.encryption` wraps — via AES-256-GCM with `modes.GCM(iv)`, a
// 9-byte nonce used verbatim, and the platform wire format
// base64(ciphertext) + "." + base64(tag). See scripts/gen_encryption_vector.py
// (mirrored from the Commerce scratchpad generator) to regenerate. They are
// hardcoded so this test needs no Python at CI time.
//
// DEK = bytes(0..31)  (32 bytes, AES-256)   IV = bytes(0..8)  (9-byte nonce)
const (
	katDEKBase64 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	katIVBase64  = "AAECAwQFBgcI"
)

var katVectors = []struct {
	name       string
	plaintext  string
	ciphertext string
}{
	{name: "email", plaintext: "swapnil@gofynd.com", ciphertext: "3yoDJ8DPxlnP+2MuDk6piMle.we+PpdGX8pBqli9M39jtlw=="},
	{name: "phone", plaintext: "+91-9876543210", ciphertext: "h2RTepeenS+doDZlURo=.TdTP0SGbFiq7Rk1HXG8iGg=="},
	{name: "empty", plaintext: "", ciphertext: ".xmBd2IfyjYoRXmXrg/bJCg=="},
	{name: "unicode_emoji", plaintext: "héllo-uniçode-😀", ciphertext: "xJ7LO8LJh2zG/cbwD07ixlasC1g=.GocXseV06PhTH574r8YFvA=="},
}

// newKATManager initializes a Manager from the cross-language DEK/IV via the
// PII_DEK_BASE64 / PII_IV_BASE64 env path. Init() succeeding here is itself part
// of the assertion: the pre-G2 guard rejected this 9-byte IV outright.
func newKATManager(t *testing.T) *Manager {
	t.Helper()
	t.Setenv("PII_DEK_BASE64", katDEKBase64)
	t.Setenv("PII_IV_BASE64", katIVBase64)
	m := NewManager()
	if err := m.Init(); err != nil {
		t.Fatalf("Init with 9-byte nonce must succeed (G2): %v", err)
	}
	return m
}

// TestInterop_DecryptsPlatformCiphertext: fit-go must Decrypt bytes produced by
// the Python crypto primitive pyfit uses. This is the direction that matters for
// reading already-stored PII.
func TestInterop_DecryptsPlatformCiphertext(t *testing.T) {
	m := newKATManager(t)
	for _, v := range katVectors {
		t.Run(v.name, func(t *testing.T) {
			got, err := m.Decrypt(v.ciphertext)
			if err != nil {
				t.Fatalf("Decrypt(%q) error: %v", v.ciphertext, err)
			}
			if got != v.plaintext {
				t.Fatalf("Decrypt mismatch:\n got  %q\n want %q", got, v.plaintext)
			}
		})
	}
}

// TestInterop_EncryptsToPlatformCiphertext: with the same DEK+fixed IV, fit-go's
// Encrypt must reproduce the platform bytes exactly (GCM is deterministic for a
// fixed nonce). This is the direction that matters for PII fit-go writes being
// readable by Node/pyfit.
func TestInterop_EncryptsToPlatformCiphertext(t *testing.T) {
	m := newKATManager(t)
	for _, v := range katVectors {
		t.Run(v.name, func(t *testing.T) {
			got, err := m.Encrypt(v.plaintext)
			if err != nil {
				t.Fatalf("Encrypt(%q) error: %v", v.plaintext, err)
			}
			if got != v.ciphertext {
				t.Fatalf("Encrypt mismatch (not byte-compatible with platform):\n got  %q\n want %q", got, v.ciphertext)
			}
		})
	}
}
