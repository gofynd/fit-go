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

// These fixed Python-generated vectors verify AES-256-GCM interoperability with
// the supported 9-byte nonce and ciphertext.tag wire format.
const (
	katDEKBase64 = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	katIVBase64  = "AAECAwQFBgcI"
)

var katVectors = []struct {
	name       string
	plaintext  string
	ciphertext string
}{
	{name: "email", plaintext: "person@example.com", ciphertext: "3DgQJMHI6nzQ9WgnDE+piMle.zOOeiZiL35jf4IjmimHrHw=="},
	{name: "phone", plaintext: "+91-9876543210", ciphertext: "h2RTepeenS+doDZlURo=.TdTP0SGbFiq7Rk1HXG8iGg=="},
	{name: "empty", plaintext: "", ciphertext: ".xmBd2IfyjYoRXmXrg/bJCg=="},
	{name: "unicode_emoji", plaintext: "héllo-uniçode-😀", ciphertext: "xJ7LO8LJh2zG/cbwD07ixlasC1g=.GocXseV06PhTH574r8YFvA=="},
}

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
