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

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
)

// managerWithNonce builds an initialized Manager via the inline DEK/IV env path
// with a random 32-byte DEK and an IV of the requested length.
func managerWithNonce(t *testing.T, ivLen int) *Manager {
	t.Helper()
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatal(err)
	}
	iv := make([]byte, ivLen)
	if _, err := rand.Read(iv); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	t.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))

	m := NewManager()
	if err := m.Init(); err != nil {
		t.Fatalf("Init with %d-byte IV: %v", ivLen, err)
	}
	return m
}

// The platform's canonical fit encryption (Node/pyfit) uses a fixed Vault IV of a
// NON-standard length (9 bytes in prod). fit-go must Init and round-trip with it —
// previously it hard-failed at "IV must be 12 bytes". Output stays "ciphertext.tag".
func TestEncryptDecrypt_NineByteNonce(t *testing.T) {
	m := managerWithNonce(t, 9)

	const plaintext = "pii-9byte-nonce-secret"
	enc, err := m.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if !strings.Contains(enc, ".") {
		t.Fatalf("expected ciphertext.tag format, got %q", enc)
	}
	dec, err := m.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if dec != plaintext {
		t.Fatalf("round-trip mismatch: got %q, want %q", dec, plaintext)
	}
}

// The standard 12-byte nonce must still work (no regression).
func TestEncryptDecrypt_TwelveByteNonce(t *testing.T) {
	m := managerWithNonce(t, 12)

	const plaintext = "pii-12byte-nonce-secret"
	enc, err := m.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	dec, err := m.Decrypt(enc)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if dec != plaintext {
		t.Fatalf("round-trip mismatch: got %q, want %q", dec, plaintext)
	}
}
