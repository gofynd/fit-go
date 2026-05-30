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

// Package encryption provides AES-256-GCM encryption and decryption with LRU
// caching and pluggable key providers (Vault, GCP KMS). Port
// modules/encryption.
package encryption

// Provider is the interface for key providers that supply a Data Encryption Key
// (DEK) and Initialization Vector (IV). Implementations fetch and decrypt these
// secrets from external systems (e.g. HashiCorp Vault, GCP KMS).
type Provider interface {
	// Init fetches and returns the plaintext DEK and IV. The DEK must be
	// exactly 32 bytes (AES-256) and the IV must be 12 bytes (GCM standard
	// nonce size). Implementations should validate these sizes before
	// returning.
	Init() (dek []byte, iv []byte, err error)
}
