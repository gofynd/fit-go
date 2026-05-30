// Example 08: Envelope encryption (AES-256-GCM)
//
// The encryption package encrypts/decrypts strings with AES-256-GCM. The data
// encryption key (DEK) and IV come from a pluggable provider selected via env:
//
//	ENCRYPTION_KEY_PROVIDER=inline|vault|gcp
//
// For local use with the inline provider, supply base64-encoded keys:
//
//	ENCRYPTION_KEY_PROVIDER=inline
//	ENCRYPTION_DEK=<base64 32-byte key>
//	ENCRYPTION_IV=<base64 12-byte iv>
//
// Vault and GCP KMS providers fetch the key material from those systems.
//
// Run:
//
//	go run ./examples/08-encryption
package main

import (
	"fmt"
	"log"

	"github.com/gofynd/fit-go/encryption"
)

func main() {
	mgr := encryption.NewManager()

	// Init resolves the configured key provider and loads key material.
	// It is idempotent and safe to call once at startup.
	if err := mgr.Init(); err != nil {
		log.Fatalf("encryption init failed (set ENCRYPTION_* env vars): %v", err)
	}
	if !mgr.IsInitialized() {
		log.Fatal("encryption manager not initialized")
	}

	plaintext := "card-number=4111111111111111"

	ciphertext, err := mgr.Encrypt(plaintext)
	if err != nil {
		log.Fatalf("encrypt: %v", err)
	}
	fmt.Println("ciphertext:", ciphertext)

	decrypted, err := mgr.Decrypt(ciphertext)
	if err != nil {
		log.Fatalf("decrypt: %v", err)
	}
	fmt.Println("decrypted :", decrypted)

	fmt.Println("round-trip ok:", decrypted == plaintext)
}
