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

package encryption

import (
	"encoding/base64"
	"os"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Manager tests
// ---------------------------------------------------------------------------

func TestNewManager(t *testing.T) {
	mgr := NewManager()
	if mgr == nil {
		t.Fatal("NewManager() returned nil")
	}
	if mgr.initialized {
		t.Error("New manager should not be initialized")
	}
}

func TestManager_Init_InlineKeys(t *testing.T) {
	// Generate valid test keys
	dek := make([]byte, 32) // AES-256 requires 32 bytes
	iv := make([]byte, 12) // GCM nonce is 12 bytes
	for i := range dek {
		dek[i] = byte(i)
	}
	for i := range iv {
		iv[i] = byte(i + 100)
	}

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	err := mgr.Init()
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if !mgr.IsInitialized() {
		t.Error("Manager should be initialized")
	}
}

func TestManager_Init_InvalidDEKBase64(t *testing.T) {
	os.Setenv("PII_DEK_BASE64", "not-valid-base64!!!")
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 12)))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	err := mgr.Init()
	if err == nil {
		t.Error("Init() should fail with invalid DEK base64")
	}
}

func TestManager_Init_InvalidIVBase64(t *testing.T) {
	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	os.Setenv("PII_IV_BASE64", "not-valid-base64!!!")
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	err := mgr.Init()
	if err == nil {
		t.Error("Init() should fail with invalid IV base64")
	}
}

func TestManager_Init_WrongDEKSize(t *testing.T) {
	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 16))) // wrong size
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 12)))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	err := mgr.Init()
	if err == nil {
		t.Error("Init() should fail with wrong DEK size")
	}
}

func TestManager_Init_WrongIVSize(t *testing.T) {
	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 32)))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(make([]byte, 16))) // wrong size
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	err := mgr.Init()
	if err == nil {
		t.Error("Init() should fail with wrong IV size")
	}
}

func TestManager_Init_Idempotent(t *testing.T) {
	dek := make([]byte, 32)
	iv := make([]byte, 12)

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	err1 := mgr.Init()
	err2 := mgr.Init() // second init should be no-op

	if err1 != nil || err2 != nil {
		t.Errorf("Init() errors: %v, %v", err1, err2)
	}
}

func TestManager_EncryptDecrypt(t *testing.T) {
	dek := make([]byte, 32)
	iv := make([]byte, 12)
	for i := range dek {
		dek[i] = byte(i % 256)
	}

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	if err := mgr.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	tests := []string{
		"hello world",
		"sensitive data 123",
		"",
		"unicode: 日本語",
		"special chars: !@#$%^&*()",
	}

	for _, plaintext := range tests {
		t.Run(plaintext, func(t *testing.T) {
			encrypted, err := mgr.Encrypt(plaintext)
			if err != nil {
				t.Fatalf("Encrypt() error = %v", err)
			}

			if encrypted == plaintext && plaintext != "" {
				t.Error("Encrypted should differ from plaintext")
			}

			decrypted, err := mgr.Decrypt(encrypted)
			if err != nil {
				t.Fatalf("Decrypt() error = %v", err)
			}

			if decrypted != plaintext {
				t.Errorf("Decrypt() = %q, want %q", decrypted, plaintext)
			}
		})
	}
}

func TestManager_EncryptNotInitialized(t *testing.T) {
	os.Unsetenv("PII_DEK_BASE64")
	os.Unsetenv("PII_IV_BASE64")

	mgr := NewManager()
	_, err := mgr.Encrypt("test")
	if err == nil {
		t.Error("Encrypt() should fail when not initialized")
	}
}

func TestManager_DecryptNotInitialized(t *testing.T) {
	os.Unsetenv("PII_DEK_BASE64")
	os.Unsetenv("PII_IV_BASE64")

	mgr := NewManager()
	_, err := mgr.Decrypt("test.test")
	if err == nil {
		t.Error("Decrypt() should fail when not initialized")
	}
}

func TestManager_DecryptInvalidFormat(t *testing.T) {
	dek := make([]byte, 32)
	iv := make([]byte, 12)

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	mgr.Init()

	// Missing separator
	_, err := mgr.Decrypt("no-separator")
	if err == nil {
		t.Error("Decrypt() should fail with invalid format")
	}

	// Invalid base64 in ciphertext
	_, err = mgr.Decrypt("!!!invalid!!.dGVzdA==")
	if err == nil {
		t.Error("Decrypt() should fail with invalid ciphertext base64")
	}

	// Invalid base64 in tag
	_, err = mgr.Decrypt("dGVzdA==.!!!invalid!!")
	if err == nil {
		t.Error("Decrypt() should fail with invalid tag base64")
	}
}

func TestManager_DecryptWrongTag(t *testing.T) {
	dek := make([]byte, 32)
	iv := make([]byte, 12)

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	mgr.Init()

	encrypted, _ := mgr.Encrypt("test message")
	// Corrupt the tag
	corruptedTag := base64.StdEncoding.EncodeToString([]byte("wrongtagvalue000"))
	parts := encrypted[:len(encrypted)-24] + "." + corruptedTag

	_, err := mgr.Decrypt(parts)
	if err == nil {
		t.Error("Decrypt() should fail with wrong authentication tag")
	}
}

func TestManager_CacheHit(t *testing.T) {
	dek := make([]byte, 32)
	iv := make([]byte, 12)

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	mgr.Init()

	plaintext := "cached message"

	// First encryption
	encrypted1, _ := mgr.Encrypt(plaintext)

	// Second encryption should hit cache
	encrypted2, _ := mgr.Encrypt(plaintext)

	if encrypted1 != encrypted2 {
		t.Error("Second encryption should return cached result")
	}

	// Decrypt and check cache
	decrypted1, _ := mgr.Decrypt(encrypted1)
	decrypted2, _ := mgr.Decrypt(encrypted1)

	if decrypted1 != decrypted2 {
		t.Error("Decryption cache should return same result")
	}
}

func TestManager_ConcurrentAccess(t *testing.T) {
	dek := make([]byte, 32)
	iv := make([]byte, 12)

	os.Setenv("PII_DEK_BASE64", base64.StdEncoding.EncodeToString(dek))
	os.Setenv("PII_IV_BASE64", base64.StdEncoding.EncodeToString(iv))
	defer func() {
		os.Unsetenv("PII_DEK_BASE64")
		os.Unsetenv("PII_IV_BASE64")
	}()

	mgr := NewManager()
	mgr.Init()

	var wg sync.WaitGroup

	// Concurrent encryption
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := mgr.Encrypt("message " + string(rune('0'+i%10)))
			if err != nil {
				t.Errorf("Concurrent Encrypt() error = %v", err)
			}
		}(i)
	}

	// Concurrent decryption
	encrypted, _ := mgr.Encrypt("shared message")
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mgr.Decrypt(encrypted)
			if err != nil {
				t.Errorf("Concurrent Decrypt() error = %v", err)
			}
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// LRU Cache tests
// ---------------------------------------------------------------------------

func TestLRUCache_Basic(t *testing.T) {
	cache := newLRUCache(3, 1000)

	cache.Set("k1", "v1")
	cache.Set("k2", "v2")
	cache.Set("k3", "v3")

	v, ok := cache.Get("k1")
	if !ok || v != "v1" {
		t.Errorf("Get(k1) = (%q, %v), want (v1, true)", v, ok)
	}

	v, ok = cache.Get("k2")
	if !ok || v != "v2" {
		t.Errorf("Get(k2) = (%q, %v), want (v2, true)", v, ok)
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	cache := newLRUCache(2, 1000) // max 2 entries

	cache.Set("k1", "v1")
	cache.Set("k2", "v2")
	cache.Set("k3", "v3") // should evict k1

	_, ok := cache.Get("k1")
	if ok {
		t.Error("k1 should be evicted")
	}

	v, ok := cache.Get("k2")
	if !ok || v != "v2" {
		t.Errorf("k2 should still exist: (%q, %v)", v, ok)
	}

	v, ok = cache.Get("k3")
	if !ok || v != "v3" {
		t.Errorf("k3 should exist: (%q, %v)", v, ok)
	}
}

func TestLRUCache_LRUOrder(t *testing.T) {
	cache := newLRUCache(2, 1000)

	cache.Set("k1", "v1")
	cache.Set("k2", "v2")

	// Access k1 to make it MRU
	cache.Get("k1")

	// Add k3 - should evict k2 (LRU)
	cache.Set("k3", "v3")

	v, ok := cache.Get("k1")
	if !ok || v != "v1" {
		t.Error("k1 should still exist after access")
	}

	_, ok = cache.Get("k2")
	if ok {
		t.Error("k2 should be evicted (was LRU)")
	}
}

func TestLRUCache_Update(t *testing.T) {
	cache := newLRUCache(5, 1000)

	cache.Set("k1", "v1")
	cache.Set("k1", "v2") // update

	v, ok := cache.Get("k1")
	if !ok || v != "v2" {
		t.Errorf("Get(k1) = (%q, %v), want (v2, true)", v, ok)
	}
}

func TestLRUCache_MaxSize(t *testing.T) {
	cache := newLRUCache(100, 20) // small max size

	// These entries exceed max size
	cache.Set("k1", "value1") // 8 bytes
	cache.Set("k2", "value2") // 8 bytes
	cache.Set("k3", "value3") // 8 bytes - should trigger eviction

	// At least one entry should be evicted
	count := 0
	for _, k := range []string{"k1", "k2", "k3"} {
		if _, ok := cache.Get(k); ok {
			count++
		}
	}

	if count == 3 {
		t.Error("Cache should evict entries when max size exceeded")
	}
}

func TestLRUCache_ConcurrentAccess(t *testing.T) {
	cache := newLRUCache(100, 10000)

	var wg sync.WaitGroup

	// Concurrent sets
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i%10))
			cache.Set(key, "value"+string(rune('0'+i%10)))
		}(i)
	}

	// Concurrent gets
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('0'+i%10))
			cache.Get(key)
		}(i)
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// Provider tests
// ---------------------------------------------------------------------------

func TestCreateProvider_Vault(t *testing.T) {
	os.Setenv("ENCRYPTION_PROVIDER", "vault")
	defer os.Unsetenv("ENCRYPTION_PROVIDER")

	mgr := NewManager()
	provider, err := mgr.createProvider()
	if err != nil {
		t.Fatalf("createProvider() error = %v", err)
	}

	if _, ok := provider.(*VaultProvider); !ok {
		t.Error("Expected VaultProvider")
	}
}

func TestCreateProvider_GCP(t *testing.T) {
	os.Setenv("ENCRYPTION_PROVIDER", "gcp-kms")
	defer os.Unsetenv("ENCRYPTION_PROVIDER")

	mgr := NewManager()
	provider, err := mgr.createProvider()
	if err != nil {
		t.Fatalf("createProvider() error = %v", err)
	}

	if _, ok := provider.(*GCPProvider); !ok {
		t.Error("Expected GCPProvider")
	}
}

func TestCreateProvider_Default(t *testing.T) {
	os.Unsetenv("ENCRYPTION_PROVIDER")

	mgr := NewManager()
	provider, err := mgr.createProvider()
	if err != nil {
		t.Fatalf("createProvider() error = %v", err)
	}

	if _, ok := provider.(*VaultProvider); !ok {
		t.Error("Default should be VaultProvider")
	}
}

func TestCreateProvider_Unknown(t *testing.T) {
	os.Setenv("ENCRYPTION_PROVIDER", "unknown-provider")
	defer os.Unsetenv("ENCRYPTION_PROVIDER")

	mgr := NewManager()
	_, err := mgr.createProvider()
	if err == nil {
		t.Error("createProvider() should fail for unknown provider")
	}
}

// ---------------------------------------------------------------------------
// Vault provider tests
// ---------------------------------------------------------------------------

func TestVaultProvider_ValidateEnv_Missing(t *testing.T) {
	// Clear all vault env vars
	for _, v := range []string{
		"VAULT_DEK_KEY",
		"VAULT_DEK_KV_PATH",
		"VAULT_DEK_KV_MOUNT_PATH",
		"VAULT_API_ENDPOINT",
		"VAULT_TOKEN",
	} {
		os.Unsetenv(v)
	}

	provider := NewVaultProvider()
	err := provider.validateEnv()
	if err == nil {
		t.Error("validateEnv() should fail with missing env vars")
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path string
		prefix string
		name string
	}{
		{"transit/keys/mykey", "transit/keys", "mykey"},
		{"simple", "", "simple"},
		{"a/b/c/d", "a/b/c", "d"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			prefix, name := splitPath(tt.path)
			if prefix != tt.prefix || name != tt.name {
				t.Errorf("splitPath(%q) = (%q, %q), want (%q, %q)",
					tt.path, prefix, name, tt.prefix, tt.name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GCP provider tests
// ---------------------------------------------------------------------------

func TestGCPProvider_ValidateEnv_Missing(t *testing.T) {
	for _, v := range []string{
		"GCP_PII_KMS_KEY_RING",
		"GCP_PII_KMS_KEY",
		"GSM_PII_DEK_SECRET_ID",
		"GSM_PII_IV_SECRET_ID",
	} {
		os.Unsetenv(v)
	}

	provider := NewGCPProvider()
	err := provider.validateEnv()
	if err == nil {
		t.Error("validateEnv() should fail with missing env vars")
	}
}

func TestGCPProvider_DefaultRegion(t *testing.T) {
	os.Unsetenv("GCP_PII_REGION")
	os.Unsetenv("GCP_REGION")

	provider := NewGCPProvider()
	if provider.location != "asia-south1" {
		t.Errorf("Default region = %q, want asia-south1", provider.location)
	}
}

func TestGCPProvider_CustomRegion(t *testing.T) {
	os.Setenv("GCP_PII_REGION", "us-central1")
	defer os.Unsetenv("GCP_PII_REGION")

	provider := NewGCPProvider()
	if provider.location != "us-central1" {
		t.Errorf("Region = %q, want us-central1", provider.location)
	}
}

func TestGCPProvider_FallbackRegion(t *testing.T) {
	os.Unsetenv("GCP_PII_REGION")
	os.Setenv("GCP_REGION", "europe-west1")
	defer os.Unsetenv("GCP_REGION")

	provider := NewGCPProvider()
	if provider.location != "europe-west1" {
		t.Errorf("Fallback region = %q, want europe-west1", provider.location)
	}
}

// ---------------------------------------------------------------------------
// Helper tests
// ---------------------------------------------------------------------------

func TestEnvInt(t *testing.T) {
	t.Run("valid int", func(t *testing.T) {
		os.Setenv("TEST_INT", "123")
		defer os.Unsetenv("TEST_INT")

		if got := envInt("TEST_INT", 999); got != 123 {
			t.Errorf("envInt() = %d, want 123", got)
		}
	})

	t.Run("invalid int", func(t *testing.T) {
		os.Setenv("TEST_INT", "not-a-number")
		defer os.Unsetenv("TEST_INT")

		if got := envInt("TEST_INT", 999); got != 999 {
			t.Errorf("envInt() = %d, want 999 (default)", got)
		}
	})

	t.Run("missing", func(t *testing.T) {
		os.Unsetenv("TEST_INT")

		if got := envInt("TEST_INT", 999); got != 999 {
			t.Errorf("envInt() = %d, want 999 (default)", got)
		}
	})
}
