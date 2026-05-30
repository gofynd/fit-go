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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// VaultProvider fetches and decrypts the DEK and IV from HashiCorp Vault.
// It reads the encrypted DEK from a KV v2 secret, then decrypts it using
// Vault's Transit engine. Port encryption/provider/vault.ts.
//
// Required environment variables:
// - VAULT_API_ENDPOINT: Vault server URL (e.g. https://vault.example.com)
// - VAULT_TOKEN: authentication token
// - VAULT_DEK_KEY: transit key path (e.g. "transit/mykey")
// - VAULT_DEK_KV_PATH: KV secret path (e.g. "pii/encryption")
// - VAULT_DEK_KV_MOUNT_PATH: KV mount path (e.g. "secret")
type VaultProvider struct {
	endpoint string
	token string
	dekName string
	dekMountPath string
	kvPath string
	kvMountPath string
	client *http.Client
}

// NewVaultProvider creates a VaultProvider configured from environment
// variables. It does not validate or connect to Vault until Init is called.
func NewVaultProvider() *VaultProvider {
	return &VaultProvider{
		endpoint: os.Getenv("VAULT_API_ENDPOINT"),
		token: os.Getenv("VAULT_TOKEN"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Init validates the environment, reads the encrypted DEK from Vault KV v2,
// decrypts it via the Transit engine, and returns the plaintext DEK and IV.
// It retries up to 3 times on transient failures.
func (v *VaultProvider) Init() ([]byte, []byte, error) {
	if err := v.validateEnv(); err != nil {
		return nil, nil, err
	}

	mountPath, dekName := splitPath(os.Getenv("VAULT_DEK_KEY"))
	v.dekMountPath = mountPath
	v.dekName = dekName
	v.kvPath = os.Getenv("VAULT_DEK_KV_PATH")
	v.kvMountPath = os.Getenv("VAULT_DEK_KV_MOUNT_PATH")

	dek, iv, err := v.refreshWithRetry(3)
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/vault: init failed: %w", err)
	}

	if len(dek) == 0 {
		return nil, nil, fmt.Errorf("encryption/vault: DEK was not properly initialized from vault secret")
	}
	if len(iv) == 0 {
		return nil, nil, fmt.Errorf("encryption/vault: IV was not properly initialized from vault secret")
	}

	return dek, iv, nil
}

// validateEnv checks that all required Vault environment variables are set.
func (v *VaultProvider) validateEnv() error {
	required := []string{
		"VAULT_DEK_KEY",
		"VAULT_DEK_KV_PATH",
		"VAULT_DEK_KV_MOUNT_PATH",
		"VAULT_API_ENDPOINT",
		"VAULT_TOKEN",
	}
	for _, name := range required {
		if os.Getenv(name) == "" {
			return fmt.Errorf("encryption/vault: %s missing in environment", name)
		}
	}
	return nil
}

// splitPath splits a slash-separated path into (prefix, lastSegment).
// e.g. "transit/keys/mykey" -> ("transit/keys", "mykey").
func splitPath(path string) (string, string) {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return "", path
	}
	return path[:idx], path[idx+1:]
}

// refreshWithRetry attempts to fetch and decrypt the DEK up to maxRetries
// times, returning the plaintext DEK and IV on success.
func (v *VaultProvider) refreshWithRetry(maxRetries int) ([]byte, []byte, error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		dek, iv, err := v.refresh()
		if err == nil {
			return dek, iv, nil
		}
		lastErr = err
		if attempt < maxRetries-1 {
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
		}
	}
	return nil, nil, fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

// refresh performs a single attempt to read the KV secret and decrypt the DEK.
func (v *VaultProvider) refresh() ([]byte, []byte, error) {
	secret, err := v.readKVSecret()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch KV secret: %w", err)
	}

	data, ok := secret["data"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("unexpected KV response structure: missing data.data")
	}
	innerData, ok := data["data"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("unexpected KV response structure: missing data.data.data")
	}

	encryptedDEK, _ := innerData["DEK"].(string)
	ivB64, _ := innerData["IV"].(string)
	if encryptedDEK == "" || ivB64 == "" {
		return nil, nil, fmt.Errorf("DEK or IV not found in KV secret")
	}

	dek, err := v.decryptDEK(encryptedDEK, ivB64)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decrypt DEK: %w", err)
	}

	iv, err := base64.StdEncoding.DecodeString(ivB64)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode IV: %w", err)
	}

	return dek, iv, nil
}

// readKVSecret reads a secret from Vault KV v2 engine.
func (v *VaultProvider) readKVSecret() (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/v1/%s/data/%s", v.endpoint, v.kvMountPath, v.kvPath)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", v.token)

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault KV read returned %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse vault response: %w", err)
	}
	return result, nil
}

// decryptDEK decrypts the encrypted DEK using Vault's Transit engine.
// ciphertext is the Vault-encrypted DEK, context is the base64-encoded IV used
// as the encryption context.
func (v *VaultProvider) decryptDEK(ciphertext, context string) ([]byte, error) {
	url := fmt.Sprintf("%s/v1/%s/decrypt/%s", v.endpoint, v.dekMountPath, v.dekName)

	payload, err := json.Marshal(map[string]string{
		"ciphertext": ciphertext,
		"context": context,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", v.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := v.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vault transit decrypt returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse transit response: %w", err)
	}

	return base64.StdEncoding.DecodeString(result.Data.Plaintext)
}
