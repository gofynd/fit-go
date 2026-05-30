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
	"time"
)

// GCPProvider fetches and decrypts the DEK and IV from Google Cloud Platform.
// The encrypted DEK is stored in Secret Manager and decrypted via Cloud KMS.
// The IV is stored as a base64-encoded string in Secret Manager.
//
// Required environment variables:
// - GCP_PII_REGION (or GCP_REGION): GCP region (default: "asia-south1")
// - GCP_PII_KMS_KEY_RING: Cloud KMS key ring name
// - GCP_PII_KMS_KEY: Cloud KMS crypto key name
// - GSM_PII_DEK_SECRET_ID: Secret Manager secret ID for the encrypted DEK
// - GSM_PII_IV_SECRET_ID: Secret Manager secret ID for the IV
//
// Authentication is handled via the GCP metadata server and Application Default
// Credentials (ADC). The provider fetches the project ID from the metadata
// server and uses ADC access tokens for API calls.
type GCPProvider struct {
	location  string
	keyRing   string
	cryptoKey string
	secretID  string
	ivSecret  string
	projectID string
	client    *http.Client
}

// NewGCPProvider creates a GCPProvider configured from environment variables.
func NewGCPProvider() *GCPProvider {
	location := os.Getenv("GCP_PII_REGION")
	if location == "" {
		location = os.Getenv("GCP_REGION")
	}
	if location == "" {
		location = "asia-south1"
	}

	return &GCPProvider{
		location:  location,
		keyRing:   os.Getenv("GCP_PII_KMS_KEY_RING"),
		cryptoKey: os.Getenv("GCP_PII_KMS_KEY"),
		secretID:  os.Getenv("GSM_PII_DEK_SECRET_ID"),
		ivSecret:  os.Getenv("GSM_PII_IV_SECRET_ID"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Init validates the environment, fetches the encrypted DEK from Secret
// Manager, decrypts it via Cloud KMS, and returns the plaintext DEK and IV.
func (g *GCPProvider) Init() ([]byte, []byte, error) {
	if err := g.validateEnv(); err != nil {
		return nil, nil, err
	}

	// Fetch project ID from metadata server.
	projectID, err := g.getProjectID()
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/gcp: could not detect GCP project ID: %w", err)
	}
	g.projectID = projectID

	// Fetch access token via ADC metadata server.
	token, err := g.getAccessToken()
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/gcp: could not get access token: %w", err)
	}

	// Fetch encrypted DEK from Secret Manager.
	encryptedDEK, err := g.accessSecret(g.secretID, token)
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/gcp: failed to fetch encrypted DEK: %w", err)
	}
	if len(encryptedDEK) == 0 {
		return nil, nil, fmt.Errorf("encryption/gcp: encrypted DEK not found in Secret Manager payload")
	}

	// Decrypt DEK using KMS.
	dek, err := g.kmsDecrypt(encryptedDEK, token)
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/gcp: KMS decryption failed: %w", err)
	}
	if len(dek) == 0 {
		return nil, nil, fmt.Errorf("encryption/gcp: DEK was not properly initialized from secret")
	}

	// Fetch IV from Secret Manager.
	ivData, err := g.accessSecret(g.ivSecret, token)
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/gcp: failed to fetch IV: %w", err)
	}
	if len(ivData) == 0 {
		return nil, nil, fmt.Errorf("encryption/gcp: IV not found in Secret Manager payload")
	}

	// IV is stored as base64 string in Secret Manager.
	iv, err := base64.StdEncoding.DecodeString(string(ivData))
	if err != nil {
		return nil, nil, fmt.Errorf("encryption/gcp: failed to decode IV: %w", err)
	}
	if len(iv) == 0 {
		return nil, nil, fmt.Errorf("encryption/gcp: IV was not properly initialized from secret")
	}

	return dek, iv, nil
}

// validateEnv checks that all required GCP environment variables are set.
func (g *GCPProvider) validateEnv() error {
	checks := map[string]string{
		"GCP_PII_KMS_KEY_RING":  g.keyRing,
		"GCP_PII_KMS_KEY":       g.cryptoKey,
		"GSM_PII_DEK_SECRET_ID": g.secretID,
		"GSM_PII_IV_SECRET_ID":  g.ivSecret,
	}
	for name, val := range checks {
		if val == "" {
			return fmt.Errorf("encryption/gcp: %s is required", name)
		}
	}
	return nil
}

// getProjectID fetches the GCP project ID from the instance metadata server.
func (g *GCPProvider) getProjectID() (string, error) {
	req, err := http.NewRequest(http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/project/project-id", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := g.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned %d: %s", resp.StatusCode, string(body))
	}
	return string(body), nil
}

// getAccessToken fetches an OAuth2 access token from the instance metadata
// server using Application Default Credentials.
func (g *GCPProvider) getAccessToken() (string, error) {
	req, err := http.NewRequest(http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	resp, err := g.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata token endpoint returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}
	return tokenResp.AccessToken, nil
}

// accessSecret reads the latest version of a secret from GCP Secret Manager.
func (g *GCPProvider) accessSecret(secretID, token string) ([]byte, error) {
	url := fmt.Sprintf(
		"https://secretmanager.googleapis.com/v1/projects/%s/secrets/%s/versions/latest:access",
		g.projectID, secretID,
	)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("secret manager returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Payload struct {
			Data string `json:"data"` // base64-encoded by the API
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse secret response: %w", err)
	}

	return base64.StdEncoding.DecodeString(result.Payload.Data)
}

// kmsDecrypt decrypts ciphertext using GCP Cloud KMS.
func (g *GCPProvider) kmsDecrypt(ciphertext []byte, token string) ([]byte, error) {
	url := fmt.Sprintf(
		"https://cloudkms.googleapis.com/v1/projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s:decrypt",
		g.projectID, g.location, g.keyRing, g.cryptoKey,
	)

	payload, err := json.Marshal(map[string]string{
		"ciphertext": base64.StdEncoding.EncodeToString(ciphertext),
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, io.NopCloser(
		jsonReader(payload),
	))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("KMS decrypt returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Plaintext string `json:"plaintext"` // base64-encoded
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse KMS response: %w", err)
	}
	if result.Plaintext == "" {
		return nil, fmt.Errorf("KMS returned empty plaintext")
	}

	return base64.StdEncoding.DecodeString(result.Plaintext)
}

// jsonReader wraps a byte slice in a strings.Reader for use as an io.Reader.
func jsonReader(data []byte) io.Reader {
	return io.NopCloser(readerFromBytes(data))
}

type bytesReader struct {
	data []byte
	pos  int
}

func readerFromBytes(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
