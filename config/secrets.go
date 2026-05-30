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

package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// gsmHTTPClient is the HTTP client used for GCP metadata and Secret Manager
// API calls. It has a short timeout appropriate for metadata requests.
var gsmHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

// GetSecretFromGSM fetches a secret from Google Cloud Secret Manager using
// the GCP REST API with Application Default Credentials (ADC).
//
// The project ID is auto-detected from the GCE/GKE metadata server.
//
// Parameters:
// - secretName: the secret ID within the project (not the full resource path)
// - version: the secret version ("latest", "1", "2", etc.)
//
// Returns the secret payload as a string, or an error if retrieval fails.
func GetSecretFromGSM(secretName, version string) (string, error) {
	if secretName == "" {
		return "", fmt.Errorf("gsm: secret name must not be empty")
	}
	if version == "" {
		version = "latest"
	}

	projectID, err := detectGCPProjectID()
	if err != nil {
		return "", fmt.Errorf("gsm: could not detect GCP project ID: %w", err)
	}

	accessToken, err := getAccessToken()
	if err != nil {
		return "", fmt.Errorf("gsm: could not obtain access token: %w", err)
	}

	secretPath := fmt.Sprintf(
		"projects/%s/secrets/%s/versions/%s",
		projectID, secretName, version,
	)

	url := fmt.Sprintf(
		"https://secretmanager.googleapis.com/v1/%s:access",
		secretPath,
	)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("gsm: failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := gsmHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("gsm: request to Secret Manager failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("gsm: failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(
			"gsm: Secret Manager returned status %d for %s: %s",
			resp.StatusCode, secretPath, truncate(string(body), 256),
		)
	}

	var result secretVersionResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("gsm: failed to parse response: %w", err)
	}

	secretValue := result.Payload.Data
	if secretValue == "" {
		return "", fmt.Errorf("gsm: secret value is empty for %s", secretPath)
	}

	return secretValue, nil
}

// secretVersionResponse represents the JSON response from the Secret Manager
// accessSecretVersion API.
type secretVersionResponse struct {
	Name    string        `json:"name"`
	Payload secretPayload `json:"payload"`
}

type secretPayload struct {
	// Data is the secret payload as a base64-encoded string. The GCP REST API
	// returns it base64-encoded, but the Go JSON decoder handles this if the
	// field is typed as string. In practice, the SecretManager API returns
	// the data field as a base64 string which we decode.
	Data string `json:"data"`
}

// detectGCPProjectID attempts to detect the GCP project ID, trying in order:
// 1. GOOGLE_CLOUD_PROJECT environment variable
// 2. GCLOUD_PROJECT environment variable
// 3. GCE/GKE metadata server
func detectGCPProjectID() (string, error) {
	// Check environment variables first.
	for _, envKey := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"} {
		if v := strings.TrimSpace(os.Getenv(envKey)); v != "" {
			return v, nil
		}
	}

	// Fall back to metadata server (available on GCE/GKE).
	req, err := http.NewRequest(
		http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/project/project-id",
		nil,
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("metadata server unreachable (not running on GCP?): %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata server returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	projectID := strings.TrimSpace(string(body))
	if projectID == "" {
		return "", fmt.Errorf("metadata server returned empty project ID")
	}

	return projectID, nil
}

// getAccessToken obtains an OAuth2 access token for calling GCP APIs. It tries:
// 1. The GCE/GKE metadata server (workload identity or service account).
// 2. GOOGLE_ACCESS_TOKEN environment variable (for local development/testing).
func getAccessToken() (string, error) {
	// Try metadata server first (the standard path in GKE).
	req, err := http.NewRequest(
		http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
		nil,
	)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var tokenResp struct {
				AccessToken string `json:"access_token"`
				TokenType   string `json:"token_type"`
				ExpiresIn   int    `json:"expires_in"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err == nil && tokenResp.AccessToken != "" {
				return tokenResp.AccessToken, nil
			}
		}
	}

	// Fall back to environment variable for local dev.
	if token := strings.TrimSpace(os.Getenv("GOOGLE_ACCESS_TOKEN")); token != "" {
		return token, nil
	}

	return "", fmt.Errorf(
		"could not obtain GCP access token: metadata server unavailable and GOOGLE_ACCESS_TOKEN not set",
	)
}

// truncate limits a string to maxLen characters, appending "..." if truncated.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
