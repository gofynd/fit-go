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

package config

import (
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestGetSecretFromGSMPreservesEncodedRESTPayload(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "test-project")
	oldTransport := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := ""
		switch request.URL.Host {
		case "metadata.google.internal":
			body = `{"access_token":"test-token","token_type":"Bearer","expires_in":300}`
		case "secretmanager.googleapis.com":
			if got := request.Header.Get("Authorization"); got != "Bearer test-token" {
				t.Fatalf("Authorization = %q", got)
			}
			body = `{"payload":{"data":"` + base64.StdEncoding.EncodeToString([]byte("postgres://decoded")) + `"}}`
		default:
			t.Fatalf("unexpected request URL: %s", request.URL)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    request,
		}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = oldTransport })

	want := base64.StdEncoding.EncodeToString([]byte("postgres://decoded"))
	got, err := GetSecretFromGSM("database-url", "7")
	if err != nil {
		t.Fatalf("GetSecretFromGSM() error = %v", err)
	}
	if got != want {
		t.Fatalf("GetSecretFromGSM() = %q, want encoded payload %q", got, want)
	}

	decoded, err := GetDecodedSecretFromGSM("database-url", "7")
	if err != nil {
		t.Fatalf("GetDecodedSecretFromGSM() error = %v", err)
	}
	if decoded != "postgres://decoded" {
		t.Fatalf("GetDecodedSecretFromGSM() = %q, want decoded payload", decoded)
	}
}

func TestDecodeSecretVersionResponse(t *testing.T) {
	secret := "mongodb://user:p@ss@example.internal/db?retryWrites=true"
	body := []byte(`{"name":"projects/test/secrets/db/versions/1","payload":{"data":"` +
		base64.StdEncoding.EncodeToString([]byte(secret)) + `"}}`)

	got, err := decodeSecretVersionResponse(body, "projects/test/secrets/db/versions/1")
	if err != nil {
		t.Fatalf("decodeSecretVersionResponse() error = %v", err)
	}
	if got != secret {
		t.Fatalf("decodeSecretVersionResponse() = %q, want %q", got, secret)
	}
}

func TestDecodeSecretVersionResponseRejectsInvalidPayload(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string
	}{
		{name: "invalid JSON", body: `{`, wantErr: "failed to parse response"},
		{name: "empty payload", body: `{"payload":{"data":""}}`, wantErr: "secret value is empty"},
		{name: "invalid base64", body: `{"payload":{"data":"not-base64!"}}`, wantErr: "failed to decode secret payload"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeSecretVersionResponse([]byte(tt.body), "projects/test/secrets/db/versions/1")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}
