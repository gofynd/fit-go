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
	"fmt"
	"os"
	"strings"
)

// ResolveConnectionString resolves a database connection string value. When
// DB_CONNECTION_PROVIDER is set to "GSM", the envValue is treated as a Google
// Secret Manager secret name and the actual connection string is fetched from
// GCP. Otherwise the value is returned as-is.
func ResolveConnectionString(envValue string) (string, error) {
	provider := strings.ToUpper(strings.TrimSpace(os.Getenv("DB_CONNECTION_PROVIDER")))
	if provider == "GSM" {
		version := os.Getenv("DB_CONNECTION_SECRET_VERSION")
		if version == "" {
			version = "latest"
		}
		secret, err := GetSecretFromGSM(envValue, version)
		if err != nil {
			return "", fmt.Errorf("failed to resolve GSM secret %q (version %s): %w", envValue, version, err)
		}
		return secret, nil
	}
	return envValue, nil
}

// GetDeploymentName extracts a Kubernetes deployment name from a pod name.
// Supported pod names follow the convention:
//
//	<deployment-name>dply-<replicaset>-<pod-hash>
//	<deployment-name>cron-<replicaset>-<pod-hash>
//
// The function returns the deployment name including the "dply" or "cron"
// suffix, or an empty string if the pattern is not recognized.
func GetDeploymentName(podName string) string {
	if podName == "" {
		return ""
	}

	if idx := strings.Index(podName, "dply"); idx >= 0 {
		return podName[:idx] + "dply"
	}

	if idx := strings.Index(podName, "cron"); idx >= 0 {
		return podName[:idx] + "cron"
	}

	return ""
}

// GetAppNameForDBOptions generates the application name string used in database
// connection options (e.g., MongoDB appName, PostgreSQL application_name). The
// format is "<namespace>-<deploymentName>" when both are available, falling
// back to just the deployment name or SERVICE_NAME.
func GetAppNameForDBOptions() string {
	deploymentName := GetDeploymentName(os.Getenv("K8S_POD_NAME"))
	if deploymentName == "" {
		deploymentName = os.Getenv("SERVICE_NAME")
	}

	namespace := os.Getenv("K8S_POD_NAMESPACE")
	if namespace == "default" {
		namespace = ""
	}

	if deploymentName != "" && namespace != "" {
		return namespace + "-" + deploymentName
	}
	if deploymentName != "" {
		return deploymentName
	}
	return ""
}

// LookupEnvInt reads an environment variable and parses it as an integer.
// Returns the default value if the variable is unset or cannot be parsed.
func LookupEnvInt(key string, defaultValue int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	var result int
	_, err := fmt.Sscanf(v, "%d", &result)
	if err != nil {
		return defaultValue
	}
	return result
}

// LookupEnvBool reads an environment variable and parses it as a boolean.
// Truthy values: "true", "1", "yes", "on" (case-insensitive).
// Returns the default value if the variable is unset or unrecognized.
func LookupEnvBool(key string, defaultValue bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		return defaultValue
	}
}

// LookupEnvString reads an environment variable, returning the default if unset.
func LookupEnvString(key, defaultValue string) string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	return v
}

// LookupEnvStringSlice reads an environment variable and splits it on commas.
// Returns the default value if the variable is unset.
func LookupEnvStringSlice(key string, defaultValue []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return defaultValue
	}
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return defaultValue
	}
	return result
}

// EnvConnectionPrefix returns the environment variable prefix for a database
// service's connection options. For example, ("MONGO", "CART", "READ") returns
// "MONGO_CART_READ_".
func EnvConnectionPrefix(dbType, serviceName, connectionType string) string {
	return strings.ToUpper(dbType) + "_" +
		strings.ToUpper(serviceName) + "_" +
		strings.ToUpper(connectionType) + "_"
}
