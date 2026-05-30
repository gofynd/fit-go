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

// Package server provides the HTTP server module for the fit.go framework.
package server

import (
	"fmt"
	"strings"
)

// ServerType represents the type of server that determines which routes are loaded.
// In Fynd Commerce, services are split by server type to isolate concerns.
type ServerType int

const (
	// ServerTypeDefault is the zero value; used when no type is set.
	ServerTypeDefault ServerType = iota
	// ServerTypePlatform serves the platform (back-office) API.
	ServerTypePlatform
	// ServerTypeApplication serves the storefront/application API.
	ServerTypeApplication
	// ServerTypePartner serves the partner/extension API.
	ServerTypePartner
	// ServerTypeInternal serves internal service-to-service API.
	ServerTypeInternal
	// ServerTypeWebhook serves webhook callback endpoints.
	ServerTypeWebhook
	// ServerTypeAdministrator serves the administrator panel API.
	ServerTypeAdministrator
	// ServerTypePublic serves unauthenticated public API.
	ServerTypePublic
	// ServerTypePortal serves the portal API.
	ServerTypePortal
	// ServerTypePanel serves the panel API.
	ServerTypePanel
	// ServerTypeDev serves development-only routes.
	ServerTypeDev
	// ServerTypeCommon serves routes shared across all types.
	ServerTypeCommon
)

// serverTypeNames maps ServerType to its string representation.
var serverTypeNames = map[ServerType]string{
	ServerTypePlatform: "platform",
	ServerTypeApplication: "application",
	ServerTypePartner: "partner",
	ServerTypeInternal: "internal",
	ServerTypeWebhook: "webhook",
	ServerTypeAdministrator: "administrator",
	ServerTypePublic: "public",
	ServerTypePortal: "portal",
	ServerTypePanel: "panel",
	ServerTypeDev: "dev",
	ServerTypeCommon: "common",
}

// serverTypeFromString is the reverse lookup table.
var serverTypeFromString map[string]ServerType

func init() {
	serverTypeFromString = make(map[string]ServerType, len(serverTypeNames))
	for k, v := range serverTypeNames {
		serverTypeFromString[v] = k
	}
}

// String returns the lowercase name of the server type.
func (s ServerType) String() string {
	if name, ok := serverTypeNames[s]; ok {
		return name
	}
	return "default"
}

// ParseServerType converts a string to a ServerType.
// Returns an error if the string is not a recognised server type.
func ParseServerType(s string) (ServerType, error) {
	normalized := strings.TrimSpace(strings.ToLower(s))
	if st, ok := serverTypeFromString[normalized]; ok {
		return st, nil
	}
	return ServerTypeDefault, fmt.Errorf("server: unknown server type %q", s)
}

// ParseServerTypes parses a comma-separated list of server type names.
// This corresponds to the SERVER_TYPE env var.
func ParseServerTypes(csv string) ([]ServerType, error) {
	parts := strings.Split(csv, ",")
	var types []ServerType
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		st, err := ParseServerType(trimmed)
		if err != nil {
			return nil, err
		}
		types = append(types, st)
	}
	return types, nil
}

// AllServerTypes returns a slice of every defined ServerType (excluding Default).
func AllServerTypes() []ServerType {
	return []ServerType{
		ServerTypePlatform,
		ServerTypeApplication,
		ServerTypePartner,
		ServerTypeInternal,
		ServerTypeWebhook,
		ServerTypeAdministrator,
		ServerTypePublic,
		ServerTypePortal,
		ServerTypePanel,
		ServerTypeDev,
		ServerTypeCommon,
	}
}

// ValidServerTypes is a set for quick lookups.
var ValidServerTypes = func() map[ServerType]struct{} {
	m := make(map[ServerType]struct{})
	for _, st := range AllServerTypes() {
		m[st] = struct{}{}
	}
	return m
}()
