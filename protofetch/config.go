// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Package protofetch safely fetches API contract proto files using the legacy
// fit.config.json shape used by fitproto and pyfit.
package protofetch

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var refPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

// Config is the fit.config.json root.
type Config struct {
	APISpecifications Specification `json:"apiSpecifications"`
}

// Specification preserves both FIT.js gitlabURI and pyfit gitURI aliases.
type Specification struct {
	GitLabURI   string   `json:"gitlabURI"`
	GitURI      string   `json:"gitURI"`
	Branch      string   `json:"branch"`
	FileName    string   `json:"fileName"`
	FolderNames []string `json:"folderNames"`
}

// LoadConfig loads and validates fit.config.json.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("protofetch: read config: %w", err)
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("protofetch: decode config: %w", err)
	}
	return config, nil
}

// Repository returns the configured repository URI.
func (spec Specification) Repository() string {
	if strings.TrimSpace(spec.GitURI) != "" {
		return strings.TrimSpace(spec.GitURI)
	}
	return strings.TrimSpace(spec.GitLabURI)
}

func (spec Specification) validate(mode Mode, sourceDirectory string) error {
	gitURI := strings.TrimSpace(spec.GitURI)
	gitLabURI := strings.TrimSpace(spec.GitLabURI)
	if gitURI != "" && gitLabURI != "" && gitURI != gitLabURI {
		return errors.New("protofetch: apiSpecifications.gitURI and gitlabURI conflict")
	}
	if strings.TrimSpace(sourceDirectory) == "" && spec.Repository() == "" {
		return errors.New("protofetch: apiSpecifications.gitURI or gitlabURI is required")
	}
	branch := strings.TrimSpace(spec.Branch)
	if branch != "" && (!refPattern.MatchString(branch) || strings.HasPrefix(branch, "-") || strings.Contains(branch, "..")) {
		return fmt.Errorf("protofetch: unsafe branch %q", branch)
	}
	switch mode {
	case Get:
		if !safeName(spec.FileName) {
			return fmt.Errorf("protofetch: invalid fileName %q", spec.FileName)
		}
	case GetAll:
		if len(spec.FolderNames) == 0 {
			return errors.New("protofetch: folderNames is required for getall")
		}
		seen := make(map[string]struct{}, len(spec.FolderNames))
		for _, folder := range spec.FolderNames {
			folder = strings.ToLower(strings.TrimSpace(folder))
			if !safeName(folder) {
				return fmt.Errorf("protofetch: invalid folder name %q", folder)
			}
			if _, duplicate := seen[folder]; duplicate {
				return fmt.Errorf("protofetch: duplicate folder name %q", folder)
			}
			seen[folder] = struct{}{}
		}
	default:
		return fmt.Errorf("protofetch: unsupported mode %q", mode)
	}
	return nil
}

func safeName(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}
