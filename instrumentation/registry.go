// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

// Package instrumentation provides a statically linked replacement for
// TraceClue's runtime instrumentation extension map. Applications register
// typed factories at compile time; legacy environment names only select among
// those known factories.
package instrumentation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

const (
	// ExtraEnv is TraceClue's comma-separated package:class extension variable.
	ExtraEnv = "TRACECLUE_EXTRA_INSTRUMENTATIONS"
	// ConfigEnv is TraceClue's JSON instrumentation configuration variable.
	ConfigEnv = "TRACECLUE_INSTRUMENTATION_CONFIGS"
)

// Hook is one initialized instrumentation lifecycle.
type Hook interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}

// HookFuncs adapts functions to Hook.
type HookFuncs struct {
	StartFunc    func(context.Context) error
	ShutdownFunc func(context.Context) error
}

// Start implements Hook.
func (hook HookFuncs) Start(ctx context.Context) error {
	if hook.StartFunc == nil {
		return nil
	}
	return hook.StartFunc(ctx)
}

// Shutdown implements Hook.
func (hook HookFuncs) Shutdown(ctx context.Context) error {
	if hook.ShutdownFunc == nil {
		return nil
	}
	return hook.ShutdownFunc(ctx)
}

// Factory decodes instrumentation-specific config and creates one Hook.
type Factory func(context.Context, json.RawMessage) (Hook, error)

// Registration defines one statically compiled instrumentation.
type Registration struct {
	Name             string
	Aliases          []string
	EnabledByDefault bool
	Factory          Factory
}

type registered struct {
	Registration
	order int
}

// Registry is safe to build concurrently before Start. Registering after a
// manager has started is allowed but does not mutate that manager.
type Registry struct {
	mu      sync.RWMutex
	byName  map[string]*registered
	ordered []*registered
}

// NewRegistry creates an empty typed instrumentation registry.
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]*registered)}
}

// Register adds a compiled instrumentation and all of its legacy aliases.
func (registry *Registry) Register(registration Registration) error {
	if registry == nil {
		return errors.New("instrumentation: nil registry")
	}
	registration.Name = strings.TrimSpace(registration.Name)
	if registration.Name == "" {
		return errors.New("instrumentation: empty registration name")
	}
	if registration.Factory == nil {
		return fmt.Errorf("instrumentation: %s has nil factory", registration.Name)
	}
	names := append([]string{registration.Name}, registration.Aliases...)
	localNames := make(map[string]struct{}, len(names))
	for index := range names {
		names[index] = strings.TrimSpace(names[index])
		if names[index] == "" {
			return fmt.Errorf("instrumentation: %s has empty alias", registration.Name)
		}
		if _, duplicate := localNames[names[index]]; duplicate {
			return fmt.Errorf("instrumentation: %s repeats name or alias %q", registration.Name, names[index])
		}
		localNames[names[index]] = struct{}{}
	}
	registration.Aliases = append([]string(nil), names[1:]...)

	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, name := range names {
		if previous := registry.byName[name]; previous != nil {
			return fmt.Errorf("instrumentation: name or alias %q already belongs to %s", name, previous.Name)
		}
	}
	item := &registered{Registration: registration, order: len(registry.ordered)}
	registry.ordered = append(registry.ordered, item)
	for _, name := range names {
		registry.byName[name] = item
	}
	return nil
}

// Options select instrumentations and provide JSON configuration. Extra names
// are appended after default registrations. Explicit Config overrides values
// loaded from ConfigEnv for the same key.
type Options struct {
	Extra  []string
	Config map[string]json.RawMessage
}

// OptionsFromEnv parses TraceClue's extension and config environment variables.
func OptionsFromEnv() (Options, error) {
	return ParseOptions(os.Getenv(ExtraEnv), os.Getenv(ConfigEnv))
}

// ParseOptions parses TraceClue-compatible values from an arbitrary config
// source, including fit-go JSON/YAML files.
func ParseOptions(extraValue, configValue string) (Options, error) {
	options := Options{Config: make(map[string]json.RawMessage)}
	if raw := strings.TrimSpace(extraValue); raw != "" {
		for _, item := range strings.Split(raw, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				return Options{}, fmt.Errorf("instrumentation: %s contains an empty entry", ExtraEnv)
			}
			options.Extra = append(options.Extra, item)
		}
	}
	if raw := strings.TrimSpace(configValue); raw != "" {
		if err := json.Unmarshal([]byte(raw), &options.Config); err != nil {
			return Options{}, fmt.Errorf("instrumentation: invalid %s: %w", ConfigEnv, err)
		}
		if options.Config == nil {
			options.Config = make(map[string]json.RawMessage)
		}
		for name, config := range options.Config {
			if len(config) == 0 || !json.Valid(config) {
				return Options{}, fmt.Errorf("instrumentation: invalid JSON config for %q", name)
			}
		}
	}
	return options, nil
}

// StartFromEnv starts defaults plus legacy env-selected instrumentations.
func (registry *Registry) StartFromEnv(ctx context.Context, explicit Options) (*Manager, error) {
	environment, err := OptionsFromEnv()
	if err != nil {
		return nil, err
	}
	environment.Extra = append(environment.Extra, explicit.Extra...)
	for key, value := range explicit.Config {
		if !json.Valid(value) {
			return nil, fmt.Errorf("instrumentation: invalid explicit JSON config for %q", key)
		}
		environment.Config[key] = append(json.RawMessage(nil), value...)
	}
	return registry.Start(ctx, environment)
}

// Start initializes selected hooks in deterministic registration/selection
// order. A failure shuts down already-started hooks in reverse order.
func (registry *Registry) Start(ctx context.Context, options Options) (*Manager, error) {
	if registry == nil {
		return nil, errors.New("instrumentation: nil registry")
	}
	registry.mu.RLock()
	byName := make(map[string]*registered, len(registry.byName))
	for name, item := range registry.byName {
		byName[name] = item
	}
	ordered := append([]*registered(nil), registry.ordered...)
	registry.mu.RUnlock()

	selected := make([]*registered, 0, len(ordered)+len(options.Extra))
	selectedNames := make(map[string]struct{})
	for _, item := range ordered {
		if item.EnabledByDefault {
			selected = append(selected, item)
			selectedNames[item.Name] = struct{}{}
		}
	}
	for _, requested := range options.Extra {
		requested = strings.TrimSpace(requested)
		item := byName[requested]
		if item == nil {
			return nil, fmt.Errorf("instrumentation: %q is not statically registered", requested)
		}
		if _, duplicate := selectedNames[item.Name]; duplicate {
			continue
		}
		selected = append(selected, item)
		selectedNames[item.Name] = struct{}{}
	}

	normalizedConfig := make(map[string]json.RawMessage, len(options.Config))
	configuredItems := make(map[*registered]string, len(options.Config))
	for key, value := range options.Config {
		name := strings.TrimSpace(key)
		item := byName[name]
		if item == nil {
			return nil, fmt.Errorf("instrumentation: config key %q is not statically registered", key)
		}
		if len(value) == 0 || !json.Valid(value) {
			return nil, fmt.Errorf("instrumentation: invalid JSON config for %q", key)
		}
		if previous, duplicate := configuredItems[item]; duplicate {
			return nil, fmt.Errorf("instrumentation: config keys %q and %q select the same registration %s", previous, key, item.Name)
		}
		configuredItems[item] = key
		normalizedConfig[name] = append(json.RawMessage(nil), value...)
	}

	manager := &Manager{}
	for _, item := range selected {
		config := configFor(item, normalizedConfig)
		hook, err := item.Factory(ctx, config)
		if err != nil {
			return nil, manager.rollback(ctx, fmt.Errorf("instrumentation: create %s: %w", item.Name, err))
		}
		if hook == nil {
			return nil, manager.rollback(ctx, fmt.Errorf("instrumentation: factory %s returned nil hook", item.Name))
		}
		if err := hook.Start(ctx); err != nil {
			shutdownErr := hook.Shutdown(ctx)
			return nil, manager.rollback(ctx, errors.Join(
				fmt.Errorf("instrumentation: start %s: %w", item.Name, err),
				wrapShutdownError(item.Name, shutdownErr),
			))
		}
		manager.started = append(manager.started, running{name: item.Name, hook: hook})
	}
	return manager, nil
}

func wrapShutdownError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("instrumentation: shutdown %s after failed start: %w", name, err)
}

func configFor(item *registered, configs map[string]json.RawMessage) json.RawMessage {
	if config, exists := configs[item.Name]; exists {
		return append(json.RawMessage(nil), config...)
	}
	aliases := append([]string(nil), item.Aliases...)
	sort.Strings(aliases)
	for _, alias := range aliases {
		if config, exists := configs[alias]; exists {
			return append(json.RawMessage(nil), config...)
		}
	}
	return json.RawMessage(`{}`)
}

type running struct {
	name string
	hook Hook
}

// Manager owns started instrumentation lifecycles.
type Manager struct {
	mu           sync.Mutex
	started      []running
	shutdownOnce sync.Once
	shutdownErr  error
}

// Names returns started canonical names in startup order.
func (manager *Manager) Names() []string {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	result := make([]string, len(manager.started))
	for index, item := range manager.started {
		result[index] = item.name
	}
	return result
}

// Shutdown stops hooks once in reverse startup order.
func (manager *Manager) Shutdown(ctx context.Context) error {
	if manager == nil {
		return nil
	}
	manager.shutdownOnce.Do(func() {
		manager.mu.Lock()
		started := append([]running(nil), manager.started...)
		manager.mu.Unlock()

		var shutdownErrors []error
		for index := len(started) - 1; index >= 0; index-- {
			if err := started[index].hook.Shutdown(ctx); err != nil {
				shutdownErrors = append(shutdownErrors, fmt.Errorf("instrumentation: shutdown %s: %w", started[index].name, err))
			}
		}
		manager.shutdownErr = errors.Join(shutdownErrors...)
	})
	return manager.shutdownErr
}

func (manager *Manager) rollback(ctx context.Context, cause error) error {
	return errors.Join(cause, manager.Shutdown(ctx))
}
