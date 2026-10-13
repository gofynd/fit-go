// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package instrumentation

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestRegistryStartsDefaultsAndLegacyAliasesWithTypedConfig(t *testing.T) {
	registry := NewRegistry()
	var events []string
	var configs = make(map[string]string)
	register := func(name string, aliases []string, defaultEnabled bool) {
		t.Helper()
		if err := registry.Register(Registration{
			Name: name, Aliases: aliases, EnabledByDefault: defaultEnabled,
			Factory: func(_ context.Context, raw json.RawMessage) (Hook, error) {
				configs[name] = string(raw)
				return HookFuncs{
					StartFunc:    func(context.Context) error { events = append(events, "start:"+name); return nil },
					ShutdownFunc: func(context.Context) error { events = append(events, "stop:"+name); return nil },
				}, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	register("http", []string{"@opentelemetry/instrumentation-http:HttpInstrumentation"}, true)
	register("graphql", []string{"@opentelemetry/instrumentation-graphql:GraphQLInstrumentation"}, false)

	manager, err := registry.Start(context.Background(), Options{
		Extra: []string{"@opentelemetry/instrumentation-graphql:GraphQLInstrumentation", "http"},
		Config: map[string]json.RawMessage{
			"http": json.RawMessage(`{"safe":true}`),
			"@opentelemetry/instrumentation-graphql:GraphQLInstrumentation": json.RawMessage(`{"resolvers":false}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"http", "graphql"}; !reflect.DeepEqual(manager.Names(), want) {
		t.Fatalf("manager names = %v, want %v", manager.Names(), want)
	}
	if configs["http"] != `{"safe":true}` || configs["graphql"] != `{"resolvers":false}` {
		t.Fatalf("factory configs = %v", configs)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantEvents := []string{"start:http", "start:graphql", "stop:graphql", "stop:http"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %v, want %v", events, wantEvents)
	}
}

func TestRegistryRollsBackStartedAndPartiallyStartedHooks(t *testing.T) {
	registry := NewRegistry()
	var events []string
	boom := errors.New("boom")
	for _, registration := range []Registration{
		{
			Name: "first", EnabledByDefault: true,
			Factory: func(context.Context, json.RawMessage) (Hook, error) {
				return HookFuncs{
					StartFunc:    func(context.Context) error { events = append(events, "start:first"); return nil },
					ShutdownFunc: func(context.Context) error { events = append(events, "stop:first"); return nil },
				}, nil
			},
		},
		{
			Name: "second", EnabledByDefault: true,
			Factory: func(context.Context, json.RawMessage) (Hook, error) {
				return HookFuncs{
					StartFunc:    func(context.Context) error { events = append(events, "start:second"); return boom },
					ShutdownFunc: func(context.Context) error { events = append(events, "stop:second"); return nil },
				}, nil
			},
		},
	} {
		if err := registry.Register(registration); err != nil {
			t.Fatal(err)
		}
	}
	manager, err := registry.Start(context.Background(), Options{})
	if manager != nil || !errors.Is(err, boom) {
		t.Fatalf("Start() = %v, %v", manager, err)
	}
	want := []string{"start:first", "start:second", "stop:second", "stop:first"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestRegistryFailsClosedForUnknownNamesAndConfig(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(Registration{
		Name: "known", Factory: func(context.Context, json.RawMessage) (Hook, error) { return HookFuncs{}, nil },
	}); err != nil {
		t.Fatal(err)
	}
	for name, options := range map[string]Options{
		"extra":          {Extra: []string{"unknown"}},
		"config":         {Config: map[string]json.RawMessage{"unknown": json.RawMessage(`{}`)}},
		"invalid config": {Config: map[string]json.RawMessage{"known": json.RawMessage(`{bad`)}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := registry.Start(context.Background(), options); err == nil {
				t.Fatalf("Start() error = %v", err)
			}
		})
	}
}

func TestRegistryRejectsCanonicalAndAliasConfigForSameHook(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(Registration{
		Name: "known", Aliases: []string{"legacy"},
		Factory: func(context.Context, json.RawMessage) (Hook, error) { return HookFuncs{}, nil },
	}); err != nil {
		t.Fatal(err)
	}
	_, err := registry.Start(context.Background(), Options{
		Config: map[string]json.RawMessage{
			"known":  json.RawMessage(`{"one":true}`),
			"legacy": json.RawMessage(`{"two":true}`),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "same registration") {
		t.Fatalf("Start() error = %v", err)
	}
}

func TestConcurrentShutdownWaitsForSingleLifecycle(t *testing.T) {
	registry := NewRegistry()
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls int
	var callsMu sync.Mutex
	if err := registry.Register(Registration{
		Name: "known", EnabledByDefault: true,
		Factory: func(context.Context, json.RawMessage) (Hook, error) {
			return HookFuncs{ShutdownFunc: func(context.Context) error {
				callsMu.Lock()
				calls++
				callsMu.Unlock()
				close(entered)
				<-release
				return nil
			}}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	manager, err := registry.Start(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- manager.Shutdown(context.Background()) }()
	<-entered
	go func() { results <- manager.Shutdown(context.Background()) }()
	select {
	case err := <-results:
		t.Fatalf("concurrent Shutdown returned before lifecycle completed: %v", err)
	default:
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls != 1 {
		t.Fatalf("shutdown calls = %d, want 1", calls)
	}
}

func TestParseOptionsAndExplicitOverride(t *testing.T) {
	options, err := ParseOptions("one, package:Class", `{"one":{"value":1}}`)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"one", "package:Class"}; !reflect.DeepEqual(options.Extra, want) {
		t.Fatalf("extra = %v", options.Extra)
	}
	if string(options.Config["one"]) != `{"value":1}` {
		t.Fatalf("config = %s", options.Config["one"])
	}
	if _, err := ParseOptions("one,,two", `{}`); err == nil {
		t.Fatal("ParseOptions accepted an empty extension")
	}
	if _, err := ParseOptions("", `{bad`); err == nil {
		t.Fatal("ParseOptions accepted invalid JSON")
	}
}

func TestRegisterRejectsDuplicateAliases(t *testing.T) {
	registry := NewRegistry()
	factory := func(context.Context, json.RawMessage) (Hook, error) { return HookFuncs{}, nil }
	if err := registry.Register(Registration{Name: "one", Aliases: []string{"legacy"}, Factory: factory}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(Registration{Name: "two", Aliases: []string{"legacy"}, Factory: factory}); err == nil {
		t.Fatal("Register accepted duplicate alias")
	}
}

func TestRegisterRejectsDuplicateAliasWithinRegistration(t *testing.T) {
	registry := NewRegistry()
	factory := func(context.Context, json.RawMessage) (Hook, error) { return HookFuncs{}, nil }
	if err := registry.Register(Registration{
		Name:    "mongo",
		Aliases: []string{"mongodb", "mongodb"},
		Factory: factory,
	}); err == nil || !strings.Contains(err.Error(), "repeats name or alias") {
		t.Fatalf("Register() error = %v", err)
	}
}

func TestRegisterOwnsNormalizedAliasesForConfiguration(t *testing.T) {
	registry := NewRegistry()
	aliases := []string{" legacy "}
	var received string
	if err := registry.Register(Registration{
		Name: "known", Aliases: aliases,
		Factory: func(_ context.Context, raw json.RawMessage) (Hook, error) {
			received = string(raw)
			return HookFuncs{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	aliases[0] = "mutated"
	manager, err := registry.Start(context.Background(), Options{
		Extra:  []string{"legacy"},
		Config: map[string]json.RawMessage{"legacy": json.RawMessage(`{"enabled":true}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if received != `{"enabled":true}` {
		t.Fatalf("factory config = %s", received)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
