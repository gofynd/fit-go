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

// Package errors provides Sentry error reporting integration for the fit.go framework.

package health

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestConcurrentStopHonorsOwnDeadline(t *testing.T) {
	c := NewChecker()
	started, release := make(chan struct{}), make(chan struct{})
	c.AddCheck(func() string { close(started); <-release; return "" })
	c.StartPeriodicCheck(3600)
	<-started
	c.periodicMu.Lock()
	state := c.periodicStates[0]
	c.periodicMu.Unlock()
	first := make(chan struct{})
	go func() { c.StopPeriodicCheck(); close(first) }()
	t.Cleanup(func() { close(release); <-first; _ = os.Remove("/tmp/_healthz") })
	until := time.Now().Add(time.Second)
	for {
		stopped := state.stopped.Load()
		if stopped {
			break
		}
		if time.Now().After(until) {
			t.Fatal("first stop did not signal check")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- c.StopPeriodicCheckContext(ctx) }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline-bound stop blocked behind background stop")
	}
	select {
	case <-first:
		t.Fatal("background stop returned before the check finished")
	default:
	}
}

func TestResetSupersedesPendingManagedReplacement(t *testing.T) {
	c := NewChecker()
	started, release := make(chan struct{}), make(chan struct{})
	c.AddCheck(func() string { close(started); <-release; return "" })
	c.startPeriodicCheck(time.Hour)
	<-started
	c.periodicMu.Lock()
	state := c.periodicStates[0]
	c.periodicMu.Unlock()
	replaced := make(chan struct{})
	go func() { c.startPeriodicCheck(time.Hour); close(replaced) }()
	t.Cleanup(func() { c.StopPeriodicCheck(); _ = os.Remove("/tmp/_healthz") })
	until := time.Now().Add(time.Second)
	for {
		stopped := state.stopped.Load()
		if stopped {
			break
		}
		if time.Now().After(until) {
			close(release)
			t.Fatal("replacement did not signal old loop")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := c.ResetContext(ctx)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reset = %v", err)
	}
	select {
	case <-replaced:
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish")
	}
	c.periodicMu.Lock()
	loops := len(c.periodicStops)
	c.periodicMu.Unlock()
	if loops != 0 {
		t.Fatalf("reset was followed by %d late loops", loops)
	}
	if _, err := os.Stat("/tmp/_healthz"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late liveness file: %v", err)
	}
}

func TestStopSupersedesQueuedManagedReplacement(t *testing.T) {
	c := NewChecker()
	c.periodicStartMu.Lock()
	// Simulate a start that captured its lifecycle generation before queuing.
	c.periodicMu.Lock()
	generation := c.periodicGeneration
	c.periodicMu.Unlock()
	started := make(chan struct{})
	go func() { c.startPeriodicCheckAtGeneration(time.Hour, generation); close(started) }()
	c.StopPeriodicCheck()
	c.periodicStartMu.Unlock()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queued start did not finish")
	}
	t.Cleanup(func() { c.StopPeriodicCheck(); _ = os.Remove("/tmp/_healthz") })
	c.periodicMu.Lock()
	loops := len(c.periodicStops)
	c.periodicMu.Unlock()
	if loops != 0 {
		t.Fatalf("stop was followed by %d queued loops", loops)
	}
	// A genuinely new request after the stop still works.
	c.startPeriodicCheck(time.Hour)
	c.periodicMu.Lock()
	loops = len(c.periodicStops)
	c.periodicMu.Unlock()
	if loops != 1 {
		t.Fatalf("fresh managed start created %d loops", loops)
	}
}
