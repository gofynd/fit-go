// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPeriodicStopSignalsEvenWhenWriteHasNotFinished(t *testing.T) {
	checker := NewChecker()
	state := &periodicCheckState{}
	stop, done := make(chan struct{}), make(chan struct{})
	// A writer's unfinished completion channel is also the graceful-stop
	// boundary. No real /tmp/_healthz file or global filesystem is altered.
	checker.periodicStates = []*periodicCheckState{state}
	checker.periodicStops = []chan struct{}{stop}
	checker.periodicDones = []chan struct{}{done}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := checker.StopPeriodicCheckContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished writer stop = %v", err)
	}
	if !state.stopped.Load() {
		t.Fatal("stop did not signal writer")
	}
	select {
	case <-stop:
	default:
		t.Fatal("stop channel was not closed")
	}
	closed := make(chan struct{})
	go func() { checker.StopPeriodicCheck(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("unbounded stop did not wait for admitted write")
	default:
	}
	close(done)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("graceful stop did not finish after writer completion")
	}
}

func TestHealthFileCleanupHonorsDeadlineDuringBlockedIO(t *testing.T) {
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release); <-finished })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- runHealthFileCleanup(ctx, func() {
			close(started)
			<-release
			close(finished)
		})
	}()
	<-started
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup ignored deadline")
	}
}

func TestHealthFileCleanupWithoutDeadlineRemainsSynchronous(t *testing.T) {
	called := false
	if err := runHealthFileCleanup(context.Background(), func() { called = true }); err != nil || !called {
		t.Fatalf("unbounded cleanup = called %v, error %v", called, err)
	}
}
