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

package migration

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReviewLeaseExecutionContextCancelledWhenRunReturns(t *testing.T) {
	store := newFencedMemoryStore()
	leaseCtx, cancelLease := context.WithCancel(context.Background())
	defer cancelLease()
	released := false
	locker := FuncLeaseLocker(func(context.Context) (Lease, error) {
		return Lease{Context: leaseCtx, FenceToken: "review", Unlock: func() error { released = true; return nil }}, nil
	})
	var execution context.Context
	runner := mustRunner(t, []Migration{{ID: "v1.0.0-1-review", Up: func(ctx context.Context) error { execution = ctx; return nil }}}, store, locker)
	if _, err := runner.Run(context.Background(), RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("unlock not called")
	}
	if execution.Err() == nil {
		t.Fatal("run execution context is still active after lease release; deferred cancelRun called its initial no-op")
	}
}

func TestLeaseCleanupOnOperationErrorUnlockErrorAndPanic(t *testing.T) {
	for _, failure := range []string{"operation", "unlock", "panic"} {
		t.Run(failure, func(t *testing.T) {
			base, cancelParent := context.WithTimeout(context.Background(), time.Hour)
			defer cancelParent()
			parent := &reviewTrackedParent{Context: base}
			leaseCtx, cancelLease := context.WithCancel(context.Background())
			defer cancelLease()
			released := false
			operationErr, unlockErr := errors.New("operation"), errors.New("unlock")
			locker := FuncLeaseLocker(func(context.Context) (Lease, error) {
				return Lease{Context: leaseCtx, FenceToken: "review", Unlock: func() error {
					released = true
					if failure == "unlock" {
						return unlockErr
					}
					return nil
				}}, nil
			})
			runner := mustRunner(t, []Migration{{ID: "v1.0.0-1-review", Up: func(context.Context) error { return nil }}}, newFencedMemoryStore(), locker)
			var execution context.Context
			func() {
				defer func() {
					if recovered := recover(); recovered != nil && (failure != "panic" || recovered != "operation panic") {
						t.Fatalf("unexpected panic: %v", recovered)
					}
				}()
				_, err := runner.withLock(parent, func(ctx context.Context) ([]Status, error) {
					execution = ctx
					if failure == "panic" {
						panic("operation panic")
					}
					if failure == "operation" {
						return nil, operationErr
					}
					return nil, nil
				})
				if failure == "operation" && !errors.Is(err, operationErr) {
					t.Fatalf("operation error = %v", err)
				}
				if failure == "unlock" && !errors.Is(err, unlockErr) {
					t.Fatalf("unlock error = %v", err)
				}
				if failure == "panic" {
					t.Fatal("panic swallowed")
				}
			}()
			if !released || !parent.stopped || execution == nil || execution.Err() == nil {
				t.Fatal("lease release did not clean up execution and parent registration")
			}
		})
	}
}

type reviewTrackedParent struct {
	context.Context
	installed bool
	stopped   bool
}

func (p *reviewTrackedParent) Value(any) any { return nil }
func (p *reviewTrackedParent) AfterFunc(func()) func() bool {
	p.installed = true
	return func() bool { p.stopped = true; return true }
}

func TestReviewLeaseRunReleasesParentCancellationRegistration(t *testing.T) {
	base, cancelParent := context.WithTimeout(context.Background(), time.Hour)
	defer cancelParent()
	parent := &reviewTrackedParent{Context: base}
	leaseCtx, cancelLease := context.WithCancel(context.Background())
	defer cancelLease()
	locker := FuncLeaseLocker(func(context.Context) (Lease, error) {
		return Lease{Context: leaseCtx, FenceToken: "review", Unlock: func() error { cancelLease(); return nil }}, nil
	})
	runner := mustRunner(t, []Migration{{ID: "v1.0.0-1-review", Up: func(context.Context) error { return nil }}}, newFencedMemoryStore(), locker)
	if _, err := runner.Run(parent, RunOptions{To: "latest"}); err != nil {
		t.Fatal(err)
	}
	if !parent.installed {
		t.Fatal("parent AfterFunc was not registered")
	}
	if !parent.stopped {
		t.Fatal("parent cancellation registration not stopped after a correctly cancelled/released lease")
	}
}
