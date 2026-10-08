// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// SPDX-License-Identifier: Apache-2.0

package goroutinectx

import "testing"

func TestScopeNestedOwnersAndCleanup(t *testing.T) {
	var scope Scope[string]
	if scope.Contains("outer") {
		t.Fatal("an unbound owner was present")
	}
	cleanupOuter := scope.Enter("outer")
	cleanupInner := scope.Enter("inner")
	if !scope.Contains("outer") || !scope.Contains("inner") || scope.Contains("other") {
		t.Fatal("nested owner membership is incorrect")
	}
	cleanupInner()
	if !scope.Contains("outer") || scope.Contains("inner") {
		t.Fatal("inner cleanup did not restore the outer scope")
	}
	cleanupOuter()
	if scope.Contains("outer") {
		t.Fatal("outer cleanup retained its owner")
	}
}

func TestScopeDoesNotCrossGoroutines(t *testing.T) {
	var scope Scope[string]
	cleanup := scope.Enter("handler")
	defer cleanup()
	result := make(chan bool, 1)
	go func() { result <- scope.Contains("handler") }()
	if <-result {
		t.Fatal("an external goroutine inherited callback ownership")
	}
	if !scope.Contains("handler") {
		t.Fatal("the callback goroutine lost its owner")
	}
}
