// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// SPDX-License-Identifier: Apache-2.0

package goroutinectx

import "sync"

// Scope tracks nested, comparable owners on the current goroutine independently
// of its active tracing context. A Scope must not be copied after first use.
type Scope[T comparable] struct {
	bindings sync.Map
}

type scopeBinding[T comparable] struct {
	owner  T
	parent *scopeBinding[T]
}

// Enter binds owner until the returned cleanup runs. Cleanup must be called
// once on the same goroutine, in reverse order of Enter calls.
func (s *Scope[T]) Enter(owner T) func() {
	id := goroutineID()
	var previous *scopeBinding[T]
	if value, ok := s.bindings.Load(id); ok {
		previous = value.(*scopeBinding[T])
	}
	s.bindings.Store(id, &scopeBinding[T]{owner: owner, parent: previous})
	return func() {
		if previous == nil {
			s.bindings.Delete(id)
		} else {
			s.bindings.Store(id, previous)
		}
	}
}

// Contains reports whether owner is bound by any active Enter on this goroutine.
func (s *Scope[T]) Contains(owner T) bool {
	value, ok := s.bindings.Load(goroutineID())
	if !ok {
		return false
	}
	for binding := value.(*scopeBinding[T]); binding != nil; binding = binding.parent {
		if binding.owner == owner {
			return true
		}
	}
	return false
}
