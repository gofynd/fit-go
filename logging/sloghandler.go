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

// sloghandler.go adapts the fit Logger to a standard library slog.Handler, so
// that ALL log/slog output — service code AND third-party libraries that log via
// slog — routes through the single fit logger: same selected JSON schema, same
// sink, and the same implicit trace context (via the active goroutine context).
// This is the Go analogue of patching Winston's default in fit.js.
package logging

import (
	"context"
	"log/slog"
	"strings"
	"sync"
)

// slogHandler implements slog.Handler by forwarding records to a fit Logger.
type slogHandler struct {
	logger *Logger
	attrs  []interface{} // accumulated WithAttrs key-values (alternating k, v)
	groups []string      // open WithGroup names, applied as a dotted key prefix
}

var slogDefaultOwners struct {
	sync.Mutex
	current *slogDefaultOwner
}

type slogDefaultOwner struct {
	installed *slog.Logger
	previous  *slogDefaultOwner
	baseline  *slog.Logger
	active    bool
}

// NewSlogHandler returns a slog.Handler that writes through l using the logger's
// selected schema and trace enrichment.
func NewSlogHandler(l *Logger) slog.Handler {
	return &slogHandler{logger: l}
}

// SetAsDefaultSlog installs l as the process-wide log/slog default, so plain
// slog.Info/Warn/Error calls (including from dependencies) land in the fit log
// stream. Call once at boot; fit.Init does this automatically. The returned
// function restores the previous default if this installation still owns it.
func SetAsDefaultSlog(l *Logger) (restore func()) {
	installed := slog.New(NewSlogHandler(l))
	slogDefaultOwners.Lock()
	previousOwner := slogDefaultOwners.current
	if previousOwner != nil && slog.Default() != previousOwner.installed {
		// An independent component replaced the process logger. Start a new
		// ownership chain from that actual baseline instead of reviving an old
		// fit logger when this owner is later restored.
		previousOwner = nil
	}
	owner := &slogDefaultOwner{
		installed: installed,
		previous:  previousOwner,
		baseline:  slog.Default(),
		active:    true,
	}
	slogDefaultOwners.current = owner
	slog.SetDefault(installed)
	slogDefaultOwners.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			slogDefaultOwners.Lock()
			defer slogDefaultOwners.Unlock()
			owner.active = false
			if slogDefaultOwners.current != owner {
				return
			}
			fallback := owner.baseline
			previous := owner.previous
			for previous != nil && !previous.active {
				fallback = previous.baseline
				previous = previous.previous
			}
			slogDefaultOwners.current = previous
			// Respect an independent component that replaced slog.Default after us.
			if slog.Default() != installed {
				return
			}
			if previous != nil {
				slog.SetDefault(previous.installed)
			} else {
				slog.SetDefault(fallback)
			}
		})
	}
}

func (h *slogHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	return h.logger.levelEnabled(slogToLevel(lvl))
}

func (h *slogHandler) Handle(ctx context.Context, r slog.Record) error {
	// Bind trace from the record's context when present (slog.*Context calls);
	// plain slog.* calls leave it empty and the logger falls back to the
	// goroutine-local active span.
	logger := h.logger
	if ctx != nil {
		logger = logger.WithContext(ctx)
	}

	kvs := make([]interface{}, 0, len(h.attrs)+r.NumAttrs()*2)
	kvs = append(kvs, h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		kvs = append(kvs, h.prefixedKey(a.Key), a.Value.Any())
		return true
	})

	logger.log(slogToLevel(r.Level), r.Message, kvs)
	return nil
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	nh := h.clone()
	for _, a := range attrs {
		nh.attrs = append(nh.attrs, nh.prefixedKey(a.Key), a.Value.Any())
	}
	return nh
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	nh := h.clone()
	nh.groups = append(nh.groups, name)
	return nh
}

func (h *slogHandler) clone() *slogHandler {
	na := make([]interface{}, len(h.attrs))
	copy(na, h.attrs)
	ng := make([]string, len(h.groups))
	copy(ng, h.groups)
	return &slogHandler{logger: h.logger, attrs: na, groups: ng}
}

// prefixedKey qualifies a key with any open WithGroup names (slog group
// semantics) so nested keys stay distinct.
func (h *slogHandler) prefixedKey(key string) string {
	if len(h.groups) == 0 {
		return key
	}
	return strings.Join(h.groups, ".") + "." + key
}

// slogToLevel maps an slog level onto the fit logging level.
func slogToLevel(l slog.Level) Level {
	switch {
	case l >= slog.LevelError:
		return LevelError
	case l >= slog.LevelWarn:
		return LevelWarn
	case l >= slog.LevelInfo:
		return LevelInfo
	default:
		return LevelDebug
	}
}
