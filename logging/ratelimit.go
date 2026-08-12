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

// This file provides opt-in per-level log rate limiting.
package logging

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"
)

// TokenBucket is a thread-safe token bucket. Tokens refill continuously, each
// action costs one token, and balances below one are denied.
type TokenBucket struct {
	tokensPerSec     float64
	maxTokensBalance float64

	mu        sync.Mutex
	bucket    float64
	lastCheck time.Time
	now       func() time.Time
}

// TokenBucketConfig configures a TokenBucket.
type TokenBucketConfig struct {
	// TokensPerSec is the refill rate. Required; a value <= 0 denies everything.
	TokensPerSec float64
	// StartingTokens is the initial balance.
	StartingTokens float64
	// MaxTokensBalance caps the burst. Zero means unbounded.
	MaxTokensBalance float64
}

// NewTokenBucket builds a bucket from cfg.
func NewTokenBucket(cfg TokenBucketConfig) *TokenBucket {
	maxBalance := cfg.MaxTokensBalance
	if maxBalance <= 0 {
		maxBalance = math.Inf(1)
	}
	return &TokenBucket{
		tokensPerSec:     cfg.TokensPerSec,
		maxTokensBalance: maxBalance,
		bucket:           cfg.StartingTokens,
		lastCheck:        time.Now(),
		now:              time.Now,
	}
}

// Allow consumes one token when capacity is available.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	current := b.now()
	timePassed := current.Sub(b.lastCheck).Seconds()
	b.lastCheck = current

	b.bucket += timePassed * b.tokensPerSec
	if b.bucket > b.maxTokensBalance {
		b.bucket = b.maxTokensBalance
	}
	if b.bucket < 1 {
		return false
	}
	b.bucket--
	return true
}

// RateLimitConfig maps log levels to token buckets.
type RateLimitConfig struct {
	// Levels is the per-level configuration, e.g. {slog.LevelDebug: {...}}.
	Levels map[slog.Level]TokenBucketConfig
	// Default applies to any level not present in Levels. Nil = no default limit.
	Default *TokenBucketConfig
	// OnDecision receives the decision for each record.
	OnDecision func(allowed bool, r slog.Record)
}

// rateLimitHandler wraps a slog.Handler, dropping records that exceed their bucket.
type rateLimitHandler struct {
	next    slog.Handler
	buckets map[slog.Level]*TokenBucket
	def     *TokenBucket
	onDec   func(bool, slog.Record)
}

// NewRateLimitHandler wraps next with per-level token-bucket rate limiting.
//
// It is a plain slog.Handler decorator, so it composes with the fit logger:
//
//	h := logging.NewRateLimitHandler(logging.NewSlogHandler(l), cfg)
//	slog.SetDefault(slog.New(h))
//
// A zero-value configuration allows every record.
func NewRateLimitHandler(next slog.Handler, cfg RateLimitConfig) slog.Handler {
	h := &rateLimitHandler{
		next:    next,
		buckets: make(map[slog.Level]*TokenBucket, len(cfg.Levels)),
		onDec:   cfg.OnDecision,
	}
	for lvl, bc := range cfg.Levels {
		h.buckets[lvl] = NewTokenBucket(bc)
	}
	if cfg.Default != nil {
		h.def = NewTokenBucket(*cfg.Default)
	}
	return h
}

// Enabled defers to the wrapped handler; rate limiting is applied in Handle, because
// the token must only be spent on a record that would actually be emitted.
func (h *rateLimitHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l)
}

// Handle drops the record when its bucket is empty.
func (h *rateLimitHandler) Handle(ctx context.Context, r slog.Record) error {
	allowed := h.allow(r.Level)
	if h.onDec != nil {
		h.onDec(allowed, r)
	}
	if !allowed {
		return nil // dropped
	}
	return h.next.Handle(ctx, r)
}

func (h *rateLimitHandler) allow(l slog.Level) bool {
	if b, ok := h.buckets[l]; ok {
		return b.Allow()
	}
	if h.def != nil {
		return h.def.Allow()
	}
	return true
}

func (h *rateLimitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &rateLimitHandler{next: h.next.WithAttrs(attrs), buckets: h.buckets, def: h.def, onDec: h.onDec}
}

func (h *rateLimitHandler) WithGroup(name string) slog.Handler {
	return &rateLimitHandler{next: h.next.WithGroup(name), buckets: h.buckets, def: h.def, onDec: h.onDec}
}

// DebugRateLimitPreset returns a debug-only preset with 1,000 tokens per
// second, an initial burst of 1,000, and a maximum balance of 6,000.
func DebugRateLimitPreset() RateLimitConfig {
	return RateLimitConfig{
		Levels: map[slog.Level]TokenBucketConfig{
			slog.LevelDebug: {
				TokensPerSec:     1000,
				StartingTokens:   1000,
				MaxTokensBalance: 6000,
			},
		},
	}
}
