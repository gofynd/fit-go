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

// Per-level log rate limiting — a Go port of traceclue's TokenBucket +
// RateLimitingFilter (traceclue/logging_rate_limiter/utils.py and its JS twin).
//
// ┌─ IMPORTANT: THIS IS A CAPABILITY, NOT A PARITY BEHAVIOUR ────────────────────┐
// │                                                                              │
// │ It is OFF BY DEFAULT and must stay that way, because the rate limiter is     │
// │ ACTIVE IN NO LEGACY SERVICE:                                                 │
// │                                                                              │
// │   * fit.js NEVER wires it. Its winston logger applies only                   │
// │     opentelemetryLogFormat — rateLimitingFormat is exported by traceclue but │
// │     never referenced. So no Node service rate-limits its logs.               │
// │                                                                              │
// │   * deployed pyfit synchronous logging DEFINES it but does not ATTACH it.     │
// │     Current pyfit 2.x async logging does attach a DEBUG limiter (default      │
// │     1000/sec, burst 1000, cap 6000), but none of the four Node migrations     │
// │     audited for Metroplex uses pyfit.                                         │
// │                                                                              │
// │ Enabling this by default would therefore DROP log lines that every legacy    │
// │ service emits — a silent, hard-to-debug regression, and exactly the kind of  │
// │ "improvement" a drop-in replacement must not make.                           │
// │                                                                              │
// │ Enable it deliberately, per service, when you actually want throttling.      │
// └──────────────────────────────────────────────────────────────────────────────┘
package logging

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"
)

// TokenBucket is a classic token bucket, ported faithfully from traceclue's Python
// implementation (tokens refill continuously at TokensPerSec; an action costs 1 token;
// the balance is capped at MaxTokensBalance).
//
// Semantics match traceclue exactly, including the "< 1 token means deny" rule — a
// fractional balance below 1 does not permit an action.
type TokenBucket struct {
	tokensPerSec     float64
	maxTokensBalance float64

	mu        sync.Mutex
	bucket    float64
	lastCheck time.Time
	now       func() time.Time // injectable for tests
}

// TokenBucketConfig mirrors traceclue's kwargs (tokens_per_sec, starting_tokens,
// max_tokens_balance).
type TokenBucketConfig struct {
	// TokensPerSec is the refill rate. Required; a value <= 0 denies everything.
	TokensPerSec float64
	// StartingTokens is the initial balance (traceclue default: 0).
	StartingTokens float64
	// MaxTokensBalance caps the balance, bounding the burst. Zero means unbounded
	// (traceclue default: math.inf).
	MaxTokensBalance float64
}

// NewTokenBucket builds a bucket from cfg.
func NewTokenBucket(cfg TokenBucketConfig) *TokenBucket {
	maxBalance := cfg.MaxTokensBalance
	if maxBalance <= 0 {
		maxBalance = math.Inf(1) // traceclue default
	}
	return &TokenBucket{
		tokensPerSec:     cfg.TokensPerSec,
		maxTokensBalance: maxBalance,
		bucket:           cfg.StartingTokens,
		lastCheck:        time.Now(),
		now:              time.Now,
	}
}

// Allow reports whether one action may proceed, consuming a token if so. It is the
// Go equivalent of traceclue's is_action_allowed().
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

// RateLimitConfig maps a slog level to its bucket. A record whose level has no entry
// falls back to the Default bucket; when Default is nil too, the record is ALLOWED
// (traceclue: absent level and absent "default" => True).
type RateLimitConfig struct {
	// Levels is the per-level configuration, e.g. {slog.LevelDebug: {...}}.
	Levels map[slog.Level]TokenBucketConfig
	// Default applies to any level not present in Levels. Nil = no default limit.
	Default *TokenBucketConfig
	// OnDecision, when set, is called for every record with the allow/deny outcome —
	// traceclue's result_callback. Useful to count dropped lines.
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
// Passing a zero-value RateLimitConfig (no levels, no default) is a PASSTHROUGH: every
// record is allowed. That is the intended default state — see the file header.
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

// Handle drops the record when its bucket is empty (traceclue's filter() returning
// False), otherwise forwards it.
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

// allow reproduces traceclue's lookup order: the level's own bucket, else the default
// bucket, else allow.
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

// PyfitDebugRateLimit returns current pyfit 2.x async logging defaults
// (DEBUG: 1000 tokens/sec, burst 1000, cap 6000; every other level unlimited).
//
// Provided for services that deliberately want that throttling. It is NOT applied
// anywhere by default: fit.js and deployed synchronous pyfit do not attach it.
func PyfitDebugRateLimit() RateLimitConfig {
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
