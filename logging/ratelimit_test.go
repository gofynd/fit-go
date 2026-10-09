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

package logging

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// countingHandler records how many records reached the wrapped handler.
type countingHandler struct{ n int }

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) Handle(context.Context, slog.Record) error {
	h.n++
	return nil
}
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

func emit(h slog.Handler, lvl slog.Level, n int) {
	for i := 0; i < n; i++ {
		_ = h.Handle(context.Background(), slog.NewRecord(time.Now(), lvl, "msg", 0))
	}
}

// TestRateLimit_ZeroConfigIsPassthrough is the MOST IMPORTANT test here.
//
// The rate limiter is inactive in fit.js and deployed synchronous pyfit services.
// Current pyfit 2.x enables it only in async mode. Enabling it by default here would
// silently drop lines for the four audited Node migrations, so zero config passes all.
func TestRateLimit_ZeroConfigIsPassthrough(t *testing.T) {
	next := &countingHandler{}
	h := NewRateLimitHandler(next, RateLimitConfig{})

	emit(h, slog.LevelError, 1000)
	emit(h, slog.LevelInfo, 1000)

	if next.n != 2000 {
		t.Fatalf("zero-config handler passed %d of 2000 records — it MUST be a passthrough; "+
			"dropping logs by default is a silent regression vs every legacy service", next.n)
	}
}

// TestTokenBucket_BurstThenThrottle: a bucket starting with N tokens allows exactly N
// immediate actions, then denies (no time has passed to refill).
func TestTokenBucket_BurstThenThrottle(t *testing.T) {
	now := time.Now()
	b := NewTokenBucket(TokenBucketConfig{TokensPerSec: 10, StartingTokens: 3})
	b.now = func() time.Time { return now } // freeze time: no refill
	b.lastCheck = now

	allowed := 0
	for i := 0; i < 10; i++ {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d actions from a 3-token bucket with time frozen, want exactly 3", allowed)
	}
}

// TestTokenBucket_RefillsOverTime: tokens accrue at TokensPerSec.
func TestTokenBucket_RefillsOverTime(t *testing.T) {
	now := time.Now()
	b := NewTokenBucket(TokenBucketConfig{TokensPerSec: 100, StartingTokens: 0})
	b.now = func() time.Time { return now }
	b.lastCheck = now

	if b.Allow() {
		t.Fatal("empty bucket allowed an action")
	}
	now = now.Add(50 * time.Millisecond) // 100/sec * 0.05s = 5 tokens
	allowed := 0
	for i := 0; i < 10; i++ {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("after 50ms at 100 tokens/sec: allowed %d, want 5", allowed)
	}
}

// TestTokenBucket_FractionBelowOneDenies pins traceclue's exact rule: a balance below
// 1 denies, even though it is > 0.
func TestTokenBucket_FractionBelowOneDenies(t *testing.T) {
	now := time.Now()
	b := NewTokenBucket(TokenBucketConfig{TokensPerSec: 1, StartingTokens: 0})
	b.now = func() time.Time { return now }
	b.lastCheck = now

	now = now.Add(500 * time.Millisecond) // 0.5 tokens — positive, but < 1
	if b.Allow() {
		t.Fatal("a balance of 0.5 tokens allowed an action; traceclue denies when bucket < 1")
	}
}

// TestTokenBucket_MaxBalanceCapsBurst: the balance never exceeds MaxTokensBalance, so
// a long idle period cannot bank an unbounded burst.
func TestTokenBucket_MaxBalanceCapsBurst(t *testing.T) {
	now := time.Now()
	b := NewTokenBucket(TokenBucketConfig{TokensPerSec: 100, StartingTokens: 0, MaxTokensBalance: 5})
	b.now = func() time.Time { return now }
	b.lastCheck = now

	now = now.Add(1 * time.Hour) // would be 360,000 tokens, but the cap is 5
	allowed := 0
	for i := 0; i < 50; i++ {
		if b.Allow() {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("allowed %d after an idle hour, want the cap of 5", allowed)
	}
}

// TestRateLimit_PerLevelAndDefault reproduces traceclue's lookup order: the level's own
// bucket, else the default bucket, else allow.
func TestRateLimit_PerLevelAndDefault(t *testing.T) {
	next := &countingHandler{}
	h := NewRateLimitHandler(next, RateLimitConfig{
		Levels:  map[slog.Level]TokenBucketConfig{slog.LevelDebug: {TokensPerSec: 0, StartingTokens: 2}},
		Default: &TokenBucketConfig{TokensPerSec: 0, StartingTokens: 4},
	})

	emit(h, slog.LevelDebug, 10) // own bucket: 2 allowed
	if next.n != 2 {
		t.Fatalf("DEBUG: %d records passed, want 2 (its own bucket)", next.n)
	}
	next.n = 0
	emit(h, slog.LevelError, 10) // no ERROR bucket -> default: 4 allowed
	if next.n != 4 {
		t.Fatalf("ERROR: %d records passed, want 4 (the default bucket)", next.n)
	}
}

// TestRateLimit_OnDecisionCallback: traceclue's result_callback fires for every record,
// allowed or dropped — so a service can count what it lost.
func TestRateLimit_OnDecisionCallback(t *testing.T) {
	var allowed, dropped int
	h := NewRateLimitHandler(&countingHandler{}, RateLimitConfig{
		Default: &TokenBucketConfig{TokensPerSec: 0, StartingTokens: 3},
		OnDecision: func(ok bool, _ slog.Record) {
			if ok {
				allowed++
			} else {
				dropped++
			}
		},
	})
	emit(h, slog.LevelInfo, 10)

	if allowed != 3 || dropped != 7 {
		t.Fatalf("callback saw allowed=%d dropped=%d, want 3/7", allowed, dropped)
	}
}

func TestDebugRateLimitPreset(t *testing.T) {
	cfg := DebugRateLimitPreset()

	dbg, ok := cfg.Levels[slog.LevelDebug]
	if !ok {
		t.Fatal("no DEBUG bucket")
	}
	if dbg.TokensPerSec != 1000 || dbg.StartingTokens != 1000 || dbg.MaxTokensBalance != 6000 {
		t.Errorf("DEBUG bucket = %+v, want {1000,1000,6000}", dbg)
	}
	if cfg.Default != nil {
		t.Error("default bucket must be nil so INFO/WARN/ERROR remain unlimited")
	}

	// ERROR must be unlimited under this config.
	next := &countingHandler{}
	h := NewRateLimitHandler(next, cfg)
	emit(h, slog.LevelError, 500)
	if next.n != 500 {
		t.Fatalf("ERROR passed %d of 500 — pyfit's config throttles DEBUG only", next.n)
	}
}

func TestDeprecatedDebugRateLimitAliasMatchesPreset(t *testing.T) {
	current := DebugRateLimitPreset().Levels[slog.LevelDebug]
	deprecated := PyfitDebugRateLimit().Levels[slog.LevelDebug]
	if current != deprecated {
		t.Fatalf("deprecated preset = %+v, want %+v", deprecated, current)
	}
}
