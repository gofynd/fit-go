// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

//go:build !race

package redact

import (
	"strings"
	"testing"
	"time"
)

// TestTextGrowthIsLinearForAdversarialShapes times Text on 8 KiB and 32 KiB
// repetitions of each shape. A linear scanner grows by ~4x; a per-start O(n)
// re-scan grows by ~16x. The 8x ceiling leaves room for timer/GC noise.
// Excluded under the race detector, whose instrumentation distorts timing.
func TestTextGrowthIsLinearForAdversarialShapes(t *testing.T) {
	shapes := []string{
		"1(", "1]", "a1", "A1", "123456789-x", "1791364800000,", "(9876543210)",
		"my_card4111111111111111 ", "card=1.1.1.1 ", "phone 1",
		"1 ", "1234 ", "1234567890 415-555-1234 ",
	}
	measure := func(input string) time.Duration {
		best := time.Duration(1<<63 - 1)
		for run := 0; run < 3; run++ {
			started := time.Now()
			_ = Text(input)
			if elapsed := time.Since(started); elapsed < best {
				best = elapsed
			}
		}
		return best
	}
	for _, shape := range shapes {
		small := strings.Repeat(shape, (8<<10)/len(shape))
		large := strings.Repeat(shape, (32<<10)/len(shape))
		smallTime := measure(small)
		largeTime := measure(large)
		// Guard against sub-resolution timings on very fast shapes.
		floor := 100 * time.Microsecond
		if smallTime < floor {
			smallTime = floor
		}
		ratio := float64(largeTime) / float64(smallTime)
		t.Logf("shape %-28q 8KiB=%-12v 32KiB=%-12v ratio=%.2f", shape, smallTime, largeTime, ratio)
		if ratio > 8 {
			t.Errorf("Text(%q x 32KiB) grew %.1fx over 8KiB; want near-linear (<=8x)", shape, ratio)
		}
	}
}
