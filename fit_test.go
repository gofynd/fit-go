package fit

import (
	"testing"

	"github.com/gofynd/fit-go/config"
)

func TestTracingOptionsFromConfigKeepsOperationalDefaults(t *testing.T) {
	cfg := config.New()
	opts := tracingOptionsFromConfig(cfg)

	if opts.SampleRate != 1 {
		t.Fatalf("SampleRate = %v, want 1", opts.SampleRate)
	}
	if opts.BatchTimeout <= 0 {
		t.Fatalf("BatchTimeout = %v, want a positive duration", opts.BatchTimeout)
	}
	if opts.MaxExportBatch <= 0 {
		t.Fatalf("MaxExportBatch = %d, want a positive size", opts.MaxExportBatch)
	}
	if opts.MaxQueueSize < opts.MaxExportBatch {
		t.Fatalf("MaxQueueSize = %d, want at least MaxExportBatch %d", opts.MaxQueueSize, opts.MaxExportBatch)
	}
	if opts.ExportTimeout <= 0 {
		t.Fatalf("ExportTimeout = %v, want a positive duration", opts.ExportTimeout)
	}
}
