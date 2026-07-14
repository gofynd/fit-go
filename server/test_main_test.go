package server

import (
	"os"
	"testing"
)

// Existing middleware tests assert the explicit platform field layout. Keep
// that fixture baseline isolated from the production TraceClue default; the
// parity tests clear the variable to exercise the global default directly.
func TestMain(m *testing.M) {
	previous, existed := os.LookupEnv("FIT_LOG_SCHEMA")
	_ = os.Setenv("FIT_LOG_SCHEMA", "platform")
	code := m.Run()
	if existed {
		_ = os.Setenv("FIT_LOG_SCHEMA", previous)
	} else {
		_ = os.Unsetenv("FIT_LOG_SCHEMA")
	}
	os.Exit(code)
}
