package logging

import (
	"os"
	"testing"
)

// Most logger tests assert the explicitly supported platform envelope. Keep
// that fixture baseline isolated from the production default (TraceClue),
// while schema_test.go has dedicated coverage for the empty-env default.
func TestMain(m *testing.M) {
	previous, existed := os.LookupEnv("FIT_LOG_SCHEMA")
	_ = os.Setenv("FIT_LOG_SCHEMA", string(SchemaPlatform))
	code := m.Run()
	if existed {
		_ = os.Setenv("FIT_LOG_SCHEMA", previous)
	} else {
		_ = os.Unsetenv("FIT_LOG_SCHEMA")
	}
	os.Exit(code)
}
