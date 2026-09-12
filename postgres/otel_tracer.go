package postgres

import (
	"strings"
	"unicode"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
)

// OTelQueryTracerOptions controls the potentially sensitive details attached
// to PostgreSQL spans. Bind parameters are never recorded by this adapter.
type OTelQueryTracerOptions struct {
	IncludeSQLStatement      bool
	IncludeConnectionDetails bool
	DisablePoolAcquire       bool
}

// NewOTelQueryTracer creates a pgx tracer backed by the process-wide
// OpenTelemetry provider. The concrete tracer also implements pgx batch,
// prepare, copy, connect, and pgxpool acquire tracing interfaces.
func NewOTelQueryTracer(config OTelQueryTracerOptions) pgx.QueryTracer {
	options := make([]otelpgx.Option, 0, 4)
	options = append(options, otelpgx.WithSpanNameFunc(sqlOperationName))
	if !config.IncludeSQLStatement {
		options = append(options, otelpgx.WithDisableSQLStatementInAttributes())
	}
	if !config.IncludeConnectionDetails {
		options = append(options, otelpgx.WithDisableConnectionDetailsInAttributes())
	}
	if config.DisablePoolAcquire {
		options = append(options, otelpgx.WithDisableAcquireTracer())
	}
	return otelpgx.NewTracer(options...)
}

func sqlOperationName(statement string) string {
	remaining := strings.TrimSpace(statement)
	for remaining != "" {
		switch {
		case strings.HasPrefix(remaining, "--"), strings.HasPrefix(remaining, "#"):
			newline := strings.IndexByte(remaining, '\n')
			if newline < 0 {
				return "SQL"
			}
			remaining = strings.TrimSpace(remaining[newline+1:])
		case strings.HasPrefix(remaining, "/*"):
			end := strings.Index(remaining[2:], "*/")
			if end < 0 {
				return "SQL"
			}
			remaining = strings.TrimSpace(remaining[end+4:])
		default:
			fields := strings.Fields(remaining)
			if len(fields) == 0 {
				return "SQL"
			}
			operation := strings.TrimRightFunc(fields[0], func(r rune) bool {
				return !unicode.IsLetter(r)
			})
			if operation == "" {
				return "SQL"
			}
			return strings.ToUpper(operation)
		}
	}
	return "SQL"
}
