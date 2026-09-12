package postgres

import "testing"

func TestSQLOperationNameSkipsLeadingComments(t *testing.T) {
	tests := map[string]string{
		"sqlc line comment": "-- name: FindUsers :many\n\nSELECT * FROM users",
		"block comment":     "/* trace annotation */\nINSERT INTO users DEFAULT VALUES",
		"plain statement":   "  update users set active = true",
		"comment only":      "-- no statement",
	}
	for name, statement := range tests {
		t.Run(name, func(t *testing.T) {
			want := map[string]string{
				"sqlc line comment": "SELECT",
				"block comment":     "INSERT",
				"plain statement":   "UPDATE",
				"comment only":      "SQL",
			}[name]
			if got := sqlOperationName(statement); got != want {
				t.Fatalf("sqlOperationName() = %q, want %q", got, want)
			}
		})
	}
}
