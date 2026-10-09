// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package fit

import (
	"context"

	"github.com/gofynd/fit-go/postgres"
)

// InitManaged initializes the framework-owned runtime lifecycle. It is the
// descriptive replacement for InitAdvanced; the original Init contract is
// unchanged.
func InitManaged(ctx context.Context, opts ...Option) (*Fit, error) {
	return InitAdvanced(ctx, opts...)
}

// WithPostgresPoolOptions enables PostgreSQL using the complete pool-tuning
// contract during InitManaged.
func WithPostgresPoolOptions(configuration postgres.ConnectionPoolOptions) Option {
	return WithPostgresAdvanced(configuration)
}
