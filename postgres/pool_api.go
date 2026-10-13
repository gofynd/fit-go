// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package postgres

import "context"

// ConnectionPoolOptions configures the complete PostgreSQL connection-pool contract.
type ConnectionPoolOptions = ConnectionAdvancedOptions

// ServicePoolOptions configures read and write pools for one service.
type ServicePoolOptions = ServiceAdvancedPoolOverrides

// PoolOptions configures one PostgreSQL pool.
type PoolOptions = PoolAdvancedOverrides

// InitWithPoolOptions initializes PostgreSQL using the complete pool contract.
func InitWithPoolOptions(ctx context.Context, opts ConnectionPoolOptions) (*Client, error) {
	return InitWithAdvancedContext(ctx, opts)
}
