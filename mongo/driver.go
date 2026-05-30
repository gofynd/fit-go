// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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

// driver.go provides the default MongoDB driver integration using the official
// go.mongodb.org/mongo-driver/v2 package. It implements the Connection interface
// defined in client.go and provides convenience functions for initializing
// MongoDB connections with sensible defaults.
//
// This is the Go equivalent of the mongoose.createConnection() calls in
///src/mongo/index.ts.
package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ---------------------------------------------------------------------------
// mongoConnection wraps *mongo.Client to implement Connection
// ---------------------------------------------------------------------------

// mongoConnection implements the Connection interface using the official
// MongoDB Go driver v2.
type mongoConnection struct {
	client *mongo.Client
}

// Ping verifies the connection is alive by issuing a ping command to the server.
func (c *mongoConnection) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, nil)
}

// Close terminates the connection and releases all resources held by the client.
func (c *mongoConnection) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

// Raw returns the underlying *mongo.Client. Callers must type-assert:
//
//	client := conn.Raw().(*mongo.Client)
func (c *mongoConnection) Raw() interface{} {
	return c.client
}

// ---------------------------------------------------------------------------
// DefaultDialFunc
// ---------------------------------------------------------------------------

// DefaultDialFunc returns a DialFunc that creates MongoDB connections using
// the official MongoDB Go driver v2. This is the Go equivalent of
// mongoose.createConnection().
//
// The returned DialFunc applies all DialOptions (pool settings, TLS, app name,
// timeouts) to the mongo-driver client options before connecting.
func DefaultDialFunc() DialFunc {
	return func(ctx context.Context, uri string, opts *DialOptions) (Connection, error) {
		clientOpts := options.Client().ApplyURI(uri)

		if opts != nil {
			applyDriverOptions(clientOpts, opts)
		}

		client, err := mongo.Connect(clientOpts)
		if err != nil {
			return nil, fmt.Errorf("mongo driver: connect failed: %w", err)
		}

		return &mongoConnection{client: client}, nil
	}
}

// applyDriverOptions maps DialOptions to mongo-driver client options.
func applyDriverOptions(clientOpts *options.ClientOptions, opts *DialOptions) {
	if opts.AppName != "" {
		clientOpts.SetAppName(opts.AppName)
	}

	if opts.TLSConfig != nil {
		clientOpts.SetTLSConfig(opts.TLSConfig)
	}

	if opts.MaxPoolSize > 0 {
		clientOpts.SetMaxPoolSize(uint64(opts.MaxPoolSize))
	}

	if opts.MinPoolSize > 0 {
		clientOpts.SetMinPoolSize(uint64(opts.MinPoolSize))
	}

	if opts.MaxIdleTimeMS > 0 {
		clientOpts.SetMaxConnIdleTime(time.Duration(opts.MaxIdleTimeMS) * time.Millisecond)
	}

	if opts.ConnectTimeoutMS > 0 {
		clientOpts.SetConnectTimeout(time.Duration(opts.ConnectTimeoutMS) * time.Millisecond)
	}

	if opts.SocketTimeoutMS > 0 {
		// mongo-driver v2 removed SetSocketTimeout; use SetTimeout which
		// applies a single timeout for the entire operation lifecycle,
		// providing equivalent behaviour for socket-level deadlines.
		clientOpts.SetTimeout(time.Duration(opts.SocketTimeoutMS) * time.Millisecond)
	}

	if opts.MaxConnecting > 0 {
		clientOpts.SetMaxConnecting(uint64(opts.MaxConnecting))
	}

	// Note: WaitQueueTimeout was removed in mongo-driver v2; the driver uses
	// context deadlines instead. We apply it as a server selection timeout
	// which provides equivalent backpressure behaviour.
	if opts.WaitQueueTimeout > 0 {
		clientOpts.SetServerSelectionTimeout(time.Duration(opts.WaitQueueTimeout) * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// InitDefault - convenience function
// ---------------------------------------------------------------------------

// InitDefault discovers MongoDB connections from environment variables and
// connects using the official mongo-driver v2. This is the Go equivalent of
// initMongo().
//
// It calls Init with DefaultDialFunc() and sensible defaults. For more control
// over connection options, use Init directly with a custom DialFunc.
//
// Example:
//
//	client, err := mongo.InitDefault(ctx)
//	if err != nil {
//	 log.Fatal(err)
//	}
//	defer client.Close()
//
//	users := client.Service("users")
//	db := users.Write.Raw().(*mongo.Client).Database("users")
func InitDefault(ctx context.Context) (*Client, error) {
	return Init(ConnectionOptions{
		Dial: DefaultDialFunc(),
		Context: ctx,
	})
}
