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

package redis

import (
	"context"
	"time"
)

const ioredisCompatibilityFIT401IORedis582Value IORedisCompatibilityProfile = "fit-4.0.1-ioredis-5.8.2"

// Deprecated: use IORedisCompatibilityV582. The old profile value remains
// accepted so existing configuration does not break.
const IORedisCompatibilityFIT401IORedis582 = ioredisCompatibilityFIT401IORedis582Value

// Deprecated: use IORedisCompatibilityV582.
const IORedisCompatibilityV5_8_2 = IORedisCompatibilityV582

// Deprecated: use IORedisMaxRetriesPerRequest.
const SlingshotIORedisMaxRetriesPerRequest = IORedisMaxRetriesPerRequest

// Deprecated: use IORedisRetryDelay.
func SlingshotIORedisRetryDelay(times int) time.Duration { return IORedisRetryDelay(times) }

// Deprecated: use IORedisMaxRetriesError.
type SlingshotIORedisMaxRetriesError = IORedisMaxRetriesError

// Deprecated: use IORedisConnectionClosedError.
type SlingshotIORedisConnectionClosedError = IORedisConnectionClosedError

// Deprecated: use IORedisAbortError.
type SlingshotIORedisAbortError = IORedisAbortError

// Deprecated: use IORedisReply.
type SlingshotIORedisReply = IORedisReply

// Deprecated: use IORedisExchange.
type SlingshotIORedisExchange = IORedisExchange

// Deprecated: use IORedisWriteDisposition.
type SlingshotIORedisWriteDisposition = IORedisWriteDisposition

// Deprecated: use IORedisTransport.
type SlingshotIORedisTransport = IORedisTransport

// Deprecated: use IORedisTransportFactory.
type SlingshotIORedisTransportFactory = IORedisTransportFactory

// Deprecated: use IORedisTransportFactoryFunc.
type SlingshotIORedisTransportFactoryFunc = IORedisTransportFactoryFunc

// Deprecated: use IORedisResult.
type SlingshotIORedisResult = IORedisResult

// Deprecated: use IORedisFuture.
type SlingshotIORedisFuture = IORedisFuture

// Deprecated: use IORedisCompatClient.
type SlingshotIORedisCompatClient = IORedisCompatClient

// Deprecated: use IORedisRESPOptions.
type SlingshotIORedisRESPOptions = IORedisRESPOptions

// Deprecated: use IORedisRESPTransportFactory.
type SlingshotIORedisRESPTransportFactory = IORedisRESPTransportFactory

const (
	// Deprecated: use IORedisWriteUnknown.
	SlingshotIORedisWriteUnknown = IORedisWriteUnknown
	// Deprecated: use IORedisNotWritten.
	SlingshotIORedisNotWritten = IORedisNotWritten
	// Deprecated: use IORedisPartiallyWritten.
	SlingshotIORedisPartiallyWritten = IORedisPartiallyWritten
	// Deprecated: use IORedisFullyWritten.
	SlingshotIORedisFullyWritten = IORedisFullyWritten
)

// Deprecated: use NewIORedisCompatClient.
func NewSlingshotIORedisCompatClient(factory IORedisTransportFactory) (*IORedisCompatClient, error) {
	return NewIORedisCompatClient(factory)
}

// Deprecated: use NewIORedisCompatClientReady.
func NewSlingshotIORedisCompatClientReady(ctx context.Context, factory IORedisTransportFactory) (*IORedisCompatClient, error) {
	return NewIORedisCompatClientReady(ctx, factory)
}

// Deprecated: use NewIORedisRESPTransportFactory.
func NewSlingshotIORedisRESPTransportFactory(options IORedisRESPOptions) (*IORedisRESPTransportFactory, error) {
	return NewIORedisRESPTransportFactory(options)
}

// Deprecated: use NewIORedisRESPCompatClient.
func NewSlingshotIORedisRESPCompatClient(options IORedisRESPOptions) (*IORedisCompatClient, error) {
	return NewIORedisRESPCompatClient(options)
}

// Deprecated: use NewIORedisRESPCompatClientReady.
func NewSlingshotIORedisRESPCompatClientReady(ctx context.Context, options IORedisRESPOptions) (*IORedisCompatClient, error) {
	return NewIORedisRESPCompatClientReady(ctx, options)
}
