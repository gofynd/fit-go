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
	"errors"
	"fmt"
)

type ioredisConnection struct {
	client  *IORedisCompatClient
	cluster bool
}

type ioredisRESPCompatibilityProfile struct {
	disableClientInfo        bool
	clientInfoLibraryVersion string
}

func resolveIORedisRESPCompatibilityProfile(profile IORedisCompatibilityProfile) (ioredisRESPCompatibilityProfile, error) {
	switch profile {
	case IORedisCompatibilityV4:
		return ioredisRESPCompatibilityProfile{disableClientInfo: true}, nil
	case IORedisCompatibilityV5:
		return ioredisRESPCompatibilityProfile{clientInfoLibraryVersion: ioredisLibraryVersion}, nil
	case IORedisCompatibilityV582, ioredisCompatibilityFIT401IORedis582Value:
		return ioredisRESPCompatibilityProfile{clientInfoLibraryVersion: "5.8.2"}, nil
	default:
		return ioredisRESPCompatibilityProfile{}, fmt.Errorf("redis: unsupported ioredis compatibility profile %q", profile)
	}
}

func applyIORedisRESPCompatibilityProfile(profile IORedisCompatibilityProfile, options *IORedisRESPOptions) error {
	resolved, err := resolveIORedisRESPCompatibilityProfile(profile)
	if err != nil {
		return err
	}
	options.DisableClientInfo = resolved.disableClientInfo
	options.clientInfoLibraryVersion = resolved.clientInfoLibraryVersion
	return nil
}

func dialIORedisCompatibleStandalone(
	ctx context.Context,
	profile IORedisCompatibilityProfile,
	options IORedisRESPOptions,
) (Connection, error) {
	if err := applyIORedisRESPCompatibilityProfile(profile, &options); err != nil {
		return nil, err
	}
	client, err := NewIORedisRESPCompatClientReady(ctx, options)
	if err != nil {
		return nil, err
	}
	return &ioredisConnection{client: client}, nil
}

func (c *ioredisConnection) Ping(ctx context.Context) error {
	if c == nil || c.client == nil {
		return errors.New("redis: ioredis compatibility client is not configured")
	}
	result, err := c.client.SubmitContext(ctx, "PING").Wait(ctx)
	if err != nil {
		return err
	}
	if len(result.Replies) != 1 {
		return fmt.Errorf("redis: ioredis PING returned %d replies", len(result.Replies))
	}
	if result.Replies[0].Error != nil {
		return result.Replies[0].Error
	}
	if pong, ok := result.Replies[0].Value.(string); !ok || pong != "PONG" {
		return fmt.Errorf("redis: ioredis PING returned %v", result.Replies[0].Value)
	}
	return nil
}

func (c *ioredisConnection) Close() error {
	if c != nil && c.client != nil {
		c.client.Disconnect()
	}
	return nil
}

func (c *ioredisConnection) Raw() interface{} {
	if c == nil {
		return (*IORedisCompatClient)(nil)
	}
	return c.client
}

func (c *ioredisConnection) IsCluster() bool { return c != nil && c.cluster }
