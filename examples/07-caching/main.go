// Example 07: Distributed caching with groupcache
//
// groupcache is a read-through, peer-to-peer cache. On a miss, exactly one
// peer runs the Getter (single-flight) and the result is shared. In
// single-node mode (no peers configured) it works as a local cache.
//
// Run:
//
//	go run ./examples/07-caching
//
// Multi-node peer discovery is configured via GROUPCACHE_PEERS or the
// Kubernetes discovery options.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gofynd/fit-go/groupcache"
	gc "github.com/mailgun/groupcache/v2"
)

func main() {
	ctx := context.Background()

	client, err := groupcache.Init(groupcache.Config{
		Self: "http://127.0.0.1:8081",
		Port: "8081",
	})
	if err != nil {
		log.Fatalf("groupcache init: %v", err)
	}
	defer client.Close()

	// Create a cache group. The Getter is the read-through loader invoked on
	// a miss; it must populate the Sink with the value for the key.
	client.CreateGroup(groupcache.GroupConfig{
		Name:     "users",
		MaxBytes: 64 << 20, // 64 MiB
		Getter: gc.GetterFunc(func(ctx context.Context, key string, dest gc.Sink) error {
			// Simulate loading from a database/API.
			value := []byte(fmt.Sprintf(`{"id":%q,"name":"User %s"}`, key, key))
			return dest.SetBytes(value, time.Now().Add(5*time.Minute))
		}),
	})

	// Start the peer HTTP server (no-op for a single node, needed for peers).
	if err := client.StartServer(); err != nil {
		log.Printf("start server: %v", err)
	}

	// First Get is a miss -> Getter runs. Subsequent Gets are served warm.
	for i := 0; i < 3; i++ {
		val, err := client.Get(ctx, "users", "u-123")
		if err != nil {
			log.Fatalf("get: %v", err)
		}
		fmt.Printf("get #%d -> %s\n", i+1, string(val))
	}

	// Inspect runtime stats and metrics.
	if stats := client.GroupStats("users"); stats != nil {
		fmt.Printf("gets=%d hits=%d loads=%d\n", stats.Gets, stats.CacheHits, stats.Loads)
	}
}
