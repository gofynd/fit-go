// Example 02: Configuration
//
// The config package loads values from (last wins): a .env file, OS
// environment variables, and any JSON/YAML files passed to Load. It then
// exposes type-safe getters and a small validation framework.
//
// Run:
//
//	PORT=9090 DEBUG=true ALLOWED_HOSTS=a.com,b.com go run ./examples/02-config
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gofynd/fit-go/config"
)

func main() {
	// Load reads .env + OS env. You may also pass config file paths:
	//   cfg, err := config.Load("config.yaml", "config.json")
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	// Type-safe getters, each with a default used when the key is absent.
	port := cfg.GetInt("PORT", 8080)
	debug := cfg.GetBool("DEBUG", false)
	name := cfg.GetString("SERVICE_NAME", "my-service")
	hosts := cfg.GetStringSlice("ALLOWED_HOSTS", []string{"localhost"})
	cacheTTL := cfg.GetDuration("CACHE_TTL", 5*time.Minute)

	fmt.Printf("name=%s port=%d debug=%t hosts=%v ttl=%s\n",
		name, port, debug, hosts, cacheTTL)

	// Set values programmatically (useful in tests).
	cfg.Set("REGION", "us-east-1")
	if cfg.Has("REGION") {
		fmt.Println("region:", cfg.GetString("REGION", ""))
	}

	// Validation: declare rules and fail fast at startup if config is invalid.
	rules := []config.ValidationRule{
		{Key: "SERVICE_NAME", Required: true, Description: "logical service name"},
		{Key: "NODE_ENV", AllowedValues: []string{"development", "staging", "production"}},
	}
	if err := cfg.Validate(rules); err != nil {
		// In a real service this would be log.Fatal; here we just report it.
		fmt.Println("config validation error:", err)
	} else {
		fmt.Println("config is valid")
	}
}
