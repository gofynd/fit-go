// Example 05: Database clients (MongoDB, PostgreSQL, Redis)
//
// Each client discovers connections from env vars and exposes them by service
// name with a read/write split. Connection strings follow the pattern:
//
//	MONGO_<SERVICE>_READ_WRITE / MONGO_<SERVICE>_READ_ONLY
//	POSTGRES_<SERVICE>_READ_WRITE / POSTGRES_<SERVICE>_READ_ONLY
//	REDIS_<SERVICE>_READ_WRITE  / REDIS_<SERVICE>_READ_ONLY
//
// Example env:
//
//	MONGO_CATALOG_READ_WRITE=mongodb://localhost:27017/catalog
//	POSTGRES_ORDERS_READ_WRITE=postgres://user:pass@localhost:5432/orders?sslmode=disable
//	REDIS_CACHE_READ_WRITE=redis://localhost:6379/0
//
// Run (requires the services to be reachable to actually connect):
//
//	go run ./examples/05-databases
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/gofynd/fit-go/health"
	"github.com/gofynd/fit-go/mongo"
	"github.com/gofynd/fit-go/postgres"
	"github.com/gofynd/fit-go/redis"
)

func main() {
	ctx := context.Background()
	checker := health.NewChecker()

	// --- MongoDB ---------------------------------------------------------
	mongoClient, err := mongo.InitDefault(ctx)
	if err != nil {
		log.Printf("mongo init: %v", err)
	} else {
		defer mongoClient.Close()
		if conn := mongoClient.Service("catalog"); conn != nil {
			// conn.Read / conn.Write are mongo.Connection; Raw() yields the
			// underlying driver handle to type-assert as needed.
			_ = conn.Write.Raw()
			fmt.Println("mongo 'catalog' read/write connections ready")
		}
		// Register the client's health probe with the shared checker.
		checker.AddCheck(mongoClient.HealthCheck())
	}

	// --- PostgreSQL ------------------------------------------------------
	pgClient, err := postgres.InitDefault()
	if err != nil {
		log.Printf("postgres init: %v", err)
	} else {
		defer pgClient.Close()
		if conn := pgClient.Service("orders"); conn != nil {
			// Postgres exposes *sql.DB directly for Read and Write.
			var rw *sql.DB = conn.Write
			_ = rw
			fmt.Println("postgres 'orders' read/write *sql.DB ready")
		}
		checker.AddCheck(pgClient.HealthCheck())
	}

	// --- Redis -----------------------------------------------------------
	redisClient, err := redis.InitDefault(ctx)
	if err != nil {
		log.Printf("redis init: %v", err)
	} else {
		if conn := redisClient.Service("cache"); conn != nil {
			_ = conn.Write.Raw() // *redis.Client / *redis.ClusterClient
			fmt.Println("redis 'cache' connection ready")
		}
	}

	// A single checker aggregates every probe; empty slice means healthy.
	if problems := checker.Check(); len(problems) == 0 {
		fmt.Println("all registered datastores healthy")
	} else {
		fmt.Println("health problems:", problems)
	}
}
