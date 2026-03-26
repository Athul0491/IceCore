package testutil

import (
	"context"
	"fmt"
	"os"

	"github.com/Athul0491/IceCore/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConfig() config.Config {
	conn := os.Getenv("PG_CONN_STRING")
	if conn == "" {
		conn = "host=127.0.0.1 port=5432 dbname=metadata user=metadata_user password=metadata_pass"
	}

	cfg := config.FromEnv()
	cfg.PGConnString = conn
	cfg.GRPCAddress = "bufnet"
	cfg.CacheCapacity = 1000
	cfg.PoolSize = 5
	return cfg
}

func ResetDB(ctx context.Context) error {
	conn := os.Getenv("PG_CONN_STRING")
	if conn == "" {
		conn = "host=127.0.0.1 port=5432 dbname=metadata user=metadata_user password=metadata_pass"
	}

	pool, err := pgxpool.New(ctx, conn)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	queries := []string{
		`TRUNCATE TABLE transactions RESTART IDENTITY CASCADE`,
		`TRUNCATE TABLE partitions RESTART IDENTITY CASCADE`,
		`TRUNCATE TABLE snapshots RESTART IDENTITY CASCADE`,
		`TRUNCATE TABLE schema_history RESTART IDENTITY CASCADE`,
		`TRUNCATE TABLE tables RESTART IDENTITY CASCADE`,
	}

	for _, q := range queries {
		if _, err := pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("reset db failed on %q: %w", q, err)
		}
	}

	return nil
}
