package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	PGConnString  string
	GRPCAddress   string
	CacheCapacity int
	PoolSize      int
	TxnTimeout    time.Duration
	DisableCache  bool
}

func FromEnv() Config {
	cfg := Config{
		PGConnString:  "host=localhost port=5432 dbname=metadata user=metadata_user password=metadata_pass",
		GRPCAddress:   "0.0.0.0:50051",
		CacheCapacity: 10000,
		PoolSize:      20,
		TxnTimeout:    300 * time.Second,
		DisableCache:  false,
	}

	if v := os.Getenv("DISABLE_CACHE"); v != "" {
		cfg.DisableCache = v == "true" || v == "1" || v == "TRUE"
	}
	if v := os.Getenv("PG_CONN_STRING"); v != "" {
		cfg.PGConnString = v
	}

	if v := os.Getenv("GRPC_PORT"); v != "" {
		cfg.GRPCAddress = fmt.Sprintf("0.0.0.0:%s", v)
	} else if v := os.Getenv("GRPC_ADDRESS"); v != "" {
		cfg.GRPCAddress = v
	}

	if v := os.Getenv("CACHE_CAPACITY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.CacheCapacity = n
		}
	}

	if v := os.Getenv("POOL_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.PoolSize = n
		}
	}

	if v := os.Getenv("TXN_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.TxnTimeout = time.Duration(n) * time.Second
		}
	}

	return cfg
}
